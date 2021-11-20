import os
import sys
import asyncio
import functools
import concurrent.futures
from threading import Thread
from asyncio import TimeoutError
from collections.abc import Callable


class FunctionType:
    CORO = 1
    FUNC_CORO = 2
    FUNC = 3


def func_type(func):
    return (asyncio.iscoroutine(func) and FunctionType.CORO
            or asyncio.iscoroutinefunction(func) and FunctionType.FUNC_CORO
            or FunctionType.FUNC)


def handle_error(task):
    try:
        task.result()
    except Exception as e:
        sys.stderr.write(f'{e.__class__}\n')
        task.print_stack()


class Queue:
    raw_queue = None
    task = None

    def __init__(self, loop=None, Queue=asyncio.Queue, **kwargs):
        self.raw_queue = Queue(**kwargs)

        coro = self.consumer()
        self.task = asyncio.ensure_future(coro, loop=loop)
        asyncio.run_coroutine_threadsafe(coro, loop=loop)

    async def consumer(self):
        while 1:
            coro = await self.raw_queue.get()
            await coro
            self.raw_queue.task_done()

    def __len__(self):
        return self.raw_queue.qsize()

    def put(self, item):
        self.raw_queue.put_nowait(item)

    def destroy(self):
        self.task.cancel()


class QueueManager:
    queues = None

    class QueueNotFound(Exception):
        pass

    def __init__(self):
        self.queues = {}

    def create(self, key, loop=None, **kwargs):
        self.queues[key] = Queue(loop=loop, **kwargs)
        return self.queues[key]

    def get(self, key):
        try:
            return self.queues[key]
        except KeyError:
            raise self.QueueNotFound

    def destroy(self, key):
        try:
            q = self.queues.pop(key)
            q.destroy()
        except KeyError:
            raise self.QueueNotFound


class Wrapper:
    def __init__(self, *args, **kwargs):
        pass

    async def run(self, func, **kwargs):
        return await func()

    async def wrap(self, func: Callable, **kwargs):
        task = asyncio.ensure_future(self.run(func, **kwargs))
        return await task


class RetryWrapper(Wrapper):
    def __init__(self, retries, cooldown=0, *args, **kwargs):
        self.retries = retries
        self.cooldown = cooldown
        super().__init__(*args, **kwargs)

    async def run(self, func, **kwargs):
        ex = None
        retries = self.retries + 1
        for _ in range(retries):
            try:
                return await asyncio.ensure_future(func())
            except Exception as e:
                ex = e
                if self.cooldown:
                    await asyncio.sleep(self.cooldown)
        raise ex


class TimeoutWrapper(Wrapper):
    def __init__(self, timeout, *args, **kwargs):
        self.timeout = timeout
        super().__init__(*args, **kwargs)

    async def run(self, func, **kwargs):
        return await asyncio.wait_for(func(), timeout=self.timeout)


class BaseWorker:
    def __init__(self, name):
        self.name = name

    def _create_task(self, coro, callback: Callable = None,
                     loop=None) -> asyncio.Task:
        task = asyncio.ensure_future(coro, loop=loop)
        task.add_done_callback(handle_error)
        if callback:
            task.add_done_callback(functools.partial(callback, self))
        return task

    def run_func_async(
        self, func: Callable, args: tuple = (), kwargs: dict = {},
        wrapper=TimeoutWrapper(1), loop=None, **ekwargs
    ) -> asyncio.Task:
        if not wrapper:
            wrapper = Wrapper()
        coro = wrapper.wrap(functools.partial(func, *args, **kwargs))
        return self.run_coro_async(coro, loop=loop, **ekwargs)

    def run_coro_async(self, coro, callback: Callable = None,
                       loop=None) -> asyncio.Task:
        task = self._create_task(coro, callback=callback, loop=loop)
        asyncio.run_coroutine_threadsafe(coro, loop=loop)
        return task


class Worker(BaseWorker):
    async def handle_workitem(*args, **kwargs):
        """
        @returns: coroutine or tuple(coroutine, callback)
        Can also use the wrappers yourself to add behaviour to your tasks
        """
        raise NotImplementedError

    def start_workflow(
        self, task, args: tuple = (), kwargs: dict = {},
        **ekwargs
    ) -> asyncio.Task:
        ret = self.handle_workitem(task, *args, **kwargs)

        if type(ret) != tuple:
            coro = ret
        else:
            coro, ekwargs['callback'] = ret

        return self.run_coro_async(coro, **ekwargs)


class Loop:
    loop = None
    thread = None

    def __init__(self):
        self.loop = asyncio.new_event_loop()

        def run():
            try:
                self.loop.run_forever()
            except KeyboardInterrupt:
                pass

        self.thread = Thread(target=run, daemon=True)
        self.thread.start()


class Workspace:
    pool = None

    def __init__(self, pool_size=1):
        self.create_pool(pool_size)

    def create_pool(self, pool_size):
        self.pool = [Loop() for _ in range(pool_size)]

    def work(self, worker, func, **kwargs):
        return worker.run_func_async(
            self.run_func(func),
            loop=self._next.loop,
            **kwargs
        )


class AsyncWorkspace(Workspace):
    _index = 0

    def run_func(self, func):
        return func

    @property
    def _next(self):
        self._index = (self._index + 1) % len(self.pool)
        return self.pool[self._index]


class SyncWorkspace(Workspace):
    threads = None

    def create_pool(self, pool_size):
        self.threads = concurrent.futures.ThreadPoolExecutor(pool_size)
        self.pool = [Loop()]

    def run_func(self, func):
        async def run(*args, **kwargs):
            return await asyncio.get_running_loop().run_in_executor(
                self.threads, functools.partial(func, *args, **kwargs)
            )
        return run

    @property
    def _next(self):
        return self.pool[0]


class WorkForce:
    workers = None
    worker_name_delimiter = '.'
    workspaces = None
    queues = QueueManager()

    class WorkerNotFound(Exception):
        pass

    def __init__(self, async_pool=1, sync_pool=1):
        self.workspaces = {
            'async': AsyncWorkspace(async_pool),
            'sync': SyncWorkspace(sync_pool),
        }
        self.workers = {
            'default': {self.worker_name_delimiter: Worker('default')}
        }

    def queue(self, key):
        return self.queues.create(key, loop=self.workspaces["async"]._next.loop)

    def get_worker(self, name):
        w = self.workers
        path = (name.strip(self.worker_name_delimiter)
                .split(self.worker_name_delimiter))
        for p in path:
            try:
                w = w[p]
            except KeyError:
                w = w[self.worker_name_delimiter]
        return w[self.worker_name_delimiter]

    def worker(self, *args, **kwargs):
        def process(cls, **pkwargs):
            name = pkwargs.get('name', cls.__name__.lower())
            path = (name.strip(self.worker_name_delimiter)
                    .split(self.worker_name_delimiter))
            w = self.workers
            for p in path:
                x = self.worker_name_delimiter if p.startswith('{') else p
                w[x] = w.get(x, {})
                w = w[x]
            w[self.worker_name_delimiter] = cls(name)
            return cls

        def wrapper(cls):
            return process(cls, **kwargs)

        return process(args[0]) if len(args) > 0 else wrapper

    def schedule_workflow(self, workitem, **kwargs) -> asyncio.Task:
        try:
            return self.get_worker(workitem).start_workflow(
                workitem, loop=self.workspaces['async']._next.loop,
                **kwargs
            )
        except KeyError:
            raise self.WorkerNotFound

    def schedule(self, func, task_type='default', **kwargs) -> asyncio.Task:
        try:
            worker = self.get_worker(task_type)
        except KeyError:
            raise self.WorkerNotFound
        return self.get_workspace(func).work(worker, func, **kwargs)

    def get_workspace(self, func):
        pool = {
            FunctionType.FUNC_CORO: 'async',
            FunctionType.FUNC: 'sync'
        }[func_type(func)]
        return self.workspaces[pool]

    def task(self, *eargs, **ekwargs):
        def process(func, **pkwargs):
            def schedule(*args, **kwargs):
                return functools.partial(
                    self.schedule, func, args=args, kwargs=kwargs,
                    **pkwargs
                )

            def queue(*args, **kwargs):
                def run(key):
                    q = self.queues.get(key)
                    q.put(func(*args, **kwargs))
                    return q
                return run

            func.s = schedule
            func.q = queue
            return func

        def wrapper(func):
            return process(func, **ekwargs)

        return process(eargs[0]) if len(eargs) > 0 else wrapper

    def lay_off_worker(self, worker_name):
        return self.workers.pop(worker_name)
