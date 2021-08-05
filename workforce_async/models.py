import os
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
    except TimeoutError:
        print('TimeoutError:', task)
    except Exception:
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

    async def run(self, func, *args, **kwargs):
        function_type = func_type(func)
        if function_type == FunctionType.FUNC_CORO:
            return await asyncio.ensure_future(func(*args, **kwargs))

        elif function_type == FunctionType.FUNC:
            return await asyncio.get_event_loop().run_in_executor(
                None, functools.partial(func, *args, **kwargs)
            )

    async def wrap(self, *args, **kwargs):
        task = asyncio.ensure_future(self.run(*args, **kwargs))
        return await task


class RetryWrapper(Wrapper):
    def __init__(self, retries, cooldown=0, *args, **kwargs):
        self.retries = retries
        self.cooldown = cooldown
        super().__init__(*args, **kwargs)

    async def run(self, func, *args, **kwargs):
        ex = None
        retries = self.retries + 1
        for _ in range(retries):
            try:
                return await super().run(func, *args, **kwargs)
            except Exception as e:
                ex = e
                if self.cooldown:
                    await asyncio.sleep(self.cooldown)
        raise ex


class TimeoutWrapper(Wrapper):
    def __init__(self, timeout, *args, **kwargs):
        self.timeout = timeout
        super().__init__(*args, **kwargs)

    async def run(self, func, *args, **kwargs):
        return await asyncio.wait_for(
            super().run(func, *args, **kwargs),
            timeout=self.timeout
        )


class BaseWorker:
    pool = None

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
        self, func: Callable, args: tuple = None, kwargs: dict = None,
        *eargs, wrapper=TimeoutWrapper(1), **ekwargs
    ) -> asyncio.Task:
        coro = (wrapper.wrap(func, *(args or ()), **(kwargs or {}))
                if wrapper else func(*(args or ()), **(kwargs or {})))
        return self.run_coro_async(coro, *eargs, **ekwargs)

    def run_coro_async(self, coro, callback: Callable = None,
                       loop=None) -> asyncio.Task:
        task = self._create_task(coro, callback=callback, loop=loop)
        asyncio.run_coroutine_threadsafe(coro, loop=loop)
        return task


class Worker(BaseWorker):
    def start_workflow(
        self, task, args: tuple = None, kwargs: dict = None,
        *eargs, **ekwargs
    ) -> asyncio.Task:
        return self.run_coro_async(
            self.task(*args, **kwargs),
            *eargs, **ekwargs
        )


class Workspace:
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


class WorkForce:
    _index = 0
    workers = None
    workspaces = None
    queues = QueueManager()

    class WorkerNotFound(Exception):
        pass

    def __init__(self, workspaces=1):
        self.workspaces = [Workspace() for _ in range(workspaces)]
        self.workers = {'default': Worker('default')}

    def queue(self, key):
        return self.queues.create(key, loop=self._next.loop)

    def get_worker(self, task_type):
        return self.workers['default']

    def schedule_workflow(self, workitem, *args,
                          **kwargs) -> asyncio.Task:
        try:
            return self.get_worker(workitem).start_workflow(
                workitem, *args, loop=self._next.loop, **kwargs
            )
        except KeyError:
            raise self.WorkerNotFound

    def schedule(self, func, *args, task_type=None,
                 **kwargs) -> asyncio.Task:
        try:
            worker = self.get_worker(task_type)
        except KeyError:
            raise self.WorkerNotFound
        return worker.run_func_async(func, *args, loop=self._next.loop,
                                     **kwargs)

    def worker(self, *args, **kwargs):
        def process(cls, **pkwargs):
            self.workers[pkwargs.get('name', cls.__name__)] = cls(cls.__name__)
            return cls

        def wrapper(cls):
            return process(cls, **kwargs)
        return process(args[0]) if len(args) > 0 else wrapper

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

    @property
    def _next(self) -> Workspace:
        self._index = (self._index + 1) % len(self.workspaces)
        return self.workspaces[self._index]

