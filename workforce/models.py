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


async def consumer(q):
    queue = q['queue']
    while 1:
        coro = await queue.get()
        await coro
        queue.task_done()


def handle_error(task):
    try:
        task.result()
    except TimeoutError:
        print('TimeoutError:', task)
    except Exception:
        task.print_stack()


class BaseWorker:
    pool = None

    def __init__(self, name):
        self.name = name

    def _wrap_coro(self, coro, callback: Callable = None,
                   loop=None) -> asyncio.Task:
        task = asyncio.ensure_future(coro, loop=loop)
        task.add_done_callback(handle_error)
        if callback:
            task.add_done_callback(functools.partial(callback, self))
        return task

    def run_func_async(
        self, func: Callable, args: tuple = None, kwargs: dict = None,
        *eargs, **ekwargs
    ) -> asyncio.Task:
        return self.run_coro_async(
            func(*(args or ()), **(kwargs or {})), *eargs, **ekwargs
        )

    def run_coro_async(self, coro, callback: Callable = None,
                       timeout: int = 1, loop=None) -> asyncio.Task:
        waited_coro = asyncio.wait_for(coro, timeout=timeout, loop=loop)
        task = self._wrap_coro(waited_coro, callback=callback, loop=loop)
        asyncio.run_coroutine_threadsafe(waited_coro, loop=loop)
        return task

    def run_in_thread(
        self, func: Callable, args: tuple = None, kwargs: dict = None,
        callback: Callable = None, timeout: int = 1, loop=None
    ) -> asyncio.Task:
        if not self.pool:
            self.pool = concurrent.futures.ThreadPoolExecutor(
                max_workers=1
            )

        coro = asyncio.wait_for(
            loop.run_in_executor(
                self.pool,
                functools.partial(
                    func, *(args or ()), **(kwargs or {})
                )
            ),
            timeout=timeout, loop=loop
        )
        return self._wrap_coro(coro, callback, loop=loop)


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
    queues = None

    class WorkerNotFound(Exception):
        pass

    class QueueNotFound(Exception):
        pass

    def __init__(self, workspaces=1):
        self.workspaces = [Workspace() for _ in range(workspaces)]
        self.workers = {'default': Worker('default')}
        self.queues = {}

    def queue(self, key, Queue=asyncio.Queue, **kwargs):
        loop = self._next.loop
        queue = Queue(loop=loop, **kwargs)

        self.queues[key] = dict(queue=queue)
        task = self.get_worker('default').run_coro_async(
            consumer(self.queues[key]), loop=loop, timeout=None
        )
        self.queues[key]['task'] = task
        return queue

    def unregister_queue(self, key):
        try:
            q = self.queues.pop(key)
            q['task'].cancel()
        except KeyError:
            raise self.QueueNotFound

    def get_queue(self, key):
        try:
            return self.queues[key]['queue']
        except KeyError:
            raise self.QueueNotFound

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

    def schedule(self, func, *args, task_type=None, function_type=None,
                 **kwargs) -> asyncio.Task:
        """
        Arguments are not `well-defined` by design. Arguments will change
        depending on whether `func` is a Callable or a coroutine
        """
        try:
            worker = self.get_worker(task_type)
        except KeyError:
            raise self.WorkerNotFound

        method = {
            FunctionType.CORO: worker.run_coro_async,
            FunctionType.FUNC_CORO: worker.run_func_async,
            FunctionType.FUNC: worker.run_in_thread,
        }[function_type or func_type(func)]
        return method(func, *args, loop=self._next.loop, **kwargs)

    def worker(self, *args, **kwargs):
        def process(cls, **pkwargs):
            self.workers[pkwargs.get('name', cls.__name__)] = cls(cls.__name__)
            return cls

        def wrapper(cls):
            return process(cls, **kwargs)
        return process(args[0]) if len(args) > 0 else wrapper

    def task(self, *eargs, **ekwargs):
        def process(func, **pkwargs):
            function_type = func_type(func)

            def schedule(*args, **kwargs):
                return self.schedule(
                    func, args=args, kwargs=kwargs,
                    function_type=function_type, **pkwargs
                )

            func.s = schedule
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

