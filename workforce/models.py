import os
import asyncio
import functools
import concurrent.futures
from threading import Thread
from asyncio import TimeoutError
from collections.abc import Callable


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


# TODO: Retry tasks*
# TODO: Chain and chord
class WorkForce:
    _index = 0
    workers = None
    workspaces = None

    class WorkerNotFound(Exception):
        pass

    def __init__(self, workspaces=1):
        self.workspaces = [Workspace() for _ in range(workspaces)]
        self.workers = {'default': Worker('default')}

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

    def schedule_coro(self, *args, task_type=None, **kwargs) -> asyncio.Task:
        try:
            return self.get_worker(task_type).run_coro_async(
                *args, loop=self._next.loop, **kwargs
            )
        except KeyError:
            raise self.WorkerNotFound

    def schedule_async(self, *args, task_type=None, **kwargs) -> asyncio.Task:
        try:
            return self.get_worker(task_type).run_func_async(
                *args, loop=self._next.loop, **kwargs
            )
        except KeyError:
            raise self.WorkerNotFound

    def schedule(self, *args, task_type=None, **kwargs) -> asyncio.Task:
        try:
            return self.get_worker(task_type).run_in_thread(
                *args, loop=self._next.loop, **kwargs
            )
        except KeyError:
            raise self.WorkerNotFound

    def worker(self, *args, **kwargs):
        def process(cls, **pkwargs):
            self.workers[pkwargs.get('name', cls.__name__)] = cls(cls.__name__)
            return cls

        def wrapper(cls):
            return process(cls, **kwargs)
        return process(args[0]) if len(args) > 0 else wrapper

    def task(self, *eargs, **ekwargs):
        def process(func, **pkwargs):
            def schedule_async(*args, **kwargs):
                return self.schedule_async(func, args=args, kwargs=kwargs,
                                           **pkwargs)

            def schedule(*args, **kwargs):
                return self.schedule(func, args=args, kwargs=kwargs, **pkwargs)

            func.s = {
                'async': schedule_async,
                'sync': schedule,
            }[pkwargs.get('run_type', 'async')]
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

