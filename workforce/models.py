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
        print('TimeoutError')
    except Exception as e:
        print('Exception:', e)


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

    def _wrap_coro(self, coro, callback: Callable = None) -> asyncio.Task:
        task = asyncio.ensure_future(coro, loop=self.loop)
        task.add_done_callback(handle_error)
        if callback:
            task.add_done_callback(functools.partial(callback, task, self))
        return task

    def run_func_async(
        self, func: Callable, args: tuple = None, kwargs: dict = None,
        *eargs, **ekwargs
    ) -> asyncio.Task:
        return self.run_coro_async(
            func(*(args or ()), **(kwargs or {})), *eargs, **ekwargs
        )

    def run_coro_async(self, coro, callback: Callable = None,
                       timeout: int = 1) -> asyncio.Task:
        coro = asyncio.wait_for(coro, timeout=timeout)
        task = self._wrap_coro(coro, callback)
        asyncio.run_coroutine_threadsafe(coro, self.loop)
        return task

    def run_in_thread(
        self, func: Callable, args: tuple = None, kwargs: dict = None,
        callback: Callable = None, timeout: int = 1
    ) -> asyncio.Task:
        if not self.pool:
            self.pool = concurrent.futures.ThreadPoolExecutor(
                max_workers=1
            )

        coro = asyncio.wait_for(
            self.loop.run_in_executor(
                self.pool,
                functools.partial(
                    func, *(args or ()), **(kwargs or {})
                )
            ),
            timeout=timeout
        )
        return self._wrap_coro(coro, callback)


# TODO: Retry tasks*
# TODO: Chain and chord
class WorkForce:
    _index = 0
    workspaces = None

    def __init__(self, workspaces=1):
        self.workspaces = [Workspace() for _ in range(workspaces)]

    def schedule_async(self, *args, **kwargs) -> asyncio.Task:
        return self._next.run_func_async(*args, **kwargs)

    def schedule(self, *args, **kwargs) -> asyncio.Task:
        return self._next.run_in_thread(*args, **kwargs)

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

    @property
    def _next(self) -> Workspace:
        self._index = (self._index + 1) % len(self.workspaces)
        return self.workspaces[self._index]

