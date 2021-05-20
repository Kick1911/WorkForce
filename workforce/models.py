import os
import asyncio
import functools
import concurrent.futures
from threading import Thread
from collections.abc import Callable


def _handle_result(coro, task):
    try:
        task.result()
    except Exception as e:
        print(e)


class Worker:
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
        task.add_done_callback(functools.partial(_handle_result, task))
        if callback:
            task.add_done_callback(callback)
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
        task = self._wrap_coro(coro, callback)
        asyncio.run_coroutine_threadsafe(
            asyncio.wait_for(coro, timeout=timeout), self.loop
        )
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
                self.pool, functools.partial(func, *(args or ()), **(kwargs or {}))
            ),
            timeout=timeout
        )
        return self._wrap_coro(coro, callback)


# TODO: Retry tasks*
# TODO: Chain and chord
class WorkForce:
    _index = 0
    workers = None

    def __init__(self, workers=1):
        self.workers = [Worker() for _ in range(workers)]

    def schedule_async(self, *args, **kwargs) -> asyncio.Task:
        return self._next.run_func_async(*args, **kwargs)

    def schedule(self, *args, **kwargs) -> asyncio.Task:
        return self._next.run_in_thread(*args, **kwargs)

    @property
    def _next(self) -> Worker:
        self._index = (self._index + 1) % len(self.workers)
        return self.workers[self._index]

