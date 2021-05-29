import time
import asyncio
from workforce import __version__, WorkForce


def test_version():
    assert __version__ == '0.1.0'


def test_decorator():
    class Foo:
        result = 0
    bar = Foo()
    workforce = WorkForce(2)

    def callback(task, wf, coro):
        bar.result = task.result()

    @workforce.task(callback=callback)
    async def add(a, b):
        return a + b

    task = add.s(4, 5)
    time.sleep(0.5)

    assert task.done()
    assert task.result() == 9
    assert bar.result == 9


def test_schedule_coro():
    class Foo:
        count = 0
    bar = Foo()

    workforce = WorkForce(1)

    async def foo():
        await asyncio.sleep(0.8)
        bar.count += 1

    f1 = workforce.schedule_async(foo)
    f2 = workforce.schedule_async(foo)
    time.sleep(1)
    assert f1.done()
    assert f2.done()
    assert bar.count == 2

    ex = Exception('Error occured')
    async def foo():
        raise ex

    f = workforce.schedule_async(foo)
    time.sleep(0.5)
    assert f.done()
    assert f.exception() == ex

