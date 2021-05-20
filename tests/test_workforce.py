import time
import asyncio
from workforce import __version__, WorkForce


def test_version():
    assert __version__ == '0.1.0'


def test_schedule_coro():
    class Foo:
        count = 0
    bar = Foo()

    workforce = WorkForce(2)

    async def foo():
        await asyncio.sleep(1)
        bar.count += 1

    f1 = workforce.schedule_async(foo)
    f2 = workforce.schedule_async(foo)
    time.sleep(2)
    assert f1.done()
    assert f2.done()
    assert bar.count == 2

