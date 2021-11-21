import time
import asyncio
from workforce_async import (
    __version__, WorkForce, Worker, func_type, FunctionType, TimeoutWrapper,
    RetryWrapper
)
from workforce_async.aiohttp import get, post, delete


def test_version():
    assert __version__ == "0.11.2"


def test_aiohttp():
    workforce = WorkForce()

    workforce.task(wrapper=None)(get)
    workforce.task(wrapper=None)(post)
    workforce.task(wrapper=None)(delete)

    payload = dict(key="value", context="POST Request")
    task = post.s(url="http://httpbin.org/post", json=payload)()
    time.sleep(1.5)
    assert task.done()
    response = task.result()
    assert response.payload["json"] == payload
    assert response.status == 200

    task = delete.s(url="http://httpbin.org/delete")()
    time.sleep(1.5)
    assert task.done()
    response = task.result()
    assert response.status == 200

    task = get.s(url="http://httpbin.org/cjdsnco")()
    time.sleep(1.5)
    assert task.done()
    response = task.result()
    assert response.status == 404


def test_workers():
    workforce = WorkForce()

    @workforce.worker
    class Dev(Worker):
        pass

    assert len(workforce.workers) == 2
    assert isinstance(workforce.workers['dev']['.'], Dev)

    @workforce.worker(name='programmer')
    class Developer(Worker):
        pass

    assert len(workforce.workers) == 3
    assert 'developer' not in workforce.workers
    assert isinstance(workforce.workers['programmer']['.'], Developer)

    workforce.lay_off_worker('dev')
    assert len(workforce.workers) == 2
    assert 'dev' not in workforce.workers


def test_framework():
    class Foo:
        arr = []
    bar = Foo()

    # You can make a Task Factory as well
    class NewFeature:
        tasks = ['design', 'code', 'test']
        requirements = ['scalable', 'flexable']

        def __init__(self, name):
            self.name = name

        def callback(self, wf, task):
            """ make_pr """
            time.sleep(0.2)
            bar.arr.append('make_pr')

    class Hire:
        platform = 'linkedin'
        task = 'hire_new_talent'

    class EmployeeCounseling:
        problem_employee = 'john'
        task = 'handle_problem_employee'

        def callback(self, wf, task):
            """ write_a_report """
            pass

    class Company(WorkForce):
        def get_worker(self, workitem):
            try:
                worker_name = {
                    'NewFeature': 'developer',
                    'Hire': 'hr',
                    'EmployeeCounseling': 'hr'
                }[type(workitem).__name__]

                return super().get_worker(worker_name)
            except KeyError:
                raise self.WorkerNotFound

    company = Company()

    @company.worker
    class HR(Worker):
        def handle_workitem(self, workitem, *args, **kwargs):
            callback = getattr(self, workitem.callback, None)
            return getattr(self, workitem.task)(workitem), callback

        async def hire_new_talent(self, workitem):
            pass

        async def handle_problem_employee(self, workitem):
            pass

    @company.worker
    class Developer(Worker):
        def handle_workitem(self, workitem, *args, **kwargs):
            callback = getattr(workitem, 'callback', None)

            # All tasks here run concurrent
            coros = (getattr(self, task_name)(workitem)
                     for task_name in workitem.tasks)

            # Hack because asyncio.gather is not recognised as a coroutine
            async def gather(*aws, **kwargs):
                return await asyncio.gather(*aws, **kwargs)

            return gather(*coros), callback

        async def design(self, workitem):
            await asyncio.sleep(3)
            bar.arr.append('design')

        async def code(self, workitem):
            await asyncio.sleep(2)
            bar.arr.append('code')

        async def test(self, workitem):
            await asyncio.sleep(1)
            bar.arr.append('test')

    company.schedule_workflow(NewFeature('New trendy ML'))
    time.sleep(1.2)
    assert bar.arr == ['test']
    time.sleep(1)
    assert bar.arr == ['test', 'code']
    time.sleep(1.2)
    assert bar.arr == ['test', 'code', 'design', 'make_pr']


def test_decorator():
    class Foo:
        result = 0
    bar = Foo()
    workforce = WorkForce()

    def callback(wf, task):
        bar.result = task.result()

    @workforce.task(callback=callback)
    async def add(a, b):
        return a + b

    task = add.s(4, 5)()
    time.sleep(0.5)

    assert task.done()
    assert task.result() == 9
    assert bar.result == 9

    @workforce.task()
    async def sleep(sec):
        await asyncio.sleep(sec)

    workforce.queue('channel1')
    queue = sleep.q(0.5)('channel1')
    assert len(queue) == 1
    time.sleep(0.6)
    assert not len(queue)
    workforce.queues.destroy('channel1')


def test_func_type():
    def foo():
        pass
    async def bar():
        pass

    coro = bar()
    assert func_type(foo) == FunctionType.FUNC
    assert func_type(bar) == FunctionType.FUNC_CORO
    assert func_type(coro) == FunctionType.CORO
    coro.close()


def test_schedule_coro():
    class Foo:
        count = 0
    bar = Foo()

    workforce = WorkForce()

    async def add(a, b):
        return a + b

    f = workforce.schedule(add, args=(4, 2))
    time.sleep(0.2)
    assert f.done()
    assert f.result() == 6

    f = workforce.schedule(add, args=(4, 2), wrapper=RetryWrapper(1))
    time.sleep(0.2)
    assert f.done()
    assert f.result() == 6

    def add(a, b):
        return a + b

    f = workforce.schedule(add, args=(4, 2), wrapper=None)
    time.sleep(0.2)
    assert f.done()
    assert f.result() == 6

    f = workforce.schedule(add, args=(4, 2), wrapper=RetryWrapper(1))
    time.sleep(0.2)
    assert f.done()
    assert f.result() == 6

    async def sync_in_async():
        return await workforce.make_async(add)(3, 32)
    f = workforce.schedule(sync_in_async)
    time.sleep(0.5)
    assert f.done()
    assert f.result() == 35

    async def foo():
        await asyncio.sleep(0.8)
        bar.count += 1

    f1 = workforce.schedule(foo)
    f2 = workforce.schedule(foo)
    time.sleep(1)
    assert f1.done()
    assert f2.done()
    assert bar.count == 2

    ex = Exception('Error occured')
    async def foo():
        bar.count += 1
        raise ex

    f = workforce.schedule(foo)
    time.sleep(0.5)
    assert f.done()
    assert bar.count == 3
    assert f.exception() == ex

    f = workforce.schedule(foo, wrapper=RetryWrapper(4))
    time.sleep(0.5)
    assert f.done()
    assert bar.count == 8
    assert f.exception() == ex

    def foo():
        bar.count += 1

    f = workforce.schedule(foo)
    time.sleep(0.1)
    assert f.done()
    assert bar.count == 9
    # assert workforce.workers['default'].pool

    async def foo():
        await asyncio.sleep(2)

    f = workforce.schedule(foo, wrapper=TimeoutWrapper(1))
    time.sleep(1.2)
    assert f.done()
    assert type(f.exception()) == asyncio.TimeoutError

    def foo():
        bar.count += 1
        raise ex

    f = workforce.schedule(foo, wrapper=RetryWrapper(3))
    time.sleep(0.5)
    assert f.done()
    assert f.exception() == ex
    assert bar.count == 13

    f = workforce.schedule(foo, wrapper=RetryWrapper(2))
    time.sleep(0.5)
    assert f.done()
    assert f.exception() == ex
    assert bar.count == 16


def test_queue():
    async def foo():
        await asyncio.sleep(1)

    workforce = WorkForce()
    queue = workforce.queue('channel1')
    queue.put(foo())
    f = workforce.schedule(foo)
    queue.put(foo())
    queue.put(foo())
    assert len(queue) == 3
    time.sleep(3)
    assert not len(queue)
    assert f.done()
    workforce.queues.destroy('channel1')

