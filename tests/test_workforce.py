import time
import asyncio
from workforce import (
    __version__, WorkForce, Worker, func_type, FunctionType, TimeoutWrapper,
    RetryWrapper
)


def test_version():
    assert __version__ == '0.1.0'


def test_workers():
    workforce = WorkForce()

    @workforce.worker
    class Dev(Worker):
        pass

    assert len(workforce.workers) == 2
    assert isinstance(workforce.workers['Dev'], Dev)

    @workforce.worker(name='programmer')
    class Developer(Worker):
        pass

    assert len(workforce.workers) == 3
    assert 'Developer' not in workforce.workers
    assert isinstance(workforce.workers['programmer'], Developer)

    workforce.lay_off_worker('Dev')
    assert len(workforce.workers) == 2
    assert 'Dev' not in workforce.workers


def test_framework():

    # You can make a Task Factory as well
    class NewFeature:
        tasks = ['design', 'code', 'test']
        requirements = ['scalable', 'flexable']
        callback = 'make_pr'

        def __init__(self, name):
            self.name = name

    class Hire:
        platform = 'linkedin'
        task = 'hire_new_talent'
        callback = 'None'

    class EmployeeCounseling:
        problem_employee = 'john'
        task = 'handle_problem_employee'
        callback = 'write_a_report'

    class Company(WorkForce):
        def get_worker(self, workitem):
            """
            You could make this conditional-less by attaching a worker name
            to a task or the worker itself
            """
            if isinstance(workitem, NewFeature):
                return self.workers['Developer']
            elif (isinstance(workitem, Hire)
                  or isinstance(workitem, EmployeeCounseling)):
                return self.workers['HR']
            else:
                raise self.WorkerNotFound

    company = Company()

    @company.worker
    class HR(Worker):
        def start_workflow(self, workitem, *eargs, **ekwargs):
            callback = getattr(self, workitem.callback, None)

            return self.run_coro_async(
                getattr(self, workitem.task)(workitem),
                *eargs, callback=callback, **ekwargs
            )

        async def hire_new_talent(self, workitem):
            pass

        async def handle_problem_employee(self, workitem):
            pass

        def write_a_report(self, wf, task):
            pass

    class Foo:
        arr = []
    bar = Foo()

    @company.worker
    class Developer(Worker):
        def start_workflow(self, workitem, *args, **kwargs):
            callback = getattr(self, workitem.callback, None)

            # All tasks here run concurrent
            coros = (getattr(self, task_name)(workitem)
                     for task_name in workitem.tasks)

            # Hack because asyncio.gather is not recognised as a coroutine
            # Only tested on Py 3.6
            async def gather(*aws, **kwargs):
                return await asyncio.gather(*aws, **kwargs)

            return self.run_coro_async(
                gather(*coros, loop=kwargs['loop']),
                *args, callback=callback, wrapper=None, **kwargs
            )

        async def design(self, workitem):
            await asyncio.sleep(3)
            bar.arr.append('design')

        async def code(self, workitem):
            await asyncio.sleep(2)
            bar.arr.append('code')

        async def test(self, workitem):
            await asyncio.sleep(1)
            bar.arr.append('test')

        def make_pr(self, wf, task):
            time.sleep(0.2)
            bar.arr.append('make_pr')

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
    assert isinstance(f.exception(), asyncio.TimeoutError)

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
        asyncio.sleep(1)

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

