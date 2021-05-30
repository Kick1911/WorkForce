import time
import asyncio
from workforce import __version__, WorkForce, Worker


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

        def write_a_report(self, data):
            pass

    class Foo:
        arr = []
    bar = Foo()

    @company.worker
    class Developer(Worker):
        def start_workflow(self, workitem, *eargs, **ekwargs):
            callback = getattr(self, workitem.callback, None)

            # All tasks here run concurrent
            coros = [self.run_coro_async(
                        getattr(self, task_name)(workitem),
                        *eargs, timeout=3.2, **ekwargs
                    ) for task_name in workitem.tasks]

            return self.run_coro_async(
                asyncio.gather(*coros, loop=ekwargs['loop']),
                *eargs, callback=callback, timeout=3.2, **ekwargs
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

        def make_pr(self, task, wf):
            time.sleep(0.2)
            bar.arr.append('make_pr')

    company.schedule_workflow(NewFeature('New trendy ML'))
    time.sleep(1.2)
    assert bar.arr == ['test']
    time.sleep(1)
    assert bar.arr == ['test', 'code']
    time.sleep(1)
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

    task = add.s(4, 5)
    time.sleep(0.5)

    assert task.done()
    assert task.result() == 9
    assert bar.result == 9


def test_schedule_coro():
    class Foo:
        count = 0
    bar = Foo()

    workforce = WorkForce()

    async def foo():
        await asyncio.sleep(0.8)
        bar.count += 1

    f1 = workforce.schedule_async(foo)
    f2 = workforce.schedule_async(foo)
    time.sleep(2)
    assert f1.done()
    assert f2.done()
    assert bar.count == 2

