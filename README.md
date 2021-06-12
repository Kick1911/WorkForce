# WorkForce
Asyncio Wrapper

# Install Dependencies
> $ poetry install

# Run example
> $ poetry run python examples/http_server.py

# Run tests
> $ poetry run pytest

# Interfaces
[Snippets taken from tests](https://github.com/Kick1911/WorkForce/blob/2bf0dd7dadcefd1240bfd87df8e2aa4a32b86572/tests/test_workforce.py#L54)

## Just run async functions
```python
workforce = WorkForce()

async def foo():
    await asyncio.sleep(0.8)
    bar.count += 1

f1 = workforce.schedule(foo)
```

## Just run normal functions in another thread
```python
def foo():
    bar.count += 1

f = workforce.schedule(foo)
```

## Function-based tasks
`.s()` supports both normal and async functions
```python
workforce = WorkForce()

def callback(wf, task):
    bar.result = task.result()

@workforce.task(callback=callback)
async def add(a, b):
    return a + b

task = add.s(4, 5)()
```

## Create queues of tasks
```python
workforce = WorkForce()
queue = workforce.queue('channel1')
queue.put(foo())
queue.put(foo())
queue.put(foo())
assert len(queue) == 3
```

## Class-based framework
Make your own workforce that distributes workitems to Workers
```python
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
```

Make your own workers that perform tasks based on the workitem they receive
```python
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
```

