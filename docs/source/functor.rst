Functors
========


Functors are transformations applied to tasks. They are used to modify the output of a task
before it is passed to the next task in the pipeline. Functors are useful to apply a configurable
transformation to the output of a task.


The example below illustrates the main features of a Functor::

    class MyTask(Task[int]):
        def __init__(self, param: int):
            self.param = param

        def run(self) -> int:
            return self.param

    class MyFunctor(Functor[int, int]):
        def __init__(self, to_add: int):
            self.to_add = to_add

        def mapping(self, task_output: int, requirements: None) -> int:
            return task_output + 1

        def artifact(self, t: MyTask):
            return f'MyFunctor_{self.to_add}---' + f'MyTask_{t.param}.pkl' 
        

    mapped_task = MyFunctor()(MyTask())
    result = aq.run(mapped_task)

Functors are mostly a software engineering construct (they are useful to organize your code).
A typical usage pattern is computing error metrics.
One :class:`Task` computes the predictions, and a :class:`Functor` computes the error metrics
based on these predictions.
Functors make it easier to implement the case where multiple tasks can produce predictions, but 
we want to use the same code to compute the metric::

    class ModelA(aq.Task):
        def run(self):
            return 3

    class ModelB(aq.Task):
        def run(self):
            return 2

    class GroundTruth(aq.Task):
        def run(self):
            return 4

    class SquaredError(aq.Functor):
        def requirements(self):
            return GroundTruth()

        def mapping(self, task_output: int, requirements: Tuple[int]) -> float:
            return (task_output - requirements)**2

    loss_computation = SquaredError()
    loss_model_a = loss_computation(ModelA())
    loss_model_b = loss_computation(ModelB())


CLI Usage
---------