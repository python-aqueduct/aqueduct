Advanced Task Usage
===================


Modify Tasks output with Functors
---------------------------------

It is sometimes useful to apply a small modification to a large task.
For instance, you may have a task that generates a very large artifact.
To avoid regenerating this artifact constantly, you may want to apply modifications to 
it after reading it, instead of modifying the task itself.
Aqueduct provides a tool to make this easier, :func:`~aqueduct.task.apply`::

    import aqueduct as aq

    class MyMassiveTask(aq.Task[np.array]):
        def __init__(self, param):
            self.param = param
        ...

    def my_tweak(massive_array: np.array):
        return np.square(massive_array)

    MyTweakedTask = aq.apply(my_tweak, MyMassiveTask)

In the previous example, :class:`MyTweakedTask` is created.
It is accessible from CLI like any other task.
It has the same constructor as MyMassiveTask.
In fact, it is a subclass of MyMassiveTask.

The :func:`aq.apply` function also works with concrete tasks, so you can also call it as follows::

    tweaked_task_instance = aq.apply(my_tweak, MyMassiveTask(12))