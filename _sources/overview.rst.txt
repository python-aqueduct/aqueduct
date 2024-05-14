.. _tasks:

Usage overview
==================

This page is a short introduction to how Aqueduct works and what it can do for you.

Defining a task
---------------
Tasks are the main tool provided to you by Aqueduct.
By wrapping your code inside tasks, you allow Aqueduct to help you with parallel 
computation, command line interfaces, configuration and file management.
A Task is defined by subclassing the :class:`~aqueduct.task.Task` class::

    import aqueduct as aq

    class MyTask(aq.Task):
        def __init__(self, my_task_parameter):
            self.parameter = my_task_parameter

        def run(self, requirements=None):
            return self.parameter**2

The :func:`~aqueduct.task.task.Task.run` method contains the computation you wish to encapsulate.
This task can be executed by calling :func:`~aqueduct.run`::

    my_task = MyTask(4)
    aq.run(my_task)  # Returns 16.


Specifying dependencies
-----------------------
So far, we've only wrapped the :code:`run` function inside a complicated class, but
what did we get in return?
For one thing, we are now able to specify depdendencies. 
Aqueduct will make sure these dependencies are computed before executing the target task.
For instance, if we defined::

    class MyDependentTask(aq.Task):
        def requirements(self):
            return [MyTask(i) for i in range(10)]

        def run(self, requirements):
            return sum(requirements) + 1

we could then compute the task chain using::

    aq.run(MyDependentTask())  # Returns 46.

The result of the required :code:`MyTask` is passed as an argument to :code:`MyDependentTask`.


Parallel execution
------------------

Now, imagine :code:`MyTask` was an expensive computation.
You may wish to execute all calls to it in parallel.
Fortunately, Aqueduct has you covered.
You only have to specify the Dask computing backend::

    backend = aq.DaskBackend()
    backend.run(MyDependentTask()) # Returns 46.

This call builds the dependency graph and sends it to the Dask cluster for execution in 
parallel.
Learn more about other computing backends in the :doc:`backend` section.


Executing from command line
---------------------------
Aqueduct can call any task you define from command line.
Say you stored :code:`MyTask` in the `my_aqueduct_module.py` file, you could then call::

    aq run --module my_aqueduct_module MyDependentTask --dask 4

to execute you task.
This uses the Dask computing backend with 4 workers.

Executing from CLI this way requires you to run the task from the directory where :code:`my_aqueduct_module` exists.
See the :doc:`cli` section for how to avoid specifying the :code:`--module` option every time.

Saving results
--------------

If :code:`MyTask` is very expensive to run, you may want to store its result to a file 
instead of recomputing it every time.
In Aqueduct this is done by specifying an artifact::

    class MyTask(aq.Task):
        ... # Same as before

        def artifact(self):
            return aq.LocalStoreArtifact(f"my_task_{self.my_task_parameter}.pkl")

This will save the output of :code:`run` to disk every time it is executed.
If the artifact already exists, Aqueduct will not execute the task.
Instead, it will load the artifact from storage and return it.
The precise location where the artifact will be stored depends on your configuration.
By default it is stored in the current working directory.

Learn more about artifact storage in the :doc:`artifact` page.