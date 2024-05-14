
Aqueduct CLI
============

Aqueduct provides a series of command line tools that leverage your Task definitions
to perform various computations.


Defining an extension module
----------------------------

The CLI has to know where your tasks are stored in order to do its job.
The easy way to specify this is to add the :code:`--module` option to CLI calls.
This can become cumbersome.
You can give your list of modules to Aqueduct once and for all by defining an extension
module.
Here is an example :file:`pyproject.toml` file that does this::

    [build-system]
    requires = ["setuptools"]
    build-backend = "setuptools.build_meta"

    [project]
    name = "aqueduct-sample-project"
    dependencies = ["aqueduct"]

    [project.entry-points.aqueduct_modules]
    aqsample = "aqsample.aqueduct:aq_modules"

This file specifies that the :func:`aq_modules` function inside the :code:`aqsample.aqueduct`
can be called and will provide a list of module names to be searched for tasks.
For example, the :func:`aq_modules` function could be:

    def aq_modules():
        return ['my_package.my_aqueduct_module']


CLI tools
---------


:code:`aq run <task>`
    Execute a task.

    :code:`<task>` 
        The task to be executed. Can be specified either by name or by a Python expression.
        To specify a task by name, pass :code:`aq run TaskName option1=value1 option2=value2...`.
        To specify a task using a Python expression, pass :code:`aq run "TaskName(option1=value1, option2=value2)"`.
        Using a Python expression is useful for tasks that accepts other tasks as arguments.

    :code:`--ipython`
        After the computation is done, open an IPython console. The `result` variable
        will contain the result of the computation.

    :code:`--force-root`
        Force execution of the task (do not check if its artifact exists).

    :code:`--dask <n_cores>`
        Use the Dask computing backend. Will create a :class:`LocalCluster` with 
        :code:`n_cores` computing processes.

    :code:`--dask-url <cluster_address>`
        Use the Dask computing backend. Connect to the cluster at :code:`cluster_address`.

    :code:`--multiprocessing <n_workers>`
        Use the Multiprocessing computing backend with :code:`n_workers`.


:code:`aq ls`
    List the tasks detected by CLI tools.

    :code:`--signature`
    Show the task signature.


:code:`aq del <task> --below <task_name>`
    Delete all artifacts in the task tree that stems from :code:`<task>`. 

    :code:`<task>`
        The root task to expand. The artifact of the task itself and its children will
        be considered for deletion. Can be specified by name or using a Python expression,
        similarly to :code:`aq run`.

    :code:`--below <task_name>`
        Do not delete artifacts in tasks that are children of :code:`<task_name>`.
        When expanding the task tree to find artifacts, do not expand :code:`<task_name>`