Saving computation results with artifacts
==========================================

Using artifacts
---------------

If a task runs heavy computations, you may want to cache its result using a pattern 
like this::

    import pathlib

    import aqueduct as aq

    class MyHeavyTask(aq.Task):
        def run(self):
            if pathlib.Path('my_file.pkl').is_file():
                return load_file('my_file.pkl')
            else:
                # heavy computation
                result = ...
                save_result(result, 'my_file.pkl')
                return result

Aqueduct helps you implement this pattern with :class:`~aqueduct.artifact.Artifact`. 
An artifact is simply a pointer to a storage location, like a filesystem path. 
You can specify the artifact of a task using the :func:`artifact` method::

    # Equivalent to above.
    class MyHeavyTaskWithArtifact(aq.Task):
        def artifact(self):
            return 'my_file.pkl'

        def run(self):
            # heavy computation
            result = ...
            return result


Path resolution using the local store
---------------

You can specify 
Optionally, you can use the :class:`~aqueduct.artifact.LocalStoreArtifact` class to specify artifact location.
This way, you can automatically centralize the location of your artifacts: they are stored relative to the `AQ_LOCAL_STORE` path.


Autosave and autoload
---------------------

In the previous example, Aqueduct automatically figured out how to save and load the
computation result using the artifact file extension.
The currently supported extensions are as follows:

:code:`.pkl`
    Use :code:`pickle.dump` and :code:`pickle.load`.
:code:`.parquet`
    Use :code:`...` and :code:`pickle.load`. 
:code:`.nc`
    Use :code:`xarray.`

Specific IO
----------

Sometimes, you would like to define an artifact but save it yourself. 
You can override the save and load behavior if the automatic resolution does not work for you::

    class MyHeavyTaskWithFancyArtifact(aq.Task):
        def artifact(self):
            return 'my_file.xyz'

        def run(self):
            # heavy computation
            result = ...
            return result 

        def save(self, result):
            path = self.artifact().path
            my_fancy_save(path, result)

        def load(self):
            path = self.artifact().path
            return my_fancy_load(path)

Note how we specified the artifact path even if we save it ourselves.
This way, aqueduct is able to detect if the artifact already exists by querying the filesystem, so that it knows when to call run, save and load.