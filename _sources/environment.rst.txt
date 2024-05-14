Environment
===========

Aqueduct takes cues from its environment, allowing you to change its behavior as you
deploy it on different machines.

Environment variables
---------------------

The following environment variables are used by Aqueduct.

:code:`AQ_LOCAL_STORE`
    Path to the local store.
    This is where artifacts defined using :class:`~aqueduct.artifact.LocalStoreArtifact` 
    will be saved.

:code:`AQ_SCRATCH_STORE`
    Path to the scratch store.
    This is there artifact defined using :class:`~aqueduct.artifact.LocalStoreArtifact` 
    with :code:`scratch=True` will be saved.