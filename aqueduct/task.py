"""Task logic for an Aqueduct project.

A Task is a is a function and its requirements. If the Task specifies an
:class:`Artifact`, then it is analogous to a target in a Makefile.

Examples:

    Define a basic task::

        @taskdef() 
        def my_task():
            return np.random.rand(100,100)
            
    Save the return value of the function as an artifact every time the task is run. If
    the artifact exists on the next execution, it is loaded from file instead of being
    recomputed::
    
        @taskdef(artifact=PickleArtifact())
        def my_task():
            return"""

from __future__ import annotations

import abc

from typing import TypeAlias, Union, Generic, TypeVar, Callable


from .artifact import Artifact

T = TypeVar("T")


class Binding(Generic[T]):
    """A work bundle that is the binding of a :class:`Task` to arguments.

    This work bundle can then be offloaded to an external process to be actually
    computed. Typically, Bindings are generated automatically and should only be created
    by the Task class."""

    def __init__(self, task: Task, fn: Callable[..., T], *args, **kwargs):
        self.task = task
        self.fn = fn
        self.args = args
        self.kwargs = kwargs

    def compute(self) -> T:
        return self.fn(*self.args, **self.kwargs)

    def is_pure(self) -> bool:
        return self.task.artifact == None


RequirementSpec: TypeAlias = Union[
    tuple[Binding], list[Binding], dict[str, Binding], Binding, None
]


def taskdef(requirements: RequirementSpec = None, artifact: Artifact | None = None):
    def wrapper(fn):
        return WrappedTask(
            fn,
            requirements=requirements,
            artifact=artifact,
        )

    return wrapper


class Task(abc.ABC, Generic[T]):
    def __call__(self, *args, **kwargs):
        artifact = self.artifact()
        if artifact and artifact.exists():
            """Exclude the dependencies from the graph to avoid computing them."""
            return Binding(self, artifact.load_from_store)

        requirements = self.requirements()

        if isinstance(requirements, Binding) or isinstance(requirements, list):
            return self._create_binding(
                self.run_and_maybe_cache, requirements, *args, **kwargs
            )
        elif isinstance(requirements, dict):
            return self._create_binding(
                self.run_and_maybe_cache, *args, **requirements, **kwargs
            )
        elif isinstance(requirements, tuple):
            return self._create_binding(
                self.run_and_maybe_cache, *requirements, *args, **kwargs
            )
        elif not requirements:
            return self._create_binding(self.run_and_maybe_cache, *args, **kwargs)
        else:
            raise Exception("Unexpected case when building Binding.")

    def _create_binding(self, fn, *args, **kwargs):
        return Binding(self, fn, *args, **kwargs)

    def artifact(self) -> Artifact | None:
        return None

    def requirements(self) -> RequirementSpec:
        return None

    def run_and_maybe_cache(self, *args, **kwargs):
        result = self.run(*args, **kwargs)

        artifact = self.artifact()
        if artifact:
            artifact.dump_to_store(result)

        return result

    @abc.abstractmethod
    def run(self):
        raise NotImplementedError("Task must implement method `run`.")


class WrappedTask(Task):
    def __init__(self, fn, requirements: RequirementSpec = None, artifact=None):
        self.fn = fn
        self._artifact = artifact
        self._requirements = requirements

    def run(self, *args, **kwargs):
        return self.fn(*args, **kwargs)

    def artifact(self):
        return self._artifact

    def requirements(self):
        return self._requirements
