from typing import (
    Any,
    Callable,
    Generic,
    TypeVar,
    TypeAlias,
    Union,
    Optional,
    TYPE_CHECKING,
)

import dask
import dask.base
import inspect

from ..artifact import Artifact, ArtifactSpec, resolve_artifact_from_spec
from ..config import Config, ConfigSpec, resolve_config_from_spec
from .autoresolve import WrapInitMeta


if TYPE_CHECKING:
    from ..util import TaskTree

T = TypeVar("T")

RequirementSpec: TypeAlias = Union[
    tuple["Task"], list["Task"], dict[str, "Task"], "Task", None
]
RequirementArg: TypeAlias = Union[Callable[..., RequirementSpec], RequirementSpec]


class Task(Generic[T], metaclass=WrapInitMeta):
    """Base class for a Task. Subclass this to define your own task.

    Alternatively, the `task` decorator can be used to define simple tasks directly
    from a function. Class-based Tasks are necessary to define dynamic requirements and
    artifact."""

    CONFIG: ConfigSpec = None

    def __init__(self, *args, **kwargs):
        self._args_hash = dask.base.tokenize(*args, **kwargs)

    def __call__(self, *args, **kwargs):
        raise NotImplementedError(
            "__call__ not implemented for Task. Did you mean to use IOTask or PureTask as a parent class?"
        )

    def run(self, *args, **kwargs) -> T:
        raise NotImplementedError()

    def artifact(self) -> ArtifactSpec:
        """Express wheter the output of `run` should be saved as an :class:`Artifact`.

        When running the Task, if the artifact was previously created and is available
        inside the :class:`Store`, then the task will not be executed and the artifact
        will be loaded from the `Store` instead.

        Returns:
            If no artifact should be created, return `None`.
            If an artifact should be created, return an :class:`Artifact` instance."""
        return None

    def _resolve_artifact(self) -> Artifact | None:
        spec = self.artifact()
        return resolve_artifact_from_spec(spec)

    def is_cached(self) -> bool:
        artifact = self._resolve_artifact()
        return artifact is not None and artifact.exists()

    def requirements(self) -> Optional["TaskTree"]:
        return None

    def config(self):
        return resolve_config_from_spec(self.CONFIG, self.__class__)

    @classmethod
    def _fully_qualified_name(cls) -> str:
        module = inspect.getmodule(cls)

        if module is None:
            raise RuntimeError("Could not recover module for Task.")

        return module.__name__ + "." + cls.__qualname__

    def _unique_key(self):
        """If the key has a dash in the middle, Dask makes it pleasant to look at in
        the dashboard."""
        return "-".join(
            [self.__class__.__qualname__, self._fully_qualified_name(), self._args_hash]
        )

    def compute(self) -> T:
        from ..backend import ImmediateBackend

        immediate_backend = ImmediateBackend()
        return immediate_backend.run(self)
