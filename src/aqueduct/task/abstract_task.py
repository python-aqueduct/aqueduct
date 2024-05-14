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

import datetime
import inspect
import logging


from ..artifact import Artifact, ArtifactSpec, resolve_artifact_from_spec
from ..config import AqueductConfig, ConfigSpec, resolve_config_from_spec
from .autoresolve import WrapInitMeta
from ..task_tree import reduce_type_in_tree
from .autostore import load_artifact, store_artifact


if TYPE_CHECKING:
    from ..backend import Backend
    from ..task_tree import TaskTree

_T = TypeVar("_T")
_U = TypeVar("_U")

AppliedClass = TypeVar("AppliedClass", bound="AbstractTask")


_logger = logging.getLogger(__name__)


class AbstractTask(Generic[_T], metaclass=WrapInitMeta):
    """Base class for a all Tasks. In most cases you don't have to subclass this
    directly. Subclass either :class:`MapReduceTask` of :class:`Task` to define your own Task.
    """

    AQ_AUTOSAVE = True
    """If `True`, the result of the task is automatically stored in the artifact."""

    AQ_AUTOLOAD = True

    CONFIG: ConfigSpec = None
    """The configuration of the Task class. It specifies how the `config` method should
    behave. If set to a dict-like object, that mapping is used as configuration. If set
    to a `str`, the string is used as a key to retrieve the configuration from the
    global configuration dict. If set to `None`, the full class name is used as the
    configuration section for the task."""

    AQ_UPDATED: str | datetime.datetime | None = None
    """If set, sent through `pd.to_datetime`. Any artifacts older than the resulting
    date are considered stale and recomputed."""

    def __init__(self):
        """The __init__ method of a :class:`Task` automatically retrieves the value of
        its arguments from the configuration if they are not provided. See
        :ref:`configuration` for more details."""
        self._aq_force_root = False

        # These two values are set by the wrapper around __init__ introduced by
        # `WrapInitMeta`.
        self._args = ""
        self._kwargs = ""

    def artifact(self) -> Optional[ArtifactSpec]:
        """Describe the artifact produced by `run`. See :class:`Artifact` for more
        details.

        Returns:
            If `None`, the class does not store any artifact. It will be fully run
                every time it is called.
            If `str`,  the class stores a :class:`LocalFilesystemArtifact` at the
                location specified by the string. If that file exists, the task
                will be loaded from the filesystem instead of being run.
            If an :class:`Artifact` object, the class stores resources as specified by
                the object."""
        return None

    def is_cached(self) -> bool:
        """Indicates if the Task is currently cached.

        Returns:
            A boolean indicating if there exists a stored artifact as specified by the
            `artifact` method."""
        artifact = resolve_artifact_from_spec(self.artifact())

        if artifact is not None:
            return artifact.exists()
        else:
            return False

    def requirements(self) -> "TaskTree":
        """Subclass this to express the Tasks that are required for this Task to run.
        The tasks specified here will be computed before this Task is executed. The
        result of the required tasks is passed as an argument to the `run` method.

        Returns:
            If `None`, the task has no dependencies. If a `Task` instance, that task is
            computed, and the result is passed as argument to the `run` method. If a
            data structure containing Tasks, the tasks are replaced by their results in
            the data structure, and then the data structure is passed as an argument
            to `run`."""
        return None

    def _resolve_requirements(self, ignore_cache=False) -> "TaskTree":
        force_run = getattr(self, "_aq_force_root", False)

        if self.is_cached() and not force_run and not ignore_cache:
            return None
        else:
            return self.requirements()

    def config(self) -> AqueductConfig:
        """Resolve the configuration as specified in the `CONFIG` class variable, and
        return it."""
        return resolve_config_from_spec(self.CONFIG, self.__class__)

    @classmethod
    def _fully_qualified_name(cls) -> str:
        """Full module path to the task."""
        module = inspect.getmodule(cls)

        if module is None:
            raise RuntimeError("Could not recover module for Task.")

        return module.__name__ + "." + cls.__qualname__

    def _unique_key(self) -> str:
        """Generate a unique key that identifies the task."""

        # table = str.maketrans("{[(", "___", "}]) ")
        # manip_str = self.__str__().translate(table)

        # Here, _args_hash is set by the WrapInit metaclass. This makes things more
        # confusing, but in return the user does not have to worry about calling
        # super().__init__().
        return "-".join(
            [
                self.task_name(),
                self._args_hash,  # type: ignore
            ]
        )

    def __str__(self):
        task_name = self.task_name()
        return f"{task_name}"

    def set_force_root(self, value=True):
        self._aq_force_root = value

    @classmethod
    def task_name(cls) -> str:
        """User friendly name that is used to identify the task in a graph."""
        return cls.__qualname__

    def post(self, result: _T, requirements=None) -> _T:
        return result

    def save(self, object: _T):
        artifact = resolve_artifact_from_spec(self.artifact())

        if artifact is not None:
            store_artifact(artifact, object)

    def load(self) -> _T:
        """Load an artifact and return it.

        If an artifact is specified, this is called to load the artifact from cache
        to avoid excecuting the `run` method. Override this to implement your own
        loading behavior.
        """
        artifact = resolve_artifact_from_spec(self.artifact())

        if artifact is None:
            raise ValueError(
                f"Task {self} has no artifact specified, but tried to load one."
            )

        return load_artifact(artifact, type_hint=None)


RequirementSpec: TypeAlias = Union[
    tuple[AbstractTask], list[AbstractTask], dict[str, AbstractTask], AbstractTask, None
]
RequirementArg: TypeAlias = Union[Callable[..., RequirementSpec], RequirementSpec]
