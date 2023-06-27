from typing import (
    Any,
    Callable,
    Generic,
    TypeVar,
    TypeAlias,
    Union,
    Optional,
    TYPE_CHECKING,
    overload,
)

import datetime
import inspect
import logging


from ..artifact import Artifact, ArtifactSpec, resolve_artifact_from_spec
from ..config import Config, ConfigSpec, resolve_config_from_spec
from .autoresolve import WrapInitMeta
from ..task_tree import _reduce_type_in_tree


if TYPE_CHECKING:
    from ..backend import Backend
    from ..task_tree import TaskTree

_T = TypeVar("_T")

_logger = logging.getLogger(__name__)


class AbstractTask(Generic[_T], metaclass=WrapInitMeta):
    """Base class for a all Tasks. In most cases you don't have to subclass this
    directly. Subclass either :class:`IOTask` of :class:`Task` to define your own Task.
    """

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

    def __call__(self, *args, **kwargs):
        """Prepare the context and call `run`. Both class:`Task` and :class:`IOTask`
        overwrite this."""
        raise NotImplementedError(
            "__call__ not implemented for Task. Did you mean to use IOTask or PureTask as a parent class?"
        )

    def run(self, reqs: Any) -> _T:
        """Subclass this to specify the work done to realize the task. When called,
        the resolved requirements are passed as the first positional argument."""
        raise NotImplementedError()

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
        artifact_spec = self.artifact()
        update_time = self._resolve_update_time()

        if artifact_spec is not None:
            artifact = resolve_artifact_from_spec(artifact_spec)

            if artifact.exists() and artifact.last_modified() >= update_time:
                return True
            if artifact.exists() and artifact.last_modified() < update_time:
                _logger.info(
                    f"Detected artifact older than AQ_UPDATED for task {self}."
                )
                return False

        return False

    def _resolve_own_update_time(self) -> datetime.datetime:
        if self.AQ_UPDATED is None:
            return datetime.datetime.fromtimestamp(0)
        elif isinstance(self.AQ_UPDATED, str):
            return datetime.datetime.fromisoformat(self.AQ_UPDATED)
        elif isinstance(self.AQ_UPDATED, datetime.datetime):
            return self.AQ_UPDATED
        else:
            raise ValueError("Could not interpret value of AQ_UPDATED")

    def _resolve_update_time(self) -> datetime.datetime:
        # We don't use _resolve_requirements here because we don't care about cache
        # behavior. We want to decide what is the date of the code, and then we decide
        # if the cache is stale or not.

        # def reduce_fn(task: AbstractTask, acc: datetime.datetime):
        #     requirements = task.requirements()
        #     if requirements is not None:
        #         time_of_reqs = _reduce_type_in_tree(
        #             requirements, AbstractTask, reduce_fn, acc
        #         )
        #     else:
        #         time_of_reqs = datetime.datetime.fromtimestamp(0)

        #     own_time = task._resolve_own_update_time()

        #     return max(time_of_reqs, own_time)

        # update_time = _reduce_type_in_tree(
        #     self, AbstractTask, reduce_fn, datetime.datetime.fromtimestamp(0)
        # )

        return datetime.datetime.fromtimestamp(0)

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

    def config(self) -> Config:
        """Resolve the configuration as specified in the `CONFIG` class variable, and
        return it."""
        return resolve_config_from_spec(self.CONFIG, self.__class__)

    @classmethod
    def _fully_qualified_name(cls) -> str:
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
                self.__class__.__qualname__,
                self._args_hash,  # type: ignore
            ]
        )

    def result(self, backend: Optional["Backend"] = None) -> _T:
        """Compute the result of the Task locally and return it. This is equivalent to
        calling to using the `execute` method of the :class:`ImmediateBackend`.

        Arguments:
            backend: The computing backend to use. Defaults to :class:`ImmediateBackend`.

        Returns:
            The value returned by the `run` method."""

        if backend is None:
            from ..backend import ImmediateBackend

            backend = ImmediateBackend()

        return backend.execute(self)

    def __str__(self):
        task_name = self.__class__.__qualname__
        args_str = tuple([str(x) for x in self._args[1:]])
        return f"{task_name}(args={args_str}, kwargs={self._kwargs})"

    def set_force_root(self, value=True):
        self._aq_force_root = value

    def as_artifact(self):
        return ArtifactTaskWrapper(self)


_Task = TypeVar("_Task", bound=AbstractTask)


class ArtifactTaskWrapper(AbstractTask, Generic[_Task]):
    def __init__(self, inner: _Task):
        self.inner = inner

    def __call__(self, *args, **kwargs) -> Artifact | None:
        if not self.inner.is_cached():
            self.inner.__call__(*args, **kwargs)

        return resolve_artifact_from_spec(self.inner.artifact())

    def artifact(self):
        return None

    def requirements(self):
        return self.inner.requirements()

    def _resolve_requirements(self, ignore_cache=False) -> "TaskTree":
        if self.inner.is_cached():
            return None
        else:
            return super()._resolve_requirements(ignore_cache)

    def run(self, *args, **kwargs):
        return self.inner.run(*args, **kwargs)

    def _unique_key(self):
        inner_key = self.inner._unique_key()
        first_part, *rest = inner_key.split("-")
        return "-".join([first_part + "*as_artifact", "-".join(rest)])


RequirementSpec: TypeAlias = Union[
    tuple[AbstractTask], list[AbstractTask], dict[str, AbstractTask], AbstractTask, None
]
RequirementArg: TypeAlias = Union[Callable[..., RequirementSpec], RequirementSpec]
