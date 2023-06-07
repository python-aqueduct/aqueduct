from typing import Any, Callable, Generic, TypeVar, TypeAlias, Union

import abc
import dask
import inspect

from ..artifact import Artifact, ArtifactSpec, resolve_artifact_from_spec
from ..config import Config, ConfigSpec, resolve_config_from_spec

T = TypeVar("T")

RequirementSpec: TypeAlias = Union[
    tuple["Task"], list["Task"], dict[str, "Task"], "Task", None
]
RequirementArg: TypeAlias = Union[Callable[..., RequirementSpec], RequirementSpec]


def fetch_args_from_config(
    fn: Callable, args, kwargs, cfg: Config
) -> tuple[tuple[Any, ...], dict[str, Any]]:
    """Given a callable and a configuration dict, try and fetch argument values from
    the config dict if needed.

    Arguments:
        fn: The function for which we want to fetch arguments from config.
        args: The arguments the functino would be called with.
        kwargs: The kwargs the function would be called with.
        cfg: The config dictionary from which to tech default values if needed.

    Returns:
        args: The same args, except if an argument is `None`, it is replaced by the
            corresponding value in `cfg`, if it exists there.
        kwargs: The same kwargs, except if an argument value was `None`, it is replaced
            by the value of the corresponding key in `cfg`."""
    signature = inspect.signature(fn)
    bind = signature.bind_partial(*args, **kwargs)

    for p in signature.parameters:
        # Find all arguments which do not have a defined value.
        if p not in ["args", "kwargs", "*", "/"] and (
            not p in bind.arguments or bind.arguments[p] is None
        ):
            if p in cfg:
                bind.arguments[p] = cfg[p]

    bind.apply_defaults()

    return bind.args, bind.kwargs


class Task(abc.ABC, Generic[T]):
    """Base class for a Task. Subclass this to define your own task.

    Alternatively, the `task` decorator can be used to define simple tasks directly
    from a function. Class-based Tasks are necessary to define dynamic requirements and
    artifact."""

    def __init__(self, *args, **kwargs):
        config = self._resolve_cfg()
        new_args, new_kwargs = fetch_args_from_config(
            self.configure, args, kwargs, config
        )

        self._args_hash = dask.base.tokenize(*args, **kwargs)
        self.configure(*new_args, **new_kwargs)

    @abc.abstractmethod
    def __call__(self, *args, **kwargs):
        raise NotImplementedError(
            "__call__ not implemented for Task. Did you mean to use IOTask or PureTask as a parent class?"
        )

    def run(self, *args, **kwargs) -> T:
        pass

    def configure(self, *args, **kwargs):
        pass

    def _args_with_values_from_config(self, *args, **kwargs):
        config = self._resolve_cfg()

        return fetch_args_from_config(self.__call__, args, kwargs, config)

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

    def requirements(self) -> RequirementArg:
        return None

    def cfg(self) -> ConfigSpec:
        """Define the configuration of the Task. The behavior depends on the return
        type. The configuration is used to fetch default values for parameters that
        don't have any value defined.

        If it returns a `dict`, it is used directly as a configuration dict. If it
        returns a `str`, that string used to retrieve the corresponding configuration
        section in the global config (see :func:`set_config`). If it returns an
        empty `str` or `None`, return an empty configuration dictionary.

        Returns
            A configuration dictionary."""
        return None

    def _resolve_cfg(self):
        return resolve_config_from_spec(self.cfg(), self)

    def _fully_qualified_name(self) -> str:
        module = inspect.getmodule(self)

        if module is None:
            raise RuntimeError("Could not recover module for Task.")

        return module.__name__ + "." + self.__class__.__qualname__

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
