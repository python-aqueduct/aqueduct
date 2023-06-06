"""Task logic for an Aqueduct project.

A Task is the basic unit of work in Aqueduct. A Task is a is a function and its
requirements. If the Task specifies an :class:`Artifact`, then it is analogous to a
target in a Makefile.

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
from typing import Any, Callable, Generic, TypeAlias, TypeVar, Union


import abc
import inspect
import logging

from .config import set_config, get_deep_key, get_config
from .artifact import (
    Artifact,
    ArtifactSpec,
    resolve_artifact_from_spec,
)
from .config import Config, ConfigSpec, resolve_config_from_spec


_logger = logging.getLogger(__name__)

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

        self.configure(*new_args, **new_kwargs)

    def __call__(self, *args, **kwargs) -> T:
        result = self.run(*args, **kwargs)

        global_config = get_config()
        artifact = self._resolve_artifact()

        if artifact and get_deep_key(global_config, "aqueduct.check_storage", False):
            if not artifact.exists():
                raise KeyError(f"Task did not store artifact it promised: {artifact}")

        return result

    @abc.abstractmethod
    def run(self, *args, **kwargs) -> T:
        raise NotImplementedError()

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

    def compute(self) -> T:
        from .backend import ImmediateBackend

        immediate_backend = ImmediateBackend()
        return immediate_backend.run(self)


def fullname(o) -> str:
    """See https://stackoverflow.com/questions/2020014/get-fully-qualified-class-name-of-an-object-in-python."""

    module = o.__module__
    if module == "builtins":
        return o.__qualname__  # avoid outputs like 'builtins.str'

    if inspect.isfunction(o):
        return module + "." + o.__qualname__
    else:
        name = o.__class__.__qualname__
        return module + "." + name
