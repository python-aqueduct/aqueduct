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

import abc
import inspect
from typing import Any, Callable, Generic, TypeAlias, TypeVar, Union

from .binding import Binding

from .artifact import (
    Artifact,
    ArtifactSpec,
    resolve_artifact_from_spec,
)
from .config import Config, ConfigSpec, resolve_config_from_spec

T = TypeVar("T")

RequirementSpec: TypeAlias = Union[
    tuple[Binding], list[Binding], dict[str, Binding], Binding, None
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

    Alternatively, the `taskdef` decorator can be used to define simple tasks directly
    from a function. Class-based Tasks are necessary to define dynamic requirements and
    artifact."""

    def __call__(self, *args, **kwargs) -> Binding[T]:
        """Returns:
        A :class:`Binding` that associates the `run` method with arguments `*arg` and
        `**kwargs`. The Binding is a work bundle that can then be executed locally or
        sent to a computing backend.
        """
        requirements = self.requirements()

        # Modify args and kwargs to use values from config if they exist.
        args, kwargs = self._args_with_values_from_config(*args, **kwargs)

        # Test bind the arguments to the inner function. If it doesn't work now, there's
        # no point in creating the Binding, it will never be able ot call the function
        # correctly.
        _ = inspect.signature(self.run).bind(*args, **kwargs)

        if callable(requirements):
            requirements = requirements(*args, **kwargs)

        if isinstance(requirements, Binding) or isinstance(requirements, list):
            return self._create_binding(requirements, *args, **kwargs)
        elif isinstance(requirements, dict):
            return self._create_binding(*args, **requirements, **kwargs)
        elif isinstance(requirements, tuple):
            return self._create_binding(*requirements, *args, **kwargs)
        elif not requirements:
            return self._create_binding(*args, **kwargs)
        else:
            raise Exception("Unexpected case when building Binding.")

    def _args_with_values_from_config(self, *args, **kwargs):
        config = self._resolve_cfg()
        return fetch_args_from_config(self.run, args, kwargs, config)

    def _create_binding(self, *args, **kwargs):
        # Resolve artifact. If there is an artifact configure, use a runner function
        # that makes use of the cache.
        artifact = self._resolve_artifact(*args, **kwargs)
        if artifact:
            """Exclude the dependencies from the graph to avoid computing them."""
            return Binding(self, self._run_with_cache, *args, **kwargs)
        else:
            return Binding(self, self.run, *args, **kwargs)

    def artifact(self) -> ArtifactSpec:
        """Express wheter the output of `run` should be saved as an :class:`Artifact`.

        When running the Task, if the artifact was previously created and is available
        inside the :class:`Store`, then the task will not be executed and the artifact
        will be loaded from the `Store` instead.

        Returns:
            If no artifact should be created, return `None`.
            If an artifact should be created, return an :class:`Artifact` instance."""
        return None

    def _resolve_artifact(self, *args, **kwargs) -> Artifact | None:
        signature = inspect.signature(self.run)
        args, kwargs = self._args_with_values_from_config(*args, **kwargs)
        bind = signature.bind_partial(*args, **kwargs)

        return resolve_artifact_from_spec(
            self.artifact(), bind.arguments, *args, **kwargs
        )

    def requirements(self) -> RequirementArg:
        """Describe the inputs required to run the Task. The inputs will be passed to
        `run` on execution, according to the shape of the return value.

        If the return value is a `Binding` or a `list`, `run` will be called with the
        required value as its first argument. For instance, imagine we have::

            task = MyTask()
            binding = task(*args, **kwargs)
            binding.compute()

        The way `run` is called depends on the shape of the return value of
        `requirements.` If the return value is a `list` or a `Binding`, then `run` is
        called like this::

            self.run(requirements, *args, **kwargs)

        If the return value is a dictionary, `run` will be called with the values of the
        dictionary as keyword arguments::

            self.run(*args, **requirements, **kwargs)

        If the return value is a tuple, `run` will be called with the requirements as
        positional arguments::

            self.run(*requirements, *args, **kwargs)

        Returns:
            `None` if there are no requirements. A data structure expressing the
            requirements otherwise.
        """

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

    def _run_with_cache(self, *args, **kwargs) -> T:
        """An artifact was detected. Check cache before running the function, and
        save result to cache after computation."""
        artifact = self._resolve_artifact(*args, **kwargs)

        if artifact is None:
            raise RuntimeError("No artifact specified while it was expected to be.")

        if artifact.exists():
            return artifact.load_from_store()

        result = self.run(*args, **kwargs)

        artifact.dump_to_store(result)

        return result

    def run(self, *args, **kwargs) -> T:
        """Override this to define how to execute the Task."""
        raise NotImplementedError("Task must implement method `run`.")

    def _fully_qualified_name(self) -> str:
        module = inspect.getmodule(self)

        if module is None:
            raise RuntimeError("Could not recover module for Task.")

        return module.__name__ + "." + self.__class__.__qualname__


def task(
    *args,
    requirements: RequirementSpec = None,
    artifact: ArtifactSpec = None,
    cfg: ConfigSpec = None,
):
    """Decorator to quickly create a `Task` from a function. Example::

        @task
        def input_array():
            return np.random.rand(100, 100)

        @task(requirements={input_array: input_array()})
        def add_value(some_param, input_array=None):
            return input_array + value

        with_10 = add_value(10).compute()

    Note how we called add_value without providing `input_array`, because it was already
    specified in the requirements.

    Arguments:
        requirements: Specify the requirements. The way the requirements are passed
            to the function depends on the shape of the provided value. See the
            :func:`Task.requirements` for more details.
        artifact: Wether to store the function return value as an artifact on execution.
            If `None`, do not store the return value. If an instance of `Artifact`,
            store the return value on execution. If the artifact exists, the function
            execution will be skipped and the artifact loaded from disk instead.

    Returns:
        A wrapper that bundles the input function inside a :class:`WrappedTask`
        instance.
    """

    def wrapper(fn):
        return WrappedTask(fn, requirements=requirements, artifact=artifact, cfg=cfg)

    if args and len(args) == 1 and callable(args[0]):
        # Decorator was called directly, as in @taskdef. That means the function to
        # wrap is arg[0].
        fn = args[0]
        return wrapper(fn)
    else:
        # Decorator was called with parentheses, as in @taskdef()
        return wrapper


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


class WrappedTask(Task[T]):
    def __init__(
        self,
        fn: Callable[..., T],
        requirements: RequirementSpec = None,
        artifact: ArtifactSpec = None,
        cfg: ConfigSpec = None,
    ):
        self._artifact = artifact
        self._requirements = requirements
        self._cfg = cfg

        self._fn = fn
        self.run = fn

    def run(self, *args, **kwargs):
        # This method is never called as is, it is replaced by the input fn in the
        # constructor. We do this to preserve fn's signature. It may be possible to
        # to this cleanly using functools.wraps, but since this is a method instead of
        # a function I could not get it to work.
        return self._fn(*args, **kwargs)

    def artifact(self) -> ArtifactSpec:
        return self._artifact

    def requirements(self) -> RequirementSpec:
        return self._requirements

    def cfg(self) -> ConfigSpec:
        return self._cfg

    def _resolve_cfg(self) -> Config:
        return resolve_config_from_spec(self.cfg(), self)

    def _fully_qualified_name(self) -> str:
        return fullname(self.run)
