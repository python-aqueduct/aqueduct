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

from .artifact import Artifact
from .config import get_config

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
RequirementArg: TypeAlias = Union[Callable[..., RequirementSpec], RequirementSpec]


def fetch_args_from_config(
    fn: Callable, args, kwargs, cfg: dict[str]
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
    try:
        signature = inspect.signature(fn)
        bind = signature.bind(*args, **kwargs)
    except TypeError:
        raise

    for p in signature.parameters:
        # Find all arguments which do not have a defined value.
        if p not in ["args", "kwargs", "*", "/"] and (
            not p in bind.arguments or bind.arguments[p] is None
        ):
            if p in cfg:
                bind.arguments[p] = cfg[p]

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
        artifact = self.artifact()
        if artifact and artifact.exists():
            """Exclude the dependencies from the graph to avoid computing them."""
            return Binding(self, artifact.load_from_store)

        requirements = self.requirements()

        args, kwargs = fetch_args_from_config(
            self.fn, args, kwargs, self._resolve_cfg()
        )

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

    def _create_binding(self, *args, **kwargs):
        return Binding(self, self.run_and_maybe_cache, *args, **kwargs)

    def artifact(self) -> Artifact | None:
        """Express wheter the output of `run` should be saved as an :class:`Artifact`.

        When running the Task, if the artifact was previously created and is available
        inside the :class:`Store`, then the task will not be executed and the artifact
        will be loaded from the `Store` instead.

        Returns:
            If no artifact should be created, return `None`.
            If an artifact should be created, return an :class:`Artifact` instance."""
        return None

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

    def cfg(self) -> dict[str]:
        """Define the configuration of the Task. The behavior depends on the return
        type. The configuration is used to fetch default values for parameters that
        don't have any value defined.

        If it returns a `dict`, it is used directly as a configuration dict. If it
        returns a `str`, that string used to retrieve the corresponding configuration
        section in the global config (see :func:`set_config`). If it returns an
        empty `str` or `None`, return an empty configuration dictionary.

        Returns
            A configuration dictionary."""
        return ""

    def _resolve_cfg(self):
        user_cfg = self.cfg()

        if isinstance(user_cfg, dict):
            return user_cfg
        elif isinstance(user_cfg, str) and len(user_cfg) > 0:
            global_cfg = get_config()
            breakpoint()
            return global_cfg[user_cfg]
        elif not user_cfg:
            return {}

    def run_and_maybe_cache(self, *args, **kwargs) -> T:
        result = self.run(*args, **kwargs)

        artifact = self.artifact()
        if artifact:
            artifact.dump_to_store(result)

        return result

    @abc.abstractmethod
    def run(self, *args, **kwargs) -> T:
        """Override this to define how to execute the Task."""
        raise NotImplementedError("Task must implement method `run`.")


def task(
    *args,
    requirements: RequirementSpec = None,
    artifact: Artifact | None = None,
    cfg: str | dict[str] = None,
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

    if args and len(args) == 1:
        # Decorator was called directly, as in @taskdef
        return wrapper(args[0])
    elif args and len(args) != 1:
        raise RuntimeError
    else:
        # Decorator was called with parentheses, as in @taskdef()
        return wrapper


class WrappedTask(Task):
    def __init__(
        self,
        fn,
        requirements: RequirementSpec = None,
        artifact=None,
        cfg: str | dict[str] | None = None,
    ):
        self.fn = fn
        self._artifact = artifact
        self._requirements = requirements
        self._cfg = cfg

    def run(self, *args, **kwargs):
        return self.fn(*args, **kwargs)

    def artifact(self):
        return self._artifact

    def requirements(self):
        return self._requirements

    def cfg(self):
        return self._cfg
