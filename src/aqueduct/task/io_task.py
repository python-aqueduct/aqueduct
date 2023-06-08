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
from typing import TypeVar

import abc
import forge
import inspect
import logging

from ..config import get_deep_key, get_config
from ..artifact import Artifact
from .task import Task
from .autoresolve import WrapInitMeta


_logger = logging.getLogger(__name__)

T = TypeVar("T")


class IOTask(Task):
    def run(self, *args, **kwargs) -> None:
        raise NotImplementedError("Task must implement run method.")

    def __call__(self, *args, **kwargs) -> Artifact:
        global_config = get_config()
        artifact = self._resolve_artifact()

        if not artifact or not artifact.exists():
            self.run(*args, **kwargs)

        if artifact and get_deep_key(global_config, "aqueduct.check_storage", False):
            if not artifact.exists():
                raise KeyError(f"Task did not store artifact it promised: {artifact}")

        return artifact

    def _resolve_artifact(self) -> Artifact:
        artifact = super()._resolve_artifact()

        if artifact is None:
            raise NotImplementedError("IOTask must implement artifact method.")

        return artifact


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
