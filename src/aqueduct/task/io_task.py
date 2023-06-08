from __future__ import annotations
from typing import TypeVar, Optional

import logging

from ..config import get_deep_key, get_config
from ..artifact import Artifact
from .task import AbstractTask


_logger = logging.getLogger(__name__)

T = TypeVar("T")


class IOTask(AbstractTask[T]):
    """A Task that involves input/output or other side effects. It does not return
    any meaningful value or data. Instead, it returns the `Artifact` that it has
    produced. That artifact can then be used in subsequent tasks to retrieve the
    created file.

    Note that, in an IOTask, the return value of `run` is ignored. This matches the
    expectation that IOTasks mostly act through side effects."""

    def __call__(self, *args, **kwargs) -> Optional[Artifact]:
        """Prepare the context and call `run`.

        Note that the user is responsible for creating the artifacts during `run`. The
        :meth:`run` method should create a file as specified by the :meth:`artifact`
        method.

        Returns:
            If :meth:`artifact` specifies an artifact, return that artifact.
            If no artifact is spcified, return `None`.

        Throws:
            `RuntimeError` if the specified artifact was not stored after the execution
            or `run`. Disable this behavior by setting the `aqueduct.check_storage`
            configuraiton option to `False`."""
        global_config = get_config()
        artifact = self._resolve_artifact()

        if not artifact or not artifact.exists():
            self.run(*args, **kwargs)

        if artifact and get_deep_key(global_config, "aqueduct.check_storage", False):
            if not artifact.exists():
                raise RuntimeError(
                    f"Task did not store artifact it promised: {artifact}"
                )

        return artifact
