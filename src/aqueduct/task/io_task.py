from __future__ import annotations
from typing import TypeVar, Optional, TYPE_CHECKING

import logging

from ..artifact import resolve_artifact_from_spec
from ..config import get_deep_key, get_config
from ..artifact import Artifact
from .abstract_task import AbstractTask

if TYPE_CHECKING:
    from ..backend import BackendSpec

_logger = logging.getLogger(__name__)

T = TypeVar("T")


class IOTask(AbstractTask[T]):
    """A Task that involves input/output or other side effects. It does not return
    any meaningful value or data. Instead, it returns the `Artifact` that it has
    produced. That artifact can then be used in subsequent tasks to retrieve the
    created file.

    Note that, in an IOTask, the return value of `run` is ignored. This matches the
    expectation that IOTasks mostly act through side effects."""

    def __call__(
        self, *args, backend_spec: Optional["BackendSpec"] = None, **kwargs
    ) -> Optional[Artifact]:
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

        artifact_spec = self.artifact()
        artifact = (
            resolve_artifact_from_spec(artifact_spec)
            if artifact_spec is not None
            else None
        )

        force_root = getattr(self, "_aq_force_root", False)

        if force_root or not artifact or not artifact.exists():
            self.run(*args, **kwargs)

        if artifact and global_config.aqueduct.get("check_storage", False):
            if not artifact.exists():
                raise RuntimeError(
                    f"Task did not store artifact it promised: {artifact}"
                )

        return artifact
