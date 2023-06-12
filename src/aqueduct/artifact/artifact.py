from typing import TypeVar, Generic, TypeAlias

import abc
import datetime
import logging

_logger = logging.getLogger(__name__)


class Artifact(abc.ABC):
    """The location and metadata of a store artifact."""

    @abc.abstractmethod
    def exists(self) -> bool:
        """Check if the artifact already exists.

        Returns
            `True` if the artifact already exists, `False` otherwise."""
        raise NotImplementedError()

    def last_modified(self) -> datetime.datetime:
        return datetime.datetime.fromtimestamp(0)

    @abc.abstractmethod
    def size(self) -> int:
        """The size of the stored artifact, in bytes."""
        raise NotImplementedError()


ArtifactSpec: TypeAlias = Artifact | str | None
