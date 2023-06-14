from typing import Callable, TypeVar, Generic, TypeAlias, BinaryIO, TextIO

import abc
import datetime
import logging

_T = TypeVar("_T")

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


class TextStreamArtifact(Artifact, Generic[_T], abc.ABC):
    @abc.abstractmethod
    def dump_text(self, o: _T, writer: Callable[[_T, TextIO], None]):
        raise NotImplementedError()

    @abc.abstractclassmethod
    def load_text(self, reader: Callable[[TextIO], _T]) -> _T:
        raise NotImplementedError()


class StreamArtifact(Artifact, Generic[_T], abc.ABC):
    @abc.abstractmethod
    def dump(self, o: _T, writer: Callable[[_T, TextIO], None]):
        raise NotImplementedError()

    @abc.abstractclassmethod
    def load(self, reader: Callable[[TextIO], _T]) -> _T:
        raise NotImplementedError()


ArtifactSpec: TypeAlias = Artifact | str | None
TextStreamArtifactSpec: TypeAlias = str | TextStreamArtifact
