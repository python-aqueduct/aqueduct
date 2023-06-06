from typing import TypeVar, Generic, TypeAlias

import abc
import datetime
import logging

import abc

T = TypeVar("T")
U = TypeVar("U")

_logger = logging.getLogger(__name__)


class Artifact(abc.ABC):
    @abc.abstractmethod
    def exists(self) -> bool:
        raise NotImplementedError()

    def last_modified(self) -> datetime.datetime:
        return datetime.datetime.fromtimestamp(0)


ArtifactSpec: TypeAlias = Artifact | str | None
