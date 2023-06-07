from typing import TypeVar, Generic, TypeAlias

import abc
import datetime
import logging

_logger = logging.getLogger(__name__)


class Artifact(abc.ABC):
    @abc.abstractmethod
    def exists(self) -> bool:
        raise NotImplementedError()

    def last_modified(self) -> datetime.datetime:
        return datetime.datetime.fromtimestamp(0)


ArtifactSpec: TypeAlias = Artifact | str | None
