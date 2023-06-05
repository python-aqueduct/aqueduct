from typing import TypeVar, Generic, TypeAlias, Callable

import abc
import logging

from ..store import StoreSpec, resolve_store_from_spec


import abc
from typing import BinaryIO, Generic

T = TypeVar('T')

_logger = logging.getLogger(__name__)


class Artifact(Generic[T], abc.ABC):
    """Describes how to store the return value of a :class:`Task` to a
    :class:`Store`."""

    def __init__(self, name: str, store: StoreSpec = None):
        self._name = name
        self.store = resolve_store_from_spec(store)

    @property
    def name(self):
        return self._name

    @abc.abstractmethod
    def deserialize(self, stream: BinaryIO) -> T:
        raise NotImplementedError("Artifact must implement `load` method.")

    @abc.abstractmethod
    def serialize(self, object: T, stream: BinaryIO):
        raise NotImplementedError("Artifact must implement `dump` method.")

    def load_from_store(self, deserializer=None):
        _logger.info(f"Loading {self.name} from store.")

        deserializer = deserializer if deserializer else self.deserialize

        return self.store.load_binary(self.name, deserializer)

    def dump_to_store(self, object_to_dump):
        _logger.info(f"Saving {self.name} to store.")
        self.store.dump_binary(self.name, object_to_dump, self.serialize)

    def exists(self) -> bool:
        return self.name in self.store

    def _resolve_store(self):
        return resolve_store_from_spec(self.store)
    
ArtifactSpec: TypeAlias = Artifact | str | Callable[..., Artifact] | None
