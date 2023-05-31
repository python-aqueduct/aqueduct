from typing import BinaryIO, Callable, TextIO, TypeVar

import abc

T = TypeVar("T")


class Store(abc.ABC):
    def __contains__(self, key: str):
        return self.exists(key)

    @abc.abstractmethod
    def exists(self, name) -> bool:
        return False

    @abc.abstractmethod
    def dump_binary(
        self, name: str, object: T, serializer: Callable[[T, BinaryIO], None]
    ):
        raise NotImplementedError()

    @abc.abstractmethod
    def load_binary(self, name: str, deserializer: Callable[[BinaryIO], T]) -> T:
        raise NotImplementedError()
