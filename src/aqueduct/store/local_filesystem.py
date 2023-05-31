from typing import BinaryIO, Callable, TypeVar

import datetime
import pathlib

from .store import Store

T = TypeVar("T")


class LocalFilesystemStore(Store):
    """Expose the local filesystem to store artifacts."""

    def __init__(self, root="./"):
        self.root = pathlib.Path(root)

    def __contains__(self, key: str):
        return self.exists(key)

    def exists(self, name: str) -> bool:
        return (self.root / name).is_file()

    def last_modified(self, name: str):
        return datetime.datetime.fromtimestamp((self.root / name).stat().st_mtime)

    def load_binary(self, name: str, deserializer: Callable[[BinaryIO], T]) -> T:
        with (self.root / name).open("rb") as f:
            return deserializer(f)

    def dump_binary(
        self, name: str, object: T, serializer: Callable[[T, BinaryIO], None]
    ):
        full_path = self.root / name
        full_path.parent.mkdir(exist_ok=True, parents=True)

        with (self.root / name).open("wb") as f:
            return serializer(object, f)
