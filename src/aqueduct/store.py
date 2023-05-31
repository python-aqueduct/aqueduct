"""A `Store` is the interface between an :class:`Artifact` and a storage resource. 

So far, only the simple :class:`LocalFilesystemStore` is implemented."""

import abc
import datetime
import hydra
import pathlib
from typing import BinaryIO, TextIO

from .config import get_config


class Store(abc.ABC):
    def __contains__(self, key: str):
        return self.exists(key)

    @abc.abstractmethod
    def exists(self, name) -> bool:
        return False

    @abc.abstractmethod
    def get_read_stream(self, name) -> BinaryIO:
        raise NotImplementedError()

    @abc.abstractmethod
    def get_write_stream(self, name) -> BinaryIO:
        raise NotImplementedError()

    @abc.abstractmethod
    def get_read_stream_text(self, name) -> TextIO:
        raise NotImplementedError()

    @abc.abstractmethod
    def get_write_stream_text(self, name) -> TextIO:
        raise NotImplementedError()


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

    def get_read_stream(self, name) -> BinaryIO:
        return (self.root / name).open("rb")

    def get_write_stream(self, name) -> BinaryIO:
        return (self.root / name).open("wb")

    def get_read_stream_text(self, name) -> BinaryIO:
        return (self.root / name).open("r")

    def get_write_stream_text(self, name) -> BinaryIO:
        return (self.root / name).open("w")


def get_default_store() -> Store:
    cfg = get_config()
    if "aqueduct" in cfg and "default_store" in cfg["aqueduct"]:
        return hydra.utils.instantiate(cfg["aqueduct"]["default_store"])
    else:
        return None
