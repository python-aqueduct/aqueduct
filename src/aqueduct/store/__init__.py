"""A `Store` is the interface between an :class:`Artifact` and a storage resource. 

So far, only the simple :class:`LocalFilesystemStore` is implemented."""


from .inmemory import InMemoryStore
from .local_filesystem import LocalFilesystemStore
from .store import Store

__all__ = ["InMemoryStore", "LocalFilesystemStore"]


import abc
import hydra
from typing import BinaryIO, TextIO

from ..config import get_config


def get_default_store() -> Store:
    cfg = get_config()
    if "aqueduct" in cfg and "store" in cfg["aqueduct"]:
        return hydra.utils.instantiate(cfg["aqueduct"]["store"])
    else:
        return LocalFilesystemStore()
