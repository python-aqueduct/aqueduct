"""A `Store` is the interface between an :class:`Artifact` and a storage resource. 

So far, only the simple :class:`LocalFilesystemStore` is implemented."""
from typing import TypeAlias

from .inmemory import InMemoryStore
from .local_filesystem import LocalFilesystemStore
from .store import Store

__all__ = ["InMemoryStore", "LocalFilesystemStore"]


import hydra

from ..config import get_config

StoreSpec: TypeAlias = Store | None



def resolve_store_from_spec(spec: StoreSpec) -> Store:
    if isinstance(spec, Store):
        return spec
    else:
        return get_default_store()


def get_default_store() -> Store:
    cfg = get_config()

    if "aqueduct" in cfg and "store" in cfg["aqueduct"]:
        return hydra.utils.instantiate(cfg["aqueduct"]["store"])
    else:
        return LocalFilesystemStore()
