from typing import TypeAlias

import datetime
import pathlib

from .artifact import Artifact
from ..config import get_aqueduct_config

PathSpec: TypeAlias = pathlib.Path | str


class LocalFilesystemArtifact(Artifact):
    """Define artifacts living on a local filesystem."""

    def __init__(self, path: PathSpec):
        self.path = pathlib.Path(path)

    def exists(self) -> bool:
        return self.path.is_file()

    def last_modified(self):
        return datetime.datetime.fromtimestamp(self.path.stat().st_mtime)

    def __repr__(self):
        return f"LocalFilesystemArtifact({self.path})"


class LocalStoreArtifact(LocalFilesystemArtifact):
    """Very similar to :class:`LocalFilesystemArtifact`. If the provided path is
    relative, append it to the local store, as specified by the `artifact.local_store`
    configuration option. If that option is not specified, behave exactly as
    :class:`LocalFilesystemArtifact`."""

    def __init__(self, path: PathSpec):
        self.original_path = path
        path = pathlib.Path(path)

        if not path.is_absolute():
            cfg = get_aqueduct_config()
            local_store = cfg.get("local_store", "./")
            path = local_store / path
        else:
            path = path

        super().__init__(path)

    def __repr__(self):
        return f"LocalStoreArtifact('{self.original_path}')"
