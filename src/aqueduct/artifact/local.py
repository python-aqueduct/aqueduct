from typing import TypeAlias

import datetime
import pathlib

from .artifact import Artifact
from ..config import get_aqueduct_config

PathSpec: TypeAlias = pathlib.Path | str


class LocalFilesystemArtifact(Artifact):
    def __init__(self, path: PathSpec):
        self.path = pathlib.Path(path)

    def exists(self) -> bool:
        return self.path.is_file()

    def last_modified(self):
        return datetime.datetime.fromtimestamp(self.path.stat().st_mtime)


class LocalStoreArtifact(LocalFilesystemArtifact):
    def __init__(self, path: PathSpec):
        path = pathlib.Path(path)

        if not path.is_absolute():
            cfg = get_aqueduct_config()
            local_store = cfg.get("local_store", "./")
            path = local_store / path
        else:
            path = path

        super().__init__(path)
