from typing import TypeAlias

import datetime
import pathlib

from .artifact import Artifact

PathSpec: TypeAlias = pathlib.Path | str


class LocalFilesystemArtifact(Artifact):
    def __init__(self, path: PathSpec):
        self.path = pathlib.Path(path)

    def exists(self) -> bool:
        return self.path.is_file()

    def last_modified(self):
        return datetime.datetime.fromtimestamp(self.path.stat().st_mtime)
