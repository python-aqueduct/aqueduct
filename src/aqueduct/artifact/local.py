from typing import TypeAlias, BinaryIO, TextIO, Callable, TypeVar

import datetime
import pathlib

from .artifact import StreamArtifact, TextStreamArtifact
from ..config import get_aqueduct_config

_T = TypeVar("_T")
PathSpec: TypeAlias = pathlib.Path | str


def write_str(o: str, stream: TextIO):
    stream.write(o)


def read_str(stream: TextIO) -> str:
    return stream.read()


class LocalFilesystemArtifact(TextStreamArtifact, StreamArtifact):
    """Define artifacts living on a local filesystem."""

    def __init__(self, path: PathSpec):
        self.path = pathlib.Path(path)

    def exists(self) -> bool:
        return self.path.is_file()

    def last_modified(self):
        return datetime.datetime.fromtimestamp(self.path.stat().st_mtime)

    def __repr__(self):
        return f"LocalFilesystemArtifact({self.path})"

    def size(self) -> int:
        return self.path.stat().st_size

    def load(self, reader: Callable[[BinaryIO], _T]) -> _T:
        with self.path.open("rb") as f:
            return reader(f)

    def dump(self, object: _T, writer: Callable[[_T, BinaryIO], None]):
        with self.path.open("wb") as f:
            writer(object, f)

    def load_text(self, reader: Callable[[TextIO], _T] = read_str) -> _T:
        with self.path.open("r") as f:
            return reader(f)

    def dump_text(self, object: _T, writer: Callable[[_T, TextIO], None] = write_str):
        with self.path.open("w") as f:
            writer(object, f)


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
