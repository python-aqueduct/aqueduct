import datetime
import os
import pathlib

from typing import BinaryIO


class Store:
    def exists(self, name) -> bool:
        return False

    def get_read_stream(self, name) -> BinaryIO:
        raise NotImplementedError()

    def get_write_stream(self, name) -> BinaryIO:
        raise NotImplementedError()


class LocalFilesystemStore(Store):
    def __init__(self, root=os.getcwd()):
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
