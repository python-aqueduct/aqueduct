from typing import BinaryIO, TextIO
import io


from .store import Store


class InMemoryStore(Store):
    def __init__(self):
        self.store = {}

    def exists(self, name) -> bool:
        return name in self.store

    def dump_binary(self, name, object, serializer):
        stream = io.BytesIO()
        serializer(object, stream)
        self.store[name] = stream.getvalue()

    def load_binary(self, name, deserializer):
        body = self.store[name]
        return deserializer(io.BytesIO(body))
