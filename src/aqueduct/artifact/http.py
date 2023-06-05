from typing import Any, BinaryIO, Callable

import requests

from .artifact import Artifact

class HTTPDownloadArtifact(Artifact):
    def deserialize(self, stream: BinaryIO) -> bytes:
        return stream.read()
    
    def serialize(self, url: str, out: BinaryIO):
        r = requests.get(url, stream=True)

        for chunk in r.iter_content(chunk_size=1024):
            out.write(chunk)