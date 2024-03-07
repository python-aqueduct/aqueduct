import logging
import requests
import tqdm
import pathlib

from ..artifact import LocalStoreArtifact
from ..task import Task

logger = logging.getLogger(__name__)


class DownloadFile(Task):
    CHUNK_SIZE = 10 * 1024

    def __init__(self, url, target):
        self.url = url
        self.target = pathlib.Path(target)

    def run(self):
        head_response = requests.head(self.url)

        size = int(head_response.headers["content-length"])
        n_chunks = size // self.CHUNK_SIZE

        response = requests.get(self.url, stream=True)

        if self.target.is_file():
            logger.info("File already exists. Skipping.")
            return

        self.target.parent.mkdir(parents=True, exist_ok=True)

        with self.target.open("wb") as f:
            for b in tqdm.tqdm(
                response.iter_content(chunk_size=self.CHUNK_SIZE), total=n_chunks
            ):
                f.write(b)

    def artifact(self):
        return LocalStoreArtifact(self.target)
