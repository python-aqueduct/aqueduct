import io
import pandas as pd
import unittest
import unittest.mock

from aqueduct.artifact import ParquetArtifact
from aqueduct.store import Store


class TestParquetArtifact(unittest.TestCase):
    def setUp(self) -> None:
        self.name = "test_artifact.parquet"
        self.store: Store = unittest.mock.Mock()
        self.artifact = ParquetArtifact(self.name, store=self.store)

    def test_dump(self):
        stream = io.BytesIO()
        df = pd.DataFrame()
        self.artifact.dump(df, stream)
