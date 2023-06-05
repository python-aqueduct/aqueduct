from typing import BinaryIO

import pandas as pd

from .artifact import Artifact

class DataFrameArtifact(Artifact):
    """Store :class:`pandas.DataFrame` objects to the Parquet format using `pandas`."""

    def deserialize(self, stream: BinaryIO) -> pd.DataFrame:
        return pd.read_parquet(stream)

    def serialize(self, df: pd.DataFrame, stream: BinaryIO):
        return df.to_parquet(stream)
