from typing import BinaryIO

import xarray as xr

from .artifact import Artifact

class XarrayArtifact(Artifact):
    """Store objects using `pickle`."""

    def deserialize(self, stream: BinaryIO) -> xr.Dataset:
        return xr.load_dataset(stream)

    def serialize(self, dataset: xr.Dataset, stream: BinaryIO):
        return dataset.to_netcdf(stream)