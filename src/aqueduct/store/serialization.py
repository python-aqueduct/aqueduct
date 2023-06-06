from typing import TypeVar, Generic

import cfgrib
import pathlib
import xarray as xr

T = TypeVar('T')

class Serializer:
    pass

class Deserializer:
    pass

class BytesSerializer:
    pass

class PathSerializer(Serializer, Generic[T]):
    def __call__(self, path: pathlib.Path, object: T):
        raise NotImplementedError("PathSerializer must implement __call__")
    
class PathDeserializer(Deserializer, Generic[T]):
    def __call__(self, path: pathlib.Path) -> T:
        raise NotImplementedError("PathDeserializer must implement __call__")

class GribDeserializer(PathSerializer):
    def __call__(self, path: pathlib.Path) -> xr.Dataset:
        return cfgrib.open_dataset(path)