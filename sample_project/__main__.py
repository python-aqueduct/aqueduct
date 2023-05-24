import dask
import dask.distributed
import logging
import numpy as np
import pandas as pd

from earthsciencedata.tasks.metar.iem import fetch_one_station

from aqueduct import DaskBackend, Task, LocalFilesystemStore, taskdef
from aqueduct.artifact import Artifact, PickleArtifact, ParquetArtifact


FS_STORE = LocalFilesystemStore(root="./")


@taskdef(artifact=PickleArtifact("saved_np.pkl", FS_STORE))
def produce_np_array():
    return np.random.rand(100, 100)


@taskdef(requirements=produce_np_array())
def add(np_array):
    return np_array + 10.0


@taskdef()
def fetch_station(name, begin, end):
    df = pd.DataFrame(fetch_one_station(name, begin, end))
    return df


class FetchStations(Task):
    def __init__(self, begin, end):
        self.begin = begin
        self.end = end

    def requirements(self):
        return [
            fetch_station(x, self.begin, self.end) for x in ["CYUL", "CYVR", "CYQB"]
        ]

    def run(self, stations):
        return pd.concat(stations)

    def artifact(self) -> Artifact:
        return ParquetArtifact("observations.parquet", store=FS_STORE)


if __name__ == "__main__":
    logging.basicConfig(level="INFO")

    client = dask.distributed.Client()
    print(client.dashboard_link)

    backend = DaskBackend(client)

    dep = produce_np_array()

    print("Before A")
    a = add()
    response = backend.run(a)
    print(response)

    task = FetchStations(pd.to_datetime("2021-01-01"), pd.to_datetime("2021-02-01"))
    response = backend.run(task())
    print(response)

    client.close()
