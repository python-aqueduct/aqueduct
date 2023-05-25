import dask
import dask.distributed
import logging
import numpy as np
import pandas as pd
import random

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
    if random.random() < 0.1:
        raise RuntimeError("Task failed.")

    df = pd.DataFrame(fetch_one_station(name, begin, end))
    return df


class FetchStations(Task):
    def __init__(self, begin, end, station_list):
        self.begin = begin
        self.end = end
        self.station_list = station_list

    def requirements(self):
        return [fetch_station(x, self.begin, self.end) for x in self.station_list]

    def run(self, stations):
        return pd.concat(stations)

    def artifact(self) -> Artifact:
        return ParquetArtifact("observations.parquet", store=FS_STORE)


if __name__ == "__main__":
    logging.basicConfig(level="INFO")

    cluster = dask.distributed.LocalCluster(n_workers=4, threads_per_worker=4)
    client = dask.distributed.Client(cluster)
    print(client.dashboard_link)

    backend = DaskBackend(client)

    # dep = produce_np_array()

    # print("Before A")
    # a = add()
    # response = backend.run(a)
    # print(response)

    station_df = pd.read_csv("robust2023_stations.csv")

    task = FetchStations(
        pd.to_datetime("2021-01-01"),
        pd.to_datetime("2021-02-01"),
        station_list=station_df["station"],
    )

    task_binding = task()
    response = backend.run(task_binding)

    print(response)

    client.close()
