import logging
import os
import random

import dask
import dask.distributed
import hydra
import numpy as np
import pandas as pd
from earthsciencedata.tasks.metar.iem import fetch_one_station
import importlib.resources

from aqueduct import DaskBackend, ImmediateBackend, LocalFilesystemStore, Task, task
from aqueduct.artifact import Artifact, ParquetArtifact, PickleArtifact

FS_STORE = LocalFilesystemStore(root="./")


@task(artifact=PickleArtifact("saved_np.pkl", FS_STORE))
def produce_np_array():
    return np.random.rand(100, 100)


@task(requirements=produce_np_array())
def add(np_array):
    return np_array + 10.0


@task
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


@hydra.main(config_path="conf", config_name="sample", version_base="1.3")
def cli(cfg):
    logging.basicConfig(level="INFO")

    print(os.getcwd())

    cluster = dask.distributed.LocalCluster(n_workers=4, threads_per_worker=4)
    client = dask.distributed.Client(cluster)
    print(client.dashboard_link)

    backend = DaskBackend(client)

    immediate_backend = ImmediateBackend()

    stn_file_name = importlib.resources.files(__package__).joinpath(
        "robust2023_stations.csv"
    )
    station_df = pd.read_csv(stn_file_name)

    task = FetchStations(
        pd.to_datetime("2021-01-01"),
        pd.to_datetime("2021-02-01"),
        station_list=list(station_df["station"])[:4],
    )

    task_binding = task()
    response = immediate_backend.run(task_binding)

    print(response)

    client.close()


if __name__ == "__main__":
    cli()
