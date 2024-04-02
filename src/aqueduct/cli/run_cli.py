import argparse
import logging
import omegaconf
import pandas as pd
import xarray as xr
import numpy as np
from aqueduct.artifact.artifact import Artifact
from aqueduct.backend import resolve_backend_from_spec
from aqueduct.backend.base import TaskError

from .base import (
    build_task_from_cli_spec,
    downstream_of,
    get_config_sources,
    resolve_config,
    resolve_source_modules,
)
from .tasklang import parse_task_spec
from aqueduct.config import set_config
from aqueduct.task.abstract_task import AbstractTask
from aqueduct.task_tree import _map_tasks_in_tree
from aqueduct.taskresolve import create_task_index


logger = logging.getLogger(__name__)


def print_task_tree(task: AbstractTask, ignore_cache=False):
    def print_one_task(
        task: AbstractTask,
        indent=0,
    ):
        pad = " " * indent
        print(f"{pad}{str(task)}")

        reqs = task._resolve_requirements(ignore_cache=ignore_cache)

        if reqs is not None:
            _map_tasks_in_tree(
                task.requirements(), lambda x: print_one_task(x, indent=indent + 1)
            )

    print_one_task(task)


def run_cli(ns: argparse.Namespace):
    project_name_to_module_names = resolve_source_modules(ns)

    name2task, name2config_provider, task_class2module_name = create_task_index(
        project_name_to_module_names
    )

    root_task = build_task_from_cli_spec(ns.task, name2task, name2config_provider)
    task_class = root_task.__class__
    task_config_source = name2config_provider.get(root_task.task_name(), None)

    config_sources = get_config_sources(
        [], ns.overrides, task_class, task_config_source
    )
    cfg = resolve_config(config_sources)

    if ns.concurrent is not None:
        cfg["aqueduct"]["backend"]["type"] = "concurrent"
        cfg["aqueduct"]["backend"]["n_workers"] = ns.concurrent

    elif ns.dask_url is not None:
        cfg["aqueduct"]["backend"]["type"] = "dask"
        cfg["aqueduct"]["backend"]["address"] = ns.dask_url

    elif ns.dask is not None:
        cfg["aqueduct"]["backend"]["type"] = "dask"
        cfg["aqueduct"]["backend"]["n_workers"] = ns.dask

    elif ns.multiprocessing is not None:
        cfg["aqueduct"]["backend"]["type"] = "multiprocessing"
        cfg["aqueduct"]["backend"]["n_workers"] = ns.multiprocessing

    if ns.cfg:
        print(omegaconf.OmegaConf.to_yaml(cfg, resolve=ns.resolve))
        return

    set_config(cfg)

    if ns.tree:
        print_task_tree(root_task)
        return

    force_tasks = set()
    if ns.force_downstream_of:
        downstream_of_target = downstream_of(
            root_task, name2task[ns.force_downstream_of]
        )
        force_tasks.update(downstream_of_target)

    backend = resolve_backend_from_spec(cfg.aqueduct.backend)
    try:
        logger.info(f"Using backend {backend}.")

        logger.info(f"Running task {root_task.__class__.__qualname__}")

        if ns.force_root:
            force_tasks.add(root_task.__class__)

        result = backend.run(root_task, force_tasks=force_tasks)

        if ns.ipython:
            import IPython

            header = "\n".join(
                [
                    "Available variables",
                    "   result: The value returned by the task.",
                    "   task: The task object.",
                ]
            )
            IPython.embed(header=header)
        else:
            if isinstance(result, (pd.DataFrame, xr.Dataset, Artifact)):
                print(result)
    except TaskError as e:
        logger.exception(e)
    finally:
        backend.close()


def add_run_cli_to_parser(parser: argparse.ArgumentParser):
    parser.add_argument("task", type=str, nargs="+", help="Task specification")
    parser.add_argument(
        "--ipython",
        action="store_true",
        help="Start IPython interpreter after the result has been computed.",
    )
    parser.add_argument(
        "--overrides",
        nargs="*",
        help="Overrides to apply to the global config, i.e. `my_option=1 deep.option=2`. Useful to set the parameters of the task.",
        type=str,
        default=[],
    )
    parser.add_argument(
        "--force-root",
        action="store_true",
        help="Ignore cache for the root task and force it to run.",
    )
    parser.add_argument("--force-downstream-of", type=str, default=None)
    parser.add_argument(
        "--resolve", action="store_true", help="Resolve the config before printing."
    )

    run_parser_diagnostics_group = parser.add_mutually_exclusive_group()
    run_parser_diagnostics_group.add_argument(
        "--cfg",
        action="store_true",
        help="Show the resolved configuration for the task and exit.",
    )
    run_parser_diagnostics_group.add_argument(
        "--tree",
        action="store_true",
        help="Print the task tree that would be executed and exit.",
    )

    backend_group = parser.add_mutually_exclusive_group()
    backend_group.add_argument("--concurrent", type=int, default=None)
    backend_group.add_argument("--dask-url", type=str, default=None)
    backend_group.add_argument("--dask", type=int, default=None)
    backend_group.add_argument("--multiprocessing", type=int, default=None)

    parser.set_defaults(func=run_cli)
