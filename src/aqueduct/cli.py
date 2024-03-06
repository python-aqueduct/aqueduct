from typing import (
    Callable,
    Mapping,
    Sequence,
    Type,
    Optional,
    Type,
    Iterable,
    TypeAlias,
)

import argparse
import importlib.metadata
import inspect
import logging
import omegaconf
import pandas as pd
import sys
import xarray as xr

from .artifact import Artifact
from .backend import resolve_backend_from_spec
from .config import set_config
from .config.aqueduct import DefaultAqueductConfigSource
from .config.configsource import DotListConfigSource, ConfigSource
from .config.taskargs import TaskArgsConfigSource
from .task import AbstractTask
from .taskresolve import (
    get_modules_from_extensions,
    resolve_task_class,
    create_task_index,
)
from .task_tree import _map_tasks_in_tree
from .util import tasks_in_module

OmegaConfig: TypeAlias = omegaconf.DictConfig | omegaconf.ListConfig

logger = logging.getLogger(__name__)


def resolve_source_modules(ns: argparse.Namespace) -> Mapping[str, Iterable[str]]:
    if ns.module is not None:
        return {"default": [ns.module]}
    else:
        return get_modules_from_extensions()


def list_tasks(ns: argparse.Namespace):
    modules_per_project = resolve_source_modules(ns)

    for p in modules_per_project:
        print(p)

        for m in modules_per_project[p]:
            print(f"    {m}")

            tasks = tasks_in_module(m)

            for task in tasks:
                task_string = task.__qualname__

                if ns.signature:
                    task_string += str(inspect.signature(task))

                print(f"        {task_string}")


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


def run(ns: argparse.Namespace):
    project_name_to_module_names = resolve_source_modules(ns)

    name2task, name2config_provider = create_task_index(project_name_to_module_names)
    task_class = name2task[ns.task_name]
    task_config_source = name2config_provider.get(ns.task_name, None)
    config_sources = get_config_sources(
        ns.parameters, ns.overrides, task_class, task_config_source
    )
    cfg = resolve_config(config_sources)

    if ns.concurrent is not None:
        cfg["aqueduct"]["backend"]["type"] = "concurrent"
        cfg["aqueduct"]["backend"]["n_workers"] = ns.concurrent

    elif ns.dask_url is not None:
        cfg["aqueduct"]["backend"]["type"] = "dask_graph"
        cfg["aqueduct"]["backend"]["address"] = ns.dask_url

    elif ns.dask is not None:
        cfg["aqueduct"]["backend"]["type"] = "dask_graph"
        cfg["aqueduct"]["backend"]["n_workers"] = ns.dask
    elif ns.multiprocessing is not None:
        cfg["aqueduct"]["backend"]["type"] = "multiprocessing"
        cfg["aqueduct"]["backend"]["n_workers"] = ns.multiprocessing

    if ns.cfg:
        print(omegaconf.OmegaConf.to_yaml(cfg, resolve=ns.resolve))
        return

    set_config(cfg)

    TaskClass = resolve_task_class(ns.task_name)
    task = TaskClass()

    if ns.tree:
        print_task_tree(task)
        return

    backend = resolve_backend_from_spec(cfg.aqueduct.backend)
    logger.info(f"Using backend {backend}.")

    logger.info(f"Running task {task.__class__.__qualname__}")

    if ns.force_root:
        task.set_force_root(True)

    result = backend.run(task)

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


def get_config_sources(
    parameters: Sequence[str],
    overrides: Sequence[str],
    task_class: Optional[Type[AbstractTask]] = None,
    task_config_provider: Optional[ConfigSource] = None,
):
    config_sources: list[ConfigSource] = [DefaultAqueductConfigSource()]

    if task_config_provider is not None:
        config_sources.append(task_config_provider)

    config_sources.append(DotListConfigSource(overrides))

    if task_class is not None:
        config_sources.append(
            DotListConfigSource(parameters, section=task_class._fully_qualified_name())
        )

    return config_sources


def resolve_config(config_sources: Iterable[ConfigSource]):
    cfgs = []
    for config_source in config_sources:
        cfg = config_source()
        omegaconf.OmegaConf.set_struct(cfg, False)
        cfgs.append(cfg)

    return omegaconf.OmegaConf.unsafe_merge(*cfgs)


def config_cli(ns):
    source_modules = resolve_source_modules(ns)
    name2task, name2config_source = create_task_index(source_modules)

    config_sources = get_config_sources(
        ns.parameters,
        ns.overrides,
        name2task.get(ns.task, None),
        name2config_source.get(ns.task, None),
    )

    if ns.sources:
        for source in config_sources:
            print(source)
            print()
            yaml_str = omegaconf.OmegaConf.to_yaml(source(), resolve=ns.resolve)
            lines = yaml_str.splitlines()
            for l in lines:
                print(f"  {l}")

            print()

    if ns.show:
        cfg = resolve_config(config_sources)
        print(omegaconf.OmegaConf.to_yaml(cfg, resolve=ns.resolve))


def cli():
    parser = argparse.ArgumentParser(prog="aq", description="Aqueduct CLI")
    parser.add_argument("--verbose", action="store_true", help="Log debug output.")
    parser.add_argument(
        "--module",
        type=str,
        help=(
            "Full path of module where to get the Task from. If left unspecified, "
            "available modules are fetched globally from extensions."
        ),
        default=None,
    )
    parser.set_defaults(func=lambda ns: parser.print_usage())

    subparsers = parser.add_subparsers(title="Actions", dest="action")

    run_parser = subparsers.add_parser("run")
    run_parser.add_argument("task_name", type=str)
    run_parser.add_argument(
        "--ipython",
        action="store_true",
        help="Start IPython interpreter after the result has been computed.",
    )
    run_parser.add_argument(
        "parameters",
        nargs="*",
        type=str,
        help="Overrides to apply to the task.",
        default=[],
    )
    run_parser.add_argument(
        "--overrides",
        nargs="*",
        help="Overrides to apply to the global config, i.e. `my_option=1 deep.option=2`. Useful to set the parameters of the task.",
        type=str,
        default=[],
    )
    run_parser.add_argument(
        "--force-root",
        action="store_true",
        help="Ignore cache for the root task and force it to run.",
    )
    run_parser.add_argument(
        "--resolve", action="store_true", help="Resolve the config before printing."
    )

    backend_group = run_parser.add_mutually_exclusive_group()
    backend_group.add_argument("--concurrent", type=int, default=None)
    backend_group.add_argument("--dask-url", type=str, default=None)
    backend_group.add_argument("--dask", type=int, default=None)
    backend_group.add_argument("--multiprocessing", type=int, default=None)

    run_parser_diagnostics_group = run_parser.add_mutually_exclusive_group()
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

    run_parser.set_defaults(func=run)

    list_parser = subparsers.add_parser("list", aliases=["ls"])
    list_parser.add_argument("--signature", action="store_true")
    list_parser.set_defaults(func=list_tasks)

    config_parser = subparsers.add_parser("config", aliases=["cfg"])
    config_parser.add_argument("--show", action="store_true")
    config_parser.add_argument(
        "--resolve",
        action="store_true",
        help="Resolve interpolations before printing the configuration.",
    )
    config_parser.add_argument(
        "--sources",
        action="store_true",
        help="Show the contribution of each config source.",
    )
    config_parser.add_argument(
        "--task",
        type=str,
        default=None,
        help="Show the config that would be used if this task was run.",
    )
    config_parser.add_argument(
        "parameters",
        nargs="*",
        help="Overrides to apply to the task parameters, i.e. `my_param=1`",
        type=str,
        default=[],
    )
    config_parser.add_argument(
        "--overrides",
        nargs="*",
        help="Overrides to apply to the global configuration, i.e. `my_option=1 deep.option=2`",
        type=str,
        default=[],
    )
    config_parser.set_defaults(func=config_cli)

    ns = parser.parse_args()

    level = "DEBUG" if ns.verbose else "INFO"
    logging.basicConfig(level=level, stream=sys.stdout)

    ns.func(ns)
