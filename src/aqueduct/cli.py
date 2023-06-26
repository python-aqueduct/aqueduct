from typing import Mapping, Sequence, Type, Optional, Type, Iterable

import argparse
import importlib.metadata
import inspect
import logging
import omegaconf
import pandas as pd
import sys
import xarray as xr

from .artifact import Artifact
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
from .util import tasks_in_module


logger = logging.getLogger(__name__)


def resolve_source_modules(ns: argparse.Namespace) -> Mapping[str, Iterable[str]]:
    if ns.module is not None:
        return {"default": [ns.module]}
    else:
        return get_modules_from_extensions()


def list_tasks(ns: argparse.Namespace, remaining_args: list):
    modules_per_project = resolve_source_modules(ns)
    breakpoint()

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


def run(ns: argparse.Namespace, remaining_args: list):
    project_name_to_module_names = resolve_source_modules(ns)

    breakpoint()
    name2task, name2config_provider = create_task_index(project_name_to_module_names)
    task_class = name2task[ns.task_name]
    config_source = name2config_provider.get(ns.task_name, None)
    cfg = resolve_config(ns.overrides, task_class, config_source)
    set_config(cfg)

    TaskClass = resolve_task_class(ns.task_name)
    task = TaskClass()

    if ns.force_root:
        task.set_force_root(True)

    result = task.result()

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


def resolve_config(
    overrides: Sequence[str],
    task_class: Optional[Type[AbstractTask]] = None,
    task_config_provider: Optional[ConfigSource] = None,
):
    config_sources: list[ConfigSource] = [DefaultAqueductConfigSource()]

    if task_config_provider is not None:
        config_sources.append(task_config_provider)

    if task_class is not None:
        config_sources.append(TaskArgsConfigSource(task_class))

    config_sources.append(DotListConfigSource(overrides))

    cfgs = []
    for config_source in config_sources:
        cfg = config_source()
        omegaconf.OmegaConf.set_struct(cfg, False)
        cfgs.append(cfg)

    return omegaconf.OmegaConf.merge(*cfgs)


def config_cli(ns, remaining_args):
    source_modules = resolve_source_modules(ns)
    name2task, name2config_source = create_task_index(source_modules)

    if ns.show:
        cfg = resolve_config(ns.overrides, task_class=name2task[ns.task])
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
        "overrides",
        nargs="*",
        help="Overrides to apply to the config, i.e. `my_option=1 deep.option=2`. Useful to set the parameters of the task.",
        type=str,
    )
    run_parser.add_argument(
        "--force-root",
        action="store_true",
        help="Ignore cache for the root task and force it to run.",
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
        "--task",
        type=str,
        default=None,
        help="Show the config that would be used if this task was run.",
    )
    config_parser.add_argument(
        "overrides",
        nargs="*",
        help="Overrides to apply to the config, i.e. `my_option=1 deep.option=2`",
        type=str,
    )
    config_parser.set_defaults(func=config_cli)

    ns, remaining_args = parser.parse_known_args()

    level = "DEBUG" if ns.verbose else "INFO"
    logging.basicConfig(level=level, stream=sys.stdout)

    ns.func(ns, remaining_args)
