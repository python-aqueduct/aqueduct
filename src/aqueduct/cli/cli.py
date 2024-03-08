from typing import TypeAlias


import argparse
import inspect
import logging
import omegaconf
import os
import pandas as pd
import sys
import xarray as xr

from aqueduct.artifact.composite import CompositeArtifact
from aqueduct.cli.run_cli import add_run_cli_to_parser

from .base import get_config_sources, resolve_config, resolve_source_modules
from .del_cli import add_del_cli_to_parser, del_cli
from ..artifact import Artifact, resolve_artifact_from_spec
from ..backend import resolve_backend_from_spec
from ..config import set_config
from ..config.aqueduct import DefaultAqueductConfigSource
from ..config.configsource import DotListConfigSource, ConfigSource
from ..config.taskargs import TaskArgsConfigSource
from ..task import AbstractTask
from ..taskresolve import create_task_index
from .tasklang import parse_task_spec
from ..task_tree import _map_tasks_in_tree, reduce_type_in_tree
from ..util import tasks_in_module

OmegaConfig: TypeAlias = omegaconf.DictConfig | omegaconf.ListConfig

logger = logging.getLogger(__name__)


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


def config_cli(ns):
    source_modules = resolve_source_modules(ns)
    name2task, name2config_source, _ = create_task_index(source_modules)

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
    add_run_cli_to_parser(run_parser)

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

    del_parser = subparsers.add_parser("del")
    add_del_cli_to_parser(del_parser)

    ns = parser.parse_args()

    level = "DEBUG" if ns.verbose else "INFO"
    logging.basicConfig(level=level, stream=sys.stdout)

    ns.func(ns)
