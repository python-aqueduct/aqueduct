from typing import TypeAlias


import argparse
import logging
import omegaconf
import sys

from aqueduct.cli.ls_cli import add_ls_cli_to_parser
from aqueduct.cli.run_cli import add_run_cli_to_parser

from .base import get_config_sources, resolve_config, resolve_source_modules
from .del_cli import add_del_cli_to_parser
from .artifact_cli import add_artifact_cli_to_parser
from ..taskresolve import create_task_index

OmegaConfig: TypeAlias = omegaconf.DictConfig | omegaconf.ListConfig

logger = logging.getLogger(__name__)


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
            for line in lines:
                print(f"  {line}")

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

    ls_parser = subparsers.add_parser("ls", help="List tasks.")
    add_ls_cli_to_parser(ls_parser)

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

    artifact_parser = subparsers.add_parser("artifact")
    add_artifact_cli_to_parser(artifact_parser)

    ns = parser.parse_args()

    level = "DEBUG" if ns.verbose else "INFO"
    logging.basicConfig(level=level, stream=sys.stdout)

    ns.func(ns)
