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
import inspect
import logging
import omegaconf
import os
import pandas as pd
import sys
import xarray as xr

from aqueduct.artifact.composite import CompositeArtifact

from .artifact import Artifact, resolve_artifact_from_spec
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
from .task_tree import _map_tasks_in_tree, reduce_type_in_tree
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

    TaskClass = resolve_task_class(ns.task_name)
    task = TaskClass()

    if ns.tree:
        print_task_tree(task)
        return

    backend = resolve_backend_from_spec(cfg.aqueduct.backend)
    try:
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
    finally:
        backend.close()


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


def resolve_config(config_sources: Iterable[ConfigSource]) -> omegaconf.DictConfig:
    cfgs = []
    for config_source in config_sources:
        cfg = config_source()
        omegaconf.OmegaConf.set_struct(cfg, False)
        cfgs.append(cfg)

    to_return = omegaconf.OmegaConf.unsafe_merge(*cfgs)
    if not isinstance(to_return, omegaconf.DictConfig):
        raise RuntimeError("Root configuration must be a dictionary.")

    return to_return


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


def flatten_artifact(artifact: Artifact) -> list[Artifact]:
    if isinstance(artifact, CompositeArtifact):
        artifacts = []
        for a in artifact.artifacts:
            artifacts.extend(flatten_artifact(a))

        return artifacts
    else:
        return [artifact]


def accumulate_artifacts_of_tree(
    tree,
    artifacts: list[tuple[AbstractTask, Artifact]],
    below: Optional[Type[AbstractTask]] = None,
):
    """Expand a task tree and gather all its artifacts in a list.
    Args:
        tree: The task tree to expand.
        artifacts: The list of artifacts to accumulate.
        below: If specified, only accumulate artifacts of tasks that are of this type or below it in the task tree.

    Returns:
        A list of of tuples (task, artifact)."""

    def accumulate_artifacts_of_task(
        task: AbstractTask,
        artifacts: list[tuple[AbstractTask, Artifact]],
    ):
        if below is None or not isinstance(task, below):
            # Expand requirements of that task because it is not of type below.
            reqs = task._resolve_requirements(ignore_cache=True)
            artifacts = accumulate_artifacts_of_tree(reqs, artifacts, below=below)

        resolved_artifact = resolve_artifact_from_spec(task.artifact())
        if resolved_artifact is not None:
            artifacts.extend([(task, x) for x in flatten_artifact(resolved_artifact)])

        return artifacts

    return reduce_type_in_tree(
        tree, AbstractTask, accumulate_artifacts_of_task, artifacts
    )


def del_cli(ns):
    project_name_to_module_names = resolve_source_modules(ns)

    name2task, name2config_provider = create_task_index(project_name_to_module_names)
    task_class = name2task[ns.root_task]
    task_config_source = name2config_provider.get(ns.root_task, None)
    config_sources = get_config_sources(
        ns.parameters, [], task_class, task_config_source
    )
    cfg = resolve_config(config_sources)

    if ns.cfg:
        print(omegaconf.OmegaConf.to_yaml(cfg, resolve=ns.resolve))
        return

    set_config(cfg)

    TaskClass = resolve_task_class(ns.root_task)
    task = TaskClass()

    BelowClass = resolve_task_class(ns.below) if ns.below is not None else None
    breakpoint()

    artifacts = accumulate_artifacts_of_tree(task, [], below=BelowClass)

    # Group the artifacts by task.
    artifacts_by_task = {}
    for task, artifact in artifacts:
        if task not in artifacts_by_task:
            artifacts_by_task[task] = []

        if artifact.exists():
            artifacts_by_task[task].append(artifact)

    n_artifacts = sum([len(x) for x in artifacts_by_task.values()])

    if n_artifacts == 0:
        print(f"No artifacts found for task {task.task_name()}.")
        return
    else:
        print(f"Will delete {n_artifacts} artifacts:")
        for task in artifacts_by_task:
            print(f"    {task.task_name()} ({len(artifacts_by_task[task])})")

            for artifact in artifacts_by_task[task]:
                print(f"        {artifact.path}")

        confirmation = input("Continue? (Y/n) ")
        if confirmation.lower() != "y":
            return

        for task in artifacts_by_task:
            for artifact in artifacts_by_task[task]:
                print(f"Removing {artifact.path}.")
                os.unlink(artifact.path)


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

    del_parser = subparsers.add_parser("del")
    del_parser.add_argument("root_task", type=str)
    del_parser.add_argument(
        "parameters",
        nargs="*",
        type=str,
        help="Overrides to apply to the task.",
        default=[],
    )
    del_parser.add_argument(
        "--below",
        type=str,
        help="Specify a task name. Only delete artifacts of itself and its children. If not specified, delete all artifacts of the task tree.",
        default=None,
    )
    del_parser.add_argument(
        "--cfg", action="store_true", help="Print config and return."
    )
    del_parser.add_argument(
        "--resolve",
        action="store_true",
        help="Fully resolve the config values before printing.",
    )
    del_parser.set_defaults(func=del_cli)

    ns = parser.parse_args()

    level = "DEBUG" if ns.verbose else "INFO"
    logging.basicConfig(level=level, stream=sys.stdout)

    ns.func(ns)
