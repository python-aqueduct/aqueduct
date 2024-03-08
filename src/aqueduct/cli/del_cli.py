from typing import Optional, Type

import argparse
import omegaconf
import os

from ..artifact import Artifact, resolve_artifact_from_spec
from ..artifact.composite import CompositeArtifact
from ..config import set_config
from ..task import AbstractTask
from ..task_tree import reduce_type_in_tree
from ..taskresolve import create_task_index, resolve_task_class
from .base import build_task_from_cli_spec, get_config_sources, resolve_config, resolve_source_modules
from .tasklang import parse_task_spec

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

    name2task, name2config_provider, task_class2module_name = create_task_index(
        project_name_to_module_names
    )

    root_task = build_task_from_cli_spec(ns.task, name2task, name2config_provider)

    BelowClass = resolve_task_class(ns.below) if ns.below is not None else None

    artifacts = accumulate_artifacts_of_tree(root_task, [], below=BelowClass)

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


def add_del_cli_to_parser(parser: argparse.ArgumentParser):
    parser.add_argument("task", type=str, nargs='+', help="The task at the root of the dependency tree. Accepts task names of Python expressions.")
    parser.add_argument(
        "--below",
        type=str,
        help="Specify a task name. Only delete artifacts of itself and its children. If not specified, delete all artifacts of the task tree.",
        default=None,
    )
    parser.add_argument(
        "--cfg", action="store_true", help="Print config and return."
    )
    parser.add_argument(
        "--resolve",
        action="store_true",
        help="Fully resolve the config values before printing.",
    )
    parser.set_defaults(func=del_cli)