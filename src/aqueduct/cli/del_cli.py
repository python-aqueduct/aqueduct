import argparse
import os
import re

from ..taskresolve import create_task_index, resolve_task_class
from .base import (
    build_task_from_cli_spec,
    resolve_source_modules,
    accumulate_artifacts_of_tree,
)


def del_cli(ns):
    project_name_to_module_names = resolve_source_modules(ns)

    name2task, name2config_provider, task_class2module_name = create_task_index(
        project_name_to_module_names
    )

    root_task = build_task_from_cli_spec(ns.task, name2task, name2config_provider)

    BelowClass = resolve_task_class(ns.below) if ns.below is not None else None

    artifacts = accumulate_artifacts_of_tree(
        root_task, [], below=BelowClass, max_depth=ns.max_depth
    )

    if ns.re is not None:
        regex = re.compile(ns.re)
        filtered_artifacts = []
        for task, artifact in artifacts:
            if regex.match(task.ui_name()):
                filtered_artifacts.append((task, artifact))

        artifacts = filtered_artifacts

    # Group the artifacts by task key.
    used_unique_keys = set()
    artifacts_by_task_name = {}
    for task, artifact in artifacts:
        if task.ui_name() not in artifacts_by_task_name:
            artifacts_by_task_name[task.ui_name()] = set()

        if artifact.exists():
            artifacts_by_task_name[task.ui_name()].add(artifact.path)

    n_artifacts = sum([len(x) for x in artifacts_by_task_name.values()])

    if n_artifacts == 0:
        print(f"No artifacts found for task {task.ui_name()}.")
        return
    else:
        print(f"Will delete {n_artifacts} artifacts:")
        for task_name in sorted(artifacts_by_task_name):
            artifacts = artifacts_by_task_name[task_name]

            if len(artifacts) == 0:
                continue
            else:
                print(f"    {task_name} ({len(artifacts)})")

                for artifact in sorted(list(artifacts)):
                    print(f"        {artifact}")

        confirmation = input("Continue? (Y/n) ")
        if confirmation.lower() != "y":
            return

        for task_name, artifacts in artifacts_by_task_name.items():
            for artifact in artifacts:
                print(f"Removing {artifact}.")
                os.unlink(artifact)


def add_del_cli_to_parser(parser: argparse.ArgumentParser):
    parser.add_argument(
        "task",
        type=str,
        nargs="+",
        help="The task at the root of the dependency tree. Accepts task names of Python expressions.",
    )
    parser.add_argument(
        "--below",
        type=str,
        help="Specify a task name. Only delete artifacts of itself and its children. If not specified, delete all artifacts of the task tree.",
        default=None,
    )
    parser.add_argument("--cfg", action="store_true", help="Print config and return.")
    parser.add_argument(
        "--resolve",
        action="store_true",
        help="Fully resolve the config values before printing.",
    )
    parser.add_argument(
        "--max-depth", type=int, default=None, help="Maximum tree depth to explore."
    )
    parser.add_argument(
        "--re", type=str, default=None, help="Regular expression to filter tasks."
    )
    parser.set_defaults(func=del_cli)
