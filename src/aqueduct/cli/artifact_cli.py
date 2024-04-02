import argparse

from ..artifact import LocalFilesystemArtifact
from .base import (
    resolve_source_modules,
    build_task_from_cli_spec,
    accumulate_artifacts_of_tree,
)
from ..taskresolve import create_task_index


def artifact_ls_cli(ns: argparse.Namespace):
    project_name_to_module_names = resolve_source_modules(ns)

    name2task, name2config_provider, task_class2module_name = create_task_index(
        project_name_to_module_names
    )

    root_task = build_task_from_cli_spec(ns.task, name2task, name2config_provider)

    artifacts = accumulate_artifacts_of_tree(root_task, [], max_depth=ns.max_depth)

    fs_artifacts = set(
        [
            artifact.path
            for (module, artifact) in artifacts
            if isinstance(artifact, LocalFilesystemArtifact)
        ]
    )

    for artifact_path in sorted(list(fs_artifacts)):
        print(artifact_path)


def add_artifact_cli_to_parser(parser: argparse.ArgumentParser):
    subparsers = parser.add_subparsers(title="artifact")
    artifact_ls_parser = subparsers.add_parser("ls", help="List artifacts.")

    artifact_ls_parser.add_argument(
        "task",
        type=str,
        nargs="+",
        help="The task at the root of the dependency tree. Accepts task names of Python expressions.",
    )
    artifact_ls_parser.add_argument(
        "--below",
        type=str,
        help="Specify a task name. Only delete artifacts of itself and its children. If not specified, delete all artifacts of the task tree.",
        default=None,
    )
    artifact_ls_parser.add_argument(
        "--max-depth", type=int, default=None, help="Maximum tree depth to explore."
    )

    artifact_ls_parser.set_defaults(func=artifact_ls_cli)
