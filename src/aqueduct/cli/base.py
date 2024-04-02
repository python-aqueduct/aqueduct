from typing import Generic, Mapping, Iterable, Sequence, Optional, Type, TypeVar

import argparse
import omegaconf
import sys

from aqueduct.cli.tasklang import parse_task_spec
from aqueduct.config import set_config

from ..artifact import Artifact, CompositeArtifact, resolve_artifact_from_spec
from ..config.configsource import ConfigSource, DotListConfigSource
from ..config.aqueduct import DefaultAqueductConfigSource
from ..task import AbstractTask
from ..taskresolve import get_modules_from_extensions
from ..task_tree import reduce_type_in_tree


def resolve_source_modules(ns: argparse.Namespace) -> Mapping[str, Iterable[str]]:
    if ns.module is not None:
        sys.path.insert(0, "")
        return {"default": [ns.module]}
    else:
        return get_modules_from_extensions()


def get_config_sources(
    parameters: Sequence[str],
    overrides: Sequence[str],
    task_class: Optional[Type[AbstractTask]] = None,
    task_config_provider: Optional[ConfigSource] = None,
) -> list[ConfigSource]:
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


def build_task_from_cli_spec(
    spec: list[str],
    name_to_task: Mapping[str, type[AbstractTask]],
    name_to_config: Mapping[str, ConfigSource],
) -> AbstractTask:
    if len(spec) >= 1 and spec[0] in name_to_task:
        task_name, *task_args = spec
        TaskClass = name_to_task[task_name]

        task_config_source = name_to_config.get(task_name, None)
        config_sources = get_config_sources(
            task_args, [], TaskClass, task_config_source
        )
        cfg = resolve_config(config_sources)
        set_config(cfg)
        root_task = TaskClass()

    else:
        root_task = parse_task_spec(spec[0], name_to_task)

    return root_task


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
    max_depth: Optional[int] = None,
):
    """Expand a task tree and gather all its artifacts in a list.
    Args:
        tree: The task tree to expand.
        artifacts: The list of artifacts to accumulate.
        below: If specified, only accumulate artifacts of tasks that are of this type or below it in the task tree.

    Returns:
        A list of of tuples (task, artifact)."""

    max_depth_closure = max_depth

    def accumulate_artifacts_of_task(
        task: AbstractTask,
        artifacts: list[tuple[AbstractTask, Artifact]],
    ):

        if max_depth_closure is not None:
            max_depth = max_depth_closure - 1
        else:
            max_depth = None

        if (below is None or not isinstance(task, below)) and (
            max_depth is None or max_depth >= 0
        ):
            # Expand requirements of that task because it is not of type below.
            reqs = task._resolve_requirements(ignore_cache=True)

            artifacts = accumulate_artifacts_of_tree(
                reqs, artifacts, below=below, max_depth=max_depth
            )

        resolved_artifact = resolve_artifact_from_spec(task.artifact())
        if resolved_artifact is not None:
            artifacts.extend([(task, x) for x in flatten_artifact(resolved_artifact)])

        return artifacts

    return reduce_type_in_tree(
        tree, AbstractTask, accumulate_artifacts_of_task, artifacts
    )


_T = TypeVar("_T")


class TaskTreeVisitor(Generic[_T]):
    def on_list(self, work: list, acc: _T):
        return acc

    def on_tuple(self, work: tuple, acc: _T):
        return acc

    def on_dict(self, work: dict, acc: _T):
        return acc

    def on_task(self, task: AbstractTask, acc: _T):
        return acc

    def visit(
        self,
        work,
        acc: _T,
        max_depth: Optional[int] = None,
        min_depth: Optional[int] = None,
    ) -> _T:
        if isinstance(work, list):
            acc = self.on_list(work, acc)

            for item in work:
                self.visit(item, acc, max_depth=max_depth, min_depth=min_depth)
        elif isinstance(work, tuple):
            acc = self.on_tuple(work, acc)

            for item in work:
                self.visit(item, acc, max_depth=max_depth, min_depth=min_depth)
        elif isinstance(work, dict):
            acc = self.on_dict(work, acc)

            for item in work:
                self.visit(work[item], acc, max_depth=max_depth, min_depth=min_depth)
        elif isinstance(work, AbstractTask):
            if min_depth is None or min_depth <= 0:
                acc = self.on_task(work, acc)

            new_min_depth = min_depth - 1 if min_depth else None

            if max_depth is None or max_depth > 0:
                new_max_depth = max_depth - 1 if max_depth else None

                reqs = work._resolve_requirements(ignore_cache=True)
                acc = self.visit(
                    reqs, acc, max_depth=new_max_depth, min_depth=new_min_depth
                )

        return acc


class PeelParentTaskClasses(TaskTreeVisitor[list[AbstractTask]]):
    def on_task(self, task, acc):
        acc.append(task)
        return acc


def peel_parents(task):
    visitor = PeelParentTaskClasses()
    return visitor.visit(task, [], max_depth=1, min_depth=1)


class BuildParentsDict(
    TaskTreeVisitor[dict[Type[AbstractTask], set[Type[AbstractTask]]]]
):
    def on_task(self, task, acc):
        parents = peel_parents(task)

        for parent in parents:
            children_set = acc.get(parent.__class__, set())
            children_set.add(task.__class__)
            acc[parent.__class__] = children_set
        return acc


def build_parents_dict(
    task: AbstractTask,
) -> dict[Type[AbstractTask], set[Type[AbstractTask]]]:
    visitor = BuildParentsDict()

    return visitor.visit(task, {})


def downstream_of(root_task, target_task: Type[AbstractTask]) -> set[AbstractTask]:
    parents_dict = build_parents_dict(root_task)

    merged_downstream = set()
    for k in parents_dict:
        if issubclass(k, target_task):
            merged_downstream.add(k)
            merged_downstream.update(parents_dict[k])

    return merged_downstream
