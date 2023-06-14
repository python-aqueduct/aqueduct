import dataclasses
from typing import MutableMapping, Type, TYPE_CHECKING

from .artifact import Artifact
from .base import resolve_artifact_from_spec
from .composite import CompositeArtifact
from ..util import resolve_task_tree

if TYPE_CHECKING:
    from ..task import AbstractTask


@dataclasses.dataclass
class ArtifactStatistics:
    count: int = 0
    in_cache: int = 0
    stored_size: int = 0


def add_artifact_to_report(
    artifact: Artifact, report: MutableMapping[Type[Artifact], ArtifactStatistics]
):
    if isinstance(artifact, CompositeArtifact):
        statistics = report.get(type(artifact), ArtifactStatistics())
        statistics.count += 1
        report[type(artifact)] = statistics

        for a in artifact.artifacts:
            add_artifact_to_report(a, report)
    else:
        stats = report.get(type(artifact), ArtifactStatistics())
        stats.count += 1

        if artifact.exists():
            stats.in_cache += 1
            stats.stored_size += artifact.size()

        report[type(artifact)] = stats


def artifact_report(
    task: "AbstractTask",
) -> MutableMapping[Type[Artifact], ArtifactStatistics]:
    """Produce a small report about the artifacts associated with a task and its
    dependencies."""
    artifacts_by_type = {}

    def accumulate_artifacts(task: "AbstractTask", *args, **kwargs):
        spec = task.artifact()

        if spec is not None:
            artifact = resolve_artifact_from_spec(spec)
            add_artifact_to_report(artifact, artifacts_by_type)

    resolve_task_tree(task, accumulate_artifacts, ignore_cache=True)

    return artifacts_by_type
