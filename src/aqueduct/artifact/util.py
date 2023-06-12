from typing import Mapping, Type, TYPE_CHECKING

from .artifact import Artifact
from .composite import CompositeArtifact
from ..util import map_task_tree, convert_size

if TYPE_CHECKING:
    from ..task import AbstractTask


def add_artifact_to_report(
    artifact: Artifact, report: Mapping[Type[Artifact], tuple[int, int]]
):
    if isinstance(artifact, CompositeArtifact):
        count, _ = report.get(type(artifact), (0, 0))
        new_stats = (count + 1, 0)
        report[type(artifact)] = new_stats

        for a in artifact.artifacts:
            add_artifact_to_report(a, report)
    else:
        count, size = report.get(type(artifact), (0, 0))
        new_stats = (count + 1, size + artifact.size())
        report[type(artifact)] = new_stats


def artifact_report(task: "AbstractTask") -> Mapping[Type[Artifact], tuple[int, str]]:
    """Produce a small report about the artifacts associated with a task and its
    dependencies."""
    artifact_statistics = {}

    def mapper(task: "AbstractTask"):
        artifact = task._resolve_artifact()

        if artifact is not None and artifact.exists():
            add_artifact_to_report(artifact, artifact_statistics)

    map_task_tree(task, mapper)

    human_readable = {}
    for k in artifact_statistics:
        count, size = artifact_statistics[k]
        human_size = convert_size(size)

        human_readable[k] = (count, human_size)

    return human_readable
