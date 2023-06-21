from typing import Optional

from aqueduct.artifact import Artifact

from .abstract_task import AbstractTask
from ..artifact import CompositeArtifact, resolve_artifact_from_spec
from .task import Task
from ..task_tree import _map_tasks_in_tree


class AggregateTask(Task):
    _ALLOW_SAVE = False

    def artifact(self) -> Optional[CompositeArtifact]:
        artifacts = []

        def accumulate_artifacts(t):
            spec = t.artifact()
            if spec is not None:
                artifacts.append(resolve_artifact_from_spec(spec))

            return t

        reqs = self.requirements()

        if reqs is not None:
            _map_tasks_in_tree(reqs, accumulate_artifacts)

        if len(artifacts) > 0:
            return CompositeArtifact(artifacts)
        else:
            return None

    def run(self, reqs):
        return reqs
