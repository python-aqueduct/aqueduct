from typing import Optional

from .abstract_task import AbstractTask
from ..artifact import CompositeArtifact
from .task import Task
from ..util import map_task_tree


class AggregateTask(Task):
    _ALLOW_SAVE = False

    def artifact(self) -> Optional[CompositeArtifact]:
        artifacts = []

        def accumulate_artifacts(t):
            artifacts.append(t._resolve_artifact())
            return t

        reqs = self.requirements()
        map_task_tree(reqs, accumulate_artifacts)

        artifacts_without_none = [x for x in artifacts if x is not None]

        if len(artifacts_without_none) > 0:
            return CompositeArtifact(artifacts)
        else:
            return None

    def run(self, reqs):
        return reqs
