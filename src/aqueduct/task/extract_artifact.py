from .abstract_task import AbstractTask
from .task import Task
from ..artifact import resolve_artifact_from_spec


class ExtractArtifact(Task):
    def __init__(self, inner: AbstractTask):
        self.inner = inner

    def requirements(self):
        return self.inner

    def run(self, req):
        return resolve_artifact_from_spec(self.inner.artifact())

    def artifact(self):
        return None

    def _unique_key(self) -> str:
        return "ExtractArtifact-" + self.inner._unique_key()


def as_artifact(task: AbstractTask) -> ExtractArtifact:
    return ExtractArtifact(task)
