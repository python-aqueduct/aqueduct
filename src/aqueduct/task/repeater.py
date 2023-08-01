from typing import Iterable, Mapping, Type, TypeVar, Generic, Sequence

import itertools

from .abstract_task import AbstractTask, ArtifactTaskWrapper
from ..artifact import CompositeArtifact, resolve_artifact_from_spec
from .task import Task
from .inline import inline, InlineTaskWrapper


_Task = TypeVar("_Task", bound=AbstractTask)


class RepeaterTask(Task, Generic[_Task]):
    def __init__(
        self,
        repeated: Type[_Task],
        iterators: Mapping[str, Iterable],
        *args,
        as_artifact: bool = False,
        inline: bool = False,
        **kwargs,
    ):
        self.repeated = repeated
        self.iterators = iterators
        self.args = args
        self.kwargs = kwargs
        self._reqs_as_artifacts = as_artifact
        self._inline = inline

        key_intersection = set(self.kwargs.keys()).intersection(
            set(self.iterators.keys())
        )
        if len(key_intersection) != 0:
            raise KeyError(
                f"Key {key_intersection.pop()} is assigned both as an iterator and as a fixed parameter."
            )

    def requirements(self) -> Sequence[_Task | ArtifactTaskWrapper | InlineTaskWrapper]:
        keys = list(self.iterators.keys())
        iterators = list(self.iterators.values())

        merged_iterator = itertools.product(*iterators)

        kwargs_for_each_task = [
            {k: v for k, v in zip(keys, arg_tuple)} for arg_tuple in merged_iterator
        ]

        requirements = []
        for generated_args in kwargs_for_each_task:
            generated_args.update(self.kwargs)
            requirements.append(self.repeated(*self.args, **generated_args))

        if self._reqs_as_artifacts:
            requirements = [r.as_artifact() for r in requirements]

        if self._inline:
            requirements = [inline(r) for r in requirements]

        return requirements

    def artifact(self) -> CompositeArtifact | None:
        requirements = self.requirements()

        all_artifacts = []
        for r in requirements:
            artifact_spec = resolve_artifact_from_spec(r.artifact())
            if artifact_spec is not None:
                all_artifacts.append(artifact_spec)

        if len(all_artifacts) > 0:
            return CompositeArtifact(all_artifacts)
        else:
            return None

    def run(self, *args, **kwargs):
        pass
