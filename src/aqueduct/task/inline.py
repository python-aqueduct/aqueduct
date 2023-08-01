from typing import TYPE_CHECKING, Optional, TypeVar

from .abstract_task import AbstractTask
from .task import Task
from ..artifact import ArtifactSpec

if TYPE_CHECKING:
    from ..task_tree import TaskTree


_T = TypeVar("_T")


class InlineTaskWrapper(Task[_T]):
    def __init__(self, wrapped_task: AbstractTask[_T]):
        self.wrapped = wrapped_task

    def __call__(self, *args, **kwargs) -> _T:
        return super().__call__(*args, **kwargs)

    def artifact(self) -> Optional[ArtifactSpec]:
        return self.wrapped.artifact()

    def requirements(self):
        return None

    def run(self, *args, **kwargs) -> _T:
        inner_requirements = self.wrapped._resolve_requirements()

        if inner_requirements is not None:
            from ..backend import ImmediateBackend

            immediate_backend = ImmediateBackend()
            requirements = immediate_backend.execute(inner_requirements)
        else:
            requirements = None

        return self.wrapped.run(requirements)

    def task_name(self) -> str:
        return self.wrapped.task_name() + "*inline"


def inline(task: AbstractTask[_T]) -> InlineTaskWrapper[_T]:
    return InlineTaskWrapper(task)
