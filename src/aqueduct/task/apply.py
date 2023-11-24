from typing import Callable, TypeVar

from .abstract_task import AbstractTask
from .task import Task


_T = TypeVar("_T")
_U = TypeVar("_U")


class TaskWithApply(Task[_U]):
    def __init__(
        self,
        fn: Callable[[_T], _U],
        inner: AbstractTask[_T],
    ):
        self.inner = inner
        self.fn = fn

    def requirements(self):
        return self.inner.requirements()

    def run(self, *args, **kwargs) -> _U:
        retval = self.inner.__call__(*args, **kwargs)
        return self.fn(retval)

    def task_name(self) -> str:
        return self.inner.task_name() + "*" + self.fn.__name__


def apply(fn: Callable[[_T], _U], task: AbstractTask[_T]) -> Task[_U]:
    return TaskWithApply(fn, task)
