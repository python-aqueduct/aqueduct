import inspect
from typing import Callable, Optional, Type, TypeVar, overload

from .abstract_task import AbstractTask
from .task import Task

_T = TypeVar("_T")
_Task = TypeVar("_Task", bound=AbstractTask)
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
        retval = self.inner.run(*args, **kwargs)
        return self.fn(retval)

    def ui_name(self) -> str:
        return self.inner.ui_name() + "*" + self.fn.__name__


@overload
def apply(fn: Callable[[_T], _U], task: type[_Task]) -> type[_Task]: ...


@overload
def apply(fn: Callable[[_T], _U], task: AbstractTask[_T]) -> TaskWithApply[_U]: ...


def apply(
    fn: Callable[[_T], _U],
    task: AbstractTask[_T] | type[_Task],
    name: Optional[str] = None,
) -> TaskWithApply[_U] | type[_Task]:
    """Create a new task that modifies the output of `task` by applying function `fn` on it.
    If `task` is a class, a new class is created that inherits from `task` and applies `fn` to its output.
    If `task` is an instance, a new task instance is created that applies `fn` to its output.

    Args:
        fn: The function to apply to the output of `task`.
        task: The task to modify.
        name: The name of the new task. If `task` is a class, the name is set to the name of `fn`.

    Returns:
        A new task class that applies `fn` to the output of `task`."""

    if inspect.isclass(task):

        class AnonymousTask(task):
            def run(self, *args, **kwargs):
                x = super().run(*args, **kwargs)
                return fn(x)

            @classmethod
            def ui_name(cls):
                if name is not None:
                    return name
                else:
                    return fn.__name__

            def load(self):
                return fn(super().load())

        return AnonymousTask
    else:
        return TaskWithApply(fn, task)  # type: ignore
