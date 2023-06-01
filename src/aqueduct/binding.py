from typing import Generic, Callable, TypeVar


T = TypeVar("T")


class Binding(Generic[T]):
    """A work bundle that is the binding of a :class:`Task` to arguments.
    This work bundle can then be offloaded to an external process to be actually
    computed. Typically, Bindings are generated automatically and should only be created
    by the Task class."""

    def __init__(self, task, fn: Callable[..., T], *args, **kwargs):
        self.task = task
        self.fn = fn
        self.args = args
        self.kwargs = kwargs

    def compute(self) -> T:
        # We need to import this automatically, otherwise it provokes a circular
        # import.
        from .backend import get_default_backend

        backend = get_default_backend()
        return backend.run(self)

    def is_pure(self) -> bool:
        return self.task.artifact == None

    def __repr__(self):
        return f"Binding({self.fn!s}, args: {self.args!s}, kwargs: {self.kwargs!s})"
