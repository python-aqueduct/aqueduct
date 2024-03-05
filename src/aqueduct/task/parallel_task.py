from typing import Iterable, TYPE_CHECKING, Generic, TypeVar

from .abstract_task import AbstractTask

if TYPE_CHECKING:
    from ..task_tree import TaskTree

_A = TypeVar("_A")
_T = TypeVar("_T")


class AbstractParallelTask(AbstractTask, Generic[_T, _A]):
    """A task with a map-reduce interface to allow parallel processing of input items
    some compouting backends. When called, runs something equivalent to

        def __call__(self):
            acc = self.accumulator()
            for item in self.items(requirements):
                acc = self.reduce(self.map(item), acc)

            return acc

        where `requirements` is the pre-computed requirements of the task."""

    def items(self) -> Iterable:
        """The list of input items to be processed in parallel."""
        raise NotImplementedError()

    def __call__(self, *args, **kwargs) -> _A:
        acc = self.accumulator()
        for item in self.items():
            acc = self.reduce(self.map(item), acc)

        return acc

    def map(self, item, requirements=None) -> _T:
        """Map function to be implemented by subclasses. Defaults to the identity
        function. Override to provide a custom map function."""
        raise NotImplementedError()

    def accumulator(self, requirements=None) -> _A:
        """Base accumulator with which the reduce operation is initialized.
        Override this method to provide a custom accumulator.

        Returns:
            Base accumulator with which the reduce operation is initialized.
            Defaults to an empty list."""
        raise NotImplementedError()

    def reduce(self, item: _T, accumulator: _A, requirements=None) -> _A:
        """Reduction function. Defaults to appending in a list. Override to provide a
        custom reduce function."""
        raise NotImplementedError()


class ParallelTask(AbstractParallelTask[_T, list[_T]]):
    """A default ParallelTask implementation with trivial choices for `items`, `map`,
    `accumulator` and `reduce`."""

    def items(self) -> Iterable:
        raise NotImplementedError("ParallelTask must implement items()")

    def __call__(self, *args, **kwargs) -> list:
        acc = self.accumulator()
        for item in self.items(*args):
            acc = self.reduce(self.map(item), acc)

        return acc

    def map(self, item) -> _T:
        """Map function to be implemented by subclasses. Defaults to the identity
        function. Override to provide a custom map function."""
        return item

    def accumulator(self) -> list[_T]:
        """Base accumulator with which the reduce operation is initialized.
        Override this method to provide a custom accumulator.

        Returns:
            Base accumulator with which the reduce operation is initialized.
            Defaults to an empty list."""
        return []

    def reduce(self, item: _T, accumulator: list[_T]) -> list[_T]:
        """Reduction function. Defaults to appending in a list. Override to provide a
        custom reduce function."""
        return accumulator + [item]


class Task(AbstractParallelTask[_T, _T]):
    """Pose a regular task as a specialized version of a parallel task."""

    def items(self, requirements) -> Iterable:
        """The list of input items to be processed in parallel."""
        return requirements

    def run(self, requirements) -> _T:
        raise NotImplementedError()

    def map(self, item) -> _T:
        """Map function to be implemented by subclasses. Defaults to the identity
        function. Override to provide a custom map function."""
        raise NotImplementedError()

    def accumulator(self) -> None:
        """Base accumulator with which the reduce operation is initialized.
        Override this method to provide a custom accumulator.

        Returns:
            Base accumulator with which the reduce operation is initialized.
            Defaults to an empty list."""
        return None

    def reduce(self, item: _T, accumulator: _T) -> _T:
        """Reduction function. Defaults to appending in a list. Override to provide a
        custom reduce function."""
        return item
