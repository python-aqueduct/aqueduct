from typing import Iterable, TYPE_CHECKING, Generic, TypeVar

from .abstract_task import AbstractTask

if TYPE_CHECKING:
    from ..task_tree import TaskTree

_A = TypeVar("_A")
_T = TypeVar("_T")
_U = TypeVar("_U")


class AbstractMapReduceTask(AbstractTask, Generic[_T, _A, _U]):
    """"""

    def items(self) -> Iterable:
        """The list of input items to be processed in parallel."""
        raise NotImplementedError()

    def __call__(self, requirements) -> _A:
        acc = self.accumulator(requirements)
        for item in self.items():
            acc = self.reduce(self.map(item, requirements), acc, requirements)

        return acc

    def map(self, item: _T, requirements=None) -> _A:
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

    def reduce(self, lhs: _A, rhs: _A, requirements=None) -> _A:
        """Reduction function. Defaults to appending in a list. Override to provide a
        custom reduce function."""
        raise NotImplementedError()

    def post(self, acc: _A, requirements=None) -> _U:
        raise NotImplementedError()


class MapReduceTask(AbstractMapReduceTask[_T, list[_T], list[_T]]):
    """A default ParallelTask implementation with trivial choices for `items`, `map`,
    `accumulator` and `reduce`."""

    def items(self) -> Iterable:
        raise NotImplementedError("ParallelTask must implement items()")

    def __call__(self, requirements) -> list:
        acc = self.accumulator(requirements)
        for item in self.items():
            acc = self.reduce(self.map(item, requirements), acc, requirements)

        return acc

    def map(self, item, requirements) -> list[_T]:
        """Map function to be implemented by subclasses. Defaults to the identity
        function. Override to provide a custom map function."""
        return [item]

    def accumulator(self, requirements) -> list[_T]:
        """Base accumulator with which the reduce operation is initialized.
        Override this method to provide a custom accumulator.

        Returns:
            Base accumulator with which the reduce operation is initialized.
            Defaults to an empty list."""
        return []

    def reduce(self, lhs: list[_T], rhs: list[_T], requirements) -> list[_T]:
        """Reduction function. Defaults to appending in a list. Override to provide a
        custom reduce function."""
        return lhs + rhs

    def post(self, acc: list[_T], requirements=None) -> list[_T]:
        return acc
