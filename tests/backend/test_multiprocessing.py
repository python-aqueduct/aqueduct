import unittest

from aqueduct import Task, run
from aqueduct.task.parallel_task import ParallelTask
from aqueduct.backend import MultiprocessingBackend


class TaskA(Task):
    def __init__(self, value):
        self.value = value

    def run(self):
        return self.value


class TaskB(ParallelTask):
    def items(self):
        return [1, 2, 3]

    def requirements(self):
        return [TaskA(4)]

    def map(self, x, requirements=None):
        return x**2

    def accumulator(self, requirements=None):
        return 0

    def reduce(self, lhs, rhs, requirements=None):
        return lhs + rhs


class TestMultiprocessingBackend(unittest.TestCase):
    def setUp(self):
        self.backend = MultiprocessingBackend()

    def test_simple_task(self):
        task = TaskA(5)
        result = self.backend.run(task)
        self.assertEqual(result, 5)

    def test_parallel_task(self):
        task = TaskB()
        result = self.backend.run(task)
        self.assertEqual(result, 14)

    def test_multiple_cores(self):
        backend = MultiprocessingBackend(n_workers=2)
        task = TaskB()
        result = backend.run(task)
        self.assertEqual(result, 14)
