import unittest


from aqueduct import run
from aqueduct.task.task import Task
from aqueduct.task.parallel_task import ParallelTask


class TaskB(Task):
    def __init__(self, value):
        self.value = value

    def run(self):
        return self.value


class TaskA(ParallelTask):
    def requirements(self):
        return [TaskB(2), TaskB(3), TaskB(2)]

    def map(self, x):
        return x**2

    def accumulator(self):
        return 0

    def reduce(self, x, acc):
        return acc + x


class TestParallelTask(unittest.TestCase):
    def test_run(self):
        task = TaskA()
        result = run(task)
        self.assertEqual(result, 17)
