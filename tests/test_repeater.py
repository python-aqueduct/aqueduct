import unittest

from aqueduct.artifact import InMemoryArtifact, CompositeArtifact
from aqueduct.task import Task
from aqueduct.task.repeater import RepeaterTask

STORE = {}


class TaskA(Task):
    def __init__(self, a, b, c):
        self.a = a
        self.b = b
        self.c = c

    def __eq__(self, rhs):
        return self.a == rhs.a and self.b == rhs.b and self.c == rhs.c

    def artifact(self):
        return InMemoryArtifact(self.c, STORE)


class TestRepeaterTask(unittest.TestCase):
    def test_one_level(self):
        t = RepeaterTask(TaskA, {"a": [1, 2, 3]}, b="toto", c="tata")

        requirements = t.requirements()
        self.assertEqual(3, len(requirements))

        expected_tasks = [TaskA(a=x, b="toto", c="tata") for x in range(1, 4)]
        for lhs, rhs in zip(requirements, expected_tasks):
            self.assertEqual(lhs, rhs)

    def test_two_levels(self):
        t = RepeaterTask(TaskA, {"a": [0, 1], "b": [0, 1]}, c="toto")

        requirements = t.requirements()

        expected_tasks = [TaskA(a=x, b=y, c="toto") for x in [0, 1] for y in [0, 1]]

        for lhs, rhs in zip(requirements, expected_tasks):
            self.assertEqual(lhs, rhs)

    def test_bound_iterator(self):
        self.assertRaises(
            KeyError, lambda: RepeaterTask(TaskA, {"a": [1, 2, 3]}, a=1, b=2, c=3)
        )

    def test_artifact(self):
        t = RepeaterTask(TaskA, {"a": [0, 1]}, b="toto", c="tata")

        artifact = t.artifact()
        self.assertIsInstance(artifact, CompositeArtifact)
