from typing import Generic, TypeVar

import unittest

import aqueduct as aq
from aqueduct.artifact import ArtifactSpec

T = TypeVar("T")


class TaskA(aq.PureTask):
    def configure(self, a):
        self.a = a

    def run(self):
        return self.a


class TaskB(aq.PureTask):
    def requirements(self):
        return [TaskA(2), TaskA(1), TaskA(3)]

    def run(self, reqs):
        return sum(*reqs)


class TaskC(aq.PureTask):
    def requirements(self):
        return [TaskA(1), TaskA(1), TaskA(1)]

    def run(self, reqs):
        return sum(*reqs)


class AlwaysExistsArtifact(aq.Artifact):
    def exists(self):
        return True


class TaskD(aq.PureTask):
    def artifact(self):
        return AlwaysExistsArtifact()

    def requirements(self):
        return TaskA(1)


class TaskE(aq.PureTask):
    def requirements(self):
        return TaskD()


class TestCountTasks(unittest.TestCase):
    def test_count(self):
        t = TaskB()
        count = aq.count_tasks(t)

        self.assertDictEqual(count, {"TaskA": 3, "TaskB": 1})

    def test_remove_duplicates(self):
        t = TaskC()
        count = aq.count_tasks(t, remove_duplicates=True)

        self.assertDictEqual(count, {"TaskA": 1, "TaskC": 1})

    def test_keep_duplicates(self):
        t = TaskC()
        count = aq.count_tasks(t, remove_duplicates=False)

        self.assertDictEqual(count, {"TaskA": 3, "TaskC": 1})

    def test_cached(self):
        t = TaskE()

        count = aq.count_tasks(t, use_cache=True)

        self.assertEqual({"TaskE": 1, "TaskD": 1}, count)

    def test_keep_cached(self):
        t = TaskE()
        count = aq.count_tasks(t, use_cache=False)
        self.assertEqual({"TaskA": 1, "TaskD": 1, "TaskE": 1}, count)

    def test_root_is_cached(self):
        t = TaskD()

        count = aq.count_tasks(t, use_cache=True)

        self.assertEqual({"TaskD": 1}, count)
