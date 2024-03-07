import unittest

from aqueduct import Task
from aqueduct.backend.dask import add_work_to_dask_graph

class TaskB(Task):
    def __init__(self, value):
        self.value = value

    def run(self, requirements):
        return self.value**2


class TaskA(Task):
    def requirements(self):
        return [TaskB(2), TaskB(3), TaskB(2)]

    def run(self, reqs):
        return sum(reqs) + 2

class TestDaskUtils(unittest.TestCase):
    def test_add_task(self):
        work = TaskB(2)
        computation, graph = add_work_to_dask_graph(work, {})

        self.assertEqual(work._unique_key(), computation)
        self.assertEqual(len(graph), 1)
        self.assertIn(work._unique_key(), graph)

    def test_add_list(self):
        task1 = TaskB(1)
        task2 = TaskB(2)
        work = [task1, task2]
        computation, graph = add_work_to_dask_graph(work, {})

        self.assertListEqual([task1._unique_key(), task2._unique_key()], computation)
        self.assertEqual(len(graph), 2)
        self.assertIn(task1._unique_key(), graph)
        self.assertIn(task2._unique_key(), graph)

    def test_add_task_with_deps(self):
        work = TaskA()
        computation, graph = add_work_to_dask_graph(work, {})

        self.assertEqual(work._unique_key(), computation)
        self.assertEqual(len(graph), 3)
