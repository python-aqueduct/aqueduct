import numpy as np
import unittest

from aqueduct import Task, MapReduceTask
from aqueduct.artifact import InMemoryArtifact
from aqueduct.backend.dask import DaskBackend
from aqueduct.backend.immediate import ImmediateBackend
from aqueduct.backend.multiprocessing import MultiprocessingBackend


ARTIFACT_STORE = {}

class TaskA(Task):
    def __init__(self, value):
        self.value = value

    def run(self, requirements=None):
        return self.value


class TaskC(Task):
    def requirements(self):
        return TaskA(4)
    
    def run(self, requirements):
        return requirements + 3


class TaskB(MapReduceTask):
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

class NoItemsTask(TaskB):
    def items(self):
        return []

class OneItemTask(TaskB):
    def items(self):
        return [1]
    

class TaskWithPost(TaskB):
    def post(self, acc, requirements=None):
        return f"{acc}"


class TaskWithArtifact(Task):
    def run(self, requirements=None):
        return np.random.random((10,10))

    def artifact(self):
        return InMemoryArtifact('backend_artifact', ARTIFACT_STORE)


class TestImmediateBackend(unittest.TestCase):
    BACKEND_CLASS = ImmediateBackend

    def setUp(self):
        self.backend = self.BACKEND_CLASS()
        global ARTIFACT_STORE
        ARTIFACT_STORE = {}

    def tearDown(self):
        self.backend.close()

    def test_simple_task(self):
        task = TaskA(5)
        result = self.backend.run(task)
        self.assertEqual(result, 5)

    def test_parallel_task(self):
        task = TaskB()
        result = self.backend.run(task)
        self.assertEqual(result, 14)

    def test_task_with_dep(self):
        task = TaskC()
        result = self.backend.run(task)
        self.assertEqual(result, 7)
 
    def test_store_artifact(self):
        task = TaskWithArtifact()
        result = self.backend.run(task)

        self.assertTrue(task.artifact().exists())

    def test_load_artifact(self):
        global ARTIFACT_STORE
        random_matrix = np.random.random((10,10))
        ARTIFACT_STORE['backend_artifact'] = random_matrix

        task = TaskWithArtifact()
        result = self.backend.run(task)
        self.assertTrue((random_matrix == result).all())

    def test_no_items(self):
        task = NoItemsTask()
        result = self.backend.run(task)
        self.assertEqual(result, 0)

    def test_one_item(self):
        task = OneItemTask()
        result = self.backend.run(task)
        self.assertEqual(result, 1)

    def test_task_with_post(self):
        task = TaskWithPost()
        result = self.backend.run(task)
        self.assertEqual(result, "14") 


class TestMultiprocessingBackend(TestImmediateBackend):
    BACKEND_CLASS = MultiprocessingBackend

    def test_multiple_cores(self):
        backend = MultiprocessingBackend(n_workers=2)
        try:
            task = TaskB()
            result = backend.run(task)
            self.assertEqual(result, 14)
        finally:
            backend.close()



class TestDaskBackend(TestImmediateBackend):
    BACKEND_CLASS = DaskBackend

    def test_store_artifact(self):
        # This cannot be tested with in memory store because the storage happens in
        # worker processes.
        pass

    def test_load_artifact(self):
        pass