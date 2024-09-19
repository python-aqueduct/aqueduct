import unittest

from aqueduct.backend import (
    resolve_backend_from_spec,
    ImmediateBackend,
    ConcurrentBackend,
)
from aqueduct.config import set_config
from aqueduct.task import Task


class TestBackendResolution(unittest.TestCase):
    def tearDown(self) -> None:
        set_config({})

    def test_use_config_on_none(self):
        set_config(
            {"aqueduct": {"backend": {"_target_": "aqueduct.backend.ImmediateBackend"}}}
        )

        backend = resolve_backend_from_spec(None)
        self.assertIsInstance(backend, ImmediateBackend)

    def test_use_default_on_none(self):
        backend = resolve_backend_from_spec(None)
        self.assertIsInstance(backend, ImmediateBackend)

    def test_by_name(self):
        backend = resolve_backend_from_spec("immediate")
        self.assertIsInstance(backend, ImmediateBackend)

    def test_by_dict(self):
        backend = resolve_backend_from_spec({"type": "concurrent", "n_workers": 4})
        self.assertIsInstance(backend, ConcurrentBackend)


class SimpleTask(Task):
    def run(self):
        return 2


class TaskA(Task[int]):
    def run(self):
        return 2


class TaskC(Task):
    def requirements(self):
        return TaskA()

    def items(self):
        return [1, 2, 3]

    def map(self, x, requirements):
        return [x**2 + requirements]

    def accumulator(self, requirements=None) -> list:
        return []

    def reduce(self, lhs, rhs, requirements=None):
        return sorted(lhs + rhs)


class TaskB(Task[int]):
    def run(self, reqs):
        return reqs * 2

    def requirements(self) -> TaskA:
        return TaskA()


class TestImmediateBackend(unittest.TestCase):
    def setUp(self):
        self.backend = ImmediateBackend()

    def test_run_task(self):
        t = SimpleTask()
        backend = self.backend

        result = backend.run(t)
        self.assertEqual(result, 2)

    def test_run_dep(self):
        t = TaskB()

        self.assertEqual(self.backend.run(t), 4)

    def test_run_parallel_task(self):
        t = TaskC()

        self.assertListEqual(self.backend.run(t), [3, 6, 11])


class TestConcurrentBackend(TestImmediateBackend):
    def setUp(self):
        self.backend = ConcurrentBackend()
