import unittest

from aqueduct.backend import (
    resolve_backend_from_spec,
    DaskBackend,
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


class SimpleTask(Task):
    def run(self):
        return 2


class TaskA(Task[int]):
    def run(self):
        return 2


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
        self.assertEqual(backend.execute(t), 2)

    def test_run_dep(self):
        t = TaskB()
        self.assertEqual(self.backend.execute(t), 4)


class TestDaskBackend(TestImmediateBackend):
    def setUp(self):
        self.backend = DaskBackend()


class TestConcurrentBackend(TestImmediateBackend):
    def setUp(self):
        self.backend = ConcurrentBackend()
