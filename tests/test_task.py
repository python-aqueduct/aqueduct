import unittest

from aqueduct.artifact import ArtifactSpec, InMemoryArtifact
from aqueduct.config import set_config
from aqueduct.task import (
    fetch_args_from_config,
    resolve_config_from_spec,
    Task,
)


class TestCompute(unittest.TestCase):
    def test_simple_task(self):
        class SimpleTask(Task):
            def configure(self, value):
                self.value = value

            def run(self):
                return self.value

        t = SimpleTask(2)
        self.assertEqual(2, t.compute())


class PretenseTask(Task):
    def configure(self, a, b, c=12):
        self.a = a
        self.b = b
        self.c = c

    def run(self):
        return self.a + self.b + self.c


class TestFullyQualifiedName(unittest.TestCase):
    def test_fqn(self):
        t = PretenseTask(1, 2, 3)
        self.assertEqual("tests.test_task.PretenseTask", t._fully_qualified_name())


class TestResolveConfig(unittest.TestCase):
    def setUp(self):
        pass

    def test_resolve_dict(self):
        class LocalTask(Task):
            def cfg(self):
                return {}

            def run(self):
                pass

        task = LocalTask()
        self.assertDictEqual(task.cfg(), task._resolve_cfg())

    def test_resolve_str(self):
        cfg = {"section": {"value": 2}}
        set_config(cfg)

        class LocalTask(Task):
            def run(self):
                pass

            def cfg(self):
                return "section"

        task = LocalTask()

        self.assertDictEqual({"value": 2}, task._resolve_cfg())

    def test_resolve_object_name_class(self):
        inner_dict = {"a": 1, "b": 2}
        set_config({"tests": {"test_task": {"PretenseTask": inner_dict}}})

        t = PretenseTask()
        config = resolve_config_from_spec(None, t)

        self.assertDictEqual(config, inner_dict)


class TestFetchArgs(unittest.TestCase):
    def test_empty_config(self):
        def fn(a, b=None):
            self.assertEqual(a, 2)
            self.assertIsNone(b)

        new_args, new_kwargs = fetch_args_from_config(fn, (2,), {}, {})

        fn(*new_args, **new_kwargs)

    def test_should_use_config(self):
        def fn(a, b=None):
            self.assertEqual(b, 13)
            self.assertEqual(a, 14)

        new_args, new_kwargs = fetch_args_from_config(fn, (14,), {}, {"b": 13})

        fn(*new_args, **new_kwargs)


class TestFetchArgsOnCall(unittest.TestCase):
    def tearDown(self):
        set_config({})

    def test_missing_params(self):
        inner_dict = {"a": 2, "b": 3}
        set_config({"tests": {"test_task": {"PretenseTask": inner_dict}}})

        t = PretenseTask()
        self.assertEqual(17, t.compute())


store = {}


class StoringTask(Task):
    def configure(self, should_succeed=True):
        self.succeed = should_succeed

    def run(self):
        if self.succeed:
            store["testkey"] = 1

    def artifact(self):
        return InMemoryArtifact("testkey", store)


class TestStorageCheck(unittest.TestCase):
    def tearDown(self) -> None:
        set_config({})

    def test_storage_success(self):
        set_config({"aqueduct": {"check_storage": True}})

        t = StoringTask()
        t.compute()

        self.assertTrue(t.artifact().exists())

    def test_storage_failure(self):
        set_config({"aqueduct": {"check_storage": True}})
        t = StoringTask(should_succeed=False)

        self.assertRaises(KeyError, lambda: t.compute())
