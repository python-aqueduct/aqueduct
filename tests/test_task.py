import unittest
from aqueduct.artifact import ArtifactSpec

from aqueduct.binding import Binding
from aqueduct.config import set_config
from aqueduct.task import (
    fetch_args_from_config,
    resolve_config_from_spec,
    Task,
)


class PretenseTask(Task):
    def run(self, a, b, c=12):
        return a + b + c


class TestSubclass(unittest.TestCase):
    def test_subclass_call(self):
        t = PretenseTask()
        binding = t(1, 2, 3)
        self.assertIsInstance(binding, Binding)


class TestFullyQualifiedName(unittest.TestCase):
    def test_fqn(self):
        t = PretenseTask()
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
        binding = t()
        self.assertEqual(17, binding.compute())


class TestBinding(unittest.TestCase):
    def test_fail_on_missing_args(self):
        task = PretenseTask()
        fetch_args_lambda = lambda: task(2).compute()

        self.assertRaises(TypeError, fetch_args_lambda)


class StoringTask(Task):
    def run(self):
        pass

    def artifact(self) -> ArtifactSpec:
        return
