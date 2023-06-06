import pickle
import unittest

from aqueduct.artifact import Artifact
from aqueduct.binding import Binding
from aqueduct.config import set_config
from aqueduct.store import InMemoryStore
from aqueduct.task import (
    fetch_args_from_config,
    WrappedTask,
    resolve_config_from_spec,
    Task,
    task,
)


class PretenseTask(Task):
    def run(self, a, b, c=12):
        return a + b + c


@task
def wrapped_task(a, b):
    return a + b


@task()
def wrapped_task_args(a, b):
    return a + b


class TestWrapper(unittest.TestCase):
    def test_wrapped_call(self):
        binding = wrapped_task(2, 2)
        self.assertIsInstance(binding, Binding)

    def test_wrapped_call_with_args(self):
        binding = wrapped_task_args(2, 2)
        self.assertIsInstance(binding, Binding)


class TestSubclass(unittest.TestCase):
    def test_subclass_call(self):
        t = PretenseTask()
        binding = t(1, 2, 3)
        self.assertIsInstance(binding, Binding)


class TestFullyQualifiedName(unittest.TestCase):
    def test_fqn_child(self):
        t = PretenseTask()
        self.assertEqual("tests.test_task.PretenseTask", t._fully_qualified_name())

    def test_fqn_wrapped(self):
        self.assertEqual(
            "tests.test_task.wrapped_task", wrapped_task._fully_qualified_name()
        )


class TestResolveConfig(unittest.TestCase):
    def setUp(self):
        pass

    def test_resolve_dict(self):
        cfg = {}
        task = WrappedTask(lambda x: x, cfg=cfg)

        self.assertDictEqual(cfg, task._resolve_cfg())

    def test_resolve_str(self):
        cfg = {"section": {"value": 2}}
        set_config(cfg)

        task = WrappedTask(lambda x: x, cfg="section")

        self.assertDictEqual({"value": 2}, task._resolve_cfg())

    def test_resolve_object_name_class(self):
        inner_dict = {"a": 1, "b": 2}
        set_config({"tests": {"test_task": {"PretenseTask": inner_dict}}})

        t = PretenseTask()
        config = resolve_config_from_spec(None, t)

        self.assertDictEqual(config, inner_dict)

    def test_resolve_object_name_fn(self):
        inner_dict = {"a": 2}
        set_config({"tests": {"test_task": {"wrapped_task": inner_dict}}})
        config = resolve_config_from_spec(None, wrapped_task)

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
        set_config({"tests": {"test_task": {"wrapped_task": inner_dict}}})

        binding = wrapped_task()
        self.assertEqual(5, binding.compute())


class TestWrappedTask(unittest.TestCase):
    def test_simple(self):
        def sample_fn(a, b):
            return a + b

        t = WrappedTask(sample_fn)
        binding = t(2, 2)

        self.assertIsInstance(binding, Binding)
        self.assertEqual(4, binding.compute())


class TestBinding(unittest.TestCase):
    def test_fail_on_missing_args(self):
        fetch_args_lambda = lambda: wrapped_task(2).compute()

        self.assertRaises(TypeError, fetch_args_lambda)



store = InMemoryStore()

class CrashingArtifact(Artifact):
    def deserialize(self, stream):
        raise RuntimeError("Custom deserializer should not use ")
    
    def serialize(self, object, stream):
        return pickle.dump(object, stream)
    
def custom_deserializer(stream):
    o = pickle.load(stream)
    return {**o, 'c': 4}

@task(artifact=CrashingArtifact(name='artifact_a', store=store))
def task_a():
    return {'a': 2, 'b': 3}

@task(requirements=task_a(deserializer=custom_deserializer))
def task_b(reqs):
    return reqs['c']


class TestCustomDeserializer(unittest.TestCase):
    def test_custom_deserializer(self):
        task_a_wo_custom_deserializer = task_a()
        task_a_binding = task_a(deserializer=custom_deserializer)

        result = task_a_wo_custom_deserializer.compute() # Run a first time. Should save to store.
        self.assertRaises(RuntimeError, lambda: task_a_wo_custom_deserializer.compute())  # Should fail because no custom deserializer.

        deserialized_result = task_a_binding.compute()

        self.assertDictContainsSubset(result, deserialized_result)

    def test_deserializer_always_applied(self):
        task_b_binding = task_b()
        result = task_b_binding.compute()

        self.assertEqual(result, 4)