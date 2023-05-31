import unittest

from aqueduct.artifact import PickleArtifact
from aqueduct.config import set_config
from aqueduct.task import (
    fetch_args_from_config,
    WrappedTask,
    resolve_artifact_from_spec,
    get_default_artifact_cls,
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


class TestFetchArgs(unittest.TestCase):
    def setUp(self):
        pass

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

    def test_should_fail_on_missing(self):
        def fn(a, b):
            pass

        fetch_args_lambda = lambda: fetch_args_from_config(fn, tuple(), {}, {"a": 13})

        self.assertRaises(TypeError, fetch_args_lambda)


class TestResolveArtifact(unittest.TestCase):
    def test_str(self):
        spec = "artifact.pkl"
        artifact = resolve_artifact_from_spec(spec)

        self.assertIsInstance(artifact, get_default_artifact_cls())
        self.assertEquals(artifact.name, spec)

    def test_str_template(self):
        name = "toto"
        spec = "artifact_{name}.pkl"

        artifact = resolve_artifact_from_spec(spec, name=name)

        self.assertEquals(spec.format(name=name), artifact.name)
        self.assertIsInstance(artifact, get_default_artifact_cls())

    def test_callable(self):
        def spec(name):
            return PickleArtifact(name)

        artifact = resolve_artifact_from_spec(spec, "toto")

        self.assertEquals("toto", artifact.name)
        self.assertIsInstance(artifact, PickleArtifact)

    def test_artifact(self):
        artifact = PickleArtifact("toto")

        returned = resolve_artifact_from_spec(artifact)

        self.assertEqual(artifact, returned)

    def test_none(self):
        self.assertIsNone(resolve_artifact_from_spec(None))
