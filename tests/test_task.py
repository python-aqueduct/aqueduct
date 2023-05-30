import unittest

from aqueduct.config import set_config
from aqueduct.task import fetch_args_from_config, WrappedTask


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
