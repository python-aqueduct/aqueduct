import unittest

from aqueduct.backend import resolve_backend_from_spec, DaskBackend, ImmediateBackend
from aqueduct.config import set_config


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
