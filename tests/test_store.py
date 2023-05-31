import omegaconf
import unittest

from aqueduct.config import set_config, get_config
from aqueduct.store import get_default_store, LocalFilesystemStore


class TestDefaultStore(unittest.TestCase):
    def tearDown(self):
        set_config({})

    def test_fetch_default_on_none(self):
        c = omegaconf.DictConfig(
            {
                "aqueduct": {
                    "default_store": {"_target_": "aqueduct.store.LocalFilesystemStore"}
                }
            }
        )
        set_config(c)
        store = get_default_store()
        self.assertIsInstance(store, LocalFilesystemStore)
