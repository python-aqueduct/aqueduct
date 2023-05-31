import unittest

from aqueduct.artifact import PickleArtifact
from aqueduct.store import InMemoryStore


class TestCache(unittest.TestCase):
    def test_store(self):
        store = InMemoryStore()
        artifact = PickleArtifact("test", store=store)

        body = [1, 2, 3]
        artifact.dump_to_store(body)

        self.assertIn("test", store.store)

        deserialized = artifact.load_from_store()

        self.assertListEqual(body, deserialized)
