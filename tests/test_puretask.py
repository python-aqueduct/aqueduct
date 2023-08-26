import unittest

import aqueduct as aq


class StoringTask(aq.Task):
    def __init__(self, store):
        self.store = store

    def run(self):
        return {"a": 1, "b": 2}

    def artifact(self) -> aq.artifact.InMemoryArtifact:
        return aq.artifact.InMemoryArtifact("test", self.store)


class TestQuickTask(unittest.TestCase):
    def setUp(self):
        self.store = {}

    def test_auto_cache(self):
        t = StoringTask(self.store)
        result = aq.run(t)

        self.assertIn("test", self.store)
        self.assertDictEqual(self.store["test"], result)
