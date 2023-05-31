import unittest

from aqueduct.config import get_deep_key, has_deep_key


class TestDeepDict(unittest.TestCase):
    def test_deep_key(self):
        d = {"a": {"b": 1}}

        self.assertEqual(get_deep_key(d, "a.b"), 1)

    def test_deep_key_flat(self):
        d = {"a": {"b": 1}, "c": 3}
        self.assertEqual(get_deep_key(d, "c"), 3)

    def test_has_deep_key(self):
        pass
