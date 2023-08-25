import unittest

from aqueduct.task_tree import _map_type_in_tree, reduce_type_in_tree


class TestTypeTree(unittest.TestCase):
    def test_acc(self):
        tree = [1, 2, {"a": 3}]

        result = reduce_type_in_tree(tree, int, int.__add__, 0)
        self.assertEqual(result, 6)

    def test_map(self):
        tree = [1, 2, {"a": {"b": 5}}]

        result = _map_type_in_tree(tree, int, lambda x: 2 * x)

        self.assertEqual(result[0], 2)
        self.assertEqual(result[1], 4)

        self.assertEqual(result[2]["a"]["b"], 10)  # type: ignore
