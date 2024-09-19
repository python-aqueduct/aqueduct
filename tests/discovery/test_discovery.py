import unittest

from aqueduct.util import tasks_in_module
import aqueduct as aq

from .tasks_for_test import RootTask


class TestDiscovery(unittest.TestCase):
    def test_task_hierarchy(self):
        result = aq.run(RootTask())

        self.assertEqual(result, 1)

    def test_tasks_in_module(self):
        tasks = tasks_in_module(
            "tests.discovery.tasks_for_test",
        )

        self.assertEqual(len(tasks), 1)
