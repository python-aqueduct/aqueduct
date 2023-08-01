import unittest

import aqueduct as aq


class TaskA(aq.Task):
    def run(self):
        return 1


class TaskB(aq.Task):
    def requirements(self):
        return TaskA()

    def run(self, reqs) -> int:
        return reqs + 1


class TestInlineTask(unittest.TestCase):
    def test_no_requirements(self):
        inline_b = aq.inline(TaskB())

        to_run = aq.count_tasks_to_run(inline_b)
        self.assertDictEqual({"TaskB*inline": 1}, to_run)

    def test_result_correct(self):
        inline_b = aq.inline(TaskB())

        result = inline_b.result()

        self.assertEqual(2, result)
