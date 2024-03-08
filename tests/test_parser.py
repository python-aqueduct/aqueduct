import unittest

from aqueduct import Task
from aqueduct.cli.tasklang import parse_task_spec


class TaskA(Task):
    def run(self):
        return 1


class TaskC(Task):
    def __init__(self, value):
        self.value = value

    def run(self):
        return self.value


class TaskB(Task):
    def __init__(self, task_a):
        self.task_a = task_a

    def requirements(self):
        return self.task_a

    def run(self, requirements):
        return requirements + 1


class TestParser(unittest.TestCase):
    def setUp(self):
        self.task_name_to_task_class = {"TaskA": TaskA, "TaskB": TaskB, "TaskC": TaskC}

    def test_simple_task(self):
        task = parse_task_spec("TaskA()", self.task_name_to_task_class)
        self.assertIsInstance(task, TaskA)

    def test_task_with_args(self):
        task = parse_task_spec("TaskC(value=3)", self.task_name_to_task_class)
        self.assertIsInstance(task, TaskC)

        self.assertEqual(task.value, 3) # type: ignore
