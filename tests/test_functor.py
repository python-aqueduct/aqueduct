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


class FunctorA(aq.Functor[int, int]):
    def __init__(self, param: int = 1):
        self.param = param

    def mapping(self, task_output: int, requirements: None) -> int:
        return task_output + self.param


class FunctorB(aq.Functor[int, int]):
    def mapping(self, task_output: int, requirements: int) -> int:
        return task_output + requirements

    def requirements(self):
        return TaskA()


class TestFunctor(unittest.TestCase):
    def test_application(self):
        mapped_task = FunctorA(2)(TaskB())

        mapped_result = aq.run(mapped_task)

        self.assertEqual(mapped_result, 4)

    def test_simple_dependency(self):
        task_before_mapping = TaskB()
        mapped_task = FunctorA(2)(task_before_mapping)

        self.assertEqual(mapped_task.requirements(), (task_before_mapping, None))

    def test_functor_dependency(self):
        task_before_mapping = TaskB()
        functor = FunctorB()
        mapped_task = functor(task_before_mapping)

        mapped_task_req, functor_req = mapped_task.requirements()
        self.assertTrue(isinstance(mapped_task_req, TaskB))
        self.assertTrue(isinstance(functor_req, TaskA))

    def test_unique_key_depends_on_mapee(self):
        task_before_mapping_a = TaskA()
        task_before_mapping_b = TaskB()
        functor = FunctorA(2)
        mapped_task_a = functor(task_before_mapping_a)
        mapped_task_b = functor(task_before_mapping_b)

        self.assertNotEqual(mapped_task_a._unique_key(), mapped_task_b._unique_key())
