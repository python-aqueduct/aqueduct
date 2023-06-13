import pathlib
import unittest

import aqueduct as aq


class TestingNotebookTask(aq.NotebookTask):
    def add_to_sys(self) -> list[str]:
        return [pathlib.Path(__file__).parent]


class ExampleNotebookTask(TestingNotebookTask):
    def __init__(self, a, b=2):
        self.a = a
        self.b = b

    def notebook(self):
        return "notebook_for_tests.ipynb"

    def export(self):
        return "export_notebook_for_tests.ipynb"


class ConfigNotebookTask(TestingNotebookTask):
    def notebook(self):
        return "notebook_with_config.ipynb"

    def export(self):
        return "export_notebook_with_config.ipynb"


class TestNotebookIntegration(unittest.TestCase):
    def setUp(self):
        aq.set_config({})

    def test_notebook_run(self):
        t = ExampleNotebookTask(33)
        t.result()

    def test_config_injection(self):
        aq.set_config({"test_config_injection": 1})
        t = ConfigNotebookTask()
        t.result()
