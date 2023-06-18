import pathlib
import unittest
import tempfile

import aqueduct as aq


class NotebookTaskWithPath(aq.NotebookTask):
    def add_to_sys(self) -> list[str]:
        return [str(pathlib.Path(__file__).parent)]


class ExampleNotebookTask(NotebookTaskWithPath):
    def __init__(self, a, b={}):
        self.a = a
        self.b = b

    def notebook(self):
        return "notebook_for_tests.ipynb"


class ConfigNotebookTask(NotebookTaskWithPath):
    def notebook(self):
        return "notebook_with_config.ipynb"


class EmptyNotebookTask(NotebookTaskWithPath):
    def notebook(self):
        return "empty_notebook.ipynb"


class BackendNotebookTask(NotebookTaskWithPath):
    def notebook(self):
        return "return_backend.ipynb"


class NotebookWithExport(NotebookTaskWithPath):
    def __init__(self, export_path):
        self.export_path = export_path

    def notebook(self):
        return "empty_notebook.ipynb"

    def export(self):
        return self.export_path


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

    def test_backend_injection(self):
        backend = aq.DaskBackend()

        backend._spec()
        t = BackendNotebookTask()
        inner_backend = t.result(backend=backend)

        self.assertDictEqual(backend._spec(), inner_backend)

    def test_sink(self):
        input_dict = {"test_return_value": 1}
        t = ExampleNotebookTask(a=33, b=input_dict)
        output_dict = t.result()

        self.assertDictEqual(input_dict, output_dict)

    def test_sink_no_return(self):
        t = EmptyNotebookTask()
        retval = t.result()

        self.assertIsNone(retval)


class TextNotebookExport(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = pathlib.Path(tempfile.mkdtemp())

    def export_with_filename(self, filename):
        export_path = self.tmp_dir / filename
        t = NotebookWithExport(str(export_path))

        t.result()

        self.assertTrue(export_path.is_file())
        self.assertLess(0, export_path.stat().st_size)

        export_path.unlink()

    def test_export_md(self):
        self.export_with_filename("test_notebook.md")

    def test_export_html(self):
        self.export_with_filename("test_notebook.html")

    def test_export_ipynb(self):
        self.export_with_filename("test_notebook.ipynb")

    def tearDown(self):
        self.tmp_dir.rmdir()
