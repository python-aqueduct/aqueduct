from typing import TypeAlias

import asyncio
import base64
import cloudpickle
import importlib
import jupyter_client
import nbclient
import nbformat
import pathlib

from ..artifact import ArtifactSpec
from .abstract_task import AbstractTask
from ..config import get_config


NotebookExportSpec: TypeAlias = ArtifactSpec


class MyOutputHook:
    def __call__(self, message):
        if message["header"]["msg_type"] == "execute_result":
            self.extracted = cloudpickle.loads(
                base64.b64decode(message["content"]["data"]["text/plain"][2:-1])
            )


def encode_for_ipython(object) -> str:
    return base64.b64encode(cloudpickle.dumps(object)).decode()


def decode_program_string(base64_payload):
    return f'cloudpickle.loads(base64.b64decode("{base64_payload}"))'


def object_to_payload_program(object) -> str:
    return decode_program_string(encode_for_ipython(object))


class NotebookTask(AbstractTask):
    def notebook(self) -> str | pathlib.Path:
        raise NotImplementedError("Notebook Tasks must implement `notebook` method.")

    def export(self) -> NotebookExportSpec:
        return None

    def add_to_sys(self) -> list[str]:
        return []

    def __call__(self):
        notebook_path = self._resolve_notebook()

        with open(notebook_path) as f:
            notebook_source = nbformat.read(f, as_version=4)

        kernel_manager = jupyter_client.manager.AsyncKernelManager()
        asyncio.run(kernel_manager.start_kernel())
        notebook_client = nbclient.NotebookClient(notebook_source, km=kernel_manager)
        kernel_client = asyncio.run(notebook_client.async_start_new_kernel_client())

        add_sys_string = str([str(x) for x in self.add_to_sys()])

        task_load_program = object_to_payload_program(self)

        cfg = get_config()
        config_load_program = object_to_payload_program(cfg)

        injected_code = ";".join(
            [
                "import sys",
                f"sys.path.extend({add_sys_string})",
                "import aqueduct.notebook",
                "aqueduct.notebook.AQ_MANAGED_EXECUTION = True",
                "import cloudpickle",
                "import base64",
                f"aqueduct.notebook.AQ_INJECTED_TASK = {task_load_program}",
                f"aqueduct.set_config({config_load_program})",
            ]
        )

        print(injected_code)

        response = asyncio.run(
            kernel_client.execute_interactive(
                injected_code,
            )
        )

        try:
            for i, c in enumerate(notebook_source["cells"]):
                notebook_client.execute_cell(c, i)
        except nbclient.exceptions.CellExecutionError as e:
            raise e
        finally:
            export_spec = self.export()
            if export_spec is not None:
                nbformat.write(notebook_source, export_spec)

    def _resolve_notebook(self) -> pathlib.Path():
        path_of_module = pathlib.Path(importlib.util.find_spec(self.__module__).origin)
        path = pathlib.Path(self.notebook())
        if path.is_absolute():
            return path
        else:
            return path_of_module.parent / path
