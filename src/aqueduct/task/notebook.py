from typing import TypeAlias, Any, TypedDict, Optional

import asyncio
import base64
from aqueduct.artifact import Artifact
import cloudpickle
import importlib.util
import jupyter_client
import nbclient.client
import nbclient.exceptions
import nbconvert
import nbformat
import pathlib

from ..artifact import (
    CompositeArtifact,
    TextStreamArtifactSpec,
    TextStreamArtifact,
    LocalStoreArtifact,
    resolve_artifact_from_spec,
)
from .abstract_task import AbstractTask
from ..config import get_config


class FullNotebookExportSpec(TypedDict):
    format: str
    artifact: TextStreamArtifactSpec


NotebookExportSpec: TypeAlias = str | TextStreamArtifactSpec | FullNotebookExportSpec


def encode_for_ipython(object) -> str:
    return base64.b64encode(cloudpickle.dumps(object)).decode()


def decode_program_string(base64_payload):
    return f'cloudpickle.loads(base64.b64decode("{base64_payload}"))'


def object_to_payload_program(object) -> str:
    return decode_program_string(encode_for_ipython(object))


def export_notebook(
    artifact: TextStreamArtifact,
    exporter: nbconvert.Exporter,
    notebook: nbformat.NotebookNode,
):
    exported, _ = exporter.from_notebook_node(notebook)
    artifact.dump_text(exported)


EXTENSION_OF_EXPORTER_NAME = {
    ".ipynb": "notebook",
    ".html": "html",
    ".md": "markdown",
    ".rst": "rst",
    ".pdf": "pdf",
}


def resolve_notebook_export_spec(
    spec: NotebookExportSpec,
) -> tuple[TextStreamArtifact, nbconvert.Exporter]:
    if isinstance(spec, str):
        path = pathlib.Path(spec)

        exporter_name = EXTENSION_OF_EXPORTER_NAME.get(path.suffix, "notebook")

        artifact = LocalStoreArtifact(spec)
        exporter_class = nbconvert.get_exporter(exporter_name)
    elif isinstance(spec, TextStreamArtifact):
        artifact = spec
        exporter_class = nbconvert.get_exporter("notebook")
    elif isinstance(spec, (dict, TypedDict)):
        artifact = resolve_artifact_from_spec(spec["artifact"])
        exporter_class = nbconvert.get_exporter(spec["format"])

    return artifact, exporter_class()


class NotebookTask(AbstractTask):
    def notebook(self) -> str | pathlib.Path:
        raise NotImplementedError("Notebook Tasks must implement `notebook` method.")

    def export(self) -> Optional[NotebookExportSpec]:
        return None

    def artifact(self) -> Optional[Artifact]:
        export_spec = self.export()

        if export_spec is not None:
            artifact, _ = resolve_notebook_export_spec(export_spec)
            return artifact
        else:
            return None

    def add_to_sys(self) -> list[str]:
        return []

    def __call__(self):
        notebook_path = self._resolve_notebook()

        with open(notebook_path) as f:
            notebook_source = nbformat.read(f, as_version=4)

        kernel_manager = jupyter_client.manager.AsyncKernelManager()
        asyncio.run(kernel_manager.start_kernel())  # type: ignore
        notebook_client = nbclient.client.NotebookClient(
            notebook_source, km=kernel_manager
        )
        kernel_client = asyncio.run(notebook_client.async_start_new_kernel_client())

        self._prepare_kernel_with_injected_code(kernel_client)

        try:
            for i, c in enumerate(notebook_source["cells"]):
                notebook_client.execute_cell(c, i)
        except nbclient.exceptions.CellExecutionError as e:
            raise e
        finally:
            export_spec = self.export()
            if export_spec is not None:
                artifact, exporter = resolve_notebook_export_spec(export_spec)
                export_notebook(artifact, exporter, notebook_source)

        sinked_value = self._fetch_sinked_value(kernel_client)

        return sinked_value

    def _prepare_kernel_with_injected_code(self, kernel_client):
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

        response = asyncio.run(
            kernel_client.execute_interactive(
                injected_code,
            )
        )

        print(response)

    def _fetch_sinked_value(self, kernel_client) -> Any:
        response = asyncio.run(
            kernel_client.execute_interactive(
                code="import aqueduct.notebook",
                user_expressions={
                    "aq_return_value": "aqueduct.notebook.AQ_ENCODED_RETURN"
                },
            )
        )

        aq_return_value_dict = response["content"]["user_expressions"][
            "aq_return_value"
        ]

        if aq_return_value_dict["status"] != "ok":
            raise RuntimeError("Failed to try and recover return value from notebook.")

        if aq_return_value_dict["data"]["text/plain"] == "None":
            sinked_value = None
        else:
            sinked_value = cloudpickle.loads(
                base64.b64decode(aq_return_value_dict["data"]["text/plain"][1:-1])
            )

        return sinked_value

    def _resolve_notebook(self) -> pathlib.Path:
        module = importlib.util.find_spec(self.__module__)

        if module is None or module.origin is None:
            raise RuntimeError("Could not resolve notebook from specified path.")

        path_of_module = pathlib.Path(module.origin)
        path = pathlib.Path(self.notebook())
        if path.is_absolute():
            return path
        else:
            return path_of_module.parent / path
