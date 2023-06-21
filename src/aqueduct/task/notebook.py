from typing import TypeAlias, Any, TypedDict, Optional, TYPE_CHECKING

import asyncio
import base64
from aqueduct.artifact import Artifact
import cloudpickle
import importlib.util
import jupyter_client
import logging
import nbclient.client
import nbclient.exceptions
import nbconvert
import nbformat
import pathlib
import tqdm

from ..artifact import (
    TextStreamArtifactSpec,
    TextStreamArtifact,
    LocalStoreArtifact,
    LocalFilesystemArtifact,
    resolve_artifact_from_spec,
)
from .abstract_task import AbstractTask
from ..config import get_config
from ..task_tree import OptionalTaskTree

if TYPE_CHECKING:
    from ..backend import BackendSpec


class FullNotebookExportSpec(TypedDict):
    format: str
    artifact: TextStreamArtifactSpec


NotebookExportSpec: TypeAlias = str | TextStreamArtifactSpec | FullNotebookExportSpec


_logger = logging.getLogger(__name__)


def encode_for_ipython(object) -> str:
    return base64.b64encode(cloudpickle.dumps(object)).decode()


def decode_program_string(base64_payload):
    return f'cloudpickle.loads(base64.b64decode("{base64_payload}"))'


def object_to_payload_program(object: Optional[Any] = None) -> str:
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
    if isinstance(spec, (str, pathlib.Path)):
        path = pathlib.Path(spec)

        exporter_name = EXTENSION_OF_EXPORTER_NAME.get(path.suffix, "notebook")

        artifact = LocalStoreArtifact(spec)
        exporter_class = nbconvert.get_exporter(exporter_name)
    elif isinstance(spec, LocalFilesystemArtifact):
        suffix = spec.path.suffix
        exporter_name = EXTENSION_OF_EXPORTER_NAME.get(suffix, "notebook")
        exporter_class = nbconvert.get_exporter(exporter_name)

        artifact = spec
    elif isinstance(spec, TextStreamArtifact):
        artifact = spec
        exporter_class = nbconvert.get_exporter("notebook")
    elif isinstance(spec, (dict, TypedDict)):
        artifact = resolve_artifact_from_spec(spec["artifact"])
        exporter_class = nbconvert.get_exporter(spec["format"])

    return artifact, exporter_class()


class NotebookTask(AbstractTask):
    REQUIREMENTS_INJECTION = False
    """Tells the task to inject the requirements into the kernel from memory. Since the
    requirement objects need to be serialized and deserialized, this can be very slow.
    If False, no requirements are injected, and the requirements are computed directly
    inside the Jupyter kernel. Defaults: `False`."""

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

    def _resolve_requirements(self, ignore_cache=False) -> OptionalTaskTree:
        if self.REQUIREMENTS_INJECTION == False and not ignore_cache:
            _logger.info(
                f"Skipping requirements for NotebookTask {self.__class__.__qualname__}."
                "Requirements will be loaded in-kernel."
            )
            return None
        else:
            return super()._resolve_requirements(ignore_cache=ignore_cache)

    def __call__(self, requirements=None, backend_spec: "Optional[BackendSpec]" = None):
        notebook_path = self._resolve_notebook()

        with open(notebook_path) as f:
            notebook_source = nbformat.read(f, as_version=4)

        kernel_manager = jupyter_client.manager.AsyncKernelManager()
        asyncio.run(kernel_manager.start_kernel())  # type: ignore
        notebook_client = nbclient.client.NotebookClient(
            notebook_source, km=kernel_manager
        )
        kernel_client = asyncio.run(notebook_client.async_start_new_kernel_client())

        if self.REQUIREMENTS_INJECTION:
            injected_requirements = requirements
        else:
            injected_requirements = None

        self._prepare_kernel_with_injected_code(
            kernel_client, injected_requirements, backend_spec=backend_spec
        )

        try:
            _logger.info("Executing notebook...")
            for i, c in tqdm.tqdm(
                enumerate(notebook_source["cells"]),
                total=len(notebook_source["cells"]),
                unit="cell",
            ):
                notebook_client.execute_cell(c, i)
        except nbclient.exceptions.CellExecutionError as e:
            raise e
        finally:
            export_spec = self.export()
            if export_spec is not None:
                artifact, exporter = resolve_notebook_export_spec(export_spec)
                export_notebook(artifact, exporter, notebook_source)

        sinked_value = self._fetch_sinked_value(kernel_client)

        future = kernel_manager.shutdown_kernel()
        asyncio.run(future)  # type: ignore

        return sinked_value

    def _prepare_kernel_with_injected_code(
        self, kernel_client, requirements, backend_spec=None
    ):
        add_sys_string = str([str(x) for x in self.add_to_sys()])

        _logger.info("Serializing task...")
        task_load_program = object_to_payload_program(self)

        req_load_program = object_to_payload_program(requirements)

        _logger.info("Serializing config...")
        cfg = get_config()
        config_load_program = object_to_payload_program(cfg)

        _logger.info("Serializing backend...")
        backend_load_program = object_to_payload_program(backend_spec)

        injected_code = ";\n".join(
            [
                "import sys",
                f"sys.path.extend({add_sys_string})",
                "import aqueduct.notebook",
                "import cloudpickle",
                "import base64",
                "aqueduct.notebook.AQ_MANAGED_EXECUTION = True",
                f"aqueduct.notebook.AQ_INJECTED_TASK = {task_load_program}",
                f"aqueduct.notebook.AQ_INJECTED_REQUIREMENTS = {req_load_program}",
                f"aqueduct.notebook.AQ_INJECTED_BACKEND_SPEC = {backend_load_program}",
                f"aqueduct.set_config({config_load_program})",
            ]
        )

        _logger.info("Injecting code...")
        response = asyncio.run(
            kernel_client.execute_interactive(
                injected_code,
            ),
            debug=False,
        )
        _logger.info("Done preparing kernel.")

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

    def __str__(self):
        task_name = self.__class__.__qualname__
        return f"{task_name}(notebook={self.notebook()}, export={self.export()})"
