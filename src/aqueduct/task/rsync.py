from typing import Optional

import logging
import pathlib
import subprocess
import tempfile

from .abstract_task import AbstractTask
from ..artifact import LocalFilesystemArtifact
from ..config import get_aqueduct_config
from .task import Task
from ..artifact.util import head_artifacts

logger = logging.getLogger(__name__)


class RsyncTask(Task):
    """Call `rsync` to send the artifacts of an inner task to a remote host. The task
    tree of the inner task is explored, and only the first layer of artifacts is
    synced.

    Args:
        synced: The inner task. The first layer of artifacts produced by that task are
            sent to the remote host.
        target: The target destination in `rync` format.
        source: The source directory on the local filesystem. If `None`, use the value
            of the `aqueduct.local_store` config option.
    """

    def __init__(
        self,
        synced: AbstractTask,
        target: str,
        source: Optional[str] = None,
    ):
        self.synced = synced
        self.target = target

        if source is None:
            cfg = get_aqueduct_config()
            self.source = pathlib.Path(cfg["local_store"])
        else:
            self.source = pathlib.Path(source)

    def requirements(self):
        return self.synced

    def run(self):
        head = head_artifacts(self.synced)

        with tempfile.NamedTemporaryFile("w") as f:
            artifact_paths = []
            for a in head:
                if isinstance(a, LocalFilesystemArtifact):
                    artifact_paths.append(str(a.path.relative_to(self.source)) + "\n")
            f.writelines(artifact_paths)
            f.flush()

            rsync_cmd = self.cmd(f.name)
            cmd_str = " ".join(rsync_cmd)

            logger.info(f"Running external command: {cmd_str}")
            subprocess.run(rsync_cmd, check=True)

    def cmd(self, files_list: str) -> list[str]:
        return [
            "rsync",
            "--files-from",
            files_list,
            str(self.source),
            str(self.target),
        ]
