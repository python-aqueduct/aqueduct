from typing import Optional, Type, TypeVar, Any

import base64
import cloudpickle
import logging

from .backend import BackendSpec, resolve_backend_from_spec, Backend
from .config import Config
from .task import AbstractTask
from .util import map_task_tree

T = TypeVar("T")

_logger = logging.getLogger(__name__)

AQ_INJECTED_BACKEND_SPEC: Optional[Backend] = None
AQ_INJECTED_CONFIG: Optional[Config] = None
AQ_INJECTED_TASK: Optional[AbstractTask] = None
AQ_MAGIC_DEFINED_TASK_CLASS: Optional[Type[AbstractTask]] = None
AQ_INJECTED_REQUIREMENTS: Optional[Any] = None
AQ_MANAGED_EXECUTION: bool = False
AQ_ENCODED_RETURN: Optional[Any] = None


def get_task(*args, **kwargs) -> AbstractTask:
    global AQ_MANAGED_EXECUTION
    global AQ_INJECTED_TASK
    global AQ_MAGIC_DEFINED_TASK_CLASS

    if AQ_MANAGED_EXECUTION:
        if AQ_INJECTED_TASK is None:
            raise RuntimeError("Managed execution with no injected task.")

        return AQ_INJECTED_TASK
    else:
        if AQ_MAGIC_DEFINED_TASK_CLASS is None:
            raise RuntimeError(
                "Unable to get task because the task class was not provided. Please use"
                "%aq_task magic."
            )

        AQ_INJECTED_TASK = AQ_MAGIC_DEFINED_TASK_CLASS(*args, **kwargs)

    return AQ_INJECTED_TASK


def mapper(task: AbstractTask[T]) -> T:
    reqs = task._resolve_requirements()

    if reqs is None:
        return task()
    else:
        return task(reqs)


def get_requirements(backend: Optional[BackendSpec] = None):
    global AQ_INJECTED_TASK
    global AQ_INJECTED_REQUIREMENTS

    if AQ_INJECTED_REQUIREMENTS is not None:
        _logger.info("Using injected requirements.")
        return AQ_INJECTED_REQUIREMENTS

    if AQ_INJECTED_TASK is None:
        raise RuntimeError(
            "No task was injected in current context. If you are inside a NotebookTask, make sure to use the %aq_task magic."
        )

    requirement_spec = AQ_INJECTED_TASK.requirements()
    if requirement_spec is None:
        raise RuntimeError("No requirements specified for task.")

    requirements = AQ_INJECTED_TASK._resolve_requirements(ignore_cache=True)
    backend = resolve_backend_from_spec(backend)

    return backend.execute(requirements)


def get_backend(default: BackendSpec):
    if AQ_INJECTED_BACKEND_SPEC is not None:
        print(AQ_INJECTED_BACKEND_SPEC)
        return resolve_backend_from_spec(AQ_INJECTED_BACKEND_SPEC)
    else:
        return resolve_backend_from_spec(default)


def sink(object):
    global AQ_ENCODED_RETURN

    AQ_ENCODED_RETURN = base64.b64encode(cloudpickle.dumps(object)).decode()
