from typing import Optional, Type, TypeVar, Any

import base64
import cloudpickle

from .config import Config
from .task import AbstractTask
from .task.util import compute_requirements
from .util import map_task_tree

T = TypeVar("T")

AQ_INJECTED_CONFIG: Optional[Config] = None
AQ_INJECTED_TASK: Optional[AbstractTask] = None
AQ_MAGIC_DEFINED_TASK_CLASS: Optional[Type[AbstractTask]] = None
AQ_INJECTED_REQUIREMENTS: Optional[Any] = None
AQ_MANAGED_EXECUTION: bool = False
AQ_ENCODED_RETURN: Optional[Any] = None


def get_task(*args, **kwargs) -> AbstractTask:
    global AQ_INJECTED_TASK
    global AQ_MAGIC_DEFINED_TASK_CLASS

    if AQ_INJECTED_TASK is None:
        AQ_INJECTED_TASK = AQ_MAGIC_DEFINED_TASK_CLASS(*args, **kwargs)

    return AQ_INJECTED_TASK


def mapper(task: AbstractTask[T]) -> T:
    reqs = task._resolve_requirements()

    if reqs is None:
        return task()
    else:
        return task(reqs)


def get_requirements():
    global AQ_INJECTED_TASK
    global AQ_INJECTED_REQUIREMENTS

    if AQ_INJECTED_REQUIREMENTS is not None:
        return AQ_INJECTED_REQUIREMENTS

    if AQ_INJECTED_TASK is None:
        raise RuntimeError(
            "No task was injected in current context. If you are inside a NotebookTask, make sure to use the %aq_task magic."
        )

    requirements = AQ_INJECTED_TASK._resolve_requirements(ignore_cache=True)

    return map_task_tree(requirements, mapper)


def sink(object):
    global AQ_ENCODED_RETURN

    AQ_ENCODED_RETURN = base64.b64encode(cloudpickle.dumps(object)).decode()
