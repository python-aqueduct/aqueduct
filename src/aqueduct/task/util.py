from typing import Any

from .abstract_task import AbstractTask
from ..task_tree import _map_tasks_in_tree


def execute_task(task: AbstractTask):
    if task.is_cached():
        return task()

    requirements = task._resolve_requirements()

    if requirements is None:
        return task()
    else:
        mapped_requirements = _map_tasks_in_tree(requirements, execute_task)
        return task(mapped_requirements)
