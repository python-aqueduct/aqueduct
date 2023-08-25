from typing import Any

from .backend import Backend
from ..task import AbstractTask

from ..task_tree import TaskTree, _resolve_task_tree, _reduce_type_in_tree


def register_task(task: AbstractTask, backend_state):
    key = task._unique_key()

    

    return backend_state


class FullConcurrentBackend(Backend):
    def __init__(self):
        pass

    def execute(self, work: TaskTree) -> Any:
        relationships = _reduce_type_in_tree(
            work,
            AbstractTask,
            register_task,
            {"tasks": {}, "parents": {}, "children": {}},
        )

        tasks = relationships["tasks"]
        parents = relationships["parents"]
        children = relationships["children"]

    def _spec(self) -> str:
        return "full_concurrent"
