"""Module to define a Task specification language that can be used by the user to
specify a task from command line."""

from typing import Mapping, Type

import importlib
import re


from aqueduct.task.abstract_task import AbstractTask


def parse_task_spec( spec: str, name_to_task: Mapping[str, Type[AbstractTask]]) -> AbstractTask:
    task_name_regex = re.compile("([A-Za-z0-9_]+)\(")
    task_name_matches = task_name_regex.findall(spec)

    locals = {k: name_to_task[k] for k in task_name_matches}

    return eval(spec, {}, locals)


if __name__ == "__main__":
    from ..task.task import Task
    from .. import run

    class TaskA(Task):
        def __init__(self, toto, tata):
            self.value = toto
            self.tata = tata

        def requirements(self):
            return self.tata

        def run(self, requirements):
            return self.value + requirements

    class TaskB(Task):
        def run(self):
            return 3

    task = parse_task_spec(
        "TaskA(toto=3.7234, tata=TaskB())", {}, {"TaskA": TaskA, "TaskB": TaskB}
    )

    print(run(task))
