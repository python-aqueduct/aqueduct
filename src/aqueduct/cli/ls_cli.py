import argparse
import inspect

from aqueduct.task.abstract_task import AbstractTask
from aqueduct.task.functor import Functor

from ..util import tasks_in_module
from .base import resolve_source_modules


def list_tasks(ns: argparse.Namespace):
    modules_per_project = resolve_source_modules(ns)

    for p in modules_per_project:
        print(p)

        for m in modules_per_project[p]:
            print(f"    {m}")

            tasks = tasks_in_module(m, include_functors=True)

            for task in tasks:
                if isinstance(task, Functor):
                    to_print = "*" + task.ui_name()

                elif isinstance(task, AbstractTask):
                    to_print = task.ui_name()

                else:
                    raise TypeError(f"Unknown task type: {type(task)}")

                if ns.signature:
                    to_print += str(inspect.signature(task))
                print(f"        {to_print}")


def add_ls_cli_to_parser(parser: argparse.ArgumentParser):
    parser.add_argument(
        "--signature",
        action="store_true",
        help="Print the signature of the task.",
    )
    parser.set_defaults(func=list_tasks)
