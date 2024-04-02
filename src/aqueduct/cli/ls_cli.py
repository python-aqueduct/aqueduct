import argparse
import inspect

from ..util import tasks_in_module
from .base import resolve_source_modules


def list_tasks(ns: argparse.Namespace):
    modules_per_project = resolve_source_modules(ns)

    for p in modules_per_project:
        print(p)

        for m in modules_per_project[p]:
            print(f"    {m}")

            tasks = tasks_in_module(m)

            for task in tasks:
                task_string = task.__qualname__

                if ns.signature:
                    task_string += str(inspect.signature(task))

                print(f"        {task_string}")


def add_ls_cli_to_parser(parser: argparse.ArgumentParser):
    parser.add_argument(
        "--signature",
        action="store_true",
        help="Print the signature of the task.",
    )
    parser.set_defaults(func=list_tasks)
