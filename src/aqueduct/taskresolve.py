from typing import Any, Mapping, Sequence, Type, Optional, Iterable
import importlib.metadata
import logging
import omegaconf

from .task import AbstractTask
from .util import tasks_in_module
from .config.configsource import ConfigSource

_logger = logging.getLogger(__name__)


def create_task_index(
    project_name_to_module_names: Mapping[str, Iterable[str]]
) -> tuple[Mapping[str, Type[AbstractTask]], Mapping[str, ConfigSource]]:
    """Build various indexes for extension modules so that we can recover tasks and
    their configurations."""
    project_of_module_name = {
        module_name: p
        for p in project_name_to_module_names
        for module_name in project_name_to_module_names[p]
    }

    module_name_of_task_class: Mapping[Type[AbstractTask], Any] = {}
    for project in module_names_of_project:
        for module_name in module_names_of_project[project]:
            task_classes = tasks_in_module(module_name)
            for t in task_classes:
                module_name_of_task_class[t] = module_name

    task_class_of_name = {}
    for t in module_name_of_task_class:
        if t.__qualname__ in task_class_of_name:
            _logger.warn(
                f"Found two tasks with non unique names: {t._fully_qualified_name()} "
                f"and {task_class_of_name[t.__qualname__]._fully_qualified_name()}"
            )

        task_class_of_name[t.__qualname__] = t

    config_entry_points = importlib.metadata.entry_points(group="aqueduct_config")
    config_provider_of_project = {ep.name: ep.load() for ep in config_entry_points}

    config_provider_of_name = {
        n: config_provider_of_project[
            project_of_module_name[module_name_of_task_class[task_class_of_name[n]]]
        ]
        for n in task_class_of_name
    }

    return task_class_of_name, config_provider_of_name


def get_modules_from_extensions() -> Mapping[str, Sequence[str]]:
    module_entry_points = importlib.metadata.entry_points(group="aqueduct_modules")
    modules = {}
    for ep in module_entry_points:
        project_name = ep.name
        module_fn = ep.load()
        modules[project_name] = module_fn()

    return modules


def _resolve_task_class_from_modules_dict(
    modules_dict: Mapping[str, Sequence[Type[AbstractTask]]], task_name: str
) -> Type[AbstractTask]:
    for module_name in modules_dict:
        for task in modules_dict[module_name]:
            if task.__qualname__ == task_name:
                return task

    raise KeyError(f"Could not find task with name {task_name}")


def resolve_task_class(task_name: str) -> Type[AbstractTask]:
    modules_per_project = get_modules_from_extensions()
    tasks_per_module = {
        m: tasks_in_module(m)
        for p in modules_per_project
        for m in modules_per_project[p]
    }
    TaskClass = _resolve_task_class_from_modules_dict(tasks_per_module, task_name)

    return TaskClass


def resolve_task_and_project_config_source(
    task_name: str,
) -> tuple[Type[AbstractTask], Optional[ConfigSource]]:
    modules_per_project = get_modules_from_extensions()

    tasks_per_module = {
        m: tasks_in_module(m)
        for p in modules_per_project
        for m in modules_per_project[p]
    }

    TaskClass = _resolve_task_class_from_modules_dict(tasks_per_module, task_name)

    breakpoint()

    return (TaskClass, None)
