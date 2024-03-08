from typing import Mapping, Iterable, Sequence, Optional, Type

import argparse
import omegaconf

from aqueduct.cli.tasklang import parse_task_spec
from aqueduct.config import set_config

from ..config.configsource import ConfigSource, DotListConfigSource
from ..config.aqueduct import DefaultAqueductConfigSource
from ..task import AbstractTask
from ..taskresolve import get_modules_from_extensions


def resolve_source_modules(ns: argparse.Namespace) -> Mapping[str, Iterable[str]]:
    if ns.module is not None:
        return {"default": [ns.module]}
    else:
        return get_modules_from_extensions()
    

def get_config_sources(
    parameters: Sequence[str],
    overrides: Sequence[str],
    task_class: Optional[Type[AbstractTask]] = None,
    task_config_provider: Optional[ConfigSource] = None,
) -> list[ConfigSource]:
    config_sources: list[ConfigSource] = [DefaultAqueductConfigSource()]

    if task_config_provider is not None:
        config_sources.append(task_config_provider)

    config_sources.append(DotListConfigSource(overrides))

    if task_class is not None:
        config_sources.append(
            DotListConfigSource(parameters, section=task_class._fully_qualified_name())
        )

    return config_sources


def resolve_config(config_sources: Iterable[ConfigSource]) -> omegaconf.DictConfig:
    cfgs = []
    for config_source in config_sources:
        cfg = config_source()
        omegaconf.OmegaConf.set_struct(cfg, False)
        cfgs.append(cfg)

    to_return = omegaconf.OmegaConf.unsafe_merge(*cfgs)
    if not isinstance(to_return, omegaconf.DictConfig):
        raise RuntimeError("Root configuration must be a dictionary.")

    return to_return


def build_task_from_cli_spec(
        spec: list[str],
        name_to_task: Mapping[str, type[AbstractTask]],
        name_to_config: Mapping[str, ConfigSource]) -> AbstractTask:
    if len(spec) >= 1 and spec[0] in name_to_task:
        task_name, *task_args = spec
        TaskClass = name_to_task[task_name]

        task_config_source = name_to_config.get(task_name, None)
        config_sources = get_config_sources(task_args, [], TaskClass, task_config_source)
        cfg = resolve_config(config_sources)
        set_config(cfg)
        root_task = TaskClass()

    else:
        root_task = parse_task_spec(spec[0], name_to_task)

    return root_task

        