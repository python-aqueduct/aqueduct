from typing import Type

import inspect
import omegaconf

from ..task import AbstractTask


def root_conf_from_signature(signature: inspect.Signature) -> omegaconf.DictConfig:
    d = {}
    for p in signature.parameters:
        default = signature.parameters[p].default
        d[p] = "???" if default is inspect._empty else default

    return omegaconf.OmegaConf.create(d)


def generate_deep_conf(
    signature: inspect.Signature, task_full_name: str
) -> omegaconf.DictConfig:
    components = task_full_name.split(".")

    # Generate the innermost config (the config for the task parameters)
    task_dict = {p: "${" + p + "}" for p in signature.parameters}

    # Wrap the inner config in successive layers until we reach the desired structure.
    cursor = task_dict
    for c in components[::-1]:
        cursor = {c: cursor}

    return omegaconf.OmegaConf.create(cursor)


class TaskArgsConfigSource:
    def __init__(self, task: Type[AbstractTask]):
        self.TaskClass = task

    def __call__(self):
        signature = inspect.signature(self.TaskClass)

        signature_conf = root_conf_from_signature(signature)
        deep_conf = generate_deep_conf(
            signature, self.TaskClass._fully_qualified_name()
        )

        merged_cfg = omegaconf.OmegaConf.merge(deep_conf, signature_conf)

        return merged_cfg
