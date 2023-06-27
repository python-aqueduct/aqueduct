from typing import Any, TypeAlias, TypeVar, TYPE_CHECKING, Type, Dict, Mapping

import omegaconf as oc

if TYPE_CHECKING:
    from ..task import AbstractTask

OmegaConfig: TypeAlias = oc.DictConfig | oc.ListConfig
config: oc.DictConfig = oc.OmegaConf.create()


def set_config(cfg: OmegaConfig | Dict[Any, Any]):
    global config

    if isinstance(cfg, oc.DictConfig):
        config = cfg
    elif isinstance(cfg, oc.ListConfig):
        raise ValueError("Root config must be a DictConfig")
    else:
        user_config = oc.OmegaConf.create(cfg)
        config = user_config


def get_config() -> oc.DictConfig:
    global config
    return config


AqueductConfig: TypeAlias = oc.DictConfig
ConfigSpec: TypeAlias = AqueductConfig | str | None
T = TypeVar("T")


def has_deep_key(d: oc.DictConfig, deep_key: str) -> bool:
    keys = deep_key.split(".")

    for k in keys[:-1]:
        if k in d:
            d = d[k]
        else:
            return False

    return keys[-1] in d


def get_deep_key(d: Mapping[Any, Any], deep_key: str, default=None) -> Any:
    keys = deep_key.split(".")

    cursor = d

    try:
        for k in keys:
            if k in cursor:
                cursor = cursor[k]
        return cursor
    except (TypeError, KeyError) as e:
        if default:
            return default
        else:
            raise e


def resolve_config_from_spec(
    spec: ConfigSpec, calling_task: Type["AbstractTask"] | "AbstractTask"
) -> AqueductConfig:
    if isinstance(spec, oc.DictConfig):
        return spec
    elif isinstance(spec, dict):
        return oc.OmegaConf.create(spec)  # type: ignore

    global_cfg = get_config()

    if isinstance(spec, str) and len(spec) > 0:
        return get_deep_key(global_cfg, spec, {})
    elif spec is None and has_deep_key(
        global_cfg, calling_task._fully_qualified_name()
    ):
        return get_deep_key(global_cfg, calling_task._fully_qualified_name())
    else:
        return oc.OmegaConf.create({})


def get_aqueduct_config() -> dict:
    cfg = get_config()

    if "aqueduct" in cfg:
        return cfg["aqueduct"]
    else:
        return {}
