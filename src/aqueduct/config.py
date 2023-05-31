from typing import Any, TypeAlias, TypeVar, TYPE_CHECKING

if TYPE_CHECKING:
    from .task import Task


config: dict[str, Any] = {}


def set_config(cfg):
    global config
    config = cfg


def get_config():
    global config
    return config


Config: TypeAlias = dict[str, Any]
ConfigSpec: TypeAlias = Config | str | None
T = TypeVar("T")


def has_deep_key(d: dict[str, Any], deep_key: str) -> bool:
    keys = deep_key.split(".")

    for k in keys[:-1]:
        if k in d:
            d = d[k]
        else:
            return False

    return keys[-1] in d


def get_deep_key(d: dict[str, Any], deep_key: str) -> Any:
    keys = deep_key.split(".")

    cursor = d

    for k in keys:
        if isinstance(cursor, dict) and k in cursor:
            cursor = cursor[k]
        else:
            raise KeyError("Unable to find key in dict.")

    return cursor


def resolve_config_from_spec(spec: ConfigSpec, calling_task: "Task") -> Config:
    if isinstance(spec, dict):
        return spec

    global_cfg = get_config()

    if isinstance(spec, str) and len(spec) > 0:
        return global_cfg[spec]
    elif spec is None and has_deep_key(
        global_cfg, calling_task._fully_qualified_name()
    ):
        return get_deep_key(global_cfg, calling_task._fully_qualified_name())
    else:
        return {}


def get_aqueduct_config() -> dict:
    cfg = get_config()

    if "aqueduct" in cfg:
        return cfg["aqueduct"]
    else:
        return {}
