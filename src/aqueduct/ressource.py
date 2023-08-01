from typing import Any, Optional


RESSOURCES: dict[str, Any] = {}


def register(key: str, ressource: Any):
    global RESSOURCES
    RESSOURCES[key] = ressource


def get(key: str, default: Optional[Any] = None) -> Any:
    global RESSOURCES
    return RESSOURCES.get(key, default)
