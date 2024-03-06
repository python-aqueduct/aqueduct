from typing import Any
from dataclasses import dataclass, field

import omegaconf

from ..backend.base import BackendDictSpec


@dataclass
class AqueductConfig:
    local_store: str
    backend: Any
    check_storage: bool = False


class DefaultAqueductConfigSource:
    def __call__(self):
        # aq_config = omegaconf.OmegaConf.structured(AqueductConfig)
        return omegaconf.OmegaConf.create(
            {"aqueduct": {"local_store": "./", "backend": {"type": "immediate"}}}
        )
