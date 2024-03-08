from typing import Any
from dataclasses import dataclass

import omegaconf


@dataclass
class AqueductConfig:
    local_store: str
    backend: Any


class DefaultAqueductConfigSource:
    def __call__(self):
        # aq_config = omegaconf.OmegaConf.structured(AqueductConfig)
        return omegaconf.OmegaConf.create(
            {
                "aqueduct": {
                    "scratch_store": "${oc.env:AQ_SCRATCH_STORE,./}",
                    "local_store": "${oc.env:AQ_LOCAL_STORE,./}",
                    "backend": {"type": "immediate"},
                }
            }
        )
