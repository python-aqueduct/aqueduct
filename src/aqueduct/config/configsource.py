from typing import Callable, TypeAlias

import omegaconf

ConfigSource: TypeAlias = Callable[[], omegaconf.OmegaConf]


class YamlConfigSource:
    def __init__(self, file):
        self.file = file

    def __call__(self):
        return omegaconf.OmegaConf.load(self.file)


class DotListConfigSource:
    def __init__(self, dotlist):
        self.dotlist = dotlist

    def __call__(self):
        return omegaconf.OmegaConf.from_dotlist(self.dotlist)
