from typing import Callable, TypeAlias

import omegaconf

ConfigSource: TypeAlias = Callable[[], omegaconf.DictConfig | omegaconf.ListConfig]


class YamlConfigSource:
    def __init__(self, file):
        self.file = file

    def __call__(self):
        return omegaconf.OmegaConf.load(self.file)


class DotListConfigSource:
    def __init__(self, dotlist, section=""):
        self.dotlist = dotlist
        self.section = section

    def __call__(self):
        if not self.section:
            return omegaconf.OmegaConf.from_dotlist(self.dotlist)
        else:
            base = omegaconf.OmegaConf.create()
            inner = omegaconf.OmegaConf.from_dotlist(self.dotlist)
            omegaconf.OmegaConf.update(base, self.section, inner)

            return base
