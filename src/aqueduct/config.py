config: dict[str] = {}


def set_config(cfg):
    global config
    config = cfg


def get_config():
    global config
    return config
