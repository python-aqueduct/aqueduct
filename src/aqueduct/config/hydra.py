import hydra
import omegaconf


class HydraModuleConfigSource:
    def __init__(self, module_name, config_name):
        self.module_name = module_name
        self.config_name = config_name

    def __call__(self) -> omegaconf.DictConfig:
        with hydra.initialize_config_module(self.module_name, job_name="Aqueduct"):
            cfg = hydra.compose(self.config_name)

        omegaconf.OmegaConf.set_struct(cfg, False)

        return cfg
