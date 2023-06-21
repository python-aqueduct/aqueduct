from IPython.core.interactiveshell import InteractiveShell
import importlib

from IPython.core.magic import (
    magics_class,
    line_magic,
    Magics,
)

from IPython.core.magic_arguments import argument, magic_arguments, parse_argstring

from .config import set_config

from . import notebook as notebook_module


@magics_class
class AqueductMagics(Magics):
    @line_magic
    def aq_task(self, line):
        *module_path, classname = line.split(".")

        module_name = ".".join(module_path)

        module = importlib.import_module(module_name)
        task_class = getattr(module, classname)

        notebook_module.AQ_MAGIC_DEFINED_TASK_CLASS = task_class

    @magic_arguments()
    @argument(
        "--module", "-m", help="Name of the module where the Hydra configuration is."
    )
    @argument(
        "--name",
        "-n",
        help="Name of the configuration file to use.",
    )
    @argument("--overrides", "-o", help="Overrides to apply to the config", nargs="+")
    @argument("--version-base", default="1.3", help="Hydra version_base.")
    @line_magic
    def aq_hydra(self, line):
        args = parse_argstring(self.aq_hydra, line)

        import hydra

        with hydra.initialize_config_module(
            args.module, version_base=args.version_base
        ):
            cfg = hydra.compose(args.name, overrides=args.overrides)

        set_config(cfg)


def load_ipython_extension(ipython: InteractiveShell):
    ipython.register_magics(AqueductMagics)


def unload_ipython_extension(ipython):
    pass
