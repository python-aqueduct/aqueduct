import functools

from typing import Dict, Iterable, Tuple, Union

from .artifact import Artifact


class Binding:
    def __init__(self, fn, *args, **kwargs):
        self.fn = fn
        self.args = args
        self.kwargs = kwargs

    def compute(self):
        return self.fn(*self.args, **self.kwargs)

    def __call__(self, *args, **kwargs):
        return Binding(self.compute, *args, **kwargs)


RequirementSpec = Union[
    tuple[Binding], list[Binding], dict[str, Binding], Binding, None
]


def taskdef(requirements: RequirementSpec = None, artifact: Artifact = None, **kwargs):
    def wrapper(fn):
        return WrappedTask(
            fn,
            requirements=requirements,
            artifact=artifact,
        )

    return wrapper


class Task:
    def __call__(self, *args, **kwargs):
        artifact = self.artifact()

        if artifact and artifact.exists():
            """Exclude the dependencies from the graph to avoid computing them."""
            return Binding(artifact.load_from_store)

        requirements = self.requirements()

        if isinstance(requirements, Binding) or isinstance(requirements, list):
            return Binding(self.run_and_maybe_cache, requirements, *args, **kwargs)
        elif isinstance(requirements, dict):
            return Binding(self.run_and_maybe_cache, *args, **requirements, **kwargs)
        elif isinstance(requirements, tuple):
            return Binding(self.run_and_maybe_cache, *requirements, *args, **kwargs)
        elif not requirements:
            return Binding(self.run_and_maybe_cache, *args, **kwargs)
        else:
            raise Exception("Unexpected case when building Binding.")

    @property
    def artifact(self):
        return None

    def requirements(self) -> RequirementSpec:
        return None

    def run_and_maybe_cache(self, *args, **kwargs):
        result = self.run(*args, **kwargs)

        artifact = self.artifact()
        if artifact:
            artifact.dump_to_store(result)

        return result

    def run(self):
        raise NotImplementedError("Task must implement method `run`.")


class WrappedTask(Task):
    def __init__(self, fn, requirements: RequirementSpec = None, artifact=None):
        self.fn = fn
        self._artifact = artifact
        self._requirements = requirements

    def run(self, *args, **kwargs):
        return self.fn(*args, **kwargs)

    def artifact(self):
        return self._artifact

    def requirements(self):
        return self._requirements


# def normalize_requirements(
#     requirements: RequirementSpec,
# ) -> tuple[tuple[Binding], dict[str, Binding]]:
#     if (
#         (isinstance(requirements, tuple) or isinstance(requirements, list))
#         and len(requirements) == 2
#         and isinstance(requirements[1], dict)
#         and (isinstance(requirements[0], list) or isinstance(requirements[0], tuple))
#     ):
#         return requirements
#     elif isinstance(requirements, dict):
#         return (), requirements
#     elif isinstance(requirements, tuple):
#         return requirements, {}
#     else:
#         return tuple([requirements]), {}
