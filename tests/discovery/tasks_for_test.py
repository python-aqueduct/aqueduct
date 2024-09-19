import aqueduct as aq


from .other_tasks_for_test import ForeignTask


class RootTask(aq.Task):
    def run(self, requirements):
        [foreign] = requirements
        return foreign

    def requirements(self):
        return [ForeignTask()]
