import aqueduct as aq


class ForeignTask(aq.Task):
    def run(self):
        return 1
