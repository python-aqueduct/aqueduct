import aqueduct as aq


class TaskA(aq.IOTask):
    CONFIG = "taska"

    def __init__(self, b, a=None):
        print("Task A Init")
        self.a = a


aq.set_config({"taska": {"a": 1}})

t = TaskA(2, 3)

print(t.a)
print("In class: ", TaskA.__init__)
print("In instance: ", t.__init__)
