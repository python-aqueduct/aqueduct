import datetime
from typing import Optional
import unittest

import omegaconf as oc
import pandas as pd

from aqueduct.artifact import (
    Artifact,
    ArtifactSpec,
    InMemoryArtifact,
    CompositeArtifact,
    resolve_artifact_from_spec,
)
from aqueduct.config import set_config, resolve_config_from_spec
from aqueduct.task import (
    IOTask,
    Task,
    AggregateTask,
)

from aqueduct.task.autoresolve import fetch_args_from_config
from aqueduct.task.task import resolve_writer


class TestCompute(unittest.TestCase):
    def test_simple_task(self):
        class SimpleTask(Task):
            def __init__(self, value):
                self.value = value

            def run(self):
                return self.value

        t = SimpleTask(2)
        self.assertEqual(2, t.result())


class PretenseTask(Task):
    def __init__(self, a, b=None, c=12):
        self.a = a
        self.b = b
        self.c = c

    def run(self):
        return self.a + self.b + self.c


class TestFullyQualifiedName(unittest.TestCase):
    def test_fqn(self):
        t = PretenseTask(1, 2, 3)
        self.assertEqual("tests.test_task.PretenseTask", t._fully_qualified_name())


class TestResolveConfig(unittest.TestCase):
    def setUp(self):
        pass

    def test_resolve_dict(self):
        class LocalTask(Task):
            def cfg(self):
                return {}

            def run(self):
                pass

        task = LocalTask()
        self.assertEqual(task.cfg(), oc.OmegaConf.create(task.config()))

    def test_resolve_str(self):
        cfg = {"section": {"value": 2}}
        set_config(cfg)

        class LocalTask(Task):
            CONFIG = "section"

            def run(self):
                pass

        task = LocalTask()

        self.assertEqual(oc.OmegaConf.create({"value": 2}), task.config())

    def test_resolve_object_name_class(self):
        inner_dict = {"a": 1, "b": 2}
        set_config({"tests": {"test_task": {"PretenseTask": inner_dict}}})

        t = PretenseTask(14)
        config = resolve_config_from_spec(None, t)

        self.assertEqual(config, inner_dict)


class TestFetchArgs(unittest.TestCase):
    def test_empty_config(self):
        def fn(a, b=None):
            self.assertEqual(a, 2)
            self.assertIsNone(b)

        new_args, new_kwargs = fetch_args_from_config({}, fn, 2)

        fn(*new_args, **new_kwargs)

    def test_should_use_config(self):
        def fn(a, b=None):
            self.assertEqual(b, 13)
            self.assertEqual(a, 14)

        new_args, new_kwargs = fetch_args_from_config({"b": 13}, fn, 14)

        fn(*new_args, **new_kwargs)


class TestFetchArgsOnCall(unittest.TestCase):
    def tearDown(self):
        set_config({})

    def test_missing_params(self):
        inner_dict = {"a": 2, "b": 3}
        set_config({"tests": {"test_task": {"PretenseTask": inner_dict}}})

        t = PretenseTask(3)
        self.assertEqual(18, t.result())


store = {}


class StoringTask(IOTask):
    def __init__(self, should_succeed=True):
        self.succeed = should_succeed

    def run(self):
        if self.succeed:
            store["testkey"] = 1

    def artifact(self):
        return InMemoryArtifact("testkey", store)


class TestStorageCheck(unittest.TestCase):
    def tearDown(self) -> None:
        set_config({})

    def test_storage_success(self):
        set_config({"aqueduct": {"check_storage": True}})

        t = StoringTask()
        t.result()

        self.assertTrue(t.artifact().exists())

    def test_storage_failure(self):
        set_config({"aqueduct": {"check_storage": True}})
        t = StoringTask(should_succeed=False)

        self.assertRaises(RuntimeError, lambda: t.result())


class TestTaskIO(unittest.TestCase):
    def test_resolve_writer(self):
        writer = resolve_writer(pd.DataFrame)
        self.assertEqual(writer.__name__, "write_to_parquet")


class TaskWithArtifact(Task):
    def __init__(self):
        self.store = {}
        super().__init__()

    def artifact(self):
        return InMemoryArtifact("test", self.store)


class TaskWithoutArtifact(Task):
    pass


class TestAggregateTask(unittest.TestCase):
    def setUp(self):
        self.store = {}

    def test_empty_artifacts(self):
        class Aggregate(AggregateTask):
            def requirements(self):
                return [TaskWithoutArtifact(), TaskWithoutArtifact()]

        t = Aggregate()
        self.assertIsNone(t.artifact())

    def test_with_artifacts(self):
        class Aggregate(AggregateTask):
            def requirements(self):
                return [TaskWithArtifact(), TaskWithArtifact()]

        t = Aggregate()
        spec = t.artifact()
        assert spec is not None
        resolved_artifact = resolve_artifact_from_spec(spec)

        self.assertIsInstance(resolved_artifact, CompositeArtifact)
        self.assertEqual(2, len(resolved_artifact.artifacts))


class FixedDateArtifact(Artifact):
    def __init__(self, date):
        self.date = date

    def last_modified(self):
        return self.date

    def exists(self):
        return True

    def size(self):
        return 0


class TaskDependsOnDate(Task):
    def requirements(self):
        return {"nested": TaskWithDate()}

    def artifact(self):
        return FixedDateArtifact(datetime.datetime(2021, 1, 1))


class TaskNoDate(Task):
    pass


class TaskWithDate(Task):
    AQ_UPDATED = "2022-01-01"

    def artifact(self):
        return FixedDateArtifact(datetime.datetime(2023, 1, 1))


class FarDepOnDate(Task):
    def requirements(self):
        return TaskDependsOnDate()


class TestUpdateTime(unittest.TestCase):
    def test_own(self):
        t = TaskWithDate()
        self.assertEqual(datetime.datetime(2022, 1, 1), t._resolve_update_time())

    def test_dependencies(self):
        t = TaskDependsOnDate()
        self.assertEqual(datetime.datetime(2022, 1, 1), t._resolve_update_time())

    def test_stale_cache(self):
        t = TaskDependsOnDate()
        self.assertFalse(t.is_cached())

    def test_good_cache(self):
        t = TaskWithDate()
        self.assertTrue(t.is_cached())

    def test_doubly_nested_date(self):
        t = FarDepOnDate()
        self.assertEqual(datetime.datetime(2022, 1, 1), t._resolve_update_time())
