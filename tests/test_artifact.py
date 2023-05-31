import io
import inspect
import pandas as pd
import unittest
import unittest.mock

from aqueduct.artifact import (
    ParquetArtifact,
    resolve_artifact_from_spec,
    Artifact,
    PickleArtifact,
)
from aqueduct.store import Store


class TestParquetArtifact(unittest.TestCase):
    def setUp(self) -> None:
        self.name = "test_artifact.parquet"
        self.store: Store = unittest.mock.Mock()
        self.artifact = ParquetArtifact(self.name, store=self.store)

    def test_dump(self):
        stream = io.BytesIO()
        df = pd.DataFrame()
        self.artifact.serialize(df, stream)


def useless_fn(name):
    return name


class TestResolveArtifact(unittest.TestCase):
    def test_str(self):
        spec = "artifact.pkl"
        artifact = resolve_artifact_from_spec(spec, inspect.signature(useless_fn))

        self.assertIsInstance(artifact, Artifact)
        self.assertEqual(artifact.name, spec)

    def test_str_template(self):
        name = "toto"
        spec = "artifact_{name}.pkl"

        artifact = resolve_artifact_from_spec(
            spec, inspect.signature(useless_fn), name=name
        )

        self.assertEqual(spec.format(name=name), artifact.name)
        self.assertIsInstance(artifact, Artifact)

    def test_callable(self):
        def spec(name):
            return PickleArtifact(name)

        artifact = resolve_artifact_from_spec(
            spec, inspect.signature(useless_fn), "toto"
        )

        self.assertEqual("toto", artifact.name)
        self.assertIsInstance(artifact, PickleArtifact)

    def test_artifact(self):
        artifact = PickleArtifact("toto")

        returned = resolve_artifact_from_spec(artifact, inspect.signature(useless_fn))

        self.assertEqual(artifact, returned)

    def test_none(self):
        self.assertIsNone(
            resolve_artifact_from_spec(None, inspect.signature(useless_fn))
        )
