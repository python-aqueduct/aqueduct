import unittest

from aqueduct.artifact import (
    resolve_artifact_from_spec,
    LocalFilesystemArtifact,
)


class TestResolveArtifact(unittest.TestCase):
    def test_str(self):
        spec = "artifact.pkl"
        artifact = resolve_artifact_from_spec(spec)

        self.assertIsInstance(artifact, LocalFilesystemArtifact)
        self.assertEqual(str(artifact.path), spec)

    def test_artifact(self):
        artifact = LocalFilesystemArtifact("./test.toto")

        returned = resolve_artifact_from_spec(artifact)

        self.assertEqual(artifact, returned)

    def test_none(self):
        self.assertIsNone(resolve_artifact_from_spec(None))
