from typing import cast

import pathlib
import unittest

from aqueduct.artifact import (
    resolve_artifact_from_spec,
    LocalFilesystemArtifact,
    LocalStoreArtifact,
)

import aqueduct as aq


class TestResolveArtifact(unittest.TestCase):
    def test_str(self):
        spec = "artifact.pkl"
        artifact = cast(LocalFilesystemArtifact, resolve_artifact_from_spec(spec))

        self.assertIsInstance(artifact, LocalFilesystemArtifact)
        self.assertEqual(str(artifact.path), spec)

    def test_artifact(self):
        artifact = LocalFilesystemArtifact("./test.toto")

        returned = resolve_artifact_from_spec(artifact)

        self.assertEqual(artifact, returned)

    def test_none(self):
        self.assertIsNone(resolve_artifact_from_spec(None))


class TestLocalStoreArtifact(unittest.TestCase):
    def setUp(self):
        aq.set_config({})

    def test_unchanged_when_no_config(self):
        rel_path = "test/dir"
        a = LocalStoreArtifact(rel_path)
        self.assertEqual(pathlib.Path(rel_path), a.path)

    def test_use_config_on_relative(self):
        local_store_path = "/test/path"
        aq.set_config({"aqueduct": {"local_store": local_store_path}})

        rel_path = "test/dir"
        a = LocalStoreArtifact(rel_path)
        self.assertEqual(pathlib.Path(local_store_path) / rel_path, a.path)

    def test_avoid_config_on_absolute(self):
        local_store_path = "/test/path"
        aq.set_config({"aqueduct": {"local_store": local_store_path}})

        abs_path = "/my/dir"
        a = LocalStoreArtifact(abs_path)
        self.assertEqual(pathlib.Path(abs_path), a.path)
