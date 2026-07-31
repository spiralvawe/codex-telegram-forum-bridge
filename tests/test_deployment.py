from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path

from codex_telegram_bridge.deployment import (
    DeploymentIntegrityError,
    deployment_health,
    package_tree_digest,
    read_deployment_manifest,
    validate_deployment_transition,
)


class DeploymentTests(unittest.TestCase):
    def make_package(self, root: Path, text: str = "value = 1\n") -> Path:
        package = root / "codex_telegram_bridge"
        package.mkdir()
        (package / "__init__.py").write_text(
            '__version__ = "0.4.1"\n', encoding="utf-8"
        )
        (package / "service.py").write_text(text, encoding="utf-8")
        return package

    def test_digest_is_stable_and_detects_runtime_edit(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            package = self.make_package(Path(directory))
            first = package_tree_digest(package)
            (package / "__pycache__").mkdir()
            (package / "__pycache__" / "ignored.pyc").write_bytes(b"cache")
            self.assertEqual(first, package_tree_digest(package))
            (package / "service.py").write_text("value = 2\n", encoding="utf-8")
            self.assertNotEqual(first, package_tree_digest(package))

    def test_transition_refuses_downgrade_and_same_version_drift(self) -> None:
        existing = {
            "packageVersion": "0.4.1",
            "packageDigest": "a" * 64,
        }
        with self.assertRaisesRegex(DeploymentIntegrityError, "older"):
            validate_deployment_transition(
                existing,
                candidate_version="0.4.0",
                candidate_digest="a" * 64,
            )
        with self.assertRaisesRegex(DeploymentIntegrityError, "version bump"):
            validate_deployment_transition(
                existing,
                candidate_version="0.4.1",
                candidate_digest="b" * 64,
            )
        validate_deployment_transition(
            existing,
            candidate_version="0.4.2",
            candidate_digest="b" * 64,
        )

    def test_health_requires_owner_only_matching_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state = root / "state"
            state.mkdir(mode=0o700)
            package = self.make_package(root)
            digest = package_tree_digest(package)
            manifest = state / "deployment-manifest.json"
            manifest.write_text(
                json.dumps(
                    {
                        "formatVersion": 1,
                        "packageVersion": "0.4.1",
                        "packageDigest": digest,
                        "sourceCommit": "c" * 40,
                    }
                ),
                encoding="utf-8",
            )
            os.chmod(manifest, 0o600)
            self.assertTrue(deployment_health(state, package)["deploymentIntegrity"])
            parsed = read_deployment_manifest(state)
            self.assertEqual(parsed["sourceCommit"], "c" * 40)
            (package / "service.py").write_text("tampered = True\n", encoding="utf-8")
            self.assertFalse(deployment_health(state, package)["deploymentIntegrity"])


if __name__ == "__main__":
    unittest.main()
