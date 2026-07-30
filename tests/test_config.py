from __future__ import annotations

import json
import os
import stat
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from codex_telegram_bridge.config import (  # noqa: E402
    BridgeConfig,
    default_instance_id,
    read_file_secret,
    read_proton_pass_secret,
)


class PortableConfigTests(unittest.TestCase):
    def test_instance_id_is_stable_and_path_specific(self) -> None:
        first = default_instance_id("/srv/projects/example")
        second = default_instance_id("/srv/projects/example")
        other = default_instance_id("/opt/projects/example")
        self.assertEqual(first, second)
        self.assertNotEqual(first, other)
        self.assertRegex(first, r"^example-[0-9a-f]{10}$")

    def test_json_round_trip_contains_reference_but_never_token(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            workspace = root / "workspace"
            state = root / "state"
            workspace.mkdir()
            config = BridgeConfig.from_paths(
                workspace,
                state,
                instance_id="example",
                secret_backend="proton-pass",
                secret_reference="Codex Telegram Bot - example",
                secret_vault="Work",
            )
            path = state / "config.json"
            state.mkdir()
            path.write_text(
                json.dumps(config.as_file_payload()),
                encoding="utf-8",
            )
            path.chmod(0o600)

            restored = BridgeConfig.from_file(path)

        self.assertEqual(config.workspace, restored.workspace)
        self.assertEqual("proton-pass", restored.secret_backend)
        self.assertEqual("Codex Telegram Bot - example", restored.secret_reference)
        self.assertNotIn("bot_token", config.as_file_payload())
        self.assertNotIn("token", config.as_file_payload())

    def test_max_active_turns_defaults_to_unlimited_and_round_trips(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            workspace = root / "workspace"
            workspace.mkdir()
            default = BridgeConfig.from_paths(workspace, root / "default")
            limited = BridgeConfig.from_paths(
                workspace,
                root / "limited",
                max_active_turns=1,
            )
            path = root / "limited.json"
            path.write_text(
                json.dumps(limited.as_file_payload()),
                encoding="utf-8",
            )
            path.chmod(0o600)
            restored = BridgeConfig.from_file(path)

        self.assertEqual(default.max_active_turns, 0)
        self.assertEqual(limited.max_active_turns, 1)
        self.assertEqual(restored.max_active_turns, 1)

    def test_max_active_turns_can_come_from_environment(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory) / "workspace"
            workspace.mkdir()
            with mock.patch.dict(
                os.environ,
                {"CODEX_TELEGRAM_MAX_ACTIVE_TURNS": "2"},
            ):
                config = BridgeConfig.from_paths(workspace)

        self.assertEqual(config.max_active_turns, 2)

    def test_negative_max_active_turns_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "non-negative integer"):
            BridgeConfig(
                workspace=Path.cwd(),
                state_dir=Path.cwd(),
                max_active_turns=-1,
            )
        with self.assertRaisesRegex(ValueError, "non-negative integer"):
            BridgeConfig(
                workspace=Path.cwd(),
                state_dir=Path.cwd(),
                max_active_turns=True,
            )

    def test_json_config_requires_owner_only_permissions(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.json"
            path.write_text("{}", encoding="utf-8")
            path.chmod(0o644)
            with self.assertRaisesRegex(ValueError, "owner-only"):
                BridgeConfig.from_file(path)

    def test_file_secret_requires_owner_only_regular_file(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "bot-token"
            path.write_text("123:secret\n", encoding="utf-8")
            path.chmod(0o600)
            self.assertEqual("123:secret", read_file_secret(path))
            path.chmod(0o644)
            with self.assertRaisesRegex(RuntimeError, "owner-only"):
                read_file_secret(path)

    def test_proton_pass_lookup_never_places_secret_in_arguments(self) -> None:
        completed = subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout="123456:telegram-secret\n",
            stderr="",
        )
        with (
            mock.patch(
                "codex_telegram_bridge.config.shutil.which",
                return_value="/usr/local/bin/pass-cli",
            ),
            mock.patch(
                "codex_telegram_bridge.config.subprocess.run",
                return_value=completed,
            ) as invoke,
        ):
            secret = read_proton_pass_secret("Telegram Bot", "Work")

        self.assertEqual("123456:telegram-secret", secret)
        arguments = invoke.call_args.args[0]
        self.assertEqual(
            [
                "/usr/local/bin/pass-cli",
                "item",
                "view",
                "--item-title",
                "Telegram Bot",
                "--vault-name",
                "Work",
                "--field",
                "password",
            ],
            arguments,
        )
        self.assertNotIn(secret, arguments)

    def test_symlinked_file_secret_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            target = root / "target"
            link = root / "link"
            target.write_text("secret", encoding="utf-8")
            target.chmod(stat.S_IRUSR | stat.S_IWUSR)
            link.symlink_to(target)
            with self.assertRaisesRegex(RuntimeError, "symlink"):
                read_file_secret(link)


if __name__ == "__main__":
    unittest.main()
