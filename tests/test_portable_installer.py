from __future__ import annotations

import plistlib
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import installer  # noqa: E402
from codex_telegram_bridge.config import BridgeConfig  # noqa: E402


class PortableInstallerTests(unittest.TestCase):
    def make_config(self, root: Path) -> BridgeConfig:
        workspace = root / "workspace with spaces"
        state = root / "state"
        workspace.mkdir()
        state.mkdir(mode=0o700)
        config = BridgeConfig(
            workspace=workspace,
            state_dir=state,
            instance_id="example-a1b2c3",
            secret_backend="file",
            secret_reference=str(state / "secrets" / "bot-token"),
            codex_binary="/usr/local/bin/codex",
            ffmpeg_binary="/usr/local/bin/ffmpeg",
        )
        installer.atomic_json(
            installer.config_path(config),
            config.as_file_payload(),
        )
        return config

    def test_instance_validation_is_service_safe(self) -> None:
        self.assertEqual("project-one", installer.validate_instance("Project One"))
        with self.assertRaises(Exception):
            installer.validate_instance("---")

    def test_prepare_parser_accepts_pi_safe_turn_limit(self) -> None:
        args = installer.build_parser().parse_args(
            [
                "prepare",
                "--workspace",
                "/srv/project",
                "--max-active-turns",
                "1",
            ]
        )

        self.assertEqual(args.max_active_turns, 1)

    def test_prepare_parser_accepts_explicit_codex_full_access(self) -> None:
        args = installer.build_parser().parse_args(
            [
                "prepare",
                "--workspace",
                "/srv/project",
                "--codex-full-access",
            ]
        )

        self.assertTrue(args.codex_full_access)

    def test_prepare_persists_explicit_codex_full_access(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            workspace = root / "workspace"
            state = root / "state"
            workspace.mkdir()
            args = installer.build_parser().parse_args(
                [
                    "prepare",
                    "--workspace",
                    str(workspace),
                    "--state-dir",
                    str(state),
                    "--secret-backend",
                    "file",
                    "--codex-binary",
                    "/bin/true",
                    "--codex-full-access",
                    "--skip-app-server-bootstrap",
                ]
            )
            with (
                mock.patch("installer.prepare_runtime"),
                mock.patch(
                    "installer.shutil.which",
                    return_value="/bin/true",
                ),
            ):
                result = installer.prepare(args)
            config = BridgeConfig.from_file(result["config"])

        self.assertTrue(config.codex_full_access)

    def test_systemd_working_directory_uses_directive_specific_syntax(
        self,
    ) -> None:
        self.assertEqual(
            "/srv/workspace with spaces/100%% ready",
            installer.systemd_working_directory(
                "/srv/workspace with spaces/100% ready"
            ),
        )
        self.assertEqual(
            "/srv/trailing space /",
            installer.systemd_working_directory("/srv/trailing space "),
        )
        self.assertEqual(
            "/srv/trailing\\/",
            installer.systemd_working_directory("/srv/trailing\\"),
        )
        for invalid in (
            "relative/path",
            "/srv/line\nbreak",
            "/srv/line\rbreak",
            "/srv/nul\x00byte",
        ):
            with self.subTest(invalid=repr(invalid)):
                with self.assertRaises(installer.InstallerError):
                    installer.systemd_working_directory(invalid)

    def test_systemd_units_contain_no_bot_token(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            home = root / "home"
            home.mkdir()
            config = self.make_config(root)
            with (
                mock.patch.object(Path, "home", return_value=home),
                mock.patch("installer.shutil.which", return_value="/bin/systemctl"),
                mock.patch("installer.run") as invoke,
            ):
                services = installer.install_systemd(config)

            unit_dir = home / ".config" / "systemd" / "user"
            service_text = (unit_dir / services[0]).read_text(encoding="utf-8")
            health_text = (
                unit_dir
                / "codex-telegram-bridge-example-a1b2c3-health.service"
            ).read_text(encoding="utf-8")
            backup_text = (
                unit_dir
                / "codex-telegram-bridge-example-a1b2c3-backup.service"
            ).read_text(encoding="utf-8")
            backup_timer_text = (
                unit_dir
                / "codex-telegram-bridge-example-a1b2c3-backup.timer"
            ).read_text(encoding="utf-8")
            health_timer_text = (
                unit_dir
                / "codex-telegram-bridge-example-a1b2c3-health.timer"
            ).read_text(encoding="utf-8")

        expected_working_directory = (
            f"WorkingDirectory={config.workspace}\n"
        )
        self.assertIn(expected_working_directory, service_text)
        self.assertIn(expected_working_directory, health_text)
        self.assertNotIn(
            f'WorkingDirectory="{config.workspace}"',
            service_text,
        )
        self.assertIn("config.json", service_text)
        self.assertNotIn("bot-token", service_text)
        self.assertNotIn("bot-token", health_text)
        self.assertNotIn("bot-token", backup_text)
        self.assertIn("Type=notify", service_text)
        self.assertIn("Restart=always", service_text)
        self.assertIn("StartLimitIntervalSec=0", service_text)
        self.assertIn("WatchdogSec=120", service_text)
        self.assertIn("WatchdogSignal=SIGKILL", service_text)
        self.assertNotIn("network-online.target", service_text)
        self.assertIn(" probe-local", health_text)
        self.assertNotIn(" doctor", health_text)
        self.assertIn("TimeoutStartSec=120", health_text)
        self.assertIn("OnUnitInactiveSec=5min", health_timer_text)
        self.assertNotIn("Persistent=true", health_timer_text)
        self.assertIn("backup --retention 96", backup_text)
        self.assertIn("OnCalendar=*:0/30", backup_timer_text)
        self.assertIn("Persistent=true", backup_timer_text)
        self.assertEqual(
            services[2],
            "codex-telegram-bridge-example-a1b2c3-backup.timer",
        )
        self.assertGreaterEqual(invoke.call_count, 3)

    @unittest.skipUnless(
        sys.platform.startswith("linux") and shutil.which("systemd-analyze"),
        "systemd-analyze is available only on Linux hosts",
    )
    def test_generated_systemd_units_pass_systemd_analyze_verify(self) -> None:
        analyzer = shutil.which("systemd-analyze")
        assert analyzer is not None
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            home = root / "home"
            home.mkdir()
            config = self.make_config(root)
            cli = installer.runtime_cli(config)
            cli.parent.mkdir(parents=True)
            cli.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
            cli.chmod(0o755)
            with (
                mock.patch.object(Path, "home", return_value=home),
                mock.patch(
                    "installer.shutil.which",
                    return_value="/bin/systemctl",
                ),
                mock.patch("installer.run"),
            ):
                installer.install_systemd(config)

            unit_dir = home / ".config" / "systemd" / "user"
            unit_paths = sorted(unit_dir.glob("*.service"))
            unit_paths.extend(sorted(unit_dir.glob("*.timer")))
            result = subprocess.run(
                [analyzer, "verify", *map(str, unit_paths)],
                check=False,
                text=True,
                capture_output=True,
            )

        self.assertEqual(
            0,
            result.returncode,
            msg=f"{result.stdout}\n{result.stderr}".strip(),
        )

    def test_launchd_plists_are_generated_per_instance(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            home = root / "home"
            home.mkdir()
            config = self.make_config(root)
            with (
                mock.patch.object(Path, "home", return_value=home),
                mock.patch("installer.shutil.which", return_value="/bin/launchctl"),
                mock.patch("installer.run"),
            ):
                labels = installer.install_launchd(config)

            plist_path = (
                home / "Library" / "LaunchAgents" / f"{labels[0]}.plist"
            )
            with plist_path.open("rb") as stream:
                payload = plistlib.load(stream)
            backup_path = (
                home / "Library" / "LaunchAgents" / f"{labels[2]}.plist"
            )
            with backup_path.open("rb") as stream:
                backup_payload = plistlib.load(stream)
            health_path = (
                home / "Library" / "LaunchAgents" / f"{labels[1]}.plist"
            )
            with health_path.open("rb") as stream:
                health_payload = plistlib.load(stream)

        self.assertEqual(labels[0], payload["Label"])
        self.assertIn("config.json", " ".join(payload["ProgramArguments"]))
        self.assertNotIn("bot-token", repr(payload))
        self.assertIs(payload["KeepAlive"], True)
        self.assertEqual(payload["ThrottleInterval"], 10)
        self.assertEqual(
            payload["ProgramArguments"][-1],
            "serve",
        )
        self.assertEqual(health_payload["ProgramArguments"][-1], "probe-local")
        self.assertEqual(
            backup_payload["ProgramArguments"][-3:],
            ["backup", "--retention", "96"],
        )
        self.assertEqual(backup_payload["StartInterval"], 1800)

    def test_linux_deactivate_removes_services_but_retains_state(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            home = root / "home"
            unit_dir = home / ".config" / "systemd" / "user"
            unit_dir.mkdir(parents=True)
            config = self.make_config(root)
            names = (
                "codex-telegram-bridge-example-a1b2c3.service",
                "codex-telegram-bridge-example-a1b2c3-health.service",
                "codex-telegram-bridge-example-a1b2c3-health.timer",
                "codex-telegram-bridge-example-a1b2c3-backup.service",
                "codex-telegram-bridge-example-a1b2c3-backup.timer",
            )
            for name in names:
                (unit_dir / name).write_text("test\n", encoding="utf-8")
            args = mock.Mock(config=str(installer.config_path(config)))
            with (
                mock.patch.object(Path, "home", return_value=home),
                mock.patch("installer.sys.platform", "linux"),
                mock.patch("installer.run"),
            ):
                result = installer.deactivate(args)

            self.assertTrue(result["ok"])
            self.assertEqual(
                config.state_dir.resolve(),
                Path(result["stateRetained"]).resolve(),
            )
            self.assertTrue(config.state_dir.is_dir())
            self.assertTrue(all(not (unit_dir / name).exists() for name in names))

    def test_codex_setup_contract_and_skill_are_present(self) -> None:
        contract = (ROOT / "SETUP_WITH_CODEX.md").read_text(encoding="utf-8")
        skill = (
            ROOT
            / "skills"
            / "codex-telegram-bootstrap"
            / "SKILL.md"
        ).read_text(encoding="utf-8")
        self.assertIn("Never ask the user to paste the bot token", contract)
        self.assertIn("one new Telegram bot token", contract)
        self.assertIn("SETUP_WITH_CODEX.md", skill)


if __name__ == "__main__":
    unittest.main()
