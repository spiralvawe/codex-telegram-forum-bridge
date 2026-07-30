from __future__ import annotations

import ast
import contextlib
import io
import json
import os
import plistlib
import signal
import ssl
import stat
import subprocess
import sys
import tempfile
import threading
import unittest
from dataclasses import fields
from pathlib import Path
from types import SimpleNamespace
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from codex_telegram_bridge.media_worker_cli import (  # noqa: E402
    _executable_path,
    main,
    probe_configuration,
    render_launchd,
    render_systemd,
    serve_worker,
)
from codex_telegram_bridge.media_worker_config import (  # noqa: E402
    FORBIDDEN_CONFIG_KEY_FRAGMENTS,
    MediaWorkerConfig,
    MediaWorkerConfigError,
)


class MediaWorkerFixture:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.state_dir = root / "worker-state"
        self.tls_dir = root / "worker-tls"
        self.state_dir.mkdir(mode=0o700)
        self.tls_dir.mkdir(mode=0o700)
        self.ffmpeg = Path("/usr/bin/true")
        self.server_cert = self._private_file("server.crt", "certificate")
        self.server_key = self._private_file(
            "server.key",
            "private-key-marker",
        )
        self.client_ca = self._private_file("client-ca.crt", "client-ca")
        self.config_path = root / "worker.json"
        self.payload: dict[str, object] = {
            "listen_host": "127.0.0.1",
            "listen_port": 9443,
            "state_dir": str(self.state_dir),
            "ffmpeg_binary": str(self.ffmpeg),
            "tls_server_cert": str(self.server_cert),
            "tls_server_key": str(self.server_key),
            "tls_client_ca": str(self.client_ca),
            "queue_capacity": 4,
            "concurrency": 1,
            "request_timeout_seconds": 30,
            "processing_timeout_seconds": 120,
            "shutdown_timeout_seconds": 15,
            "retention_seconds": 3600,
        }
        self.write_config()

    def _private_file(self, name: str, value: str) -> Path:
        path = self.tls_dir / name
        path.write_text(value, encoding="utf-8")
        path.chmod(0o600)
        return path

    def write_config(self) -> None:
        self.config_path.write_text(
            json.dumps(self.payload),
            encoding="utf-8",
        )
        self.config_path.chmod(0o600)


class MediaWorkerConfigTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.fixture = MediaWorkerFixture(Path(self.temporary.name))

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_owner_only_config_loads_bounded_worker_fields(self) -> None:
        config = MediaWorkerConfig.from_file(self.fixture.config_path)
        context = object()
        processor = object()

        self.assertEqual(config.listen_host, "127.0.0.1")
        self.assertEqual(config.listen_port, 9443)
        self.assertEqual(config.queue_capacity, 4)
        self.assertEqual(config.concurrency, 1)
        self.assertEqual(
            set(
                config.server_keyword_arguments(
                    ssl_context=context,
                    processor=processor,
                )
            ),
            {
                "host",
                "port",
                "ssl_context",
                "spool_directory",
                "processor",
                "queue_capacity",
                "processing_concurrency",
                "request_timeout_seconds",
                "shutdown_timeout_seconds",
                "ttl_seconds",
            },
        )

    def test_config_file_must_be_owner_only_regular_file(self) -> None:
        self.fixture.config_path.chmod(0o640)
        with self.assertRaises(MediaWorkerConfigError):
            MediaWorkerConfig.from_file(self.fixture.config_path)

        self.fixture.config_path.unlink()
        self.fixture.config_path.symlink_to(
            self.fixture.root / "actual-worker.json"
        )
        with self.assertRaises(MediaWorkerConfigError):
            MediaWorkerConfig.from_file(self.fixture.config_path)

    def test_integration_and_privilege_fields_are_rejected(self) -> None:
        forbidden_fields = (
            "telegram_bot_token",
            "codex_binary",
            "proton_pass_item",
            "ssh_username",
            "sudo_command",
            "workspace",
            "permission_profile",
        )
        for field in forbidden_fields:
            with self.subTest(field=field):
                payload = dict(self.fixture.payload)
                payload[field] = "must-not-be-accepted"
                with self.assertRaises(MediaWorkerConfigError):
                    MediaWorkerConfig.from_mapping(payload)

    def test_runtime_paths_reject_symlinks_and_bridge_state(self) -> None:
        actual_key = self.fixture.server_key
        linked_key = self.fixture.tls_dir / "linked.key"
        linked_key.symlink_to(actual_key)
        payload = dict(self.fixture.payload)
        payload["tls_server_key"] = str(linked_key)
        with self.assertRaises(MediaWorkerConfigError):
            MediaWorkerConfig.from_mapping(payload)

        (self.fixture.state_dir / "bridge.sqlite3").touch(mode=0o600)
        with self.assertRaises(MediaWorkerConfigError):
            MediaWorkerConfig.from_mapping(self.fixture.payload)

    def test_runtime_paths_reject_replaceable_ancestors(self) -> None:
        replaceable = self.fixture.root / "replaceable"
        replaceable.mkdir(mode=0o700)
        state = replaceable / "state"
        state.mkdir(mode=0o700)
        replaceable.chmod(0o777)
        payload = dict(self.fixture.payload)
        payload["state_dir"] = str(state)
        try:
            with self.assertRaisesRegex(
                MediaWorkerConfigError,
                "replaceable",
            ):
                MediaWorkerConfig.from_mapping(payload)
        finally:
            replaceable.chmod(0o700)

    def test_state_and_numeric_limits_fail_closed(self) -> None:
        self.fixture.state_dir.chmod(0o750)
        with self.assertRaises(MediaWorkerConfigError):
            MediaWorkerConfig.from_mapping(self.fixture.payload)
        self.fixture.state_dir.chmod(0o700)

        payload = dict(self.fixture.payload)
        payload["listen_port"] = 443
        with self.assertRaises(MediaWorkerConfigError):
            MediaWorkerConfig.from_mapping(payload)

        payload = dict(self.fixture.payload)
        payload["concurrency"] = 5
        with self.assertRaises(MediaWorkerConfigError):
            MediaWorkerConfig.from_mapping(payload)

        payload = dict(self.fixture.payload)
        payload["queue_capacity"] = 17
        with self.assertRaises(MediaWorkerConfigError):
            MediaWorkerConfig.from_mapping(payload)

        payload = dict(self.fixture.payload)
        payload["request_timeout_seconds"] = 61
        with self.assertRaises(MediaWorkerConfigError):
            MediaWorkerConfig.from_mapping(payload)

        payload = dict(self.fixture.payload)
        payload["processing_timeout_seconds"] = 301
        with self.assertRaises(MediaWorkerConfigError):
            MediaWorkerConfig.from_mapping(payload)

        for host in ("0.0.0.0", "::"):
            with self.subTest(host=host):
                payload = dict(self.fixture.payload)
                payload["listen_host"] = host
                config = MediaWorkerConfig.from_mapping(payload)
                self.assertEqual(config.listen_host, host)

        for host in ("8.8.8.8", "224.0.0.1"):
            with self.subTest(host=host):
                payload = dict(self.fixture.payload)
                payload["listen_host"] = host
                with self.assertRaises(MediaWorkerConfigError):
                    MediaWorkerConfig.from_mapping(payload)

    def test_schema_has_no_bridge_or_privilege_access_fields(self) -> None:
        schema_fields = {field.name.casefold() for field in fields(
            MediaWorkerConfig
        )}
        for field in schema_fields:
            self.assertFalse(
                any(
                    fragment in field
                    for fragment in FORBIDDEN_CONFIG_KEY_FRAGMENTS
                ),
                field,
            )


class MediaWorkerCliTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.fixture = MediaWorkerFixture(Path(self.temporary.name))
        self.config = MediaWorkerConfig.from_file(
            self.fixture.config_path
        )
        self.service_python = Path("/usr/bin/python3")
        if (
            not self.service_python.is_file()
            or self.service_python.stat().st_uid != 0
        ):
            self.skipTest("a root-owned system Python is required")
        self.account_lookup = mock.patch(
            "codex_telegram_bridge.media_worker_cli.pwd.getpwnam",
            return_value=SimpleNamespace(
                pw_uid=os.geteuid(),
                pw_gid=499,
                pw_shell="/usr/bin/false",
            ),
        )
        self.group_lookup = mock.patch(
            "codex_telegram_bridge.media_worker_cli.os.getgrouplist",
            return_value=[499],
        )
        self.account_lookup.start()
        self.group_lookup.start()

    def tearDown(self) -> None:
        self.group_lookup.stop()
        self.account_lookup.stop()
        self.temporary.cleanup()

    def test_probe_is_read_only_and_does_not_expose_paths_or_key_data(
        self,
    ) -> None:
        before = sorted(
            str(path.relative_to(self.fixture.root))
            for path in self.fixture.root.rglob("*")
        )
        with mock.patch(
            "codex_telegram_bridge.media_worker_cli.validate_tls_material"
        ):
            result = probe_configuration(self.config)
        after = sorted(
            str(path.relative_to(self.fixture.root))
            for path in self.fixture.root.rglob("*")
        )

        self.assertTrue(result["ok"])
        self.assertTrue(result["readOnlyProbe"])
        self.assertFalse(result["integrationCredentialsConfigured"])
        self.assertEqual(before, after)
        serialized = json.dumps(result)
        self.assertNotIn(str(self.fixture.server_key), serialized)
        self.assertNotIn("private-key-marker", serialized)

    def test_launchd_render_is_pure_and_has_restart_controls(self) -> None:
        rendered = render_launchd(
            config_path=self.fixture.config_path,
            python_executable=self.service_python,
            service_user="_codexmedia",
        )
        payload = plistlib.loads(rendered.encode("utf-8"))

        self.assertTrue(payload["KeepAlive"])
        self.assertTrue(payload["RunAtLoad"])
        self.assertEqual(payload["ThrottleInterval"], 10)
        self.assertEqual(payload["Umask"], 0o077)
        self.assertEqual(payload["ProgramArguments"][-1], "serve")
        self.assertEqual(
            payload["EnvironmentVariables"],
            {
                "PYTHONDONTWRITEBYTECODE": "1",
                "PYTHONUNBUFFERED": "1",
            },
        )
        self.assertEqual(payload["StandardOutPath"], "/dev/null")
        self.assertEqual(payload["StandardErrorPath"], "/dev/null")
        self.assertEqual(
            payload["HardResourceLimits"]["FileSize"],
            32 * 1024 * 1024,
        )
        self.assertEqual(
            payload["HardResourceLimits"]["NumberOfProcesses"],
            64,
        )
        self.assertEqual(
            payload["HardResourceLimits"]["ResidentSetSize"],
            768 * 1024 * 1024,
        )
        self.assertNotIn(str(self.fixture.server_key), rendered)
        self.assertNotIn("private-key-marker", rendered)
        self.assertEqual(payload["UserName"], "_codexmedia")

    def test_render_preserves_real_venv_python_entrypoint(self) -> None:
        environment = self.fixture.root / "service-venv"
        subprocess.run(
            [
                sys.executable,
                "-m",
                "venv",
                "--without-pip",
                str(environment),
            ],
            check=True,
        )
        python = environment / "bin" / "python"
        self.assertTrue(python.is_symlink())

        with mock.patch(
            "codex_telegram_bridge.media_worker_cli._executable_path",
            return_value=python,
        ):
            rendered = render_launchd(
                config_path=self.fixture.config_path,
                python_executable=python,
                service_user="_codexmedia",
            )
        payload = plistlib.loads(rendered.encode("utf-8"))
        self.assertEqual(payload["ProgramArguments"][0], str(python))
        self.assertNotEqual(str(python), str(python.resolve()))
        result = subprocess.run(
            [
                str(python),
                "-c",
                "import sys; print(sys.prefix)",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        self.assertEqual(
            Path(result.stdout.strip()).resolve(),
            environment.resolve(),
        )
        with self.assertRaisesRegex(
            MediaWorkerConfigError,
            "root|replaceable",
        ):
            _executable_path(python)

    def test_systemd_render_is_notify_restart_and_unprivileged(self) -> None:
        rendered = render_systemd(
            config_path=self.fixture.config_path,
            python_executable=self.service_python,
            state_dir=self.fixture.state_dir,
            service_user="codexmedia",
        )

        self.assertIn("Type=notify", rendered)
        self.assertIn("User=codexmedia", rendered)
        self.assertIn("Restart=always", rendered)
        self.assertIn("RestartSec=5", rendered)
        self.assertIn("TimeoutStopSec=40", rendered)
        self.assertIn("NotifyAccess=main", rendered)
        self.assertIn("MemoryMax=805306368", rendered)
        self.assertIn("TasksMax=64", rendered)
        self.assertIn("LimitFSIZE=33554432", rendered)
        self.assertIn("OOMPolicy=stop", rendered)
        self.assertIn("Environment=PYTHONUNBUFFERED=1", rendered)
        self.assertIn("StandardOutput=journal", rendered)
        self.assertIn("StandardError=journal", rendered)
        self.assertNotIn("WatchdogSec=", rendered)
        self.assertIn("NoNewPrivileges=true", rendered)
        self.assertIn("UMask=0077", rendered)
        self.assertIn("InaccessiblePaths=-%h/.ssh -%h/.codex", rendered)
        self.assertIn("-%h/.config/proton-pass", rendered)
        self.assertIn("ProtectHome=true", rendered)
        self.assertIn("WantedBy=multi-user.target", rendered)
        self.assertNotIn("sudo", rendered.casefold())
        self.assertNotIn(str(self.fixture.server_key), rendered)
        self.assertNotIn("private-key-marker", rendered)

    def test_serve_uses_only_worker_kwargs_and_sanitized_notifier(
        self,
    ) -> None:
        calls: dict[str, object] = {}

        class FakeServer:
            def __init__(self, **kwargs: object) -> None:
                calls["kwargs"] = kwargs

            def start(self) -> None:
                calls["started"] = True

            def serve_forever(self) -> None:
                if not calls.get("started"):
                    raise AssertionError("server was announced before start")
                calls["served"] = True

            def shutdown(self) -> None:
                calls["shutdown"] = True

        class FakeProcessor:
            def __init__(self, **kwargs: object) -> None:
                calls["processor_kwargs"] = kwargs

        class FakeNotifier:
            watchdog_interval_seconds = None

            def __init__(self) -> None:
                self.messages: list[str] = []

            def notify(self, message: str) -> bool:
                self.messages.append(message)
                return True

        notifier = FakeNotifier()
        with mock.patch(
            "codex_telegram_bridge.media_worker_cli.build_tls_context",
            return_value=mock.Mock(spec=ssl.SSLContext),
        ):
            result = serve_worker(
                self.config,
                server_factory=FakeServer,
                processor_factory=FakeProcessor,
                notifier=notifier,  # type: ignore[arg-type]
            )

        self.assertEqual(result, 0)
        self.assertTrue(calls["started"])
        self.assertTrue(calls["served"])
        self.assertTrue(calls["shutdown"])
        self.assertEqual(
            set(calls["kwargs"]),  # type: ignore[arg-type]
            {
                "host",
                "port",
                "ssl_context",
                "spool_directory",
                "processor",
                "queue_capacity",
                "processing_concurrency",
                "request_timeout_seconds",
                "shutdown_timeout_seconds",
                "ttl_seconds",
            },
        )
        self.assertEqual(
            calls["processor_kwargs"],
            {
                "ffmpeg_binary": self.config.ffmpeg_binary,
                "timeout_seconds": self.config.processing_timeout_seconds,
            },
        )
        self.assertIn(
            "READY=1\nSTATUS=Media worker ready",
            notifier.messages,
        )
        self.assertNotIn("private-key-marker", repr(notifier.messages))
        log_path = self.config.state_dir / "logs" / "worker.log"
        self.assertEqual(stat.S_IMODE(log_path.stat().st_mode), 0o600)
        log_text = log_path.read_text(encoding="utf-8")
        self.assertIn("media_worker_ready", log_text)
        self.assertIn("media_worker_stopped", log_text)
        self.assertNotIn("private-key-marker", log_text)

    def test_sigterm_shutdown_runs_outside_serve_forever_thread(
        self,
    ) -> None:
        calls: dict[str, int] = {}
        shutdown_complete = threading.Event()

        class SignalServer:
            def __init__(self, **kwargs: object) -> None:
                del kwargs

            def serve_forever(self) -> None:
                calls["serve_thread"] = threading.get_ident()
                os.kill(os.getpid(), signal.SIGTERM)
                self.assert_shutdown()

            def assert_shutdown(self) -> None:
                if not shutdown_complete.wait(timeout=2):
                    raise AssertionError("shutdown thread did not run")

            def shutdown(self) -> None:
                calls["shutdown_thread"] = threading.get_ident()
                shutdown_complete.set()

        class FakeProcessor:
            def __init__(self, **kwargs: object) -> None:
                del kwargs

        class FakeNotifier:
            watchdog_interval_seconds = None

            def notify(self, message: str) -> bool:
                del message
                return True

        with mock.patch(
            "codex_telegram_bridge.media_worker_cli.build_tls_context",
            return_value=mock.Mock(spec=ssl.SSLContext),
        ):
            result = serve_worker(
                self.config,
                server_factory=SignalServer,
                processor_factory=FakeProcessor,
                notifier=FakeNotifier(),  # type: ignore[arg-type]
            )

        self.assertEqual(result, 0)
        self.assertNotEqual(
            calls["serve_thread"],
            calls["shutdown_thread"],
        )

    def test_worker_log_rejects_symlink_before_server_start(self) -> None:
        logs = self.config.state_dir / "logs"
        logs.mkdir(mode=0o700)
        target = self.fixture.root / "outside.log"
        target.write_text("do not overwrite", encoding="utf-8")
        (logs / "worker.log").symlink_to(target)

        with self.assertRaises(MediaWorkerConfigError):
            serve_worker(self.config)

        self.assertEqual(
            target.read_text(encoding="utf-8"),
            "do not overwrite",
        )

    def test_render_cli_does_not_install_or_create_files(self) -> None:
        before = sorted(
            str(path.relative_to(self.fixture.root))
            for path in self.fixture.root.rglob("*")
        )
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            result = main(
                [
                    "--config",
                    str(self.fixture.config_path),
                    "render-launchd",
                    "--python-executable",
                    str(self.service_python),
                    "--service-user",
                    "_codexmedia",
                ]
            )
        after = sorted(
            str(path.relative_to(self.fixture.root))
            for path in self.fixture.root.rglob("*")
        )

        self.assertEqual(result, 0)
        self.assertEqual(before, after)
        self.assertIn("<plist", output.getvalue())

    def test_service_render_rejects_privileged_shared_accounts(self) -> None:
        for account in ("root", "daemon", "nobody", "bad user"):
            with self.subTest(account=account):
                with self.assertRaises(MediaWorkerConfigError):
                    render_launchd(
                        config_path=self.fixture.config_path,
                        python_executable=self.service_python,
                        service_user=account,
                    )

        with mock.patch(
            "codex_telegram_bridge.media_worker_cli.pwd.getpwnam",
            side_effect=KeyError,
        ):
            with self.assertRaises(MediaWorkerConfigError):
                render_launchd(
                    config_path=self.fixture.config_path,
                    python_executable=self.service_python,
                    service_user="missingworker",
                )

        with mock.patch(
            "codex_telegram_bridge.media_worker_cli.pwd.getpwnam",
            return_value=SimpleNamespace(
                pw_uid=501,
                pw_gid=20,
                pw_shell="/bin/zsh",
            ),
        ):
            with self.assertRaises(MediaWorkerConfigError):
                render_launchd(
                    config_path=self.fixture.config_path,
                    python_executable=self.service_python,
                    service_user="interactiveuser",
                )

        with (
            mock.patch(
                "codex_telegram_bridge.media_worker_cli.grp.getgrnam",
                return_value=SimpleNamespace(gr_gid=80),
            ),
            mock.patch(
                "codex_telegram_bridge.media_worker_cli.os.getgrouplist",
                return_value=[80],
            ),
        ):
            with self.assertRaises(MediaWorkerConfigError):
                render_launchd(
                    config_path=self.fixture.config_path,
                    python_executable=self.service_python,
                    service_user="adminworker",
                )

        with (
            mock.patch(
                "codex_telegram_bridge.media_worker_cli.grp.getgrnam",
                side_effect=KeyError,
            ),
            mock.patch(
                "codex_telegram_bridge.media_worker_cli.sys.platform",
                "linux",
            ),
            mock.patch(
                "codex_telegram_bridge.media_worker_cli.os.getgrouplist",
                return_value=[499, 123],
            ),
        ):
            with self.assertRaisesRegex(
                MediaWorkerConfigError,
                "supplementary",
            ):
                render_launchd(
                    config_path=self.fixture.config_path,
                    python_executable=self.service_python,
                    service_user="groupedworker",
                )

        automatic_groups = {
            12: "everyone",
            61: "localaccounts",
            100: "_lpoperator",
            701: "com.apple.sharepoint.group.1",
        }
        with (
            mock.patch(
                "codex_telegram_bridge.media_worker_cli.grp.getgrnam",
                side_effect=KeyError,
            ),
            mock.patch(
                "codex_telegram_bridge.media_worker_cli.grp.getgrgid",
                side_effect=lambda group_id: SimpleNamespace(
                    gr_name=automatic_groups[group_id]
                ),
            ),
            mock.patch(
                "codex_telegram_bridge.media_worker_cli.sys.platform",
                "darwin",
            ),
            mock.patch(
                "codex_telegram_bridge.media_worker_cli.os.getgrouplist",
                return_value=[499, *automatic_groups],
            ),
        ):
            rendered = render_launchd(
                config_path=self.fixture.config_path,
                python_executable=self.service_python,
                service_user="_codexmedia",
            )
        self.assertIn("_codexmedia", rendered)

    def test_probe_cli_fails_without_exposing_invalid_tls_material(
        self,
    ) -> None:
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            result = main(
                [
                    "--config",
                    str(self.fixture.config_path),
                    "probe-config",
                ]
            )

        self.assertEqual(result, 1)
        payload = json.loads(output.getvalue())
        self.assertFalse(payload["ok"])
        self.assertNotIn(str(self.fixture.server_key), output.getvalue())
        self.assertNotIn("private-key-marker", output.getvalue())

    def test_cli_imports_no_bridge_data_or_secret_clients(self) -> None:
        source = (
            ROOT
            / "codex_telegram_bridge"
            / "media_worker_cli.py"
        ).read_text(encoding="utf-8")
        imported_modules = {
            node.module or ""
            for node in ast.walk(ast.parse(source))
            if isinstance(node, ast.ImportFrom)
        }
        self.assertFalse(
            imported_modules
            & {
                "codex",
                "config",
                "store",
                "telegram",
                "codex_telegram_bridge.codex",
                "codex_telegram_bridge.config",
                "codex_telegram_bridge.store",
                "codex_telegram_bridge.telegram",
            }
        )


if __name__ == "__main__":
    unittest.main()
