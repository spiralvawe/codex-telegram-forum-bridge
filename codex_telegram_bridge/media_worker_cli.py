from __future__ import annotations

import argparse
import grp
import json
import logging
import os
import plistlib
import pwd
import re
import signal
import ssl
import stat
import sys
import threading
from collections.abc import Callable, Sequence
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

from .media_worker_config import (
    MediaWorkerConfig,
    MediaWorkerConfigError,
    _validate_protected_ancestors,
)
from .systemd_notify import SystemdNotifier


DEFAULT_LAUNCHD_LABEL = "com.codex.telegram-media-worker"
WORKER_MODULE = "codex_telegram_bridge.media_worker_cli"
WORKER_LOG_BYTES = 1024 * 1024
WORKER_LOG_BACKUPS = 3
WORKER_FILE_SIZE_LIMIT = 32 * 1024 * 1024
WORKER_MEMORY_HIGH = 512 * 1024 * 1024
WORKER_MEMORY_MAX = 768 * 1024 * 1024
WORKER_TASK_LIMIT = 64
MACOS_AUTOMATIC_GROUPS = frozenset(
    {
        "_lpoperator",
        "everyone",
        "localaccounts",
    }
)
LOGGER = logging.getLogger(__name__)


class _OwnerOnlyRotatingFileHandler(RotatingFileHandler):
    def _open(self) -> Any:
        stream = super()._open()
        os.chmod(self.baseFilename, 0o600)
        return stream


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run an isolated mTLS Telegram media worker."
    )
    parser.add_argument(
        "--config",
        required=True,
        help="Owner-only worker JSON configuration.",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("serve", help="Run the media worker.")
    commands.add_parser(
        "probe-config",
        help="Validate local worker configuration without serving.",
    )
    launchd = commands.add_parser(
        "render-launchd",
        help="Print a launchd plist without installing it.",
    )
    launchd.add_argument("--label", default=DEFAULT_LAUNCHD_LABEL)
    launchd.add_argument("--python-executable", default=sys.executable)
    launchd.add_argument(
        "--service-user",
        required=True,
        help="Dedicated non-admin worker account (never the main SSH user).",
    )
    systemd = commands.add_parser(
        "render-systemd",
        help="Print a systemd system service without installing it.",
    )
    systemd.add_argument("--python-executable", default=sys.executable)
    systemd.add_argument(
        "--service-user",
        required=True,
        help="Dedicated non-admin worker account (never the main SSH user).",
    )
    return parser


def build_tls_context(config: MediaWorkerConfig) -> ssl.SSLContext:
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.verify_mode = ssl.CERT_REQUIRED
    context.load_verify_locations(cafile=str(config.tls_client_ca))
    context.load_cert_chain(
        certfile=str(config.tls_server_cert),
        keyfile=str(config.tls_server_key),
    )
    return context


def validate_tls_material(config: MediaWorkerConfig) -> None:
    build_tls_context(config)


def probe_configuration(config: MediaWorkerConfig) -> dict[str, object]:
    tls_valid = True
    try:
        validate_tls_material(config)
    except (OSError, ssl.SSLError, ValueError):
        tls_valid = False
    state_accessible = os.access(
        config.state_dir,
        os.R_OK | os.W_OK | os.X_OK,
    )
    executable_ready = os.access(config.ffmpeg_binary, os.X_OK)
    return {
        "ok": bool(tls_valid and state_accessible and executable_ready),
        "configurationOwnerOnly": True,
        "stateDirectoryReady": state_accessible,
        "ffmpegReady": executable_ready,
        "mutualTlsReady": tls_valid,
        "queueCapacity": config.queue_capacity,
        "concurrency": config.concurrency,
        "integrationCredentialsConfigured": False,
        "readOnlyProbe": True,
    }


def _load_protocol_classes() -> tuple[type[Any], type[Any]]:
    # Keep protocol import localized so render/probe remain standalone and so
    # the server constructor can be adapted in one place as the protocol lands.
    from .media_worker import (
        FFmpegMediaWorkerProcessor,
        MediaWorkerServer,
    )

    return MediaWorkerServer, FFmpegMediaWorkerProcessor


def create_server(
    config: MediaWorkerConfig,
    *,
    server_factory: Callable[..., Any] | None = None,
    processor_factory: Callable[..., Any] | None = None,
    ssl_context: ssl.SSLContext | None = None,
) -> Any:
    if server_factory is None or processor_factory is None:
        server_class, processor_class = _load_protocol_classes()
        server_factory = server_factory or server_class
        processor_factory = processor_factory or processor_class
    selected_context = ssl_context or build_tls_context(config)
    processor = processor_factory(
        ffmpeg_binary=config.ffmpeg_binary,
        timeout_seconds=config.processing_timeout_seconds,
    )
    return server_factory(
        **config.server_keyword_arguments(
            ssl_context=selected_context,
            processor=processor,
        )
    )


def serve_worker(
    config: MediaWorkerConfig,
    *,
    server_factory: Callable[..., Any] | None = None,
    processor_factory: Callable[..., Any] | None = None,
    notifier: SystemdNotifier | None = None,
) -> int:
    log_handler = _configure_worker_logging(config.state_dir)
    try:
        ssl_context = build_tls_context(config)
        server = create_server(
            config,
            server_factory=server_factory,
            processor_factory=processor_factory,
            ssl_context=ssl_context,
        )
        selected_notifier = notifier or SystemdNotifier.from_environment()
    except BaseException as error:
        LOGGER.error(
            "media_worker_startup_failed reason=%s",
            type(error).__name__,
        )
        _close_worker_log(log_handler)
        raise

    previous_handlers: dict[int, Any] = {}
    shutdown_requested = False
    shutdown_thread: threading.Thread | None = None

    def request_shutdown(signum: int, frame: object) -> None:
        del signum, frame
        nonlocal shutdown_requested, shutdown_thread
        if shutdown_requested:
            return
        shutdown_requested = True
        selected_notifier.notify("STOPPING=1")
        # socketserver.BaseServer.shutdown() must run from a thread other than
        # the one executing serve_forever(), otherwise it deadlocks.
        shutdown_thread = threading.Thread(
            target=_safe_shutdown,
            args=(server,),
            name="media-worker-shutdown",
            daemon=True,
        )
        shutdown_thread.start()

    for selected_signal in (signal.SIGTERM, signal.SIGINT):
        try:
            previous_handlers[selected_signal] = signal.signal(
                selected_signal,
                request_shutdown,
            )
        except ValueError:
            # Signal handlers can only be installed by the main thread.
            previous_handlers.clear()
            break

    try:
        start = getattr(server, "start", None)
        if callable(start):
            start()
        LOGGER.info("media_worker_ready")
        selected_notifier.notify("READY=1\nSTATUS=Media worker ready")
        server.serve_forever()
        return 0
    except BaseException as error:
        LOGGER.error(
            "media_worker_runtime_failed reason=%s",
            type(error).__name__,
        )
        raise
    finally:
        selected_notifier.notify("STOPPING=1")
        if not shutdown_requested:
            _safe_shutdown(server)
        elif shutdown_thread is not None:
            shutdown_thread.join(timeout=config.shutdown_timeout_seconds)
        for selected_signal, previous in previous_handlers.items():
            signal.signal(selected_signal, previous)
        LOGGER.info("media_worker_stopped")
        _close_worker_log(log_handler)


def _safe_shutdown(server: Any) -> None:
    shutdown = getattr(server, "shutdown", None)
    if not callable(shutdown):
        return
    try:
        shutdown()
    except Exception:
        # Service output must never serialize protocol or media payloads.
        pass


def _configure_worker_logging(
    state_dir: Path,
) -> _OwnerOnlyRotatingFileHandler:
    logs = state_dir / "logs"
    if logs.is_symlink():
        raise MediaWorkerConfigError("worker log directory is unsafe")
    logs.mkdir(mode=0o700, exist_ok=True)
    status = logs.lstat()
    if (
        not stat.S_ISDIR(status.st_mode)
        or status.st_uid != os.geteuid()
        or stat.S_IMODE(status.st_mode) & 0o077
    ):
        raise MediaWorkerConfigError("worker log directory is unsafe")
    os.chmod(logs, 0o700)
    log_path = logs / "worker.log"
    if log_path.exists() or log_path.is_symlink():
        log_status = log_path.lstat()
        if (
            stat.S_ISLNK(log_status.st_mode)
            or not stat.S_ISREG(log_status.st_mode)
            or log_status.st_uid != os.geteuid()
            or stat.S_IMODE(log_status.st_mode) & 0o077
        ):
            raise MediaWorkerConfigError("worker log file is unsafe")
    handler = _OwnerOnlyRotatingFileHandler(
        log_path,
        maxBytes=WORKER_LOG_BYTES,
        backupCount=WORKER_LOG_BACKUPS,
        encoding="utf-8",
    )
    os.chmod(log_path, 0o600)
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s %(levelname)s %(message)s",
        )
    )
    LOGGER.handlers.clear()
    LOGGER.addHandler(handler)
    LOGGER.setLevel(logging.INFO)
    LOGGER.propagate = False
    return handler


def _close_worker_log(handler: RotatingFileHandler) -> None:
    LOGGER.removeHandler(handler)
    handler.close()


def render_launchd(
    *,
    config_path: str | Path,
    python_executable: str | Path,
    service_user: str,
    label: str = DEFAULT_LAUNCHD_LABEL,
) -> str:
    validated_label = _service_label(label)
    account = _service_account(service_user)
    config = _service_path(config_path, field="config")
    python = _executable_path(python_executable)
    payload = {
        "Label": validated_label,
        "ProgramArguments": [
            str(python),
            "-m",
            WORKER_MODULE,
            "--config",
            str(config),
            "serve",
        ],
        "RunAtLoad": True,
        "KeepAlive": True,
        "ThrottleInterval": 10,
        "ProcessType": "Background",
        "AbandonProcessGroup": False,
        "Umask": 0o077,
        "UserName": account,
        "EnvironmentVariables": {
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONUNBUFFERED": "1",
        },
        "StandardOutPath": "/dev/null",
        "StandardErrorPath": "/dev/null",
        "SoftResourceLimits": {
            "FileSize": WORKER_FILE_SIZE_LIMIT,
            "NumberOfFiles": 256,
            "NumberOfProcesses": 48,
            "ResidentSetSize": WORKER_MEMORY_HIGH,
        },
        "HardResourceLimits": {
            "FileSize": WORKER_FILE_SIZE_LIMIT,
            "NumberOfFiles": 256,
            "NumberOfProcesses": WORKER_TASK_LIMIT,
            "ResidentSetSize": WORKER_MEMORY_MAX,
        },
    }
    return plistlib.dumps(
        payload,
        fmt=plistlib.FMT_XML,
        sort_keys=True,
    ).decode("utf-8")


def render_systemd(
    *,
    config_path: str | Path,
    python_executable: str | Path,
    state_dir: str | Path,
    service_user: str,
) -> str:
    config = _service_path(config_path, field="config")
    python = _executable_path(python_executable)
    state = _service_path(state_dir, field="state_dir")
    account = _service_account(service_user)
    arguments = (
        python,
        "-m",
        WORKER_MODULE,
        "--config",
        config,
        "serve",
    )
    exec_start = " ".join(_systemd_quote(value) for value in arguments)
    return "\n".join(
        [
            "[Unit]",
            "Description=Isolated Telegram media worker",
            "StartLimitIntervalSec=0",
            "",
            "[Service]",
            "Type=notify",
            f"User={account}",
            f"ExecStart={exec_start}",
            "Restart=always",
            "RestartSec=5",
            "TimeoutStartSec=90",
            "TimeoutStopSec=40",
            "KillMode=control-group",
            f"MemoryHigh={WORKER_MEMORY_HIGH}",
            f"MemoryMax={WORKER_MEMORY_MAX}",
            f"TasksMax={WORKER_TASK_LIMIT}",
            "LimitNOFILE=256",
            f"LimitNPROC={WORKER_TASK_LIMIT}",
            f"LimitFSIZE={WORKER_FILE_SIZE_LIMIT}",
            "OOMPolicy=stop",
            "NotifyAccess=main",
            "Environment=PYTHONUNBUFFERED=1",
            "StandardOutput=journal",
            "StandardError=journal",
            "UMask=0077",
            "NoNewPrivileges=true",
            "PrivateTmp=true",
            "PrivateDevices=true",
            "ProtectSystem=strict",
            "ProtectHome=true",
            f"ReadWritePaths={_systemd_quote(state)}",
            (
                "InaccessiblePaths=-%h/.ssh -%h/.codex "
                "-%h/.config/Proton -%h/.config/proton-pass "
                "-%h/.local/share/proton-pass"
            ),
            "ProtectKernelTunables=true",
            "ProtectKernelModules=true",
            "ProtectControlGroups=true",
            "RestrictSUIDSGID=true",
            "LockPersonality=true",
            "RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6",
            "",
            "[Install]",
            "WantedBy=multi-user.target",
            "",
        ]
    )


def _service_label(value: str) -> str:
    if (
        not value
        or len(value) > 128
        or any(
            not (
                character.isascii()
                and (
                    character.isalnum()
                    or character in {".", "-"}
                )
            )
            for character in value
        )
    ):
        raise MediaWorkerConfigError("service label is invalid")
    return value


def _service_account(value: str) -> str:
    account = str(value)
    if (
        re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.-]{0,63}", account) is None
        or account.casefold() in {"root", "daemon", "nobody"}
    ):
        raise MediaWorkerConfigError(
            "service_user must name a dedicated non-admin account"
        )
    try:
        record = pwd.getpwnam(account)
    except KeyError:
        raise MediaWorkerConfigError(
            "service_user must already exist"
        ) from None
    if (
        record.pw_uid == 0
        or record.pw_uid != os.geteuid()
        or Path(record.pw_shell)
        not in {
            Path("/bin/false"),
            Path("/usr/bin/false"),
            Path("/sbin/nologin"),
            Path("/usr/sbin/nologin"),
        }
    ):
        raise MediaWorkerConfigError(
            "run the renderer as the selected non-login non-root account"
        )
    try:
        group_ids = set(os.getgrouplist(account, record.pw_gid))
    except OSError:
        raise MediaWorkerConfigError(
            "service_user groups cannot be verified"
        ) from None
    privileged_group_ids: set[int] = set()
    for group_name in (
        "root",
        "admin",
        "sudo",
        "wheel",
        "operator",
        "docker",
        "lxd",
        "incus",
        "podman",
        "libvirt",
        "disk",
        "_developer",
        "com.apple.access_ssh",
        "com.apple.access_remote_ae",
        "com.apple.access_screensharing",
    ):
        try:
            privileged_group_ids.add(grp.getgrnam(group_name).gr_gid)
        except KeyError:
            continue
    if group_ids & privileged_group_ids:
        raise MediaWorkerConfigError(
            "service_user must not belong to an administrator group"
        )
    supplementary_group_ids = group_ids - {record.pw_gid}
    if supplementary_group_ids:
        if sys.platform != "darwin":
            raise MediaWorkerConfigError(
                "service_user must not have supplementary groups"
            )
        for group_id in supplementary_group_ids:
            try:
                group_name = grp.getgrgid(group_id).gr_name
            except KeyError:
                raise MediaWorkerConfigError(
                    "service_user supplementary groups cannot be verified"
                ) from None
            if (
                group_name not in MACOS_AUTOMATIC_GROUPS
                and re.fullmatch(
                    r"com\.apple\.sharepoint\.group\.[0-9]+",
                    group_name,
                )
                is None
            ):
                raise MediaWorkerConfigError(
                    "service_user has a non-system supplementary group"
                )
    return account


def _service_path(value: str | Path, *, field: str) -> Path:
    path = Path(value)
    text = str(path)
    if not path.is_absolute():
        raise MediaWorkerConfigError(f"{field} must be an absolute path")
    if any(character in text for character in ("\x00", "\r", "\n", "$")):
        raise MediaWorkerConfigError(
            f"{field} contains unsafe service characters"
        )
    return path.resolve(strict=True)


def _executable_path(value: str | Path) -> Path:
    path = Path(value)
    text = str(path)
    if not path.is_absolute():
        raise MediaWorkerConfigError(
            "python_executable must be an absolute path"
        )
    if any(character in text for character in ("\x00", "\r", "\n", "$")):
        raise MediaWorkerConfigError(
            "python_executable contains unsafe service characters"
        )
    try:
        entry_metadata = path.lstat()
        resolved = path.resolve(strict=True)
        metadata = resolved.stat()
    except OSError:
        raise MediaWorkerConfigError(
            "python_executable cannot be inspected"
        ) from None
    _validate_protected_ancestors(
        path,
        field="python_executable",
        allow_worker_owned=False,
    )
    _validate_protected_ancestors(
        resolved,
        field="python_executable target",
        allow_worker_owned=False,
    )
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != 0
        or metadata.st_mode & (stat.S_IWGRP | stat.S_IWOTH)
        or not os.access(path, os.X_OK)
        or entry_metadata.st_uid != 0
    ):
        raise MediaWorkerConfigError(
            "python_executable must be a protected executable regular file"
        )
    return path


def _systemd_quote(value: str | Path) -> str:
    text = str(value)
    if any(character in text for character in ("\x00", "\r", "\n", "$")):
        raise MediaWorkerConfigError(
            "service argument contains unsafe characters"
        )
    escaped = (
        text.replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("%", "%%")
    )
    return f'"{escaped}"'


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        config = MediaWorkerConfig.from_file(args.config)
        config_path = Path(args.config).resolve(strict=True)
        if args.command == "serve":
            return serve_worker(config)
        if args.command == "probe-config":
            probe = probe_configuration(config)
            print(json.dumps(probe, sort_keys=True))
            return 0 if probe["ok"] else 1
        if args.command == "render-launchd":
            print(
                render_launchd(
                    config_path=config_path,
                    python_executable=args.python_executable,
                    service_user=args.service_user,
                    label=args.label,
                ),
                end="",
            )
            return 0
        if args.command == "render-systemd":
            print(
                render_systemd(
                    config_path=config_path,
                    python_executable=args.python_executable,
                    state_dir=config.state_dir,
                    service_user=args.service_user,
                ),
                end="",
            )
            return 0
    except MediaWorkerConfigError:
        print(
            json.dumps(
                {
                    "ok": False,
                    "error": "worker_configuration_rejected",
                },
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2
    except (OSError, ssl.SSLError, ValueError):
        print(
            json.dumps(
                {
                    "ok": False,
                    "error": "worker_startup_failed",
                },
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 1
    except Exception:
        print(
            json.dumps(
                {
                    "ok": False,
                    "error": "worker_runtime_failed",
                },
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 1
    raise AssertionError("unreachable command")


if __name__ == "__main__":
    raise SystemExit(main())
