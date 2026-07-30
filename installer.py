#!/usr/bin/env python3
from __future__ import annotations

import argparse
import getpass
import json
import os
import plistlib
import re
import shlex
import shutil
import stat
import subprocess
import sys
import venv
from pathlib import Path
from typing import Any

from codex_telegram_bridge.config import (
    BridgeConfig,
    default_instance_id,
    read_bot_token,
)


ROOT = Path(__file__).resolve().parent
SERVICE_PREFIX = "dev.codex.telegram-bridge"


class InstallerError(RuntimeError):
    pass


def run(
    arguments: list[str],
    *,
    check: bool = True,
    capture: bool = False,
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        arguments,
        check=False,
        text=True,
        capture_output=capture,
    )
    if check and result.returncode != 0:
        detail = result.stderr.strip() if capture else ""
        suffix = f": {detail}" if detail else ""
        raise InstallerError(f"Command failed: {arguments[0]}{suffix}")
    return result


def validate_instance(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9-]+", "-", value.casefold()).strip("-")
    if not normalized or len(normalized) > 63:
        raise argparse.ArgumentTypeError(
            "instance must contain 1-63 letters, digits, or hyphens"
        )
    return normalized


def ensure_private_directory(path: Path) -> None:
    if path.is_symlink():
        raise InstallerError(f"Refusing symlinked state directory: {path}")
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    status = path.stat()
    if status.st_uid != os.getuid() or not stat.S_ISDIR(status.st_mode):
        raise InstallerError(f"State directory has an unexpected owner: {path}")
    os.chmod(path, 0o700)


def atomic_json(path: Path, payload: dict[str, Any]) -> None:
    ensure_private_directory(path.parent)
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    descriptor = os.open(
        temporary,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL,
        0o600,
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(payload, stream, ensure_ascii=True, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        os.chmod(path, 0o600)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def runtime_python(config: BridgeConfig) -> Path:
    return config.state_dir / "runtime" / "bin" / "python"


def runtime_cli(config: BridgeConfig) -> Path:
    return config.state_dir / "runtime" / "bin" / "codex-telegram-bridge"


def runtime_installer(config: BridgeConfig) -> Path:
    return (
        config.state_dir
        / "runtime"
        / "bin"
        / "codex-telegram-bridge-installer"
    )


def config_path(config: BridgeConfig) -> Path:
    return config.state_dir / "config.json"


def prepare_runtime(config: BridgeConfig) -> None:
    ensure_private_directory(config.state_dir)
    environment = config.state_dir / "runtime"
    if not (environment / "bin" / "python").is_file():
        venv.EnvBuilder(with_pip=True).create(environment)
    python = str(runtime_python(config))
    run(
        [
            python,
            "-m",
            "pip",
            "install",
            "--disable-pip-version-check",
            "--require-hashes",
            "-r",
            str(ROOT / "requirements.lock"),
        ]
    )
    run(
        [
            python,
            "-m",
            "pip",
            "install",
            "--disable-pip-version-check",
            "--no-deps",
            str(ROOT),
        ]
    )


def bootstrap_codex_daemon(config: BridgeConfig) -> None:
    result = run(
        [
            config.codex_binary,
            "app-server",
            "daemon",
            "bootstrap",
        ],
        check=False,
        capture=True,
    )
    if result.returncode != 0:
        raise InstallerError(
            "Codex CLI cannot bootstrap its local app-server daemon. "
            "Update/login to Codex CLI and retry."
        )
    run(
        [config.codex_binary, "app-server", "daemon", "start"],
        check=True,
        capture=True,
    )
    if sys.platform == "darwin":
        launchctl = shutil.which("launchctl") or "/bin/launchctl"
        run(
            [
                launchctl,
                "setenv",
                "CODEX_APP_SERVER_USE_LOCAL_DAEMON",
                "1",
            ]
        )


def prepare(args: argparse.Namespace) -> dict[str, Any]:
    workspace = Path(args.workspace).expanduser().resolve()
    if not workspace.is_dir():
        raise InstallerError(f"Workspace does not exist: {workspace}")
    instance = args.instance or default_instance_id(workspace)
    config = BridgeConfig.from_paths(
        workspace=workspace,
        state_dir=args.state_dir,
        instance_id=instance,
        secret_backend=args.secret_backend,
        secret_reference=args.secret_reference,
        secret_vault=args.secret_vault,
        max_active_turns=args.max_active_turns,
        codex_full_access=args.codex_full_access,
    )
    if args.codex_binary:
        payload = config.as_file_payload()
        payload["codex_binary"] = str(
            Path(args.codex_binary).expanduser().resolve()
        )
        temporary_path = config.state_dir / ".prepare-config.json"
        ensure_private_directory(config.state_dir)
        atomic_json(temporary_path, payload)
        config = BridgeConfig.from_file(temporary_path)
        temporary_path.unlink()
    if not shutil.which(config.codex_binary) and not Path(
        config.codex_binary
    ).is_file():
        raise InstallerError(
            "Codex CLI is unavailable. Install/login to Codex before setup."
        )
    if not shutil.which(config.ffmpeg_binary) and not Path(
        config.ffmpeg_binary
    ).is_file():
        raise InstallerError("ffmpeg is required for voice/video messages")

    existing_config_path = config_path(config)
    if existing_config_path.exists():
        existing = BridgeConfig.from_file(existing_config_path)
        if (
            existing.instance_id != config.instance_id
            or existing.workspace != config.workspace
        ):
            raise InstallerError(
                "Existing instance belongs to a different workspace; choose "
                "a new --instance or --state-dir"
            )
        if config.database_path.exists() and (
            existing.secret_backend != config.secret_backend
            or existing.secret_reference != config.secret_reference
        ):
            raise InstallerError(
                "Refusing to replace the secret binding of a live instance"
            )

    prepare_runtime(config)
    if not args.skip_app_server_bootstrap:
        bootstrap_codex_daemon(config)
    atomic_json(config_path(config), config.as_file_payload())
    installer = runtime_installer(config)
    next_commands = []
    if config.secret_backend != "proton-pass":
        next_commands.append(
            shlex.join(
                [
                    str(installer),
                    "configure-secret",
                    "--config",
                    str(config_path(config)),
                ]
            )
        )
    next_commands.extend(
        [
            shlex.join(
                [
                    str(installer),
                    "secret-check",
                    "--config",
                    str(config_path(config)),
                ]
            ),
            shlex.join(
                [
                    str(runtime_cli(config)),
                    "--config",
                    str(config_path(config)),
                    "bootstrap",
                    "--wait-seconds",
                    "900",
                ]
            ),
            shlex.join(
                [
                    str(installer),
                    "activate",
                    "--config",
                    str(config_path(config)),
                ]
            ),
        ]
    )
    return {
        "ok": True,
        "instance": config.instance_id,
        "platform": "macos" if sys.platform == "darwin" else "linux",
        "workspace": str(config.workspace),
        "config": str(config_path(config)),
        "secretBackend": config.secret_backend,
        "next": next_commands,
        "desktopRestartMayBeRequired": sys.platform == "darwin",
    }


def systemd_quote(value: str | Path) -> str:
    text = str(value)
    if "\n" in text or "\r" in text or "\x00" in text:
        raise InstallerError("Service path contains unsupported characters")
    return '"' + text.replace("\\", "\\\\").replace('"', '\\"') + '"'


def systemd_working_directory(value: str | Path) -> str:
    """Render one absolute path for systemd's WorkingDirectory= directive."""
    text = str(value)
    if "\n" in text or "\r" in text or "\x00" in text:
        raise InstallerError("Service path contains unsupported characters")
    if not os.path.isabs(text):
        raise InstallerError("Service working directory must be absolute")

    # WorkingDirectory= consumes the complete, unquoted value. Unlike
    # ExecStart=, surrounding quotes are retained as literal path characters.
    # A trailing slash keeps otherwise-trimmed whitespace, and prevents a
    # terminal backslash from continuing the next unit-file line.
    if text[-1].isspace() or text.endswith("\\"):
        text += "/"

    # Unit specifiers are expanded in WorkingDirectory=. A doubled percent is
    # the systemd spelling for one literal percent in the actual filesystem
    # path.
    return text.replace("%", "%%")


def install_systemd(config: BridgeConfig) -> list[str]:
    if not shutil.which("systemctl"):
        raise InstallerError("systemd user services are unavailable")
    user_dir = Path.home() / ".config" / "systemd" / "user"
    user_dir.mkdir(parents=True, exist_ok=True)
    unit_name = f"codex-telegram-bridge-{config.instance_id}.service"
    health_name = f"codex-telegram-bridge-{config.instance_id}-health.service"
    timer_name = f"codex-telegram-bridge-{config.instance_id}-health.timer"
    unit_path = user_dir / unit_name
    health_path = user_dir / health_name
    timer_path = user_dir / timer_name
    cli = runtime_cli(config)
    configuration = config_path(config)
    service_path = os.environ.get(
        "PATH",
        "/usr/local/bin:/usr/bin:/bin",
    )
    working_directory = systemd_working_directory(config.workspace)

    unit_path.write_text(
        "\n".join(
            [
                "[Unit]",
                f"Description=Codex Telegram bridge ({config.instance_id})",
                "After=network-online.target",
                "Wants=network-online.target",
                "",
                "[Service]",
                "Type=simple",
                f"WorkingDirectory={working_directory}",
                (
                    f"ExecStart={systemd_quote(cli)} --config "
                    f"{systemd_quote(configuration)} serve"
                ),
                f"Environment={systemd_quote(f'PATH={service_path}')}",
                "Restart=on-failure",
                "RestartSec=10",
                "UMask=0077",
                "NoNewPrivileges=true",
                "PrivateTmp=true",
                "",
                "[Install]",
                "WantedBy=default.target",
                "",
            ]
        ),
        encoding="utf-8",
    )
    health_path.write_text(
        "\n".join(
            [
                "[Unit]",
                f"Description=Health check for {config.instance_id}",
                "",
                "[Service]",
                "Type=oneshot",
                f"WorkingDirectory={working_directory}",
                (
                    f"ExecStart={systemd_quote(cli)} --config "
                    f"{systemd_quote(configuration)} doctor"
                ),
                f"Environment={systemd_quote(f'PATH={service_path}')}",
                "UMask=0077",
                "NoNewPrivileges=true",
                "",
            ]
        ),
        encoding="utf-8",
    )
    timer_path.write_text(
        "\n".join(
            [
                "[Unit]",
                f"Description=Periodic health check for {config.instance_id}",
                "",
                "[Timer]",
                "OnBootSec=2min",
                "OnUnitActiveSec=5min",
                "Persistent=true",
                f"Unit={health_name}",
                "",
                "[Install]",
                "WantedBy=timers.target",
                "",
            ]
        ),
        encoding="utf-8",
    )
    for path in (unit_path, health_path, timer_path):
        os.chmod(path, 0o644)
    run(["systemctl", "--user", "daemon-reload"])
    run(["systemctl", "--user", "enable", "--now", unit_name])
    run(["systemctl", "--user", "enable", "--now", timer_name])
    return [unit_name, timer_name]


def install_launchd(config: BridgeConfig) -> list[str]:
    launchctl = shutil.which("launchctl") or "/bin/launchctl"
    directory = Path.home() / "Library" / "LaunchAgents"
    directory.mkdir(parents=True, exist_ok=True)
    safe_instance = config.instance_id.replace("-", ".")
    label = f"{SERVICE_PREFIX}.{safe_instance}"
    health_label = f"{label}.health"
    cli = str(runtime_cli(config))
    configuration = str(config_path(config))
    common_environment = {
        "PATH": os.environ.get(
            "PATH",
            "/usr/local/bin:/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin",
        ),
        "PYTHONDONTWRITEBYTECODE": "1",
    }
    bridge_plist = directory / f"{label}.plist"
    health_plist = directory / f"{health_label}.plist"
    with bridge_plist.open("wb") as stream:
        plistlib.dump(
            {
                "Label": label,
                "ProgramArguments": [
                    cli,
                    "--config",
                    configuration,
                    "serve",
                ],
                "WorkingDirectory": str(config.workspace),
                "EnvironmentVariables": common_environment,
                "RunAtLoad": True,
                "KeepAlive": {"SuccessfulExit": False},
                "ThrottleInterval": 30,
                "ProcessType": "Background",
                "StandardOutPath": str(config.state_dir / "service.out.log"),
                "StandardErrorPath": str(config.state_dir / "service.err.log"),
            },
            stream,
            sort_keys=True,
        )
    with health_plist.open("wb") as stream:
        plistlib.dump(
            {
                "Label": health_label,
                "ProgramArguments": [
                    cli,
                    "--config",
                    configuration,
                    "doctor",
                ],
                "WorkingDirectory": str(config.workspace),
                "EnvironmentVariables": common_environment,
                "RunAtLoad": True,
                "StartInterval": 300,
                "ProcessType": "Background",
                "StandardOutPath": str(config.state_dir / "health.out.log"),
                "StandardErrorPath": str(config.state_dir / "health.err.log"),
            },
            stream,
            sort_keys=True,
        )
    os.chmod(bridge_plist, 0o644)
    os.chmod(health_plist, 0o644)
    domain = f"gui/{os.getuid()}"
    for current_label, path in (
        (label, bridge_plist),
        (health_label, health_plist),
    ):
        run(
            [launchctl, "bootout", f"{domain}/{current_label}"],
            check=False,
            capture=True,
        )
        run([launchctl, "bootstrap", domain, str(path)])
    return [label, health_label]


def bridge_command(
    config: BridgeConfig,
    command: str,
) -> subprocess.CompletedProcess[str]:
    return run(
        [
            str(runtime_cli(config)),
            "--config",
            str(config_path(config)),
            command,
        ],
        capture=True,
    )


def activate(args: argparse.Namespace) -> dict[str, Any]:
    config = BridgeConfig.from_file(args.config)
    if not runtime_cli(config).is_file():
        raise InstallerError("Prepared runtime is missing; run prepare first")
    bridge_command(config, "sync-once")
    bridge_command(config, "doctor")
    services = (
        install_launchd(config)
        if sys.platform == "darwin"
        else install_systemd(config)
    )
    return {
        "ok": True,
        "instance": config.instance_id,
        "services": services,
        "config": str(config_path(config)),
    }


def deactivate(args: argparse.Namespace) -> dict[str, Any]:
    config = BridgeConfig.from_file(args.config)
    removed: list[str] = []
    if sys.platform == "darwin":
        launchctl = shutil.which("launchctl") or "/bin/launchctl"
        safe_instance = config.instance_id.replace("-", ".")
        label = f"{SERVICE_PREFIX}.{safe_instance}"
        labels = (label, f"{label}.health")
        domain = f"gui/{os.getuid()}"
        directory = Path.home() / "Library" / "LaunchAgents"
        for current_label in labels:
            run(
                [launchctl, "bootout", f"{domain}/{current_label}"],
                check=False,
                capture=True,
            )
            path = directory / f"{current_label}.plist"
            path.unlink(missing_ok=True)
            removed.append(current_label)
    else:
        unit_names = (
            f"codex-telegram-bridge-{config.instance_id}.service",
            f"codex-telegram-bridge-{config.instance_id}-health.timer",
        )
        for unit in unit_names:
            run(
                ["systemctl", "--user", "disable", "--now", unit],
                check=False,
                capture=True,
            )
        directory = Path.home() / ".config" / "systemd" / "user"
        for name in (
            *unit_names,
            f"codex-telegram-bridge-{config.instance_id}-health.service",
        ):
            (directory / name).unlink(missing_ok=True)
            removed.append(name)
        run(["systemctl", "--user", "daemon-reload"])
    return {
        "ok": True,
        "instance": config.instance_id,
        "servicesRemoved": removed,
        "stateRetained": str(config.state_dir),
    }


def configure_secret(args: argparse.Namespace) -> dict[str, Any]:
    config = BridgeConfig.from_file(args.config)
    if config.secret_backend == "macos-keychain":
        if sys.platform != "darwin":
            raise InstallerError("macOS Keychain backend requires macOS")
        security = "/usr/bin/security"
        print(
            "macOS Keychain will prompt for the Telegram bot token. "
            "The token is not passed in command arguments."
        )
        run(
            [
                security,
                "add-generic-password",
                "-U",
                "-a",
                getpass.getuser(),
                "-s",
                config.secret_reference,
                "-w",
            ]
        )
    elif config.secret_backend == "file":
        secret_path = Path(config.secret_reference).expanduser()
        ensure_private_directory(secret_path.parent)
        token = getpass.getpass("Telegram bot token: ").strip()
        if not token:
            raise InstallerError("Token was empty")
        descriptor = os.open(
            secret_path,
            os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
            0o600,
        )
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(token)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(secret_path, 0o600)
    else:
        raise InstallerError(
            "Create a Proton Pass login item whose password is the bot token, "
            "authenticate pass-cli, then run secret-check. The installer "
            "never writes Proton Pass secrets."
        )
    token = read_bot_token(config)
    if not token:
        raise InstallerError("Secret backend returned an empty token")
    return {
        "ok": True,
        "instance": config.instance_id,
        "secretBackend": config.secret_backend,
    }


def secret_check(args: argparse.Namespace) -> dict[str, Any]:
    config = BridgeConfig.from_file(args.config)
    token = read_bot_token(config)
    return {
        "ok": bool(token),
        "instance": config.instance_id,
        "secretBackend": config.secret_backend,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Install one isolated Codex Telegram bridge instance."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    prepare_parser = subparsers.add_parser("prepare")
    prepare_parser.add_argument("--workspace", default=str(Path.cwd()))
    prepare_parser.add_argument("--instance", type=validate_instance)
    prepare_parser.add_argument("--state-dir")
    prepare_parser.add_argument(
        "--secret-backend",
        choices=("macos-keychain", "proton-pass", "file"),
    )
    prepare_parser.add_argument("--secret-reference")
    prepare_parser.add_argument("--secret-vault")
    prepare_parser.add_argument("--codex-binary")
    prepare_parser.add_argument(
        "--max-active-turns",
        type=int,
        default=None,
        help=(
            "Maximum simultaneous Codex turns; 0 keeps the default "
            "unlimited behavior."
        ),
    )
    prepare_parser.add_argument(
        "--codex-full-access",
        action="store_true",
        help=(
            "Run Telegram-started Codex turns with approval policy 'never' "
            "and the danger-full-access sandbox."
        ),
    )
    prepare_parser.add_argument(
        "--skip-app-server-bootstrap",
        action="store_true",
    )
    for name in ("activate", "deactivate", "configure-secret", "secret-check"):
        command = subparsers.add_parser(name)
        command.add_argument("--config", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    os.umask(0o077)
    args = build_parser().parse_args(argv)
    try:
        if args.command == "prepare":
            result = prepare(args)
        elif args.command == "activate":
            result = activate(args)
        elif args.command == "deactivate":
            result = deactivate(args)
        elif args.command == "configure-secret":
            result = configure_secret(args)
        elif args.command == "secret-check":
            result = secret_check(args)
        else:
            raise InstallerError(f"Unsupported command: {args.command}")
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    except (InstallerError, OSError, RuntimeError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
