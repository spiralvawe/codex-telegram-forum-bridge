from __future__ import annotations

import json
import os
import re
import shutil
import stat
import subprocess
import sys
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any


DEFAULT_COMPATIBLE_CODEX_VERSIONS = ("0.146.0-alpha.3.1",)
DEFAULT_CODEX_APP_SERVER_SOCKET = (
    Path.home()
    / ".codex"
    / "app-server-control"
    / "app-server-control.sock"
)
TELEGRAM_CLOUD_DOWNLOAD_LIMIT_BYTES = 20 * 1024 * 1024
SECRET_BACKENDS = frozenset({"macos-keychain", "proton-pass", "file"})


def default_state_root() -> Path:
    override = os.environ.get("CODEX_TELEGRAM_STATE_ROOT")
    if override:
        return Path(override).expanduser()
    if sys.platform == "darwin":
        return (
            Path.home()
            / "Library"
            / "Application Support"
            / "CodexTelegramBridge"
        )
    data_home = os.environ.get("XDG_DATA_HOME")
    if data_home:
        return Path(data_home).expanduser() / "codex-telegram-bridge"
    return Path.home() / ".local" / "share" / "codex-telegram-bridge"


def default_instance_id(workspace: str | Path) -> str:
    resolved = Path(workspace).expanduser().resolve()
    slug = re.sub(r"[^a-z0-9]+", "-", resolved.name.casefold()).strip("-")
    slug = (slug or "project")[:32]
    digest = sha256(str(resolved).encode("utf-8")).hexdigest()[:10]
    return f"{slug}-{digest}"


def detect_codex_binary() -> str:
    override = os.environ.get("CODEX_TELEGRAM_CODEX_BINARY")
    if override:
        return str(Path(override).expanduser())
    discovered = shutil.which("codex")
    if discovered:
        return discovered
    bundled = Path("/Applications/ChatGPT.app/Contents/Resources/codex")
    if bundled.is_file():
        return str(bundled)
    return "codex"


def detect_ffmpeg_binary() -> str:
    override = os.environ.get("CODEX_TELEGRAM_FFMPEG_BINARY")
    if override:
        return str(Path(override).expanduser())
    discovered = shutil.which("ffmpeg")
    return discovered or "ffmpeg"


def default_secret_backend() -> str:
    override = os.environ.get("CODEX_TELEGRAM_SECRET_BACKEND")
    if override:
        return override
    if sys.platform == "darwin":
        return "macos-keychain"
    if shutil.which("pass-cli"):
        return "proton-pass"
    return "file"


@dataclass(frozen=True)
class BridgeConfig:
    workspace: Path
    state_dir: Path
    instance_id: str = "default"
    secret_backend: str = "macos-keychain"
    secret_reference: str = "Codex-Telegram-bot"
    secret_vault: str | None = None
    keychain_service: str | None = None
    codex_binary: str = "codex"
    codex_app_server_socket: Path = DEFAULT_CODEX_APP_SERVER_SOCKET
    thread_poll_seconds: float = 3.0
    telegram_long_poll_seconds: int = 25
    initial_history_messages: int = 0
    reasoning_mode: str = "off"
    compatible_codex_versions: tuple[str, ...] = DEFAULT_COMPATIBLE_CODEX_VERSIONS
    ffmpeg_binary: str = "ffmpeg"
    media_processing_timeout_seconds: float = 120
    media_retention_days: int = 30
    media_storage_limit_bytes: int = 512 * 1024 * 1024
    telegram_media_max_bytes: int = TELEGRAM_CLOUD_DOWNLOAD_LIMIT_BYTES

    def __post_init__(self) -> None:
        backend = self.secret_backend.strip().casefold()
        if backend not in SECRET_BACKENDS:
            raise ValueError(
                f"Unsupported secret backend {self.secret_backend!r}; "
                f"choose one of {sorted(SECRET_BACKENDS)}"
            )
        object.__setattr__(self, "secret_backend", backend)
        if self.keychain_service and backend == "macos-keychain":
            object.__setattr__(self, "secret_reference", self.keychain_service)

    @property
    def database_path(self) -> Path:
        return self.state_dir / "bridge.sqlite3"

    @property
    def media_directory(self) -> Path:
        return self.state_dir / "media"

    @property
    def log_path(self) -> Path:
        return self.state_dir / "bridge.log"

    @classmethod
    def from_paths(
        cls,
        workspace: str | Path,
        state_dir: str | Path | None = None,
        keychain_service: str | None = None,
        *,
        instance_id: str | None = None,
        secret_backend: str | None = None,
        secret_reference: str | None = None,
        secret_vault: str | None = None,
    ) -> "BridgeConfig":
        workspace_path = Path(workspace).expanduser().resolve()
        selected_instance = (
            instance_id
            or os.environ.get("CODEX_TELEGRAM_INSTANCE")
            or default_instance_id(workspace_path)
        )
        state_path = (
            Path(state_dir).expanduser().resolve()
            if state_dir
            else (default_state_root() / selected_instance).resolve()
        )
        backend = secret_backend or default_secret_backend()
        backend = backend.strip().casefold()
        default_reference = {
            "macos-keychain": f"CodexTelegramBridge-{selected_instance}",
            "proton-pass": f"Codex Telegram Bot - {selected_instance}",
            "file": str(state_path / "secrets" / "bot-token"),
        }.get(backend, "")
        reference = (
            secret_reference
            or keychain_service
            or os.environ.get("CODEX_TELEGRAM_SECRET_REFERENCE")
            or os.environ.get("CODEX_TELEGRAM_KEYCHAIN_SERVICE")
            or default_reference
        )
        return cls(
            workspace=workspace_path,
            state_dir=state_path,
            instance_id=selected_instance,
            secret_backend=backend,
            secret_reference=reference,
            secret_vault=(
                secret_vault
                or os.environ.get("CODEX_TELEGRAM_PROTON_PASS_VAULT")
                or None
            ),
            keychain_service=(
                reference if backend == "macos-keychain" else None
            ),
            codex_binary=detect_codex_binary(),
            codex_app_server_socket=Path(
                os.environ.get(
                    "CODEX_TELEGRAM_APP_SERVER_SOCKET",
                    str(DEFAULT_CODEX_APP_SERVER_SOCKET),
                )
            ).expanduser(),
            thread_poll_seconds=float(
                os.environ.get("CODEX_TELEGRAM_THREAD_POLL_SECONDS", "3")
            ),
            telegram_long_poll_seconds=int(
                os.environ.get("CODEX_TELEGRAM_LONG_POLL_SECONDS", "25")
            ),
            initial_history_messages=int(
                os.environ.get("CODEX_TELEGRAM_INITIAL_HISTORY_MESSAGES", "0")
            ),
            reasoning_mode=os.environ.get(
                "CODEX_TELEGRAM_REASONING_MODE", "off"
            ).lower(),
            ffmpeg_binary=detect_ffmpeg_binary(),
            media_processing_timeout_seconds=float(
                os.environ.get(
                    "CODEX_TELEGRAM_MEDIA_PROCESSING_TIMEOUT_SECONDS",
                    "120",
                )
            ),
            media_retention_days=max(
                0,
                int(
                    os.environ.get(
                        "CODEX_TELEGRAM_MEDIA_RETENTION_DAYS",
                        "30",
                    )
                ),
            ),
            media_storage_limit_bytes=max(
                1,
                int(
                    os.environ.get(
                        "CODEX_TELEGRAM_MEDIA_STORAGE_LIMIT_BYTES",
                        str(512 * 1024 * 1024),
                    )
                ),
            ),
            telegram_media_max_bytes=min(
                TELEGRAM_CLOUD_DOWNLOAD_LIMIT_BYTES,
                max(
                    1,
                    int(
                        os.environ.get(
                            "CODEX_TELEGRAM_MEDIA_MAX_BYTES",
                            str(TELEGRAM_CLOUD_DOWNLOAD_LIMIT_BYTES),
                        )
                    ),
                ),
            ),
            compatible_codex_versions=tuple(
                version.strip()
                for version in os.environ.get(
                    "CODEX_TELEGRAM_COMPATIBLE_CODEX_VERSIONS",
                    ",".join(DEFAULT_COMPATIBLE_CODEX_VERSIONS),
                ).split(",")
                if version.strip()
            ),
        )

    @classmethod
    def from_file(cls, path: str | Path) -> "BridgeConfig":
        requested_path = Path(path).expanduser()
        if requested_path.is_symlink():
            raise ValueError("Bridge configuration must not be a symlink")
        status = requested_path.stat()
        if (
            not stat.S_ISREG(status.st_mode)
            or status.st_uid != os.getuid()
            or stat.S_IMODE(status.st_mode) & 0o077
        ):
            raise ValueError(
                "Bridge configuration must be an owner-only regular file"
            )
        config_path = requested_path.resolve()
        payload = json.loads(config_path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("Bridge configuration must be a JSON object")
        required = {"workspace", "state_dir", "instance_id"}
        missing = sorted(required - payload.keys())
        if missing:
            raise ValueError(
                f"Bridge configuration is missing: {', '.join(missing)}"
            )
        versions = payload.get("compatible_codex_versions")
        return cls(
            workspace=Path(str(payload["workspace"])).expanduser().resolve(),
            state_dir=Path(str(payload["state_dir"])).expanduser().resolve(),
            instance_id=str(payload["instance_id"]),
            secret_backend=str(
                payload.get("secret_backend") or default_secret_backend()
            ),
            secret_reference=str(payload.get("secret_reference") or ""),
            secret_vault=(
                str(payload["secret_vault"])
                if payload.get("secret_vault")
                else None
            ),
            keychain_service=(
                str(payload["secret_reference"])
                if payload.get("secret_backend") == "macos-keychain"
                else None
            ),
            codex_binary=str(
                payload.get("codex_binary") or detect_codex_binary()
            ),
            codex_app_server_socket=Path(
                str(
                    payload.get("codex_app_server_socket")
                    or DEFAULT_CODEX_APP_SERVER_SOCKET
                )
            ).expanduser(),
            thread_poll_seconds=float(payload.get("thread_poll_seconds", 3)),
            telegram_long_poll_seconds=int(
                payload.get("telegram_long_poll_seconds", 25)
            ),
            initial_history_messages=int(
                payload.get("initial_history_messages", 0)
            ),
            reasoning_mode=str(payload.get("reasoning_mode", "off")).lower(),
            compatible_codex_versions=(
                tuple(str(value) for value in versions)
                if isinstance(versions, list)
                else DEFAULT_COMPATIBLE_CODEX_VERSIONS
            ),
            ffmpeg_binary=str(
                payload.get("ffmpeg_binary") or detect_ffmpeg_binary()
            ),
            media_processing_timeout_seconds=float(
                payload.get("media_processing_timeout_seconds", 120)
            ),
            media_retention_days=max(
                0, int(payload.get("media_retention_days", 30))
            ),
            media_storage_limit_bytes=max(
                1,
                int(
                    payload.get(
                        "media_storage_limit_bytes",
                        512 * 1024 * 1024,
                    )
                ),
            ),
            telegram_media_max_bytes=min(
                TELEGRAM_CLOUD_DOWNLOAD_LIMIT_BYTES,
                max(
                    1,
                    int(
                        payload.get(
                            "telegram_media_max_bytes",
                            TELEGRAM_CLOUD_DOWNLOAD_LIMIT_BYTES,
                        )
                    ),
                ),
            ),
        )

    def as_file_payload(self) -> dict[str, Any]:
        return {
            "format_version": 1,
            "workspace": str(self.workspace),
            "state_dir": str(self.state_dir),
            "instance_id": self.instance_id,
            "secret_backend": self.secret_backend,
            "secret_reference": self.secret_reference,
            "secret_vault": self.secret_vault,
            "codex_binary": self.codex_binary,
            "codex_app_server_socket": str(self.codex_app_server_socket),
            "ffmpeg_binary": self.ffmpeg_binary,
            "compatible_codex_versions": list(
                self.compatible_codex_versions
            ),
            "thread_poll_seconds": self.thread_poll_seconds,
            "telegram_long_poll_seconds": self.telegram_long_poll_seconds,
            "initial_history_messages": self.initial_history_messages,
            "reasoning_mode": self.reasoning_mode,
            "media_processing_timeout_seconds": (
                self.media_processing_timeout_seconds
            ),
            "media_retention_days": self.media_retention_days,
            "media_storage_limit_bytes": self.media_storage_limit_bytes,
            "telegram_media_max_bytes": self.telegram_media_max_bytes,
        }


def read_keychain_secret(service: str) -> str:
    result = subprocess.run(
        [
            "/usr/bin/security",
            "find-generic-password",
            "-w",
            "-s",
            service,
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"macOS Keychain item is unavailable for service {service!r}"
        )
    secret = result.stdout.strip()
    if not secret:
        raise RuntimeError(f"Keychain item for service {service!r} is empty")
    return secret


def read_proton_pass_secret(reference: str, vault: str | None = None) -> str:
    command = shutil.which("pass-cli")
    if not command:
        raise RuntimeError("Proton Pass CLI (pass-cli) is unavailable")
    arguments = [command, "item", "view"]
    if reference.startswith("pass://"):
        arguments.append(reference)
    else:
        arguments.extend(["--item-title", reference])
    if vault:
        arguments.extend(["--vault-name", vault])
    arguments.extend(["--field", "password"])
    result = subprocess.run(
        arguments,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    if result.returncode != 0:
        raise RuntimeError(
            "Proton Pass item is unavailable; authenticate pass-cli and "
            "verify the configured item reference"
        )
    secret = result.stdout.strip()
    if not secret:
        raise RuntimeError("Proton Pass returned an empty password field")
    return secret


def read_file_secret(path: str | Path) -> str:
    secret_path = Path(path).expanduser()
    if secret_path.is_symlink():
        raise RuntimeError("Secret file must not be a symlink")
    status = secret_path.stat()
    if (
        not stat.S_ISREG(status.st_mode)
        or status.st_uid != os.getuid()
        or stat.S_IMODE(status.st_mode) & 0o077
    ):
        raise RuntimeError("Secret file must be an owner-only regular file")
    secret = secret_path.read_text(encoding="utf-8").strip()
    if not secret:
        raise RuntimeError("Secret file is empty")
    return secret


def read_bot_token(config: BridgeConfig) -> str:
    if config.secret_backend == "macos-keychain":
        return read_keychain_secret(config.secret_reference)
    if config.secret_backend == "proton-pass":
        return read_proton_pass_secret(
            config.secret_reference,
            config.secret_vault,
        )
    if config.secret_backend == "file":
        return read_file_secret(config.secret_reference)
    raise RuntimeError(f"Unsupported secret backend: {config.secret_backend}")
