from __future__ import annotations

import json
import ipaddress
import math
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
MEDIA_WORKER_REQUIRED_FIELDS = frozenset(
    {
        "host",
        "port",
        "server_name",
        "ca_certificate",
        "client_certificate",
        "client_key",
    }
)
MEDIA_WORKER_OPTIONAL_FIELDS = frozenset(
    {
        "request_timeout_seconds",
        "processing_timeout_seconds",
        "failure_threshold",
        "cooldown_seconds",
    }
)


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


def _validated_media_worker_name(value: object, *, field: str) -> str:
    name = str(value).strip()
    if (
        not name
        or len(name) > 253
        or any(character.isspace() for character in name)
        or any(character in name for character in ("/", "\\", "\x00"))
    ):
        raise ValueError(f"media worker {field} is invalid")
    try:
        ipaddress.ip_address(name)
    except ValueError:
        try:
            ascii_name = name.encode("idna").decode("ascii")
        except UnicodeError:
            raise ValueError(f"media worker {field} is invalid") from None
        labels = ascii_name.split(".")
        if any(
            not label
            or len(label) > 63
            or not label[0].isalnum()
            or not label[-1].isalnum()
            or any(
                not (character.isalnum() or character == "-")
                for character in label
            )
            for label in labels
        ):
            raise ValueError(f"media worker {field} is invalid")
        name = ascii_name
    return name


@dataclass(frozen=True)
class MediaWorkerClientConfig:
    """Connection metadata for one optional, least-privilege media worker."""

    host: str
    port: int
    server_name: str
    ca_certificate: Path
    client_certificate: Path
    client_key: Path
    request_timeout_seconds: float = 30.0
    processing_timeout_seconds: float = 180.0
    failure_threshold: int = 3
    cooldown_seconds: float = 300.0

    def __post_init__(self) -> None:
        host = _validated_media_worker_name(self.host, field="host")
        server_name = _validated_media_worker_name(
            self.server_name,
            field="TLS server name",
        )
        if (
            isinstance(self.port, bool)
            or not isinstance(self.port, int)
            or not 1 <= self.port <= 65535
        ):
            raise ValueError("media worker port must be between 1 and 65535")
        request_timeout = float(self.request_timeout_seconds)
        if not math.isfinite(request_timeout) or not 1 <= request_timeout <= 60:
            raise ValueError(
                "media worker request timeout must be between 1 and 60 seconds"
            )
        processing_timeout = float(self.processing_timeout_seconds)
        if (
            not math.isfinite(processing_timeout)
            or not 1 <= processing_timeout <= 300
        ):
            raise ValueError(
                "media worker processing timeout must be between 1 and "
                "300 seconds"
            )
        if (
            isinstance(self.failure_threshold, bool)
            or not isinstance(self.failure_threshold, int)
            or not 1 <= self.failure_threshold <= 10
        ):
            raise ValueError(
                "media worker failure threshold must be between 1 and 10"
            )
        cooldown = float(self.cooldown_seconds)
        if not math.isfinite(cooldown) or not 1 <= cooldown <= 3600:
            raise ValueError(
                "media worker cooldown must be between 1 and 3600 seconds"
            )
        object.__setattr__(self, "host", host)
        object.__setattr__(self, "server_name", server_name)
        object.__setattr__(self, "request_timeout_seconds", request_timeout)
        object.__setattr__(
            self,
            "processing_timeout_seconds",
            processing_timeout,
        )
        object.__setattr__(self, "cooldown_seconds", cooldown)
        for field_name in (
            "ca_certificate",
            "client_certificate",
            "client_key",
        ):
            path = Path(
                os.path.abspath(Path(getattr(self, field_name)).expanduser())
            )
            object.__setattr__(self, field_name, path)

    @classmethod
    def from_payload(
        cls,
        payload: dict[str, Any],
    ) -> "MediaWorkerClientConfig":
        unknown = set(payload) - (
            MEDIA_WORKER_REQUIRED_FIELDS | MEDIA_WORKER_OPTIONAL_FIELDS
        )
        if unknown:
            raise ValueError(
                "media worker configuration contains unsupported fields: "
                + ", ".join(sorted(unknown))
            )
        missing = MEDIA_WORKER_REQUIRED_FIELDS - set(payload)
        if missing:
            raise ValueError(
                "media worker configuration is missing: "
                + ", ".join(sorted(missing))
            )
        return cls(
            host=str(payload["host"]),
            port=payload["port"],
            server_name=str(payload["server_name"]),
            ca_certificate=Path(str(payload["ca_certificate"])),
            client_certificate=Path(str(payload["client_certificate"])),
            client_key=Path(str(payload["client_key"])),
            request_timeout_seconds=float(
                payload.get("request_timeout_seconds", 30)
            ),
            processing_timeout_seconds=float(
                payload.get("processing_timeout_seconds", 180)
            ),
            failure_threshold=payload.get("failure_threshold", 3),
            cooldown_seconds=float(payload.get("cooldown_seconds", 300)),
        )

    @classmethod
    def from_file(
        cls,
        path: str | Path,
    ) -> "MediaWorkerClientConfig":
        requested_path = Path(path).expanduser()
        if requested_path.is_symlink():
            raise ValueError(
                "media worker client configuration must not be a symlink"
            )
        status = requested_path.stat()
        if (
            not stat.S_ISREG(status.st_mode)
            or status.st_uid != os.getuid()
            or stat.S_IMODE(status.st_mode) & 0o077
        ):
            raise ValueError(
                "media worker client configuration must be an owner-only "
                "regular file"
            )
        payload = json.loads(requested_path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError(
                "media worker client configuration must be a JSON object"
            )
        return cls.from_payload(payload)

    def as_file_payload(self) -> dict[str, Any]:
        return {
            "host": self.host,
            "port": self.port,
            "server_name": self.server_name,
            "ca_certificate": str(self.ca_certificate),
            "client_certificate": str(self.client_certificate),
            "client_key": str(self.client_key),
            "request_timeout_seconds": self.request_timeout_seconds,
            "processing_timeout_seconds": self.processing_timeout_seconds,
            "failure_threshold": self.failure_threshold,
            "cooldown_seconds": self.cooldown_seconds,
        }


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
    max_active_turns: int = 0
    codex_full_access: bool = False
    compatible_codex_versions: tuple[str, ...] = DEFAULT_COMPATIBLE_CODEX_VERSIONS
    ffmpeg_binary: str = "ffmpeg"
    media_processing_timeout_seconds: float = 120
    media_retention_days: int = 30
    media_storage_limit_bytes: int = 512 * 1024 * 1024
    telegram_media_max_bytes: int = TELEGRAM_CLOUD_DOWNLOAD_LIMIT_BYTES
    media_worker: MediaWorkerClientConfig | None = None

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
        if (
            isinstance(self.max_active_turns, bool)
            or not isinstance(self.max_active_turns, int)
            or self.max_active_turns < 0
        ):
            raise ValueError(
                "max_active_turns must be a non-negative integer"
            )
        if not isinstance(self.codex_full_access, bool):
            raise ValueError("codex_full_access must be a boolean")
        if self.media_worker is not None and not isinstance(
            self.media_worker,
            MediaWorkerClientConfig,
        ):
            raise ValueError(
                "media_worker must be a MediaWorkerClientConfig or null"
            )

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
        max_active_turns: int | None = None,
        codex_full_access: bool = False,
        media_worker: MediaWorkerClientConfig | None = None,
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
            max_active_turns=(
                max_active_turns
                if max_active_turns is not None
                else int(
                    os.environ.get(
                        "CODEX_TELEGRAM_MAX_ACTIVE_TURNS",
                        "0",
                    )
                )
            ),
            codex_full_access=codex_full_access,
            media_worker=media_worker,
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
        media_worker_payload = payload.get("media_worker")
        if media_worker_payload is not None and not isinstance(
            media_worker_payload,
            dict,
        ):
            raise ValueError("media worker configuration must be an object")
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
            max_active_turns=payload.get("max_active_turns", 0),
            codex_full_access=payload.get("codex_full_access", False),
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
            media_worker=(
                MediaWorkerClientConfig.from_payload(media_worker_payload)
                if isinstance(media_worker_payload, dict)
                else None
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
            "max_active_turns": self.max_active_turns,
            "codex_full_access": self.codex_full_access,
            "media_processing_timeout_seconds": (
                self.media_processing_timeout_seconds
            ),
            "media_retention_days": self.media_retention_days,
            "media_storage_limit_bytes": self.media_storage_limit_bytes,
            "telegram_media_max_bytes": self.telegram_media_max_bytes,
            "media_worker": (
                self.media_worker.as_file_payload()
                if self.media_worker is not None
                else None
            ),
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
