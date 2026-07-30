from __future__ import annotations

import ipaddress
import json
import math
import os
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Any


MAX_CONFIG_BYTES = 64 * 1024
WORKER_CONFIG_FIELDS = frozenset(
    {
        "listen_host",
        "listen_port",
        "state_dir",
        "ffmpeg_binary",
        "tls_server_cert",
        "tls_server_key",
        "tls_client_ca",
        "queue_capacity",
        "concurrency",
        "request_timeout_seconds",
        "processing_timeout_seconds",
        "shutdown_timeout_seconds",
        "retention_seconds",
    }
)
FORBIDDEN_CONFIG_KEY_FRAGMENTS = frozenset(
    {
        "telegram",
        "bot_token",
        "codex",
        "workspace",
        "proton",
        "password",
        "secret",
        "ssh",
        "sudo",
        "username",
        "permission",
    }
)
SENSITIVE_DIRECTORY_NAMES = frozenset(
    {
        ".codex",
        ".ssh",
        ".password-store",
        ".telegram",
        "codextelegrambridge",
        "codex-telegram-bridge",
        "proton pass",
        "proton-pass",
        "protonpass",
        "telegram desktop",
        "telegram-desktop",
    }
)
BRIDGE_STATE_MARKERS = (
    "bridge.sqlite3",
    "bridge.sqlite3-shm",
    "bridge.sqlite3-wal",
)


class MediaWorkerConfigError(ValueError):
    """A sanitized configuration validation error."""


@dataclass(frozen=True)
class MediaWorkerConfig:
    listen_host: str
    listen_port: int
    state_dir: Path
    ffmpeg_binary: Path
    tls_server_cert: Path
    tls_server_key: Path
    tls_client_ca: Path
    queue_capacity: int
    concurrency: int
    request_timeout_seconds: float
    processing_timeout_seconds: float
    shutdown_timeout_seconds: float
    retention_seconds: int

    @classmethod
    def from_file(cls, path: str | Path) -> "MediaWorkerConfig":
        config_path = _absolute_path(path, field="config")
        _reject_sensitive_location(config_path, field="config")
        payload = _read_owner_only_json(config_path)
        return cls.from_mapping(payload)

    @classmethod
    def from_mapping(
        cls,
        payload: dict[str, Any],
    ) -> "MediaWorkerConfig":
        if not isinstance(payload, dict):
            raise MediaWorkerConfigError(
                "configuration root must be a JSON object"
            )

        supplied_fields = frozenset(payload)
        forbidden = [
            key
            for key in supplied_fields
            if not isinstance(key, str) or _is_forbidden_key(key)
        ]
        if forbidden:
            raise MediaWorkerConfigError(
                "configuration contains a forbidden integration field"
            )
        missing = WORKER_CONFIG_FIELDS - supplied_fields
        unknown = supplied_fields - WORKER_CONFIG_FIELDS
        if missing:
            raise MediaWorkerConfigError(
                "configuration is missing required worker fields"
            )
        if unknown:
            raise MediaWorkerConfigError(
                "configuration contains unsupported fields"
            )

        listen_host = _listen_host(payload["listen_host"])
        listen_port = _bounded_integer(
            payload["listen_port"],
            field="listen_port",
            minimum=1024,
            maximum=65535,
        )
        state_dir = _absolute_path(
            _string(payload["state_dir"], field="state_dir"),
            field="state_dir",
        )
        ffmpeg_binary = _absolute_path(
            _string(payload["ffmpeg_binary"], field="ffmpeg_binary"),
            field="ffmpeg_binary",
        )
        tls_server_cert = _absolute_path(
            _string(
                payload["tls_server_cert"],
                field="tls_server_cert",
            ),
            field="tls_server_cert",
        )
        tls_server_key = _absolute_path(
            _string(
                payload["tls_server_key"],
                field="tls_server_key",
            ),
            field="tls_server_key",
        )
        tls_client_ca = _absolute_path(
            _string(payload["tls_client_ca"], field="tls_client_ca"),
            field="tls_client_ca",
        )

        for field, path in (
            ("state_dir", state_dir),
            ("ffmpeg_binary", ffmpeg_binary),
            ("tls_server_cert", tls_server_cert),
            ("tls_server_key", tls_server_key),
            ("tls_client_ca", tls_client_ca),
        ):
            _reject_sensitive_location(path, field=field)

        state_dir = _validate_private_directory(
            state_dir,
            field="state_dir",
        )
        ffmpeg_binary = _validate_executable(
            ffmpeg_binary,
            field="ffmpeg_binary",
        )
        tls_server_cert = _validate_owner_only_file(
            tls_server_cert,
            field="tls_server_cert",
        )
        tls_server_key = _validate_owner_only_file(
            tls_server_key,
            field="tls_server_key",
        )
        tls_client_ca = _validate_owner_only_file(
            tls_client_ca,
            field="tls_client_ca",
        )
        if len(
            {
                tls_server_cert,
                tls_server_key,
                tls_client_ca,
            }
        ) != 3:
            raise MediaWorkerConfigError(
                "TLS certificate, key, and client CA must be distinct files"
            )
        if any(
            _is_relative_to(tls_path, state_dir)
            for tls_path in (
                tls_server_cert,
                tls_server_key,
                tls_client_ca,
            )
        ):
            raise MediaWorkerConfigError(
                "TLS identity files must be outside mutable worker state"
            )
        for marker in BRIDGE_STATE_MARKERS:
            marker_path = state_dir / marker
            if marker_path.exists() or marker_path.is_symlink():
                raise MediaWorkerConfigError(
                    "state_dir contains bridge database state"
                )

        queue_capacity = _bounded_integer(
            payload["queue_capacity"],
            field="queue_capacity",
            minimum=1,
            maximum=16,
        )
        concurrency = _bounded_integer(
            payload["concurrency"],
            field="concurrency",
            minimum=1,
            maximum=4,
        )
        if concurrency > queue_capacity:
            raise MediaWorkerConfigError(
                "concurrency cannot exceed queue_capacity"
            )

        request_timeout_seconds = _bounded_number(
            payload["request_timeout_seconds"],
            field="request_timeout_seconds",
            minimum=1.0,
            maximum=60.0,
        )
        processing_timeout_seconds = _bounded_number(
            payload["processing_timeout_seconds"],
            field="processing_timeout_seconds",
            minimum=1.0,
            maximum=300.0,
        )
        shutdown_timeout_seconds = _bounded_number(
            payload["shutdown_timeout_seconds"],
            field="shutdown_timeout_seconds",
            minimum=1.0,
            maximum=30.0,
        )
        retention_seconds = _bounded_integer(
            payload["retention_seconds"],
            field="retention_seconds",
            minimum=60,
            maximum=30 * 24 * 60 * 60,
        )

        return cls(
            listen_host=listen_host,
            listen_port=listen_port,
            state_dir=state_dir,
            ffmpeg_binary=ffmpeg_binary,
            tls_server_cert=tls_server_cert,
            tls_server_key=tls_server_key,
            tls_client_ca=tls_client_ca,
            queue_capacity=queue_capacity,
            concurrency=concurrency,
            request_timeout_seconds=request_timeout_seconds,
            processing_timeout_seconds=processing_timeout_seconds,
            shutdown_timeout_seconds=shutdown_timeout_seconds,
            retention_seconds=retention_seconds,
        )

    def server_keyword_arguments(
        self,
        *,
        ssl_context: object,
        processor: object,
    ) -> dict[str, object]:
        """Return only the bounded inputs needed by MediaWorkerServer."""
        return {
            "host": self.listen_host,
            "port": self.listen_port,
            "ssl_context": ssl_context,
            "spool_directory": self.state_dir,
            "processor": processor,
            "queue_capacity": self.queue_capacity,
            "processing_concurrency": self.concurrency,
            "request_timeout_seconds": self.request_timeout_seconds,
            "shutdown_timeout_seconds": self.shutdown_timeout_seconds,
            "ttl_seconds": self.retention_seconds,
        }


def _read_owner_only_json(path: Path) -> dict[str, Any]:
    _validate_protected_ancestors(path, field="config")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError:
        raise MediaWorkerConfigError(
            "configuration file cannot be opened safely"
        ) from None
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise MediaWorkerConfigError(
                "configuration must be a regular file"
            )
        if metadata.st_uid != os.geteuid():
            raise MediaWorkerConfigError(
                "configuration must be owned by the worker account"
            )
        if not metadata.st_mode & stat.S_IRUSR:
            raise MediaWorkerConfigError(
                "configuration must be readable by its owner"
            )
        if metadata.st_mode & (stat.S_IXUSR | 0o077):
            raise MediaWorkerConfigError(
                "configuration permissions must be owner-only"
            )
        if metadata.st_size <= 0 or metadata.st_size > MAX_CONFIG_BYTES:
            raise MediaWorkerConfigError(
                "configuration file size is outside the safe limit"
            )
        raw = os.read(descriptor, MAX_CONFIG_BYTES + 1)
        if len(raw) > MAX_CONFIG_BYTES:
            raise MediaWorkerConfigError(
                "configuration file size is outside the safe limit"
            )
    finally:
        os.close(descriptor)

    try:
        decoded = raw.decode("utf-8")
        payload = json.loads(
            decoded,
            object_pairs_hook=_unique_object,
            parse_constant=_reject_json_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise MediaWorkerConfigError(
            "configuration must be valid UTF-8 JSON"
        ) from None
    if not isinstance(payload, dict):
        raise MediaWorkerConfigError(
            "configuration root must be a JSON object"
        )
    return payload


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise MediaWorkerConfigError(
                "configuration contains a duplicate field"
            )
        result[key] = value
    return result


def _reject_json_constant(value: str) -> None:
    del value
    raise MediaWorkerConfigError(
        "configuration numbers must be finite"
    )


def _is_forbidden_key(key: str) -> bool:
    normalized = key.casefold().replace("-", "_")
    return any(
        fragment in normalized
        for fragment in FORBIDDEN_CONFIG_KEY_FRAGMENTS
    )


def _listen_host(value: Any) -> str:
    text = _string(value, field="listen_host")
    if "%" in text:
        raise MediaWorkerConfigError(
            "listen_host must not contain an interface scope"
        )
    try:
        address = ipaddress.ip_address(text)
    except ValueError:
        raise MediaWorkerConfigError(
            "listen_host must be a literal IPv4 or IPv6 address"
        ) from None
    if address.is_multicast:
        raise MediaWorkerConfigError(
            "listen_host must not be a multicast address"
        )
    if address.is_global:
        raise MediaWorkerConfigError(
            "listen_host must be private, local, or an all-interface wildcard"
        )
    return str(address)


def _string(value: Any, *, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise MediaWorkerConfigError(f"{field} must be a non-empty string")
    if len(value) > 4096 or any(
        character in value for character in ("\x00", "\r", "\n")
    ):
        raise MediaWorkerConfigError(
            f"{field} contains unsupported characters"
        )
    return value


def _bounded_integer(
    value: Any,
    *,
    field: str,
    minimum: int,
    maximum: int,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise MediaWorkerConfigError(f"{field} must be an integer")
    if value < minimum or value > maximum:
        raise MediaWorkerConfigError(f"{field} is outside the safe range")
    return value


def _bounded_number(
    value: Any,
    *,
    field: str,
    minimum: float,
    maximum: float,
) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise MediaWorkerConfigError(f"{field} must be a number")
    selected = float(value)
    if not math.isfinite(selected):
        raise MediaWorkerConfigError(f"{field} must be finite")
    if selected < minimum or selected > maximum:
        raise MediaWorkerConfigError(f"{field} is outside the safe range")
    return selected


def _absolute_path(value: str | Path, *, field: str) -> Path:
    path = Path(value)
    if not path.is_absolute():
        raise MediaWorkerConfigError(f"{field} must be an absolute path")
    if any(character in str(path) for character in ("\x00", "\r", "\n")):
        raise MediaWorkerConfigError(
            f"{field} contains unsupported characters"
        )
    return path


def _reject_sensitive_location(path: Path, *, field: str) -> None:
    lowered_parts = {part.casefold() for part in path.parts}
    if lowered_parts & SENSITIVE_DIRECTORY_NAMES:
        raise MediaWorkerConfigError(
            f"{field} must be isolated from user credential directories"
        )


def _validate_private_directory(path: Path, *, field: str) -> Path:
    _validate_protected_ancestors(path, field=field)
    metadata = _safe_lstat(path, field=field)
    if not stat.S_ISDIR(metadata.st_mode):
        raise MediaWorkerConfigError(f"{field} must be a directory")
    if metadata.st_uid != os.geteuid():
        raise MediaWorkerConfigError(
            f"{field} must be owned by the worker account"
        )
    required = stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR
    if metadata.st_mode & required != required or metadata.st_mode & 0o077:
        raise MediaWorkerConfigError(
            f"{field} permissions must be owner-only"
        )
    return path.resolve(strict=True)


def _validate_owner_only_file(path: Path, *, field: str) -> Path:
    _validate_protected_ancestors(path, field=field)
    metadata = _safe_lstat(path, field=field)
    if not stat.S_ISREG(metadata.st_mode):
        raise MediaWorkerConfigError(f"{field} must be a regular file")
    if metadata.st_uid != os.geteuid():
        raise MediaWorkerConfigError(
            f"{field} must be owned by the worker account"
        )
    if not metadata.st_mode & stat.S_IRUSR:
        raise MediaWorkerConfigError(
            f"{field} must be readable by its owner"
        )
    if metadata.st_mode & (stat.S_IXUSR | 0o077):
        raise MediaWorkerConfigError(
            f"{field} permissions must be owner-only"
        )
    return path.resolve(strict=True)


def _validate_executable(path: Path, *, field: str) -> Path:
    _validate_protected_ancestors(
        path,
        field=field,
        allow_worker_owned=False,
    )
    metadata = _safe_lstat(path, field=field)
    if not stat.S_ISREG(metadata.st_mode):
        raise MediaWorkerConfigError(f"{field} must be a regular file")
    if metadata.st_uid != 0:
        raise MediaWorkerConfigError(
            f"{field} must be owned by root"
        )
    if metadata.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
        raise MediaWorkerConfigError(
            f"{field} must not be group- or world-writable"
        )
    if not os.access(path, os.X_OK):
        raise MediaWorkerConfigError(f"{field} must be executable")
    return path.resolve(strict=True)


def _validate_protected_ancestors(
    path: Path,
    *,
    field: str,
    allow_worker_owned: bool = True,
) -> None:
    """Reject replaceable or symlinked path ancestors."""
    try:
        canonical = path.resolve(strict=True)
    except OSError:
        raise MediaWorkerConfigError(
            f"{field} ancestors cannot be inspected"
        ) from None

    def validate_chain(selected: Path) -> None:
        child = selected
        for ancestor in selected.parents:
            try:
                metadata = ancestor.lstat()
                child_metadata = child.lstat()
            except OSError:
                raise MediaWorkerConfigError(
                    f"{field} ancestors cannot be inspected"
                ) from None
            if stat.S_ISLNK(metadata.st_mode):
                if metadata.st_uid != 0:
                    raise MediaWorkerConfigError(
                        f"{field} has a replaceable symbolic-link ancestor"
                    )
                child = ancestor
                continue
            if not stat.S_ISDIR(metadata.st_mode):
                raise MediaWorkerConfigError(
                    f"{field} ancestors must be directories"
                )
            if metadata.st_uid not in {0, os.geteuid()}:
                raise MediaWorkerConfigError(
                    f"{field} ancestors have an untrusted owner"
                )
            if (
                not allow_worker_owned
                and os.geteuid() != 0
                and metadata.st_uid == os.geteuid()
                and metadata.st_mode & stat.S_IWUSR
            ):
                raise MediaWorkerConfigError(
                    f"{field} has a worker-replaceable ancestor"
                )
            writable_by_others = bool(
                metadata.st_mode & (stat.S_IWGRP | stat.S_IWOTH)
            )
            root_sticky_boundary = bool(
                metadata.st_uid == 0
                and metadata.st_mode & stat.S_ISVTX
                and child_metadata.st_uid in {0, os.geteuid()}
            )
            if writable_by_others and not root_sticky_boundary:
                raise MediaWorkerConfigError(
                    f"{field} ancestors must not be replaceable"
                )
            child = ancestor

    validate_chain(path)
    if canonical != path:
        validate_chain(canonical)


def _safe_lstat(path: Path, *, field: str) -> os.stat_result:
    try:
        metadata = path.lstat()
    except OSError:
        raise MediaWorkerConfigError(
            f"{field} does not exist or cannot be inspected"
        ) from None
    if stat.S_ISLNK(metadata.st_mode):
        raise MediaWorkerConfigError(f"{field} must not be a symbolic link")
    return metadata


def _is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
    except ValueError:
        return False
    return True
