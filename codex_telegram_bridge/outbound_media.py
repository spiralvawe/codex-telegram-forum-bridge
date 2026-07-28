from __future__ import annotations

import base64
import binascii
import hashlib
import mimetypes
import os
import re
import stat
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import parse_qsl, unquote, urlparse


DEFAULT_MAX_ATTACHMENT_BYTES = 49 * 1024 * 1024
MAX_REMOTE_URL_LENGTH = 8_192
MEDIA_DIRECTORY_PATTERN = re.compile(r"[0-9a-f]{32}")
SENSITIVE_QUERY_KEY = re.compile(
    r"(?i)(?:access[_-]?token|api[_-]?key|auth|credential|password|"
    r"secret|signature|sig|token)"
)
BLOCKED_PATH_PARTS = frozenset({".git", ".venv", "node_modules"})
BLOCKED_PATH_NAMES = frozenset({".env", "secrets.yaml", "secrets.yml"})
BLOCKED_PATH_SUFFIXES = frozenset(
    {
        ".db",
        ".key",
        ".p12",
        ".pfx",
        ".pem",
        ".sqlite",
        ".sqlite3",
    }
)
PHOTO_SUFFIXES = frozenset(
    {
        ".avif",
        ".bmp",
        ".heic",
        ".heif",
        ".jpeg",
        ".jpg",
        ".png",
        ".tif",
        ".tiff",
        ".webp",
    }
)
ANIMATION_SUFFIXES = frozenset({".gif"})
VIDEO_SUFFIXES = frozenset(
    {".avi", ".m4v", ".mkv", ".mov", ".mp4", ".mpeg", ".mpg", ".webm"}
)
AUDIO_SUFFIXES = frozenset(
    {".aac", ".flac", ".m4a", ".mp3", ".oga", ".ogg", ".opus", ".wav"}
)
MIME_SUFFIXES = {
    "audio/aac": ".aac",
    "audio/flac": ".flac",
    "audio/m4a": ".m4a",
    "audio/mp4": ".m4a",
    "audio/mpeg": ".mp3",
    "audio/ogg": ".ogg",
    "audio/opus": ".opus",
    "audio/wav": ".wav",
    "audio/x-wav": ".wav",
    "image/avif": ".avif",
    "image/bmp": ".bmp",
    "image/gif": ".gif",
    "image/heic": ".heic",
    "image/heif": ".heif",
    "image/jpeg": ".jpg",
    "image/png": ".png",
    "image/tiff": ".tiff",
    "image/webp": ".webp",
    "video/mp4": ".mp4",
    "video/quicktime": ".mov",
    "video/webm": ".webm",
}


class OutboundMediaError(RuntimeError):
    def __init__(self, kind: str):
        super().__init__(kind)
        self.kind = kind


@dataclass(frozen=True)
class OutboundAttachment:
    media_kind: str
    display_name: str
    fingerprint: str
    path: Path | None = None
    url: str | None = None

    def __post_init__(self) -> None:
        if self.media_kind not in {
            "animation",
            "audio",
            "document",
            "photo",
            "video",
        }:
            raise ValueError("unsupported outbound media kind")
        if (self.path is None) == (self.url is None):
            raise ValueError("exactly one outbound media source is required")


def _safe_display_name(value: str, fallback: str) -> str:
    name = Path(value).name.strip() if value else ""
    name = re.sub(r"[\x00-\x1f\x7f]+", " ", name)
    name = re.sub(r"\s+", " ", name).strip()
    return (name or fallback)[:160]


def _media_kind(
    *,
    name: str,
    mime_type: str = "",
    preferred: str | None = None,
) -> str:
    if preferred in {"animation", "audio", "document", "photo", "video"}:
        return preferred
    suffix = Path(name).suffix.lower()
    lowered_mime = mime_type.lower()
    if suffix in ANIMATION_SUFFIXES or lowered_mime == "image/gif":
        return "animation"
    if suffix in PHOTO_SUFFIXES or lowered_mime.startswith("image/"):
        return "photo"
    if suffix in VIDEO_SUFFIXES or lowered_mime.startswith("video/"):
        return "video"
    if suffix in AUDIO_SUFFIXES or lowered_mime.startswith("audio/"):
        return "audio"
    return "document"


def _suffix_for_mime(mime_type: str, media_kind: str) -> str:
    normalized = mime_type.lower().split(";", 1)[0].strip()
    suffix = MIME_SUFFIXES.get(normalized)
    if suffix:
        return suffix
    guessed = mimetypes.guess_extension(normalized, strict=False)
    if guessed:
        return ".jpg" if guessed == ".jpe" else guessed
    return {
        "animation": ".gif",
        "audio": ".bin",
        "photo": ".img",
        "video": ".bin",
    }.get(media_kind, ".bin")


def _hash_file(path: Path, *, media_kind: str) -> str:
    digest = hashlib.sha256()
    digest.update(media_kind.encode("ascii"))
    digest.update(b"\0")
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class OutboundMediaResolver:
    def __init__(
        self,
        *,
        root: str | Path,
        max_bytes: int = DEFAULT_MAX_ATTACHMENT_BYTES,
    ) -> None:
        self.root = Path(root).expanduser().resolve(strict=False)
        self.max_bytes = max(1, int(max_bytes))

    def ensure_root(self) -> None:
        if self.root.is_symlink():
            raise OutboundMediaError("unsafe_storage")
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        status = self.root.stat()
        if not stat.S_ISDIR(status.st_mode) or status.st_uid != os.getuid():
            raise OutboundMediaError("unsafe_storage")
        os.chmod(self.root, 0o700)

    def resolve_user_input(
        self,
        content: dict[str, object],
        *,
        index: int,
    ) -> OutboundAttachment | None:
        input_type = str(content.get("type") or "")
        if input_type == "image":
            return self.resolve_locator(
                str(content.get("url") or ""),
                display_name=f"codex-image-{index}.jpg",
                preferred_kind="photo",
            )
        if input_type == "localImage":
            return self.resolve_path(
                str(content.get("path") or ""),
                display_name=f"codex-image-{index}",
                preferred_kind="photo",
            )
        if input_type == "audio":
            return self.resolve_locator(
                str(content.get("url") or ""),
                display_name=f"codex-audio-{index}.mp3",
                preferred_kind="audio",
            )
        if input_type == "localAudio":
            return self.resolve_path(
                str(content.get("path") or ""),
                display_name=f"codex-audio-{index}",
                preferred_kind="audio",
            )
        if input_type == "mention":
            return self.resolve_path(
                str(content.get("path") or ""),
                display_name=str(content.get("name") or ""),
            )
        return None

    def resolve_thread_item(
        self,
        item: dict[str, object],
    ) -> list[OutboundAttachment]:
        item_type = str(item.get("type") or "")
        if item_type == "imageView":
            return [
                self.resolve_path(
                    str(item.get("path") or ""),
                    display_name="codex-image-view",
                    preferred_kind="photo",
                )
            ]
        if item_type != "imageGeneration":
            return []
        saved_path = str(item.get("savedPath") or "")
        if saved_path:
            try:
                return [
                    self.resolve_path(
                        saved_path,
                        display_name="codex-generated-image",
                        preferred_kind="photo",
                    )
                ]
            except OutboundMediaError:
                pass
        result = str(item.get("result") or "")
        if not result:
            raise OutboundMediaError("missing_attachment")
        return [
            self.resolve_locator(
                result,
                display_name="codex-generated-image.png",
                preferred_kind="photo",
            )
        ]

    def resolve_locator(
        self,
        value: str,
        *,
        display_name: str,
        preferred_kind: str | None = None,
    ) -> OutboundAttachment:
        locator = value.strip()
        if not locator:
            raise OutboundMediaError("missing_attachment")
        if locator.startswith("data:"):
            return self._materialize_data_url(
                locator,
                display_name=display_name,
                preferred_kind=preferred_kind,
            )
        parsed = urlparse(locator)
        if parsed.scheme == "file":
            if parsed.netloc not in {"", "localhost"}:
                raise OutboundMediaError("unsafe_file_url")
            return self.resolve_path(
                unquote(parsed.path),
                display_name=display_name,
                preferred_kind=preferred_kind,
            )
        if parsed.scheme in {"http", "https"}:
            return self._resolve_remote_url(
                locator,
                display_name=display_name,
                preferred_kind=preferred_kind,
            )
        if parsed.scheme:
            raise OutboundMediaError("unsupported_url")
        return self.resolve_path(
            locator,
            display_name=display_name,
            preferred_kind=preferred_kind,
        )

    def resolve_path(
        self,
        value: str | Path,
        *,
        display_name: str = "",
        preferred_kind: str | None = None,
    ) -> OutboundAttachment:
        source = Path(value).expanduser()
        try:
            resolved = source.resolve(strict=True)
            status = resolved.stat()
        except (FileNotFoundError, OSError, RuntimeError, ValueError):
            raise OutboundMediaError("missing_attachment") from None
        lowered_parts = {part.lower() for part in resolved.parts}
        lowered_name = resolved.name.lower()
        if (
            source.is_symlink()
            or not stat.S_ISREG(status.st_mode)
            or status.st_uid != os.getuid()
            or status.st_size <= 0
            or status.st_size > self.max_bytes
            or lowered_parts.intersection(BLOCKED_PATH_PARTS)
            or lowered_name in BLOCKED_PATH_NAMES
            or resolved.suffix.lower() in BLOCKED_PATH_SUFFIXES
            or any(
                marker in lowered_name
                for marker in ("credential", "password", "secret", "token")
            )
        ):
            raise OutboundMediaError("unsafe_attachment")
        name = _safe_display_name(
            display_name or resolved.name,
            resolved.name or "attachment",
        )
        mime_type = mimetypes.guess_type(resolved.name)[0] or ""
        kind = _media_kind(
            name=resolved.name,
            mime_type=mime_type,
            preferred=preferred_kind,
        )
        return OutboundAttachment(
            media_kind=kind,
            display_name=name,
            fingerprint=_hash_file(resolved, media_kind=kind),
            path=resolved,
        )

    def _resolve_remote_url(
        self,
        value: str,
        *,
        display_name: str,
        preferred_kind: str | None,
    ) -> OutboundAttachment:
        if len(value) > MAX_REMOTE_URL_LENGTH:
            raise OutboundMediaError("unsafe_remote_url")
        parsed = urlparse(value)
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
        ):
            raise OutboundMediaError("unsafe_remote_url")
        for key, _ in parse_qsl(
            parsed.query,
            keep_blank_values=True,
        ):
            if SENSITIVE_QUERY_KEY.search(key):
                raise OutboundMediaError("credential_url")
        path_name = Path(unquote(parsed.path)).name
        name = _safe_display_name(
            path_name or display_name,
            display_name or "attachment",
        )
        mime_type = mimetypes.guess_type(path_name)[0] or ""
        kind = _media_kind(
            name=path_name or name,
            mime_type=mime_type,
            preferred=preferred_kind,
        )
        digest = hashlib.sha256(
            (kind + "\0" + value).encode("utf-8")
        ).hexdigest()
        return OutboundAttachment(
            media_kind=kind,
            display_name=name,
            fingerprint=digest,
            url=value,
        )

    def _materialize_data_url(
        self,
        value: str,
        *,
        display_name: str,
        preferred_kind: str | None,
    ) -> OutboundAttachment:
        header, separator, encoded = value.partition(",")
        if not separator or not header.startswith("data:"):
            raise OutboundMediaError("invalid_data_url")
        attributes = header[5:].split(";")
        mime_type = (attributes[0] or "application/octet-stream").lower()
        if "base64" not in {attribute.lower() for attribute in attributes[1:]}:
            raise OutboundMediaError("unsupported_data_url")
        estimated_size = (len(encoded) * 3) // 4
        if estimated_size > self.max_bytes + 2:
            raise OutboundMediaError("attachment_too_large")
        try:
            payload = base64.b64decode(encoded, validate=True)
        except (binascii.Error, ValueError):
            raise OutboundMediaError("invalid_data_url") from None
        if not payload or len(payload) > self.max_bytes:
            raise OutboundMediaError(
                "attachment_too_large" if payload else "invalid_data_url"
            )
        kind = _media_kind(
            name=display_name,
            mime_type=mime_type,
            preferred=preferred_kind,
        )
        if preferred_kind == "photo" and not mime_type.startswith("image/"):
            raise OutboundMediaError("invalid_image")
        if preferred_kind == "audio" and not mime_type.startswith("audio/"):
            raise OutboundMediaError("invalid_audio")
        digest = hashlib.sha256(kind.encode("ascii") + b"\0" + payload).hexdigest()
        directory = self._data_directory(digest[:32])
        suffix = _suffix_for_mime(mime_type, kind)
        destination = directory / f"outbound-{digest[:16]}{suffix}"
        if not self._usable_materialized_file(destination, len(payload)):
            temporary = destination.with_name(
                f".{destination.name}.part-{os.getpid()}"
            )
            descriptor: int | None = None
            try:
                descriptor = os.open(
                    temporary,
                    os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                    0o600,
                )
                with os.fdopen(descriptor, "wb") as stream:
                    descriptor = None
                    stream.write(payload)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(temporary, destination)
                os.chmod(destination, 0o600)
            finally:
                if descriptor is not None:
                    os.close(descriptor)
                try:
                    temporary.unlink()
                except FileNotFoundError:
                    pass
        name = _safe_display_name(
            display_name,
            f"codex-attachment{suffix}",
        )
        if not Path(name).suffix:
            name += suffix
        return OutboundAttachment(
            media_kind=kind,
            display_name=name,
            fingerprint=digest,
            path=destination.resolve(strict=True),
        )

    def _data_directory(self, key: str) -> Path:
        if MEDIA_DIRECTORY_PATTERN.fullmatch(key) is None:
            raise ValueError("invalid outbound media key")
        self.ensure_root()
        directory = self.root / key
        if directory.is_symlink():
            raise OutboundMediaError("unsafe_storage")
        directory.mkdir(mode=0o700, exist_ok=True)
        status = directory.stat()
        if not stat.S_ISDIR(status.st_mode) or status.st_uid != os.getuid():
            raise OutboundMediaError("unsafe_storage")
        os.chmod(directory, 0o700)
        return directory

    @staticmethod
    def _usable_materialized_file(path: Path, expected_size: int) -> bool:
        try:
            status = path.stat()
        except OSError:
            return False
        return bool(
            not path.is_symlink()
            and stat.S_ISREG(status.st_mode)
            and status.st_uid == os.getuid()
            and status.st_size == expected_size
        )
