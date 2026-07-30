from __future__ import annotations

import contextlib
import os
import re
import shutil
import stat
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from .input_types import LocalInput


MEDIA_KEY_PATTERN = re.compile(r"[0-9a-f]{32}")
DEFAULT_RETENTION_SECONDS = 30 * 24 * 60 * 60
DEFAULT_STORAGE_LIMIT_BYTES = 512 * 1024 * 1024
SAFE_FFMPEG_PROTOCOLS = "file"


class MediaProcessingError(RuntimeError):
    def __init__(self, kind: str):
        super().__init__(kind)
        self.kind = kind


def _safe_ffmpeg_input_format(
    source_path: str | Path,
    *,
    kind: str,
) -> str:
    """Select a fixed demuxer from bounded magic instead of auto-detection."""
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(Path(source_path), flags)
    except OSError:
        raise MediaProcessingError("invalid_media") from None
    try:
        status = os.fstat(descriptor)
        if (
            not stat.S_ISREG(status.st_mode)
            or status.st_uid != os.getuid()
            or status.st_size <= 0
        ):
            raise MediaProcessingError("invalid_media")
        head = os.read(descriptor, 32)
    finally:
        os.close(descriptor)

    is_iso_bmff = (
        len(head) >= 12
        and head[4:8] == b"ftyp"
        and int.from_bytes(head[:4], "big") >= 8
    )
    if kind in {"video", "video_note"}:
        if is_iso_bmff:
            return "mov"
        raise MediaProcessingError("invalid_video")
    if kind != "voice":
        raise MediaProcessingError("invalid_media")
    if head.startswith(b"OggS"):
        return "ogg"
    if head.startswith(b"ID3") or (
        len(head) >= 2
        and head[0] == 0xFF
        and head[1] & 0xE0 == 0xE0
        and head[1] & 0x06 != 0
    ):
        return "mp3"
    if is_iso_bmff:
        return "mov"
    raise MediaProcessingError("invalid_audio")


def _ffmpeg_input_options(input_format: str) -> list[str]:
    options = [
        "-protocol_whitelist",
        SAFE_FFMPEG_PROTOCOLS,
        "-f",
        input_format,
    ]
    if input_format == "mov":
        # Keep ISO-BMFF data references inside the supplied media file.
        options.extend(["-enable_drefs", "0", "-use_absolute_path", "0"])
    return options


@dataclass(frozen=True)
class PreparedMedia:
    kind: str
    duration_seconds: int
    inputs: tuple[LocalInput, ...]

    @property
    def frame_count(self) -> int:
        return sum(item.input_type == "localImage" for item in self.inputs)

    @property
    def has_audio(self) -> bool:
        return any(item.input_type == "localAudio" for item in self.inputs)


@dataclass(frozen=True)
class PreparedDocument:
    display_name: str
    mime_type: str
    size_bytes: int
    input: LocalInput


@dataclass(frozen=True)
class MediaPruneResult:
    removed_directories: int
    retained_bytes: int


def media_request_text(
    prepared: PreparedMedia,
    *,
    user_text: str = "",
) -> str:
    duration = max(0, int(prepared.duration_seconds))
    if prepared.kind == "voice":
        body = (
            f"🎙 Голосовое сообщение из Telegram ({duration} с). "
            "Аудио приложено к этому запросу. Воспринимай речь как основной "
            "текст пользователя и ответь по существу; отдельная расшифровка "
            "нужна только если она полезна для ответа."
        )
    elif prepared.kind in {"video_note", "video"}:
        audio_text = (
            "аудиодорожка"
            if prepared.has_audio
            else "без доступной аудиодорожки"
        )
        if prepared.kind == "video_note":
            prefix = "⭕ Видеосообщение-кружок"
        else:
            prefix = "🎬 Видео"
        body = (
            f"{prefix} из Telegram ({duration} с): {audio_text} и "
            f"{prepared.frame_count} ключевых кадр(а) приложены в "
            "хронологическом порядке. Считай речь основной формулировкой "
            "запроса, а кадры — визуальным контекстом. Ответь по существу; "
            "отдельная расшифровка или покадровое описание не нужны без "
            "необходимости."
        )
    else:
        raise ValueError("unsupported prepared media kind")
    comment = user_text.strip()
    if comment:
        body += f"\n\nКомментарий пользователя: {comment}"
    return body


def document_request_text(
    prepared: PreparedDocument,
    *,
    user_text: str = "",
) -> str:
    mime_type = prepared.mime_type or "application/octet-stream"
    body = (
        "📎 Документ из Telegram приложен к этому запросу как локальный "
        f"файл «{prepared.display_name}» ({mime_type}, "
        f"{prepared.size_bytes} байт). Считай его входным файлом пользователя, "
        "определи подходящий процесс по содержимому и инструкции пользователя. "
        "Не исполняй файл как программу."
    )
    comment = user_text.strip()
    if comment:
        body += f"\n\nКомментарий пользователя: {comment}"
    return body


def safe_document_mime_type(value: str) -> str:
    safe = re.sub(
        r"[^A-Za-z0-9!#$&^_.+\-/]",
        "",
        str(value or ""),
    )[:100]
    return safe or "application/octet-stream"


def safe_document_name(value: str) -> str:
    raw = str(value or "").replace("\x00", "")
    raw = raw.replace("\\", "/").rsplit("/", 1)[-1]
    safe = re.sub(r"[\x00-\x1f\x7f]", "_", raw)
    safe = re.sub(r"[^\w .()+@=+-]", "_", safe, flags=re.UNICODE)
    safe = safe.strip(" .")
    if not safe:
        return "document.bin"
    if len(safe) <= 160:
        return safe
    suffix = Path(safe).suffix[:20]
    prefix_limit = max(1, 160 - len(suffix))
    prefix = safe[:prefix_limit].rstrip(" .")
    return (prefix or "document") + suffix


class MediaProcessor:
    def __init__(
        self,
        *,
        root: str | Path,
        ffmpeg_binary: str | Path,
        timeout_seconds: float = 120,
        retention_seconds: int = DEFAULT_RETENTION_SECONDS,
        storage_limit_bytes: int = DEFAULT_STORAGE_LIMIT_BYTES,
    ) -> None:
        self.root = Path(root).expanduser().resolve(strict=False)
        self.ffmpeg_binary = Path(ffmpeg_binary).expanduser()
        self.timeout_seconds = max(1.0, float(timeout_seconds))
        self.retention_seconds = max(0, int(retention_seconds))
        self.storage_limit_bytes = max(1, int(storage_limit_bytes))

    def dependency_ready(self) -> bool:
        try:
            status = self.ffmpeg_binary.stat()
        except OSError:
            return False
        return bool(
            stat.S_ISREG(status.st_mode)
            and os.access(self.ffmpeg_binary, os.X_OK)
        )

    def ensure_root(self) -> None:
        if self.root.is_symlink():
            raise MediaProcessingError("unsafe_storage")
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        status = self.root.stat()
        if not stat.S_ISDIR(status.st_mode) or status.st_uid != os.getuid():
            raise MediaProcessingError("unsafe_storage")
        os.chmod(self.root, 0o700)

    def message_directory(self, media_key: str) -> Path:
        if MEDIA_KEY_PATTERN.fullmatch(media_key) is None:
            raise ValueError("invalid media key")
        self.ensure_root()
        directory = self.root / media_key
        if directory.is_symlink():
            raise MediaProcessingError("unsafe_storage")
        directory.mkdir(mode=0o700, exist_ok=True)
        status = directory.stat()
        if not stat.S_ISDIR(status.st_mode) or status.st_uid != os.getuid():
            raise MediaProcessingError("unsafe_storage")
        os.chmod(directory, 0o700)
        return directory

    def source_path(self, media_key: str) -> Path:
        return self.message_directory(media_key) / "source.bin"

    def document_path(self, media_key: str, file_name: str) -> Path:
        return self.message_directory(media_key) / safe_document_name(file_name)

    def prepare_document(
        self,
        *,
        media_key: str,
        source_path: str | Path,
        display_name: str,
        mime_type: str,
    ) -> PreparedDocument:
        directory = self.message_directory(media_key)
        source = self._validated_source(source_path, directory)
        safe_name = safe_document_name(display_name or source.name)
        if source.name != safe_name:
            destination = directory / safe_name
            if destination.exists() and destination != source:
                raise MediaProcessingError("unsafe_storage")
            os.replace(source, destination)
            source = destination
        os.chmod(source, 0o600)
        self._touch_directory(directory)
        return PreparedDocument(
            display_name=safe_name,
            mime_type=safe_document_mime_type(mime_type),
            size_bytes=int(source.stat().st_size),
            input=LocalInput(
                "mention",
                str(source.resolve()),
                name=safe_name,
            ),
        )

    def prepare_voice(
        self,
        *,
        media_key: str,
        source_path: str | Path,
        duration_seconds: int,
    ) -> PreparedMedia:
        directory = self.message_directory(media_key)
        source = self._validated_source(source_path, directory)
        audio = directory / "audio.mp3"
        if not self._usable_file(audio):
            input_format = _safe_ffmpeg_input_format(source, kind="voice")
            self._run_ffmpeg(
                [
                    "-i",
                    str(source),
                    "-vn",
                    "-ac",
                    "1",
                    "-ar",
                    "16000",
                    "-c:a",
                    "libmp3lame",
                    "-b:a",
                    "64k",
                ],
                audio,
                error_kind="invalid_audio",
                input_format=input_format,
            )
        self._touch_directory(directory)
        return PreparedMedia(
            kind="voice",
            duration_seconds=max(0, int(duration_seconds)),
            inputs=(LocalInput("localAudio", str(audio.resolve())),),
        )

    def prepare_video_note(
        self,
        *,
        media_key: str,
        source_path: str | Path,
        duration_seconds: int,
    ) -> PreparedMedia:
        return self._prepare_video(
            kind="video_note",
            media_key=media_key,
            source_path=source_path,
            duration_seconds=duration_seconds,
        )

    def prepare_video(
        self,
        *,
        media_key: str,
        source_path: str | Path,
        duration_seconds: int,
    ) -> PreparedMedia:
        return self._prepare_video(
            kind="video",
            media_key=media_key,
            source_path=source_path,
            duration_seconds=duration_seconds,
        )

    def _prepare_video(
        self,
        *,
        kind: str,
        media_key: str,
        source_path: str | Path,
        duration_seconds: int,
    ) -> PreparedMedia:
        if kind not in {"video_note", "video"}:
            raise ValueError("unsupported video kind")
        directory = self.message_directory(media_key)
        source = self._validated_source(source_path, directory)
        duration = max(0, int(duration_seconds))
        input_format = _safe_ffmpeg_input_format(source, kind=kind)
        deadline = time.monotonic() + self.timeout_seconds
        inputs: list[LocalInput] = []

        audio = directory / "audio.mp3"
        if self._usable_file(audio) or self._try_video_audio(
            source,
            audio,
            input_format=input_format,
            deadline=deadline,
        ):
            inputs.append(LocalInput("localAudio", str(audio.resolve())))

        positions = self._frame_positions(duration)
        for index, position in enumerate(positions, start=1):
            frame = directory / f"frame-{index:02d}.jpg"
            if not self._usable_file(frame):
                self._run_ffmpeg(
                    [
                        "-ss",
                        f"{position:.3f}",
                        "-i",
                        str(source),
                        "-frames:v",
                        "1",
                        "-an",
                        "-vf",
                        "scale='min(768,iw)':-2",
                        "-q:v",
                        "3",
                    ],
                    frame,
                    error_kind="invalid_video",
                    input_format=input_format,
                    deadline=deadline,
                )
            inputs.append(
                LocalInput(
                    "localImage",
                    str(frame.resolve()),
                    detail="low",
                )
            )
        if not any(item.input_type == "localImage" for item in inputs):
            raise MediaProcessingError("invalid_video")
        self._touch_directory(directory)
        return PreparedMedia(
            kind=kind,
            duration_seconds=duration,
            inputs=tuple(inputs),
        )

    def prune(
        self,
        *,
        protected_paths: Iterable[str | Path] = (),
        now: float | None = None,
    ) -> MediaPruneResult:
        if not self.root.exists():
            return MediaPruneResult(removed_directories=0, retained_bytes=0)
        self.ensure_root()
        current_time = time.time() if now is None else float(now)
        protected_directories = self._protected_directories(protected_paths)
        candidates: list[tuple[float, Path, int]] = []
        retained_bytes = 0
        for child in self.root.iterdir():
            if child.is_symlink() or not child.is_dir():
                continue
            if MEDIA_KEY_PATTERN.fullmatch(child.name) is None:
                continue
            size = self._directory_size(child)
            retained_bytes += size
            candidates.append((child.stat().st_mtime, child, size))

        removed = 0
        kept: list[tuple[float, Path, int]] = []
        for modified, directory, size in sorted(candidates):
            expired = current_time - modified >= self.retention_seconds
            if expired and directory not in protected_directories:
                self._remove_directory(directory)
                retained_bytes -= size
                removed += 1
            else:
                kept.append((modified, directory, size))

        if retained_bytes > self.storage_limit_bytes:
            for _, directory, size in kept:
                if (
                    retained_bytes <= self.storage_limit_bytes
                    or directory in protected_directories
                ):
                    continue
                self._remove_directory(directory)
                retained_bytes -= size
                removed += 1
        return MediaPruneResult(
            removed_directories=removed,
            retained_bytes=max(0, retained_bytes),
        )

    def _validated_source(self, source_path: str | Path, directory: Path) -> Path:
        source = Path(source_path)
        try:
            resolved = source.resolve(strict=True)
            resolved.relative_to(directory.resolve(strict=True))
            status = resolved.stat()
        except (FileNotFoundError, OSError, RuntimeError, ValueError):
            raise MediaProcessingError("invalid_media") from None
        if (
            source.is_symlink()
            or not stat.S_ISREG(status.st_mode)
            or status.st_uid != os.getuid()
            or status.st_size <= 0
        ):
            raise MediaProcessingError("invalid_media")
        os.chmod(resolved, 0o600)
        return resolved

    def _try_video_audio(
        self,
        source: Path,
        destination: Path,
        *,
        input_format: str,
        deadline: float,
    ) -> bool:
        try:
            self._run_ffmpeg(
                [
                    "-i",
                    str(source),
                    "-map",
                    "0:a:0",
                    "-vn",
                    "-ac",
                    "1",
                    "-ar",
                    "16000",
                    "-c:a",
                    "libmp3lame",
                    "-b:a",
                    "64k",
                ],
                destination,
                error_kind="audio_unavailable",
                input_format=input_format,
                deadline=deadline,
            )
        except MediaProcessingError as error:
            if error.kind == "audio_unavailable":
                return False
            raise
        return True

    def _run_ffmpeg(
        self,
        arguments: list[str],
        destination: Path,
        *,
        error_kind: str,
        input_format: str,
        deadline: float | None = None,
    ) -> None:
        if not self.dependency_ready():
            raise MediaProcessingError("ffmpeg_unavailable")
        remaining = (
            self.timeout_seconds
            if deadline is None
            else deadline - time.monotonic()
        )
        if remaining <= 0:
            raise MediaProcessingError("ffmpeg_unavailable")
        temporary = destination.with_name(
            f".{destination.stem}.part-{os.getpid()}{destination.suffix}"
        )
        with contextlib.suppress(FileNotFoundError):
            temporary.unlink()
        try:
            result = subprocess.run(
                [
                    str(self.ffmpeg_binary),
                    "-hide_banner",
                    "-loglevel",
                    "error",
                    "-nostdin",
                    "-y",
                    *_ffmpeg_input_options(input_format),
                    *arguments,
                    str(temporary),
                ],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=remaining,
            )
        except (OSError, subprocess.TimeoutExpired):
            raise MediaProcessingError("ffmpeg_unavailable") from None
        try:
            if result.returncode != 0 or not self._usable_file(temporary):
                raise MediaProcessingError(error_kind)
            os.chmod(temporary, 0o600)
            os.replace(temporary, destination)
            os.chmod(destination, 0o600)
        finally:
            with contextlib.suppress(FileNotFoundError):
                temporary.unlink()

    @staticmethod
    def _frame_positions(duration_seconds: int) -> tuple[float, ...]:
        duration = max(0.1, float(duration_seconds))
        ratios = (
            (0.5,)
            if duration_seconds < 3
            else (0.25, 0.75)
            if duration_seconds < 10
            else (0.15, 0.5, 0.85)
        )
        upper = max(0.0, duration - 0.05)
        return tuple(min(max(0.0, duration * ratio), upper) for ratio in ratios)

    @staticmethod
    def _usable_file(path: Path) -> bool:
        try:
            status = path.stat()
        except OSError:
            return False
        return bool(
            not path.is_symlink()
            and stat.S_ISREG(status.st_mode)
            and status.st_size > 0
        )

    @staticmethod
    def _touch_directory(directory: Path) -> None:
        with contextlib.suppress(OSError):
            os.utime(directory, None)

    def _protected_directories(
        self,
        protected_paths: Iterable[str | Path],
    ) -> set[Path]:
        directories: set[Path] = set()
        for value in protected_paths:
            try:
                resolved = Path(value).resolve(strict=False)
                relative = resolved.relative_to(self.root)
            except (OSError, RuntimeError, ValueError):
                continue
            if relative.parts and MEDIA_KEY_PATTERN.fullmatch(relative.parts[0]):
                directories.add(self.root / relative.parts[0])
        return directories

    @staticmethod
    def _directory_size(directory: Path) -> int:
        total = 0
        for path in directory.rglob("*"):
            try:
                status = path.lstat()
            except OSError:
                continue
            if stat.S_ISREG(status.st_mode):
                total += int(status.st_size)
        return total

    def _remove_directory(self, directory: Path) -> None:
        try:
            directory.relative_to(self.root)
        except ValueError:
            raise MediaProcessingError("unsafe_storage") from None
        if (
            directory.parent != self.root
            or directory.is_symlink()
            or MEDIA_KEY_PATTERN.fullmatch(directory.name) is None
        ):
            raise MediaProcessingError("unsafe_storage")
        shutil.rmtree(directory)
