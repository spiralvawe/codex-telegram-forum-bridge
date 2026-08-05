from __future__ import annotations

import contextlib
import ctypes
import fcntl
import hashlib
import http.client
import http.server
import ipaddress
import json
import os
import queue
import re
import signal
import shutil
import socket
import ssl
import stat
import subprocess
import tempfile
import threading
import time
import uuid
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence
from urllib.parse import urlsplit

from .input_types import LocalInput
from .media import (
    MediaProcessingError,
    PreparedMedia,
    _ffmpeg_input_options,
    _safe_ffmpeg_input_format,
)
from .media_worker_config import (
    MediaWorkerConfigError,
    _validate_executable,
)


PROTOCOL_VERSION = 1
MAX_SOURCE_BYTES = 20 * 1024 * 1024
MAX_OUTPUT_BYTES = 32 * 1024 * 1024
MAX_ARTIFACT_BYTES = 20 * 1024 * 1024
MAX_DURATION_SECONDS = 60 * 60
DEFAULT_TTL_SECONDS = 24 * 60 * 60
DEFAULT_STORAGE_LIMIT_BYTES = 512 * 1024 * 1024
MAX_JSON_BYTES = 64 * 1024
JOB_STORAGE_SLACK_BYTES = 64 * 1024
DNS_CACHE_SECONDS = 60.0
FFMPEG_MAX_ALLOC_BYTES = 128 * 1024 * 1024
FFMPEG_MAX_PIXELS = 16_777_216
FFMPEG_PROCESS_GROUP_RSS_LIMIT_BYTES = 512 * 1024 * 1024
FFMPEG_MONITOR_INTERVAL_SECONDS = 0.01
TRANSCRIPT_MAX_BYTES = 64 * 1024

MEDIA_KINDS = frozenset({"transcript", "voice", "video", "video_note"})
MEDIA_KEY_PATTERN = re.compile(r"[0-9a-f]{32}")
JOB_ID_PATTERN = re.compile(r"[0-9a-f]{64}")
SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")
ERROR_KIND_PATTERN = re.compile(r"[a-z][a-z0-9_]{0,63}")
FRAME_NAME_PATTERN = re.compile(r"frame-(0[1-3])\.jpg")
UPLOAD_NAME_PATTERN = re.compile(r"\.upload-[0-9a-f]{32}\.part")
JOB_TEMP_NAME_PATTERN = re.compile(
    r"\.job-[0-9a-f]{64}-[0-9a-f]{32}\.part"
)
WORK_NAME_PATTERN = re.compile(
    r"\.(?:work|publish)-[0-9a-f]{64}-[0-9a-f]{32}\.part"
)
TERMINAL_MEDIA_ERRORS = frozenset(
    {"invalid_audio", "invalid_video", "invalid_media"}
)

_STATUS_FIELDS = frozenset(
    {
        "version",
        "job_id",
        "state",
        "kind",
        "media_key",
        "duration_seconds",
        "source_length",
        "source_sha256",
        "artifacts",
        "error",
    }
)
_ARTIFACT_FIELDS = frozenset(
    {"name", "content_type", "length", "sha256"}
)
_REQUEST_FIELDS = frozenset(
    {
        "version",
        "job_id",
        "kind",
        "media_key",
        "duration_seconds",
        "source_length",
        "source_sha256",
    }
)


class _DarwinRUsageInfoV0(ctypes.Structure):
    _fields_ = [
        ("ri_uuid", ctypes.c_ubyte * 16),
        ("ri_user_time", ctypes.c_uint64),
        ("ri_system_time", ctypes.c_uint64),
        ("ri_pkg_idle_wkups", ctypes.c_uint64),
        ("ri_interrupt_wkups", ctypes.c_uint64),
        ("ri_pageins", ctypes.c_uint64),
        ("ri_wired_size", ctypes.c_uint64),
        ("ri_resident_size", ctypes.c_uint64),
        ("ri_phys_footprint", ctypes.c_uint64),
        ("ri_proc_start_abstime", ctypes.c_uint64),
        ("ri_proc_exit_abstime", ctypes.c_uint64),
    ]


_DARWIN_LIBPROC: Any | None = None


def _darwin_process_group_footprint_bytes(process_group: int) -> int | None:
    global _DARWIN_LIBPROC
    try:
        library = _DARWIN_LIBPROC
        if library is None:
            library = ctypes.CDLL("/usr/lib/libproc.dylib", use_errno=True)
            library.proc_listpgrppids.argtypes = [
                ctypes.c_int,
                ctypes.c_void_p,
                ctypes.c_int,
            ]
            library.proc_listpgrppids.restype = ctypes.c_int
            library.proc_pid_rusage.argtypes = [
                ctypes.c_int,
                ctypes.c_int,
                ctypes.c_void_p,
            ]
            library.proc_pid_rusage.restype = ctypes.c_int
            _DARWIN_LIBPROC = library
        required = int(library.proc_listpgrppids(process_group, None, 0))
        if required <= 0:
            return None
        capacity = max(8, required // ctypes.sizeof(ctypes.c_int) + 8)
        process_ids = (ctypes.c_int * capacity)()
        count = int(
            library.proc_listpgrppids(
                process_group,
                process_ids,
                ctypes.sizeof(process_ids),
            )
        )
        if count <= 0:
            return None
        total = 0
        main_process_measured = False
        for process_id in process_ids[:count]:
            if process_id <= 0:
                continue
            usage = _DarwinRUsageInfoV0()
            if (
                library.proc_pid_rusage(
                    process_id,
                    0,
                    ctypes.byref(usage),
                )
                != 0
            ):
                continue
            total += int(usage.ri_phys_footprint)
            if process_id == process_group:
                main_process_measured = True
        return total if main_process_measured else None
    except (AttributeError, OSError, TypeError, ValueError):
        return None


def _process_group_rss_bytes(process_group: int) -> int | None:
    if sys.platform == "darwin":
        return _darwin_process_group_footprint_bytes(process_group)
    if sys.platform.startswith("linux"):
        try:
            status = Path(f"/proc/{process_group}/status").read_text(
                encoding="ascii",
                errors="strict",
            )
        except (OSError, UnicodeError):
            return None
        for line in status.splitlines():
            if line.startswith("VmRSS:"):
                fields = line.split()
                if len(fields) == 3 and fields[2] == "kB":
                    try:
                        return int(fields[1]) * 1024
                    except ValueError:
                        return None
        return None
    return None


def _kill_process_group(process: subprocess.Popen[bytes]) -> None:
    process_id = getattr(process, "pid", None)
    if isinstance(process_id, int) and process_id > 0:
        try:
            os.killpg(process_id, signal.SIGKILL)
            return
        except OSError:
            pass
    with contextlib.suppress(OSError):
        process.kill()
_RESULT_FIELDS = frozenset({"version", "kind", "duration_seconds", "artifacts"})


class MediaWorkerError(RuntimeError):
    """Base class for errors in the optional media worker transport."""

    def __init__(self, kind: str):
        super().__init__(kind)
        self.kind = kind


class MediaWorkerBusy(MediaWorkerError):
    """The worker is healthy but its bounded processing queue is full."""


class MediaWorkerUnavailable(MediaWorkerError):
    """The worker cannot currently be reached or did not finish in time."""


class MediaWorkerProtocolError(MediaWorkerError):
    """The peer returned data that does not conform to protocol version 1."""


@dataclass(frozen=True)
class _JobRequest:
    job_id: str
    kind: str
    media_key: str
    duration_seconds: int
    source_length: int
    source_sha256: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": PROTOCOL_VERSION,
            "job_id": self.job_id,
            "kind": self.kind,
            "media_key": self.media_key,
            "duration_seconds": self.duration_seconds,
            "source_length": self.source_length,
            "source_sha256": self.source_sha256,
        }

    @classmethod
    def from_dict(cls, value: Any) -> "_JobRequest":
        if not isinstance(value, dict) or set(value) != _REQUEST_FIELDS:
            raise MediaWorkerProtocolError("malformed_job_request")
        if value.get("version") != PROTOCOL_VERSION:
            raise MediaWorkerProtocolError("unsupported_protocol")
        duration = _strict_int(value.get("duration_seconds"))
        source_length = _strict_int(value.get("source_length"))
        request = cls(
            job_id=str(value.get("job_id") or ""),
            kind=str(value.get("kind") or ""),
            media_key=str(value.get("media_key") or ""),
            duration_seconds=duration,
            source_length=source_length,
            source_sha256=str(value.get("source_sha256") or ""),
        )
        _validate_request(request)
        return request


@dataclass(frozen=True)
class _Artifact:
    name: str
    content_type: str
    length: int
    sha256: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "content_type": self.content_type,
            "length": self.length,
            "sha256": self.sha256,
        }

    @classmethod
    def from_dict(cls, value: Any) -> "_Artifact":
        if not isinstance(value, dict) or set(value) != _ARTIFACT_FIELDS:
            raise MediaWorkerProtocolError("malformed_artifact")
        artifact = cls(
            name=str(value.get("name") or ""),
            content_type=str(value.get("content_type") or ""),
            length=_strict_int(value.get("length")),
            sha256=str(value.get("sha256") or ""),
        )
        if artifact.name == "audio.mp3":
            expected_type = "audio/mpeg"
        elif artifact.name == "transcript.txt":
            expected_type = "text/plain; charset=utf-8"
        elif FRAME_NAME_PATTERN.fullmatch(artifact.name):
            expected_type = "image/jpeg"
        else:
            raise MediaWorkerProtocolError("unsafe_artifact_name")
        if artifact.content_type != expected_type:
            raise MediaWorkerProtocolError("invalid_artifact_type")
        if (
            artifact.length <= 0
            or artifact.length
            > (TRANSCRIPT_MAX_BYTES if artifact.name == "transcript.txt" else MAX_ARTIFACT_BYTES)
            or SHA256_PATTERN.fullmatch(artifact.sha256) is None
        ):
            raise MediaWorkerProtocolError("invalid_artifact_metadata")
        return artifact


def _strict_int(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise MediaWorkerProtocolError("invalid_integer")
    return value


def _validate_request(request: _JobRequest, *, max_source: int = MAX_SOURCE_BYTES) -> None:
    if JOB_ID_PATTERN.fullmatch(request.job_id) is None:
        raise MediaWorkerProtocolError("invalid_job_id")
    if request.kind not in MEDIA_KINDS:
        raise MediaWorkerProtocolError("invalid_media_kind")
    if MEDIA_KEY_PATTERN.fullmatch(request.media_key) is None:
        raise MediaWorkerProtocolError("invalid_media_key")
    if not 0 <= request.duration_seconds <= MAX_DURATION_SECONDS:
        raise MediaWorkerProtocolError("invalid_duration")
    if not 0 < request.source_length <= max_source:
        raise MediaWorkerProtocolError("invalid_source_length")
    if SHA256_PATTERN.fullmatch(request.source_sha256) is None:
        raise MediaWorkerProtocolError("invalid_source_digest")


def _canonical_job_id(
    *,
    kind: str,
    media_key: str,
    duration_seconds: int,
    source_length: int,
    source_sha256: str,
) -> str:
    payload = json.dumps(
        {
            "version": PROTOCOL_VERSION,
            "kind": kind,
            "media_key": media_key,
            "duration_seconds": duration_seconds,
            "source_length": source_length,
            "source_sha256": source_sha256,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")
    return hashlib.sha256(b"codex-media-worker-v1\0" + payload).hexdigest()


def _validate_artifact_set(
    artifacts: Sequence[_Artifact],
    *,
    kind: str,
    max_output_bytes: int,
) -> None:
    if kind == "transcript":
        if len(artifacts) != 1 or artifacts[0].name != "transcript.txt":
            raise MediaWorkerProtocolError("invalid_transcript_artifacts")
        if artifacts[0].length > min(max_output_bytes, TRANSCRIPT_MAX_BYTES):
            raise MediaWorkerProtocolError("output_too_large")
        return
    if len(artifacts) > 4:
        raise MediaWorkerProtocolError("too_many_artifacts")
    names = [item.name for item in artifacts]
    if len(set(names)) != len(names):
        raise MediaWorkerProtocolError("duplicate_artifact")
    audio = [item for item in artifacts if item.name == "audio.mp3"]
    frames = [item for item in artifacts if item.name != "audio.mp3"]
    if len(audio) > 1 or len(frames) > 3:
        raise MediaWorkerProtocolError("too_many_artifacts")
    expected_frames = [f"frame-{index:02d}.jpg" for index in range(1, len(frames) + 1)]
    if [item.name for item in frames] != expected_frames:
        raise MediaWorkerProtocolError("unordered_artifacts")
    expected_order = ([audio[0].name] if audio else []) + expected_frames
    if names != expected_order:
        raise MediaWorkerProtocolError("unordered_artifacts")
    if kind == "voice":
        if len(audio) != 1 or frames:
            raise MediaWorkerProtocolError("invalid_voice_artifacts")
    elif not frames:
        raise MediaWorkerProtocolError("invalid_video_artifacts")
    aggregate = sum(item.length for item in artifacts)
    if aggregate <= 0 or aggregate > max_output_bytes:
        raise MediaWorkerProtocolError("output_too_large")


def _status_manifest(
    request: _JobRequest,
    *,
    state: str,
    artifacts: Sequence[_Artifact] = (),
    error: str | None = None,
) -> dict[str, Any]:
    return {
        "version": PROTOCOL_VERSION,
        "job_id": request.job_id,
        "state": state,
        "kind": request.kind,
        "media_key": request.media_key,
        "duration_seconds": request.duration_seconds,
        "source_length": request.source_length,
        "source_sha256": request.source_sha256,
        "artifacts": [item.to_dict() for item in artifacts],
        "error": error,
    }


def _parse_status_manifest(
    value: Any,
    *,
    expected: _JobRequest,
    max_output_bytes: int,
) -> tuple[str, tuple[_Artifact, ...], str | None]:
    if not isinstance(value, dict) or set(value) != _STATUS_FIELDS:
        raise MediaWorkerProtocolError("malformed_status")
    if (
        value.get("version") != PROTOCOL_VERSION
        or value.get("job_id") != expected.job_id
        or value.get("kind") != expected.kind
        or value.get("media_key") != expected.media_key
        or value.get("source_sha256") != expected.source_sha256
        or _strict_int(value.get("duration_seconds"))
        != expected.duration_seconds
        or _strict_int(value.get("source_length")) != expected.source_length
    ):
        raise MediaWorkerProtocolError("status_mismatch")
    state = str(value.get("state") or "")
    if state not in {"queued", "running", "complete", "failed"}:
        raise MediaWorkerProtocolError("invalid_job_state")
    raw_artifacts = value.get("artifacts")
    if not isinstance(raw_artifacts, list):
        raise MediaWorkerProtocolError("malformed_artifact_list")
    artifacts = tuple(_Artifact.from_dict(item) for item in raw_artifacts)
    raw_error = value.get("error")
    if state == "complete":
        if raw_error is not None:
            raise MediaWorkerProtocolError("malformed_status")
        _validate_artifact_set(
            artifacts,
            kind=expected.kind,
            max_output_bytes=max_output_bytes,
        )
        return state, artifacts, None
    if artifacts:
        raise MediaWorkerProtocolError("premature_artifacts")
    if state == "failed":
        if (
            not isinstance(raw_error, str)
            or ERROR_KIND_PATTERN.fullmatch(raw_error) is None
        ):
            raise MediaWorkerProtocolError("malformed_error")
        return state, (), raw_error
    if raw_error is not None:
        raise MediaWorkerProtocolError("malformed_status")
    return state, (), None


def _owner_regular_file(path: Path, *, allow_empty: bool = False) -> os.stat_result:
    try:
        status = path.lstat()
    except OSError:
        raise MediaWorkerProtocolError("missing_artifact") from None
    if (
        stat.S_ISLNK(status.st_mode)
        or not stat.S_ISREG(status.st_mode)
        or status.st_uid != os.getuid()
        or (not allow_empty and status.st_size <= 0)
    ):
        raise MediaWorkerProtocolError("unsafe_artifact")
    return status


def _ensure_private_directory(path: Path, *, create: bool) -> Path:
    expanded = path.expanduser()
    if expanded.is_symlink():
        raise MediaWorkerProtocolError("unsafe_storage")
    if create:
        expanded.mkdir(parents=True, exist_ok=True, mode=0o700)
    try:
        status = expanded.lstat()
    except OSError:
        raise MediaWorkerProtocolError("unsafe_storage") from None
    if (
        stat.S_ISLNK(status.st_mode)
        or not stat.S_ISDIR(status.st_mode)
        or status.st_uid != os.getuid()
    ):
        raise MediaWorkerProtocolError("unsafe_storage")
    with contextlib.suppress(OSError):
        os.chmod(expanded, 0o700)
    return expanded.resolve(strict=True)


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY
    if hasattr(os, "O_DIRECTORY"):
        flags |= os.O_DIRECTORY
    try:
        descriptor = os.open(path, flags)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    except OSError:
        pass
    finally:
        os.close(descriptor)


def _write_json_atomic(path: Path, value: Mapping[str, Any]) -> None:
    payload = (
        json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode("utf-8")
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}-",
        suffix=".part",
        dir=path.parent,
    )
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb", closefd=True) as stream:
            descriptor = -1
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        os.chmod(path, 0o600)
        _fsync_directory(path.parent)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        with contextlib.suppress(FileNotFoundError):
            temporary.unlink()


def _read_json(path: Path, *, fields: frozenset[str]) -> dict[str, Any]:
    status = _owner_regular_file(path)
    if status.st_size > MAX_JSON_BYTES:
        raise MediaWorkerProtocolError("oversized_manifest")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        raise MediaWorkerProtocolError("malformed_manifest") from None
    if not isinstance(value, dict) or set(value) != fields:
        raise MediaWorkerProtocolError("malformed_manifest")
    return value


def _sha256_file(path: Path, *, maximum: int) -> tuple[int, str]:
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
    except OSError:
        raise MediaProcessingError("invalid_media") from None
    digest = hashlib.sha256()
    length = 0
    try:
        status = os.fstat(descriptor)
        if (
            not stat.S_ISREG(status.st_mode)
            or status.st_uid != os.getuid()
            or status.st_size <= 0
        ):
            raise MediaProcessingError("invalid_media")
        if status.st_size > maximum:
            raise MediaProcessingError("file_too_large")
        while True:
            chunk = os.read(descriptor, min(1024 * 1024, maximum + 1 - length))
            if not chunk:
                break
            length += len(chunk)
            if length > maximum:
                raise MediaProcessingError("file_too_large")
            digest.update(chunk)
        if length != status.st_size:
            raise MediaProcessingError("invalid_media")
    finally:
        os.close(descriptor)
    return length, digest.hexdigest()


class FFmpegMediaWorkerProcessor:
    """Small ffmpeg processor intended for the isolated Mac worker process."""

    def __init__(
        self,
        ffmpeg_binary: str | Path,
        transcriber_binary: str | Path | None = None,
        transcriber_model: str | Path | None = None,
        timeout_seconds: float = 120,
        *,
        max_output_bytes: int = MAX_OUTPUT_BYTES,
        _clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.ffmpeg_binary = Path(ffmpeg_binary).expanduser()
        self.transcriber_binary = (
            Path(transcriber_binary).expanduser()
            if transcriber_binary is not None
            else None
        )
        self.transcriber_model = (
            Path(transcriber_model).expanduser()
            if transcriber_model is not None
            else None
        )
        self.timeout_seconds = max(1.0, float(timeout_seconds))
        self.set_output_limit(max_output_bytes)
        self._clock = _clock

    def set_output_limit(self, max_output_bytes: int) -> None:
        """Keep ffmpeg's write budget aligned with the server reservation."""
        maximum = int(max_output_bytes)
        if not 1 <= maximum <= MAX_OUTPUT_BYTES:
            raise ValueError("invalid max_output_bytes")
        self.max_output_bytes = maximum

    def prepare_voice(
        self,
        *,
        media_key: str,
        source_path: str | Path,
        duration_seconds: int,
        output_directory: str | Path,
    ) -> PreparedMedia:
        del media_key
        output = Path(output_directory)
        audio = output / "audio.mp3"
        input_format = _safe_ffmpeg_input_format(
            source_path,
            kind="voice",
        )
        deadline = self._clock() + self.timeout_seconds
        self._run(
            [
                "-i",
                str(source_path),
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
            deadline=deadline,
        )
        return PreparedMedia(
            kind="voice",
            duration_seconds=duration_seconds,
            inputs=(LocalInput("localAudio", str(audio.resolve())),),
        )

    def prepare_transcript(
        self,
        *,
        media_key: str,
        source_path: str | Path,
        duration_seconds: int,
        output_directory: str | Path,
    ) -> PreparedMedia:
        del media_key
        if self.transcriber_binary is None or self.transcriber_model is None:
            raise MediaProcessingError("transcriber_unavailable")
        output = Path(output_directory)
        wav = output / "speech.wav"
        deadline = self._clock() + self.timeout_seconds
        self._run(
            [
                "-i",
                str(source_path),
                "-vn",
                "-ac",
                "1",
                "-ar",
                "16000",
                "-c:a",
                "pcm_s16le",
            ],
            wav,
            error_kind="invalid_audio",
            input_format=_safe_ffmpeg_input_format(source_path, kind="voice"),
            deadline=deadline,
        )
        transcript = output / "transcript.txt"
        self._run_transcriber(
            wav=wav,
            transcript=transcript,
            deadline=deadline,
        )
        return PreparedMedia(
            kind="transcript",
            duration_seconds=duration_seconds,
            inputs=(),
        )

    def _run_transcriber(
        self,
        *,
        wav: Path,
        transcript: Path,
        deadline: float,
    ) -> None:
        assert self.transcriber_binary is not None
        assert self.transcriber_model is not None
        try:
            executable = _validate_executable(
                self.transcriber_binary,
                field="transcriber_binary",
            )
        except MediaWorkerConfigError:
            raise MediaProcessingError("transcriber_unavailable") from None
        remaining = deadline - self._clock()
        if remaining <= 0:
            raise MediaProcessingError("transcriber_unavailable")
        output_base = transcript.with_suffix("")
        process: subprocess.Popen[bytes] | None = None
        try:
            try:
                process = subprocess.Popen(
                    [
                        str(executable),
                        "-m",
                        str(self.transcriber_model),
                        "-f",
                        str(wav),
                        "-l",
                        "ru",
                        "-nt",
                        "-t",
                        "2",
                        "-otxt",
                        "-of",
                        str(output_base),
                    ],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    start_new_session=True,
                )
            except OSError:
                raise MediaProcessingError("transcriber_unavailable") from None
            while process.poll() is None:
                process_id = getattr(process, "pid", None)
                if isinstance(process_id, int) and process_id > 0:
                    resident = _process_group_rss_bytes(process_id)
                    if resident is not None and resident > FFMPEG_PROCESS_GROUP_RSS_LIMIT_BYTES:
                        _kill_process_group(process)
                        process.wait()
                        raise MediaProcessingError("transcriber_unavailable")
                remaining = deadline - self._clock()
                if remaining <= 0:
                    _kill_process_group(process)
                    process.wait()
                    raise MediaProcessingError("transcriber_unavailable")
                try:
                    process.wait(
                        timeout=min(FFMPEG_MONITOR_INTERVAL_SECONDS, remaining)
                    )
                except subprocess.TimeoutExpired:
                    continue
            if process.returncode != 0:
                raise MediaProcessingError("transcriber_unavailable")
            status = _owner_regular_file(transcript)
            if status.st_size > TRANSCRIPT_MAX_BYTES:
                raise MediaProcessingError("transcriber_unavailable")
            text = transcript.read_text(encoding="utf-8").strip()
            if not text or "\x00" in text:
                raise MediaProcessingError("transcriber_unavailable")
            transcript.write_text(text + "\n", encoding="utf-8")
            os.chmod(transcript, 0o600)
        finally:
            if process is not None and process.poll() is None:
                _kill_process_group(process)
                with contextlib.suppress(OSError, subprocess.TimeoutExpired):
                    process.wait(timeout=1)

    def prepare_video(
        self,
        *,
        media_key: str,
        source_path: str | Path,
        duration_seconds: int,
        output_directory: str | Path,
    ) -> PreparedMedia:
        return self._prepare_video(
            kind="video",
            media_key=media_key,
            source_path=source_path,
            duration_seconds=duration_seconds,
            output_directory=output_directory,
        )

    def prepare_video_note(
        self,
        *,
        media_key: str,
        source_path: str | Path,
        duration_seconds: int,
        output_directory: str | Path,
    ) -> PreparedMedia:
        return self._prepare_video(
            kind="video_note",
            media_key=media_key,
            source_path=source_path,
            duration_seconds=duration_seconds,
            output_directory=output_directory,
        )

    def _prepare_video(
        self,
        *,
        kind: str,
        media_key: str,
        source_path: str | Path,
        duration_seconds: int,
        output_directory: str | Path,
    ) -> PreparedMedia:
        del media_key
        output = Path(output_directory)
        input_format = _safe_ffmpeg_input_format(source_path, kind=kind)
        deadline = self._clock() + self.timeout_seconds
        inputs: list[LocalInput] = []
        audio = output / "audio.mp3"
        if self._run(
            [
                "-i",
                str(source_path),
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
            audio,
            error_kind="audio_unavailable",
            optional=True,
            input_format=input_format,
            deadline=deadline,
        ):
            inputs.append(LocalInput("localAudio", str(audio.resolve())))
        for index, position in enumerate(
            self._frame_positions(duration_seconds),
            start=1,
        ):
            frame = output / f"frame-{index:02d}.jpg"
            self._run(
                [
                    "-ss",
                    f"{position:.3f}",
                    "-i",
                    str(source_path),
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
            inputs.append(LocalInput("localImage", str(frame.resolve()), "low"))
        return PreparedMedia(
            kind=kind,
            duration_seconds=duration_seconds,
            inputs=tuple(inputs),
        )

    def _run(
        self,
        arguments: Sequence[str],
        destination: Path,
        *,
        error_kind: str,
        input_format: str,
        deadline: float,
        optional: bool = False,
    ) -> bool:
        resource_error_kind = "invalid_video" if optional else error_kind
        try:
            executable = _validate_executable(
                self.ffmpeg_binary,
                field="ffmpeg_binary",
            )
        except MediaWorkerConfigError:
            raise MediaProcessingError("ffmpeg_unavailable") from None
        remaining = deadline - self._clock()
        if remaining <= 0:
            raise MediaProcessingError("ffmpeg_unavailable")
        existing_output = 0
        for child in destination.parent.iterdir():
            try:
                status = child.lstat()
            except OSError:
                continue
            if (
                stat.S_ISREG(status.st_mode)
                and status.st_uid == os.getuid()
                and not child.name.startswith(".")
            ):
                existing_output += int(status.st_size)
        output_limit = min(
            MAX_ARTIFACT_BYTES,
            self._remaining_output_budget(existing_output),
        )
        temporary = destination.with_name(
            f".{destination.stem}-{uuid.uuid4().hex}.part{destination.suffix}"
        )
        process: subprocess.Popen[bytes] | None = None
        monitor_failures = 0
        try:
            try:
                process = subprocess.Popen(
                    [
                        str(executable),
                        "-hide_banner",
                        "-loglevel",
                        "error",
                        "-nostdin",
                        "-y",
                        "-max_alloc",
                        str(FFMPEG_MAX_ALLOC_BYTES),
                        "-filter_threads",
                        "1",
                        "-filter_complex_threads",
                        "1",
                        "-threads",
                        "1",
                        "-max_pixels",
                        str(FFMPEG_MAX_PIXELS),
                        *_ffmpeg_input_options(input_format),
                        *arguments,
                        "-threads",
                        "1",
                        "-fs",
                        str(output_limit + 1),
                        str(temporary),
                    ],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    start_new_session=True,
                )
            except OSError:
                raise MediaProcessingError("ffmpeg_unavailable") from None
            while process.poll() is None:
                try:
                    produced = temporary.stat().st_size
                except OSError:
                    produced = 0
                if produced > output_limit:
                    _kill_process_group(process)
                    process.wait()
                    if optional:
                        return False
                    raise MediaProcessingError(error_kind)
                process_id = getattr(process, "pid", None)
                if isinstance(process_id, int) and process_id > 0:
                    resident = _process_group_rss_bytes(process_id)
                    if resident is None:
                        monitor_failures += 1
                        if sys.platform == "darwin" and monitor_failures >= 3:
                            _kill_process_group(process)
                            process.wait()
                            raise MediaProcessingError(resource_error_kind)
                    else:
                        monitor_failures = 0
                        if (
                            resident
                            > FFMPEG_PROCESS_GROUP_RSS_LIMIT_BYTES
                        ):
                            _kill_process_group(process)
                            process.wait()
                            raise MediaProcessingError(resource_error_kind)
                remaining = deadline - self._clock()
                if remaining <= 0:
                    _kill_process_group(process)
                    process.wait()
                    raise MediaProcessingError("ffmpeg_unavailable")
                try:
                    process.wait(
                        timeout=min(
                            FFMPEG_MONITOR_INTERVAL_SECONDS,
                            remaining,
                        )
                    )
                except subprocess.TimeoutExpired:
                    continue
            if process.returncode != 0:
                if optional:
                    return False
                raise MediaProcessingError(error_kind)
            try:
                status = temporary.lstat()
            except OSError:
                status = None
            if (
                status is None
                or not stat.S_ISREG(status.st_mode)
                or status.st_size <= 0
                or status.st_size > output_limit
            ):
                if optional:
                    return False
                raise MediaProcessingError(error_kind)
            os.chmod(temporary, 0o600)
            os.replace(temporary, destination)
            os.chmod(destination, 0o600)
            return True
        finally:
            if process is not None and process.poll() is None:
                _kill_process_group(process)
                with contextlib.suppress(
                    OSError,
                    subprocess.TimeoutExpired,
                ):
                    process.wait(timeout=1)
            with contextlib.suppress(FileNotFoundError):
                temporary.unlink()

    def _remaining_output_budget(self, existing_output: int) -> int:
        remaining = self.max_output_bytes - max(0, int(existing_output))
        if remaining <= 0:
            raise MediaProcessingError("invalid_media")
        return remaining

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


class _WorkerHTTPServer(http.server.HTTPServer):
    """HTTP server whose bounded threads include the TLS handshake."""

    allow_reuse_address = True

    def __init__(
        self,
        *args: Any,
        request_timeout: float,
        ssl_context: ssl.SSLContext,
        max_request_threads: int,
        **kwargs: Any,
    ) -> None:
        self.request_timeout = request_timeout
        self.ssl_context = ssl_context
        server_address = args[0] if args else kwargs.get("server_address")
        if (
            isinstance(server_address, tuple)
            and server_address
            and ":" in str(server_address[0])
        ):
            self.address_family = socket.AF_INET6
        self._request_slots = threading.BoundedSemaphore(max_request_threads)
        self._request_lock = threading.Lock()
        self._active_connections: dict[object, socket.socket] = {}
        self._request_deadlines: dict[int, float] = {}
        self._request_threads: set[threading.Thread] = set()
        super().__init__(*args, **kwargs)

    def process_request(
        self,
        request: socket.socket,
        client_address: Any,
    ) -> None:
        if not self._request_slots.acquire(blocking=False):
            with contextlib.suppress(OSError):
                request.shutdown(socket.SHUT_RDWR)
            request.close()
            return
        deadline = time.monotonic() + self.request_timeout
        request_token = object()
        thread = threading.Thread(
            target=self._serve_request,
            args=(request_token, request, client_address, deadline),
            name="media-worker-http",
            daemon=True,
        )
        with self._request_lock:
            self._active_connections[request_token] = request
            self._request_deadlines[id(request)] = deadline
            self._request_threads.add(thread)
        try:
            thread.start()
        except BaseException:
            with self._request_lock:
                self._active_connections.pop(request_token, None)
                self._request_deadlines.pop(id(request), None)
                self._request_threads.discard(thread)
            self._request_slots.release()
            request.close()
            raise

    def _serve_request(
        self,
        request_token: object,
        request: socket.socket,
        client_address: Any,
        deadline: float,
    ) -> None:
        active = request
        expiry = threading.Timer(
            max(0.0, deadline - time.monotonic()),
            self._expire_connection,
            args=(request_token,),
        )
        expiry.daemon = True
        expiry.start()
        try:
            request.settimeout(max(0.001, deadline - time.monotonic()))
            secured = self.ssl_context.wrap_socket(
                request,
                server_side=True,
                do_handshake_on_connect=False,
            )
            active = secured
            with self._request_lock:
                self._request_deadlines.pop(id(request), None)
                self._active_connections[request_token] = secured
                self._request_deadlines[id(secured)] = deadline
            secured.settimeout(max(0.001, deadline - time.monotonic()))
            secured.do_handshake()
            self.finish_request(secured, client_address)
        except (
            ConnectionError,
            OSError,
            ssl.SSLError,
            TimeoutError,
        ):
            pass
        finally:
            expiry.cancel()
            with contextlib.suppress(OSError):
                active.shutdown(socket.SHUT_RDWR)
            active.close()
            with self._request_lock:
                self._active_connections.pop(request_token, None)
                self._request_deadlines.pop(id(request), None)
                self._request_deadlines.pop(id(active), None)
                self._request_threads.discard(threading.current_thread())
            self._request_slots.release()

    def _expire_connection(self, request_token: object) -> None:
        with self._request_lock:
            active = self._active_connections.get(request_token)
        if active is not None:
            with contextlib.suppress(OSError):
                active.shutdown(socket.SHUT_RDWR)
            active.close()

    def request_deadline(self, connection: socket.socket) -> float:
        with self._request_lock:
            return self._request_deadlines.get(
                id(connection),
                time.monotonic(),
            )

    def close_active_requests(self) -> None:
        with self._request_lock:
            connections = tuple(self._active_connections.values())
        for connection in connections:
            with contextlib.suppress(OSError):
                connection.shutdown(socket.SHUT_RDWR)
            connection.close()

    def join_request_threads(self, deadline: float) -> bool:
        current = threading.current_thread()
        while True:
            with self._request_lock:
                threads = tuple(
                    thread
                    for thread in self._request_threads
                    if thread is not current
                )
            if not threads:
                return True
            if time.monotonic() >= deadline:
                return False
            for thread in threads:
                thread.join(
                    timeout=max(0.0, deadline - time.monotonic())
                )


class MediaWorkerServer:
    """Synchronous HTTPS/mTLS server with an isolated bounded worker queue."""

    def __init__(
        self,
        *,
        host: str,
        port: int,
        ssl_context: ssl.SSLContext,
        spool_directory: str | Path,
        processor: object,
        processing_concurrency: int = 1,
        queue_capacity: int = 2,
        request_timeout_seconds: float = 30,
        shutdown_timeout_seconds: float = 30,
        ttl_seconds: int = DEFAULT_TTL_SECONDS,
        storage_limit_bytes: int = DEFAULT_STORAGE_LIMIT_BYTES,
        max_source_bytes: int = MAX_SOURCE_BYTES,
        max_output_bytes: int = MAX_OUTPUT_BYTES,
    ) -> None:
        if ssl_context.verify_mode != ssl.CERT_REQUIRED:
            raise ValueError("server TLS context must require client certificates")
        if not 1 <= int(processing_concurrency) <= 4:
            raise ValueError("processing_concurrency must be between 1 and 4")
        if not 0 <= int(queue_capacity) <= 16:
            raise ValueError("queue_capacity must be between 0 and 16")
        if not 1 <= int(max_source_bytes) <= MAX_SOURCE_BYTES:
            raise ValueError("invalid max_source_bytes")
        if not 1 <= int(max_output_bytes) <= MAX_OUTPUT_BYTES:
            raise ValueError("invalid max_output_bytes")
        if int(storage_limit_bytes) < (
            int(max_source_bytes)
            + int(max_output_bytes)
            + JOB_STORAGE_SLACK_BYTES
        ):
            raise ValueError("storage limit cannot hold one complete job")
        configure_output_limit = getattr(processor, "set_output_limit", None)
        if callable(configure_output_limit):
            configure_output_limit(int(max_output_bytes))
        self.processor = processor
        self.processing_concurrency = int(processing_concurrency)
        self.queue_capacity = int(queue_capacity)
        self.request_timeout_seconds = max(1.0, float(request_timeout_seconds))
        self.shutdown_timeout_seconds = max(
            1.0,
            float(shutdown_timeout_seconds),
        )
        self.ttl_seconds = max(0, int(ttl_seconds))
        self.storage_limit_bytes = int(storage_limit_bytes)
        self.max_source_bytes = int(max_source_bytes)
        self.max_output_bytes = int(max_output_bytes)
        self._capacity = self.processing_concurrency + self.queue_capacity
        self._state_lock = threading.RLock()
        self._stop_event = threading.Event()
        self._serving_event = threading.Event()
        self._started = False
        self._workers: list[threading.Thread] = []
        self._housekeeping_thread: threading.Thread | None = None
        self._queue: queue.Queue[str] = queue.Queue(
            maxsize=max(1, self._capacity)
        )
        self._unfinished_ids: set[str] = set()
        self._queued_ids: set[str] = set()
        self._deferred_ids: set[str] = set()
        self._reservations: dict[str, int] = {}
        self._fatal_error: BaseException | None = None
        self._spool_lock_fd = -1
        self._resources_closed = False
        self._httpd: _WorkerHTTPServer

        self.spool_directory = _ensure_private_directory(
            Path(spool_directory),
            create=True,
        )
        self._spool_lock_fd = self._acquire_spool_lock()
        try:
            self.jobs_directory = _ensure_private_directory(
                self.spool_directory / "jobs",
                create=True,
            )
            self.incoming_directory = _ensure_private_directory(
                self.spool_directory / "incoming",
                create=True,
            )
            self._clean_stale_uploads(remove_all=True)
            self._clean_stale_job_directories(remove_all=True)
            self._load_existing_jobs()
            with self._state_lock:
                self._cleanup_locked(required_bytes=0)

            handler_class = self._handler_class()
            self._httpd = _WorkerHTTPServer(
                (str(host), int(port)),
                handler_class,
                request_timeout=self.request_timeout_seconds,
                ssl_context=ssl_context,
                max_request_threads=max(
                    4,
                    min(16, self._capacity + 4),
                ),
            )
            self._httpd.media_worker = self  # type: ignore[attr-defined]
        except BaseException:
            self._release_spool_lock()
            raise

    @property
    def server_address(self) -> tuple[str, int]:
        host, port = self._httpd.server_address[:2]
        return str(host), int(port)

    def _acquire_spool_lock(self) -> int:
        lock_path = self.spool_directory / ".worker.lock"
        flags = os.O_RDWR | os.O_CREAT
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            descriptor = os.open(lock_path, flags, 0o600)
        except OSError:
            raise MediaWorkerUnavailable("spool_unavailable") from None
        try:
            status = os.fstat(descriptor)
            if (
                not stat.S_ISREG(status.st_mode)
                or status.st_uid != os.getuid()
                or status.st_mode & 0o077
            ):
                raise MediaWorkerUnavailable("unsafe_spool_lock")
            try:
                fcntl.flock(
                    descriptor,
                    fcntl.LOCK_EX | fcntl.LOCK_NB,
                )
            except BlockingIOError:
                raise MediaWorkerUnavailable("spool_locked") from None
            return descriptor
        except BaseException:
            os.close(descriptor)
            raise

    def _release_spool_lock(self) -> None:
        descriptor = self._spool_lock_fd
        if descriptor < 0:
            return
        self._spool_lock_fd = -1
        with contextlib.suppress(OSError):
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)

    def serve_forever(self) -> None:
        self.start()
        self._serving_event.set()
        shutdown_complete = False
        try:
            with self._state_lock:
                fatal_error = self._fatal_error
            if fatal_error is not None:
                raise RuntimeError("media worker thread failed") from fatal_error
            self._httpd.serve_forever(poll_interval=0.2)
        finally:
            self._serving_event.clear()
            deadline = time.monotonic() + self.shutdown_timeout_seconds
            shutdown_complete = self._finalize(deadline)
        with self._state_lock:
            fatal_error = self._fatal_error
        if fatal_error is not None:
            raise RuntimeError("media worker thread failed") from fatal_error
        if not shutdown_complete:
            raise RuntimeError("media worker shutdown timed out")

    def start(self) -> None:
        """Start processor threads once, before publishing service readiness."""
        with self._state_lock:
            if self._started or self._stop_event.is_set():
                return
            self._started = True
            for index in range(self.processing_concurrency):
                worker = threading.Thread(
                    target=self._worker_entry,
                    name=f"media-worker-{index + 1}",
                    daemon=True,
                )
                worker.start()
                self._workers.append(worker)
            self._housekeeping_thread = threading.Thread(
                target=self._housekeeping_entry,
                name="media-worker-housekeeping",
                daemon=True,
            )
            self._housekeeping_thread.start()

    def shutdown(self) -> None:
        self._stop_event.set()
        if self._resources_closed:
            return
        self._httpd.close_active_requests()
        if self._serving_event.is_set():
            self._httpd.shutdown()
        else:
            self._finalize(
                time.monotonic() + self.shutdown_timeout_seconds
            )

    def _finalize(self, deadline: float) -> bool:
        with self._state_lock:
            if self._resources_closed:
                return True
            self._resources_closed = True
        self._stop_event.set()
        self._httpd.close_active_requests()
        self._httpd.server_close()
        threads: list[threading.Thread] = list(self._workers)
        if self._housekeeping_thread is not None:
            threads.append(self._housekeeping_thread)
        for thread in threads:
            if thread is threading.current_thread():
                continue
            thread.join(timeout=max(0.0, deadline - time.monotonic()))
        request_threads_stopped = self._httpd.join_request_threads(deadline)
        complete = request_threads_stopped and not any(
            thread.is_alive()
            for thread in threads
            if thread is not threading.current_thread()
        )
        if complete:
            self._release_spool_lock()
        return complete

    def _handler_class(self) -> type[http.server.BaseHTTPRequestHandler]:
        class Handler(http.server.BaseHTTPRequestHandler):
            server_version = "CodexMediaWorker/1"
            sys_version = ""
            protocol_version = "HTTP/1.0"

            def do_POST(self) -> None:
                worker: MediaWorkerServer = self.server.media_worker  # type: ignore[attr-defined]
                worker._handle_post(self)

            def do_GET(self) -> None:
                worker: MediaWorkerServer = self.server.media_worker  # type: ignore[attr-defined]
                worker._handle_get(self)

            def do_HEAD(self) -> None:
                self._send_json(405, {"version": PROTOCOL_VERSION, "error": "method_not_allowed"})

            def log_message(self, format: str, *args: Any) -> None:
                del format, args

            def _send_json(self, status_code: int, value: Mapping[str, Any]) -> None:
                payload = (
                    json.dumps(value, sort_keys=True, separators=(",", ":"))
                    + "\n"
                ).encode("utf-8")
                self.send_response(status_code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.send_header("Cache-Control", "no-store")
                self.send_header("Connection", "close")
                self.end_headers()
                if self.command != "HEAD":
                    with contextlib.suppress(OSError):
                        self.wfile.write(payload)
                self.close_connection = True

        return Handler

    def _handle_post(self, handler: http.server.BaseHTTPRequestHandler) -> None:
        match = re.fullmatch(r"/v1/jobs/([0-9a-f]{64})", handler.path)
        if match is None:
            self._send_error(handler, 404, "unknown_endpoint")
            return
        try:
            request = self._request_from_headers(match.group(1), handler.headers)
        except MediaWorkerProtocolError as error:
            self._send_error(handler, 400, error.kind)
            return
        try:
            existing = self._reserve_or_replay(request)
        except MediaWorkerBusy as error:
            self._send_error(handler, 429, error.kind)
            return
        except MediaWorkerUnavailable as error:
            self._send_error(handler, 503, error.kind)
            return
        except MediaWorkerProtocolError as error:
            status_code = 409 if error.kind == "job_conflict" else 500
            self._send_error(handler, status_code, error.kind)
            return
        if existing is not None:
            state = str(existing.get("state") or "")
            self._send_json(handler, 200 if state in {"complete", "failed"} else 202, existing)
            return

        temporary: Path | None = None
        try:
            httpd: _WorkerHTTPServer = handler.server  # type: ignore[assignment]
            temporary = self._receive_source(
                handler.rfile,
                request.source_length,
                request.source_sha256,
                connection=handler.connection,
                deadline=httpd.request_deadline(handler.connection),
            )
            manifest = self._commit_job(request, temporary)
            temporary = None
        except MediaWorkerProtocolError as error:
            self._release_reservation(request.job_id)
            status_code = 408 if error.kind == "request_timeout" else 400
            self._send_error(handler, status_code, error.kind)
            return
        except OSError:
            self._release_reservation(request.job_id)
            self._send_error(handler, 503, "storage_unavailable")
            return
        finally:
            if temporary is not None:
                with contextlib.suppress(OSError):
                    temporary.unlink()
        self._send_json(handler, 202, manifest)

    def _handle_get(self, handler: http.server.BaseHTTPRequestHandler) -> None:
        status_match = re.fullmatch(r"/v1/jobs/([0-9a-f]{64})", handler.path)
        if status_match is not None:
            job_id = status_match.group(1)
            try:
                with self._state_lock:
                    self._cleanup_locked(required_bytes=0)
                    manifest = self._read_status(job_id)
            except FileNotFoundError:
                self._send_error(handler, 404, "job_not_found")
                return
            except MediaWorkerProtocolError as error:
                self._send_error(handler, 500, error.kind)
                return
            self._send_json(handler, 200, manifest)
            return

        artifact_match = re.fullmatch(
            r"/v1/jobs/([0-9a-f]{64})/artifacts/(audio\.mp3|transcript\.txt|frame-0[1-3]\.jpg)",
            handler.path,
        )
        if artifact_match is None:
            self._send_error(handler, 404, "unknown_endpoint")
            return
        job_id, name = artifact_match.groups()
        try:
            request = self._read_request(job_id)
            manifest = self._read_status(job_id)
            state, artifacts, _ = _parse_status_manifest(
                manifest,
                expected=request,
                max_output_bytes=self.max_output_bytes,
            )
            if state != "complete":
                raise FileNotFoundError
            artifact = next(item for item in artifacts if item.name == name)
            path = self._job_directory(job_id) / "output" / name
            status = _owner_regular_file(path)
            if status.st_size != artifact.length:
                raise MediaWorkerProtocolError("artifact_changed")
            flags = os.O_RDONLY
            if hasattr(os, "O_NOFOLLOW"):
                flags |= os.O_NOFOLLOW
            descriptor = os.open(path, flags)
        except (FileNotFoundError, StopIteration):
            self._send_error(handler, 404, "artifact_not_found")
            return
        except (OSError, MediaWorkerProtocolError) as error:
            kind = (
                error.kind
                if isinstance(error, MediaWorkerProtocolError)
                else "storage_unavailable"
            )
            self._send_error(handler, 500, kind)
            return
        try:
            handler.send_response(200)
            handler.send_header("Content-Type", artifact.content_type)
            handler.send_header("Content-Length", str(artifact.length))
            handler.send_header("Cache-Control", "no-store")
            handler.send_header("Connection", "close")
            handler.end_headers()
            remaining = artifact.length
            while remaining:
                chunk = os.read(descriptor, min(1024 * 1024, remaining))
                if not chunk:
                    break
                handler.wfile.write(chunk)
                remaining -= len(chunk)
            handler.close_connection = True
        except OSError:
            pass
        finally:
            os.close(descriptor)

    def _request_from_headers(
        self,
        job_id: str,
        headers: Mapping[str, str],
    ) -> _JobRequest:
        if headers.get("X-Media-Worker-Version") != str(PROTOCOL_VERSION):
            raise MediaWorkerProtocolError("unsupported_protocol")
        if headers.get("Content-Type") != "application/octet-stream":
            raise MediaWorkerProtocolError("invalid_content_type")
        raw_duration = headers.get("X-Media-Duration", "")
        raw_length = headers.get("Content-Length", "")
        if not re.fullmatch(r"0|[1-9][0-9]*", raw_duration):
            raise MediaWorkerProtocolError("invalid_duration")
        if not re.fullmatch(r"[1-9][0-9]*", raw_length):
            raise MediaWorkerProtocolError("invalid_source_length")
        request = _JobRequest(
            job_id=job_id,
            kind=headers.get("X-Media-Kind", ""),
            media_key=headers.get("X-Media-Key", ""),
            duration_seconds=int(raw_duration),
            source_length=int(raw_length),
            source_sha256=headers.get("X-Content-SHA256", ""),
        )
        _validate_request(request, max_source=self.max_source_bytes)
        return request

    def _reserve_or_replay(
        self,
        request: _JobRequest,
    ) -> dict[str, Any] | None:
        with self._state_lock:
            job_directory = self._job_directory(request.job_id)
            if job_directory.exists():
                stored = self._read_request(request.job_id)
                if stored != request:
                    raise MediaWorkerProtocolError("job_conflict")
                return self._read_status(request.job_id)
            if request.job_id != _canonical_job_id(
                kind=request.kind,
                media_key=request.media_key,
                duration_seconds=request.duration_seconds,
                source_length=request.source_length,
                source_sha256=request.source_sha256,
            ):
                raise MediaWorkerProtocolError("job_id_mismatch")
            if request.job_id in self._reservations:
                raise MediaWorkerBusy("duplicate_in_progress")
            self._cleanup_locked(
                required_bytes=(
                    request.source_length
                    + self.max_output_bytes
                    + JOB_STORAGE_SLACK_BYTES
                )
            )
            if len(self._unfinished_ids) + len(self._reservations) >= self._capacity:
                raise MediaWorkerBusy("queue_full")
            self._reservations[request.job_id] = (
                request.source_length
                + self.max_output_bytes
                + JOB_STORAGE_SLACK_BYTES
            )
            return None

    def _release_reservation(self, job_id: str) -> None:
        with self._state_lock:
            self._reservations.pop(job_id, None)

    def _receive_source(
        self,
        stream: Any,
        expected_length: int,
        expected_digest: str,
        *,
        connection: socket.socket,
        deadline: float,
    ) -> Path:
        temporary = self.incoming_directory / (
            f".upload-{uuid.uuid4().hex}.part"
        )
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        descriptor = os.open(temporary, flags, 0o600)
        digest = hashlib.sha256()
        received = 0
        try:
            os.fchmod(descriptor, 0o600)
            with os.fdopen(descriptor, "wb", closefd=True) as output:
                descriptor = -1
                while received < expected_length:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise MediaWorkerProtocolError("request_timeout")
                    connection.settimeout(max(0.001, remaining))
                    try:
                        chunk = stream.read(
                            min(
                                1024 * 1024,
                                expected_length - received,
                            )
                        )
                    except (OSError, TimeoutError):
                        kind = (
                            "request_timeout"
                            if time.monotonic() >= deadline
                            else "truncated_source"
                        )
                        raise MediaWorkerProtocolError(kind) from None
                    if not chunk:
                        kind = (
                            "request_timeout"
                            if time.monotonic() >= deadline
                            else "truncated_source"
                        )
                        raise MediaWorkerProtocolError(kind)
                    received += len(chunk)
                    if received > self.max_source_bytes:
                        raise MediaWorkerProtocolError("source_too_large")
                    digest.update(chunk)
                    output.write(chunk)
                output.flush()
                os.fsync(output.fileno())
            if received != expected_length or digest.hexdigest() != expected_digest:
                raise MediaWorkerProtocolError("source_digest_mismatch")
            return temporary
        except BaseException:
            with contextlib.suppress(OSError):
                temporary.unlink()
            raise
        finally:
            if descriptor >= 0:
                os.close(descriptor)

    def _commit_job(
        self,
        request: _JobRequest,
        temporary_source: Path,
    ) -> dict[str, Any]:
        temporary_directory = self.jobs_directory / (
            f".job-{request.job_id}-{uuid.uuid4().hex}.part"
        )
        final_directory = self._job_directory(request.job_id)
        temporary_directory.mkdir(mode=0o700)
        try:
            source = temporary_directory / "source.bin"
            os.replace(temporary_source, source)
            os.chmod(source, 0o600)
            _write_json_atomic(
                temporary_directory / "request.json",
                request.to_dict(),
            )
            manifest = _status_manifest(request, state="queued")
            _write_json_atomic(
                temporary_directory / "status.json",
                manifest,
            )
            _fsync_directory(temporary_directory)
            with self._state_lock:
                if final_directory.exists():
                    stored = self._read_request(request.job_id)
                    if stored != request:
                        raise MediaWorkerProtocolError("job_conflict")
                    self._reservations.pop(request.job_id, None)
                    return self._read_status(request.job_id)
                os.rename(temporary_directory, final_directory)
                _fsync_directory(self.jobs_directory)
                self._reservations.pop(request.job_id, None)
                self._unfinished_ids.add(request.job_id)
                self._enqueue_locked(request.job_id)
            return manifest
        finally:
            self._release_reservation(request.job_id)
            if temporary_directory.exists():
                shutil.rmtree(temporary_directory, ignore_errors=True)

    def _worker_entry(self) -> None:
        self._supervised_entry(self._worker_loop)

    def _housekeeping_entry(self) -> None:
        self._supervised_entry(self._housekeeping_loop)

    def _supervised_entry(self, target: Callable[[], None]) -> None:
        try:
            target()
        except BaseException as error:
            with self._state_lock:
                if self._fatal_error is None:
                    self._fatal_error = error
            self._stop_event.set()
            self._httpd.close_active_requests()
            if self._serving_event.is_set():
                self._httpd.shutdown()

    def _housekeeping_loop(self) -> None:
        interval = min(
            60.0,
            max(
                0.2,
                self.ttl_seconds / 2 if self.ttl_seconds else 1.0,
            ),
        )
        while not self._stop_event.wait(interval):
            with self._state_lock:
                self._cleanup_locked(required_bytes=0)

    def _worker_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                job_id = self._queue.get(timeout=0.2)
            except queue.Empty:
                continue
            with self._state_lock:
                self._queued_ids.discard(job_id)
            terminal = False
            try:
                terminal = self._process_job(job_id)
            finally:
                self._queue.task_done()
            if terminal:
                with self._state_lock:
                    self._unfinished_ids.discard(job_id)
                    self._enqueue_deferred_locked()
                    self._cleanup_locked(required_bytes=0)

    def _process_job(self, job_id: str) -> bool:
        request = self._read_request(job_id)
        try:
            recovered = self._recover_published_result(request)
            if recovered is not None:
                self._write_status(
                    request,
                    state="complete",
                    artifacts=recovered,
                )
                return True
            self._write_status(request, state="running")
            artifacts = self._invoke_processor(request)
            self._write_status(
                request,
                state="complete",
                artifacts=artifacts,
            )
            return True
        except MediaProcessingError as error:
            if error.kind not in TERMINAL_MEDIA_ERRORS:
                raise
            self._write_status(
                request,
                state="failed",
                error=error.kind,
            )
            return True
        finally:
            os.utime(self._job_directory(job_id), None)

    def _invoke_processor(self, request: _JobRequest) -> tuple[_Artifact, ...]:
        job_directory = self._job_directory(request.job_id)
        self._remove_partial_output_directories(job_directory)
        work_directory = job_directory / (
            f".work-{request.job_id}-{uuid.uuid4().hex}.part"
        )
        work_directory.mkdir(mode=0o700)
        try:
            method = getattr(self.processor, f"prepare_{request.kind}", None)
            if method is None or not callable(method):
                raise MediaProcessingError("worker_misconfigured")
            prepared = method(
                media_key=request.media_key,
                source_path=job_directory / "source.bin",
                duration_seconds=request.duration_seconds,
                output_directory=work_directory,
            )
            if not isinstance(prepared, PreparedMedia):
                raise MediaProcessingError("invalid_worker_output")
            return self._publish_prepared(request, prepared, work_directory)
        finally:
            if work_directory.exists():
                shutil.rmtree(work_directory, ignore_errors=True)

    def _publish_prepared(
        self,
        request: _JobRequest,
        prepared: PreparedMedia,
        work_directory: Path,
    ) -> tuple[_Artifact, ...]:
        if request.kind == "transcript":
            return self._publish_transcript(request, prepared, work_directory)
        if (
            prepared.kind != request.kind
            or prepared.duration_seconds != request.duration_seconds
        ):
            raise MediaProcessingError("invalid_worker_output")
        audio_inputs = [
            item for item in prepared.inputs if item.input_type == "localAudio"
        ]
        image_inputs = [
            item for item in prepared.inputs if item.input_type == "localImage"
        ]
        if len(audio_inputs) > 1 or len(image_inputs) > 3:
            raise MediaProcessingError("invalid_worker_output")
        if len(audio_inputs) + len(image_inputs) != len(prepared.inputs):
            raise MediaProcessingError("invalid_worker_output")
        if request.kind == "voice":
            if len(audio_inputs) != 1 or image_inputs:
                raise MediaProcessingError("invalid_worker_output")
        elif not image_inputs:
            raise MediaProcessingError("invalid_worker_output")

        ordered: list[tuple[LocalInput, str, str]] = []
        if audio_inputs:
            ordered.append((audio_inputs[0], "audio.mp3", "audio/mpeg"))
        for index, item in enumerate(image_inputs, start=1):
            ordered.append((item, f"frame-{index:02d}.jpg", "image/jpeg"))
        validated: list[tuple[Path, str, str, int, str]] = []
        aggregate = 0
        root = work_directory.resolve(strict=True)
        seen_paths: set[Path] = set()
        for item, name, content_type in ordered:
            candidate = Path(item.path)
            try:
                resolved = candidate.resolve(strict=True)
            except (OSError, RuntimeError):
                raise MediaProcessingError("invalid_worker_output") from None
            if (
                candidate.is_symlink()
                or resolved.parent != root
                or resolved in seen_paths
            ):
                raise MediaProcessingError("invalid_worker_output")
            seen_paths.add(resolved)
            try:
                status = resolved.lstat()
            except OSError:
                raise MediaProcessingError("invalid_worker_output") from None
            if (
                not stat.S_ISREG(status.st_mode)
                or status.st_uid != os.getuid()
                or status.st_size <= 0
                or status.st_size > MAX_ARTIFACT_BYTES
            ):
                raise MediaProcessingError("invalid_worker_output")
            if content_type == "image/jpeg" and not self._looks_like_jpeg(resolved):
                raise MediaProcessingError("invalid_worker_output")
            if content_type == "audio/mpeg" and not self._looks_like_mp3(resolved):
                raise MediaProcessingError("invalid_worker_output")
            length, digest = self._hash_owned_artifact(resolved)
            aggregate += length
            if aggregate > self.max_output_bytes:
                raise MediaProcessingError("output_too_large")
            validated.append((resolved, name, content_type, length, digest))

        publish_directory = self._job_directory(request.job_id) / (
            f".publish-{request.job_id}-{uuid.uuid4().hex}.part"
        )
        output_directory = self._job_directory(request.job_id) / "output"
        if output_directory.exists():
            raise MediaProcessingError("invalid_worker_output")
        publish_directory.mkdir(mode=0o700)
        artifacts: list[_Artifact] = []
        try:
            for source, name, content_type, length, digest in validated:
                destination = publish_directory / name
                os.replace(source, destination)
                os.chmod(destination, 0o600)
                with destination.open("rb") as stream:
                    os.fsync(stream.fileno())
                artifacts.append(
                    _Artifact(
                        name=name,
                        content_type=content_type,
                        length=length,
                        sha256=digest,
                    )
                )
            _validate_artifact_set(
                artifacts,
                kind=request.kind,
                max_output_bytes=self.max_output_bytes,
            )
            _write_json_atomic(
                publish_directory / "result.json",
                {
                    "version": PROTOCOL_VERSION,
                    "kind": request.kind,
                    "duration_seconds": request.duration_seconds,
                    "artifacts": [item.to_dict() for item in artifacts],
                },
            )
            _fsync_directory(publish_directory)
            os.rename(publish_directory, output_directory)
            _fsync_directory(output_directory.parent)
            return tuple(artifacts)
        finally:
            if publish_directory.exists():
                shutil.rmtree(publish_directory, ignore_errors=True)

    def _publish_transcript(
        self,
        request: _JobRequest,
        prepared: PreparedMedia,
        work_directory: Path,
    ) -> tuple[_Artifact, ...]:
        if (
            prepared.kind != request.kind
            or prepared.duration_seconds != request.duration_seconds
            or prepared.inputs
        ):
            raise MediaProcessingError("invalid_worker_output")
        source = work_directory / "transcript.txt"
        status = _owner_regular_file(source)
        if status.st_size > TRANSCRIPT_MAX_BYTES:
            raise MediaProcessingError("invalid_worker_output")
        try:
            text = source.read_text(encoding="utf-8")
        except (OSError, UnicodeError):
            raise MediaProcessingError("invalid_worker_output") from None
        if not text.strip() or "\x00" in text:
            raise MediaProcessingError("invalid_worker_output")
        length, digest = self._hash_owned_artifact(source)
        artifact = _Artifact(
            name="transcript.txt",
            content_type="text/plain; charset=utf-8",
            length=length,
            sha256=digest,
        )
        _validate_artifact_set(
            (artifact,),
            kind=request.kind,
            max_output_bytes=self.max_output_bytes,
        )
        publish_directory = self._job_directory(request.job_id) / (
            f".publish-{request.job_id}-{uuid.uuid4().hex}.part"
        )
        output_directory = self._job_directory(request.job_id) / "output"
        publish_directory.mkdir(mode=0o700)
        try:
            destination = publish_directory / artifact.name
            os.replace(source, destination)
            os.chmod(destination, 0o600)
            with destination.open("rb") as stream:
                os.fsync(stream.fileno())
            _write_json_atomic(
                publish_directory / "result.json",
                {
                    "version": PROTOCOL_VERSION,
                    "kind": request.kind,
                    "duration_seconds": request.duration_seconds,
                    "artifacts": [artifact.to_dict()],
                },
            )
            _fsync_directory(publish_directory)
            os.rename(publish_directory, output_directory)
            _fsync_directory(output_directory.parent)
            return (artifact,)
        finally:
            if publish_directory.exists():
                shutil.rmtree(publish_directory, ignore_errors=True)

    def _recover_published_result(
        self,
        request: _JobRequest,
    ) -> tuple[_Artifact, ...] | None:
        output = self._job_directory(request.job_id) / "output"
        if not output.exists():
            return None
        if output.is_symlink():
            raise MediaProcessingError("invalid_worker_output")
        value = _read_json(output / "result.json", fields=_RESULT_FIELDS)
        if (
            value.get("version") != PROTOCOL_VERSION
            or value.get("kind") != request.kind
            or _strict_int(value.get("duration_seconds"))
            != request.duration_seconds
            or not isinstance(value.get("artifacts"), list)
        ):
            raise MediaProcessingError("invalid_worker_output")
        artifacts = tuple(
            _Artifact.from_dict(item) for item in value["artifacts"]
        )
        _validate_artifact_set(
            artifacts,
            kind=request.kind,
            max_output_bytes=self.max_output_bytes,
        )
        for artifact in artifacts:
            path = output / artifact.name
            status = _owner_regular_file(path)
            if status.st_size != artifact.length:
                raise MediaProcessingError("invalid_worker_output")
            length, digest = self._hash_owned_artifact(path)
            if length != artifact.length or digest != artifact.sha256:
                raise MediaProcessingError("invalid_worker_output")
        return artifacts

    def _write_status(
        self,
        request: _JobRequest,
        *,
        state: str,
        artifacts: Sequence[_Artifact] = (),
        error: str | None = None,
    ) -> None:
        _write_json_atomic(
            self._job_directory(request.job_id) / "status.json",
            _status_manifest(
                request,
                state=state,
                artifacts=artifacts,
                error=error,
            ),
        )

    def _read_request(self, job_id: str) -> _JobRequest:
        path = self._job_directory(job_id) / "request.json"
        if not path.exists():
            raise FileNotFoundError(path)
        request = _JobRequest.from_dict(_read_json(path, fields=_REQUEST_FIELDS))
        if request.job_id != job_id:
            raise MediaWorkerProtocolError("job_directory_mismatch")
        return request

    def _read_status(self, job_id: str) -> dict[str, Any]:
        path = self._job_directory(job_id) / "status.json"
        if not path.exists():
            raise FileNotFoundError(path)
        value = _read_json(path, fields=_STATUS_FIELDS)
        request = self._read_request(job_id)
        _parse_status_manifest(
            value,
            expected=request,
            max_output_bytes=self.max_output_bytes,
        )
        return value

    def _job_directory(self, job_id: str) -> Path:
        if JOB_ID_PATTERN.fullmatch(job_id) is None:
            raise MediaWorkerProtocolError("invalid_job_id")
        return self.jobs_directory / job_id

    def _load_existing_jobs(self) -> None:
        incomplete: list[tuple[float, str]] = []
        for child in self.jobs_directory.iterdir():
            if child.is_symlink() or not child.is_dir():
                continue
            if JOB_ID_PATTERN.fullmatch(child.name) is None:
                continue
            try:
                request = self._read_request(child.name)
                manifest = self._read_status(child.name)
                state, _, _ = _parse_status_manifest(
                    manifest,
                    expected=request,
                    max_output_bytes=self.max_output_bytes,
                )
                if state in {"queued", "running"}:
                    incomplete.append((child.stat().st_mtime, child.name))
            except (OSError, MediaWorkerProtocolError) as error:
                raise MediaWorkerUnavailable(
                    "spool_unavailable"
                ) from error
        for _, job_id in sorted(incomplete):
            self._unfinished_ids.add(job_id)
            self._enqueue_locked(job_id)

    def _enqueue_locked(self, job_id: str) -> None:
        if job_id in self._queued_ids:
            return
        try:
            self._queue.put_nowait(job_id)
        except queue.Full:
            self._deferred_ids.add(job_id)
        else:
            self._queued_ids.add(job_id)
            self._deferred_ids.discard(job_id)

    def _enqueue_deferred_locked(self) -> None:
        for job_id in sorted(self._deferred_ids):
            if self._queue.full():
                break
            self._enqueue_locked(job_id)

    def _cleanup_locked(self, *, required_bytes: int) -> None:
        now = time.time()
        terminal: list[tuple[float, Path, int]] = []
        self._clean_stale_uploads(remove_all=False)
        self._clean_stale_job_directories(remove_all=False)
        used = self._quota_used_locked()
        for child in self.jobs_directory.iterdir():
            if (
                child.is_symlink()
                or not child.is_dir()
                or JOB_ID_PATTERN.fullmatch(child.name) is None
                or child.name in self._unfinished_ids
            ):
                continue
            try:
                modified = child.stat().st_mtime
                manifest = self._read_status(child.name)
                state = str(manifest.get("state") or "")
            except (OSError, MediaWorkerProtocolError):
                continue
            if state not in {"complete", "failed"}:
                continue
            size = self._quota_job_size(
                child,
                include_output=True,
            )
            terminal.append((modified, child, size))
        for modified, directory, size in sorted(terminal):
            if self.ttl_seconds == 0 or now - modified >= self.ttl_seconds:
                self._remove_job_directory(directory)
                used = max(0, used - size)
        if used + required_bytes > self.storage_limit_bytes:
            for _, directory, size in sorted(terminal):
                if not directory.exists():
                    continue
                self._remove_job_directory(directory)
                used = max(0, used - size)
                if used + required_bytes <= self.storage_limit_bytes:
                    break
        active_reserve = (
            len(self._unfinished_ids) * self.max_output_bytes
            + sum(self._reservations.values())
        )
        if (
            required_bytes > 0
            and used + required_bytes + active_reserve
            > self.storage_limit_bytes
        ):
            raise MediaWorkerUnavailable("storage_full")

    def _clean_stale_uploads(self, *, remove_all: bool) -> None:
        now = time.time()
        cutoff = max(60.0, self.request_timeout_seconds * 2) if hasattr(
            self, "request_timeout_seconds"
        ) else 60.0
        for child in self.incoming_directory.iterdir():
            try:
                status = child.lstat()
            except OSError:
                continue
            if (
                UPLOAD_NAME_PATTERN.fullmatch(child.name) is None
                or not stat.S_ISREG(status.st_mode)
                or status.st_uid != os.getuid()
            ):
                continue
            if remove_all or now - status.st_mtime >= cutoff:
                with contextlib.suppress(OSError):
                    child.unlink()

    def _clean_stale_job_directories(self, *, remove_all: bool) -> None:
        now = time.time()
        cutoff = max(60.0, self.request_timeout_seconds * 2)
        for child in self.jobs_directory.iterdir():
            try:
                status = child.lstat()
            except OSError:
                continue
            if (
                JOB_TEMP_NAME_PATTERN.fullmatch(child.name) is None
                or not stat.S_ISDIR(status.st_mode)
                or status.st_uid != os.getuid()
            ):
                continue
            if remove_all or now - status.st_mtime >= cutoff:
                shutil.rmtree(child, ignore_errors=True)

    def _remove_partial_output_directories(self, job_directory: Path) -> None:
        for child in job_directory.iterdir():
            try:
                status = child.lstat()
            except OSError:
                continue
            if (
                WORK_NAME_PATTERN.fullmatch(child.name) is None
                or not stat.S_ISDIR(status.st_mode)
                or status.st_uid != os.getuid()
            ):
                continue
            shutil.rmtree(child, ignore_errors=True)

    def _remove_job_directory(self, directory: Path) -> None:
        if (
            directory.parent != self.jobs_directory
            or directory.is_symlink()
            or JOB_ID_PATTERN.fullmatch(directory.name) is None
        ):
            raise MediaWorkerProtocolError("unsafe_storage")
        shutil.rmtree(directory)
        _fsync_directory(self.jobs_directory)

    def _quota_used_locked(self) -> int:
        total = 0
        for child in self.jobs_directory.iterdir():
            if (
                child.is_symlink()
                or not child.is_dir()
                or JOB_ID_PATTERN.fullmatch(child.name) is None
            ):
                continue
            total += self._quota_job_size(
                child,
                include_output=child.name not in self._unfinished_ids,
            )
        return total

    @staticmethod
    def _quota_job_size(
        root: Path,
        *,
        include_output: bool,
    ) -> int:
        total = JOB_STORAGE_SLACK_BYTES
        source = root / "source.bin"
        try:
            source_status = source.lstat()
        except OSError:
            source_status = None
        if source_status is not None and stat.S_ISREG(source_status.st_mode):
            total += int(source_status.st_size)
        if not include_output:
            return total
        root = root / "output"
        if not root.is_dir() or root.is_symlink():
            return total
        total = 0
        stack = [root]
        while stack:
            directory = stack.pop()
            try:
                entries = list(os.scandir(directory))
            except OSError:
                continue
            for entry in entries:
                try:
                    status = entry.stat(follow_symlinks=False)
                except OSError:
                    continue
                if stat.S_ISDIR(status.st_mode):
                    stack.append(Path(entry.path))
                elif stat.S_ISREG(status.st_mode):
                    total += int(status.st_size)
        return total + JOB_STORAGE_SLACK_BYTES + (
            int(source_status.st_size)
            if source_status is not None
            and stat.S_ISREG(source_status.st_mode)
            else 0
        )

    @staticmethod
    def _hash_owned_artifact(path: Path) -> tuple[int, str]:
        digest = hashlib.sha256()
        length = 0
        flags = os.O_RDONLY
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        descriptor = os.open(path, flags)
        try:
            status = os.fstat(descriptor)
            if not stat.S_ISREG(status.st_mode) or status.st_uid != os.getuid():
                raise MediaWorkerProtocolError("unsafe_artifact")
            while True:
                chunk = os.read(descriptor, 1024 * 1024)
                if not chunk:
                    break
                length += len(chunk)
                digest.update(chunk)
        finally:
            os.close(descriptor)
        return length, digest.hexdigest()

    @staticmethod
    def _looks_like_jpeg(path: Path) -> bool:
        try:
            with path.open("rb") as stream:
                head = stream.read(3)
                stream.seek(-2, os.SEEK_END)
                tail = stream.read(2)
        except OSError:
            return False
        return head == b"\xff\xd8\xff" and tail == b"\xff\xd9"

    @staticmethod
    def _looks_like_mp3(path: Path) -> bool:
        try:
            with path.open("rb") as stream:
                head = stream.read(3)
        except OSError:
            return False
        return head == b"ID3" or (
            len(head) >= 2 and head[0] == 0xFF and head[1] & 0xE0 == 0xE0
        )

    @staticmethod
    def _send_json(
        handler: http.server.BaseHTTPRequestHandler,
        status_code: int,
        value: Mapping[str, Any],
    ) -> None:
        handler._send_json(status_code, value)  # type: ignore[attr-defined]

    def _send_error(
        self,
        handler: http.server.BaseHTTPRequestHandler,
        status_code: int,
        kind: str,
    ) -> None:
        self._send_json(
            handler,
            status_code,
            {"version": PROTOCOL_VERSION, "error": kind},
        )


@dataclass
class _ResolverAttempt:
    event: threading.Event
    addresses: tuple[tuple[int, int, int, Any], ...] = ()
    error: BaseException | None = None


class _NamedServerHTTPSConnection(http.client.HTTPSConnection):
    def __init__(
        self,
        host: str,
        port: int,
        *,
        context: ssl.SSLContext,
        timeout: float,
        server_hostname: str,
        resolved_addresses: Sequence[tuple[int, int, int, Any]],
        deadline: float,
        clock: Callable[[], float],
    ) -> None:
        super().__init__(host, port, timeout=timeout, context=context)
        self._media_server_hostname = server_hostname
        self._media_resolved_addresses = tuple(resolved_addresses)
        self._media_deadline = deadline
        self._media_clock = clock

    def connect(self) -> None:
        last_error: OSError | None = None
        for family, socket_type, protocol, address in (
            self._media_resolved_addresses
        ):
            remaining = self._media_deadline - self._media_clock()
            if remaining <= 0:
                raise TimeoutError("media worker connection deadline expired")
            candidate = socket.socket(family, socket_type, protocol)
            self.sock = candidate
            try:
                candidate.settimeout(min(float(self.timeout), remaining))
                if self.source_address:
                    candidate.bind(self.source_address)
                candidate.connect(address)
            except OSError as error:
                last_error = error
                candidate.close()
                if self.sock is candidate:
                    self.sock = None
                continue
            break
        else:
            if last_error is not None:
                raise last_error
            raise OSError("media worker address resolution returned no address")
        if self._tunnel_host:
            self._tunnel()
        assert self.sock is not None
        remaining = self._media_deadline - self._media_clock()
        if remaining <= 0:
            raise TimeoutError("media worker TLS deadline expired")
        self.sock.settimeout(min(float(self.timeout), remaining))
        self.sock = self._context.wrap_socket(
            self.sock,
            server_hostname=self._media_server_hostname,
        )


class MediaWorkerClient:
    """Strict synchronous client for the optional version-1 media worker."""

    def __init__(
        self,
        *,
        base_url: str,
        ssl_context: ssl.SSLContext,
        tls_server_name: str | None = None,
        request_timeout_seconds: float = 30,
        processing_timeout_seconds: float = 180,
        poll_interval_seconds: float = 0.5,
        max_attempts: int = 2,
        retry_backoff_seconds: float = 0.25,
        circuit_failure_threshold: int = 3,
        circuit_recovery_seconds: float = 30,
        max_source_bytes: int = MAX_SOURCE_BYTES,
        max_output_bytes: int = MAX_OUTPUT_BYTES,
        _clock: Callable[[], float] = time.monotonic,
        _sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        parsed = urlsplit(base_url)
        if (
            parsed.scheme != "https"
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
            or parsed.path not in {"", "/"}
        ):
            raise ValueError("base_url must be a bare https origin")
        if ssl_context.verify_mode != ssl.CERT_REQUIRED:
            raise ValueError("client TLS context must verify the server")
        if not ssl_context.check_hostname:
            raise ValueError("client TLS context must check the server hostname")
        if not 1 <= int(max_attempts) <= 4:
            raise ValueError("max_attempts must be between 1 and 4")
        if not 1 <= int(circuit_failure_threshold) <= 20:
            raise ValueError("invalid circuit_failure_threshold")
        self._host = parsed.hostname
        self._port = parsed.port or 443
        self._host_header = parsed.netloc
        self.ssl_context = ssl_context
        self.tls_server_name = tls_server_name or parsed.hostname
        self.request_timeout_seconds = max(1.0, float(request_timeout_seconds))
        self.processing_timeout_seconds = max(
            self.request_timeout_seconds,
            float(processing_timeout_seconds),
        )
        self.poll_interval_seconds = max(0.01, float(poll_interval_seconds))
        self.max_attempts = int(max_attempts)
        self.retry_backoff_seconds = max(0.0, float(retry_backoff_seconds))
        self.circuit_failure_threshold = int(circuit_failure_threshold)
        self.circuit_recovery_seconds = max(
            0.01,
            float(circuit_recovery_seconds),
        )
        self.max_source_bytes = min(MAX_SOURCE_BYTES, int(max_source_bytes))
        self.max_output_bytes = min(MAX_OUTPUT_BYTES, int(max_output_bytes))
        self._clock = _clock
        self._sleep = _sleep
        self._circuit_lock = threading.Lock()
        self._circuit_state = "CLOSED"
        self._circuit_failures = 0
        self._circuit_opened_at = 0.0
        self._half_open_in_flight = False
        self._resolver_lock = threading.Lock()
        self._resolver_inflight: _ResolverAttempt | None = None
        self._resolver_cache: tuple[tuple[int, int, int, Any], ...] = ()
        self._resolver_cache_expires_at = 0.0

    @property
    def circuit_state(self) -> str:
        with self._circuit_lock:
            return self._circuit_state

    def prepare(
        self,
        kind: str,
        media_key: str,
        source_path: str | Path,
        duration_seconds: int,
        destination_directory: str | Path,
    ) -> PreparedMedia:
        if kind not in MEDIA_KINDS:
            raise MediaProcessingError("invalid_media")
        if MEDIA_KEY_PATTERN.fullmatch(media_key) is None:
            raise MediaProcessingError("invalid_media")
        if (
            isinstance(duration_seconds, bool)
            or not isinstance(duration_seconds, int)
            or duration_seconds < 0
        ):
            raise MediaProcessingError("invalid_media")
        if duration_seconds > MAX_DURATION_SECONDS:
            raise MediaWorkerUnavailable("unsupported_media")
        source = Path(source_path)
        try:
            length, digest = _sha256_file(
                source,
                maximum=self.max_source_bytes,
            )
        except MediaProcessingError as error:
            if error.kind == "file_too_large":
                raise MediaWorkerUnavailable("unsupported_media") from None
            raise
        destination = self._validate_destination(destination_directory)
        job_id = _canonical_job_id(
            kind=kind,
            media_key=media_key,
            duration_seconds=duration_seconds,
            source_length=length,
            source_sha256=digest,
        )
        request = _JobRequest(
            job_id=job_id,
            kind=kind,
            media_key=media_key,
            duration_seconds=duration_seconds,
            source_length=length,
            source_sha256=digest,
        )
        half_open = self._circuit_before_request()
        deadline = self._clock() + self.processing_timeout_seconds
        last_transient: MediaWorkerError | None = None
        for attempt in range(self.max_attempts):
            try:
                result = self._prepare_once(
                    request,
                    source,
                    destination,
                    deadline,
                )
            except (MediaWorkerBusy, MediaWorkerUnavailable) as error:
                last_transient = error
                if (
                    attempt + 1 < self.max_attempts
                    and self._clock() < deadline
                ):
                    delay = min(
                        self.retry_backoff_seconds * (2**attempt),
                        max(0.0, deadline - self._clock()),
                    )
                    self._sleep(delay)
                    continue
                self._circuit_failed(half_open)
                if self._clock() >= deadline:
                    raise MediaWorkerUnavailable(
                        "processing_timeout"
                    ) from None
                raise
            except MediaWorkerProtocolError:
                self._circuit_failed(half_open)
                raise
            except MediaProcessingError:
                self._circuit_succeeded()
                raise
            except BaseException:
                self._circuit_failed(half_open)
                raise
            else:
                self._circuit_succeeded()
                return result
        self._circuit_failed(half_open)
        raise last_transient or MediaWorkerUnavailable("worker_unavailable")

    def _prepare_once(
        self,
        request: _JobRequest,
        source: Path,
        destination: Path,
        deadline: float,
    ) -> PreparedMedia:
        self._remaining_timeout(deadline)
        manifest = self._get_status_if_present(request.job_id, deadline)
        if manifest is None:
            manifest = self._post_job(request, source, deadline)
        state, artifacts, error = _parse_status_manifest(
            manifest,
            expected=request,
            max_output_bytes=self.max_output_bytes,
        )
        while state in {"queued", "running"}:
            if self._clock() >= deadline:
                raise MediaWorkerUnavailable("processing_timeout")
            self._sleep(
                min(
                    self.poll_interval_seconds,
                    max(0.0, deadline - self._clock()),
                )
            )
            manifest = self._get_json(
                f"/v1/jobs/{request.job_id}",
                deadline,
            )
            state, artifacts, error = _parse_status_manifest(
                manifest,
                expected=request,
                max_output_bytes=self.max_output_bytes,
            )
        if state == "failed":
            assert error is not None
            if error in TERMINAL_MEDIA_ERRORS:
                raise MediaProcessingError(error)
            raise MediaWorkerUnavailable(error)
        return self._materialize(
            request,
            artifacts,
            destination,
            deadline,
        )

    def _get_status_if_present(
        self,
        job_id: str,
        deadline: float,
    ) -> dict[str, Any] | None:
        connection = self._connection(deadline)
        expiry = self._deadline_timer(connection, deadline)
        try:
            connection.request(
                "GET",
                f"/v1/jobs/{job_id}",
                headers={"Accept": "application/json", "Connection": "close"},
            )
            response = connection.getresponse()
            payload = self._read_response(
                response,
                maximum=MAX_JSON_BYTES,
                connection=connection,
                deadline=deadline,
            )
            if response.status == 404:
                try:
                    value = json.loads(payload.decode("utf-8"))
                except (UnicodeError, json.JSONDecodeError):
                    raise MediaWorkerProtocolError("malformed_error_response") from None
                if value != {
                    "version": PROTOCOL_VERSION,
                    "error": "job_not_found",
                }:
                    raise MediaWorkerProtocolError("malformed_error_response")
                return None
            return self._decode_json_response(response.status, payload)
        except MediaWorkerError:
            raise
        except (OSError, ssl.SSLError, http.client.HTTPException, TimeoutError):
            self._invalidate_resolution()
            raise MediaWorkerUnavailable(
                self._network_error_kind(deadline)
            ) from None
        finally:
            expiry.cancel()
            connection.close()

    def _post_job(
        self,
        request: _JobRequest,
        source: Path,
        deadline: float,
    ) -> dict[str, Any]:
        connection = self._connection(deadline)
        expiry = self._deadline_timer(connection, deadline)
        try:
            connection.putrequest(
                "POST",
                f"/v1/jobs/{request.job_id}",
                skip_host=True,
                skip_accept_encoding=True,
            )
            connection.putheader("Host", self._host_header)
            connection.putheader("Content-Type", "application/octet-stream")
            connection.putheader("Content-Length", str(request.source_length))
            connection.putheader(
                "X-Media-Worker-Version",
                str(PROTOCOL_VERSION),
            )
            connection.putheader("X-Media-Kind", request.kind)
            connection.putheader("X-Media-Key", request.media_key)
            connection.putheader(
                "X-Media-Duration",
                str(request.duration_seconds),
            )
            connection.putheader("X-Content-SHA256", request.source_sha256)
            connection.putheader("Connection", "close")
            connection.endheaders()
            flags = os.O_RDONLY
            if hasattr(os, "O_NOFOLLOW"):
                flags |= os.O_NOFOLLOW
            descriptor = os.open(source, flags)
            try:
                sent = 0
                while sent < request.source_length:
                    remaining = self._remaining_timeout(deadline)
                    if connection.sock is not None:
                        connection.sock.settimeout(remaining)
                    chunk = os.read(
                        descriptor,
                        min(1024 * 1024, request.source_length - sent),
                    )
                    if not chunk:
                        raise MediaProcessingError("invalid_media")
                    connection.send(chunk)
                    sent += len(chunk)
            finally:
                os.close(descriptor)
            response = connection.getresponse()
            payload = self._read_response(
                response,
                maximum=MAX_JSON_BYTES,
                connection=connection,
                deadline=deadline,
            )
            return self._decode_json_response(response.status, payload)
        except MediaProcessingError:
            raise
        except (OSError, ssl.SSLError, http.client.HTTPException, TimeoutError):
            self._invalidate_resolution()
            raise MediaWorkerUnavailable(
                self._network_error_kind(deadline)
            ) from None
        finally:
            expiry.cancel()
            connection.close()

    def _get_json(
        self,
        path: str,
        deadline: float,
    ) -> dict[str, Any]:
        connection = self._connection(deadline)
        expiry = self._deadline_timer(connection, deadline)
        try:
            connection.request(
                "GET",
                path,
                headers={"Accept": "application/json", "Connection": "close"},
            )
            response = connection.getresponse()
            payload = self._read_response(
                response,
                maximum=MAX_JSON_BYTES,
                connection=connection,
                deadline=deadline,
            )
            return self._decode_json_response(response.status, payload)
        except MediaWorkerError:
            raise
        except (OSError, ssl.SSLError, http.client.HTTPException, TimeoutError):
            self._invalidate_resolution()
            raise MediaWorkerUnavailable(
                self._network_error_kind(deadline)
            ) from None
        finally:
            expiry.cancel()
            connection.close()

    def _decode_json_response(
        self,
        status_code: int,
        payload: bytes,
    ) -> dict[str, Any]:
        try:
            value = json.loads(payload.decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError):
            raise MediaWorkerProtocolError("malformed_response") from None
        if not isinstance(value, dict):
            raise MediaWorkerProtocolError("malformed_response")
        if 200 <= status_code < 300:
            return value
        if set(value) != {"version", "error"} or value.get("version") != 1:
            raise MediaWorkerProtocolError("malformed_error_response")
        kind = str(value.get("error") or "")
        if ERROR_KIND_PATTERN.fullmatch(kind) is None:
            raise MediaWorkerProtocolError("malformed_error_response")
        if status_code == 429:
            raise MediaWorkerBusy(kind)
        if status_code in {502, 503, 504}:
            raise MediaWorkerUnavailable(kind)
        if status_code == 409:
            raise MediaWorkerProtocolError("job_conflict")
        raise MediaWorkerProtocolError(kind)

    def _materialize(
        self,
        request: _JobRequest,
        artifacts: Sequence[_Artifact],
        destination: Path,
        deadline: float,
    ) -> PreparedMedia:
        _validate_artifact_set(
            artifacts,
            kind=request.kind,
            max_output_bytes=self.max_output_bytes,
        )
        final_directory = destination / f"worker-{request.job_id}"
        if final_directory.exists() or final_directory.is_symlink():
            return self._prepared_from_existing(
                request,
                artifacts,
                final_directory,
            )
        temporary = destination / (
            f".worker-{request.job_id}-{uuid.uuid4().hex}.part"
        )
        temporary.mkdir(mode=0o700)
        try:
            for artifact in artifacts:
                self._download_artifact(
                    request.job_id,
                    artifact,
                    temporary,
                    deadline,
                )
            _fsync_directory(temporary)
            try:
                os.rename(temporary, final_directory)
            except FileExistsError:
                return self._prepared_from_existing(
                    request,
                    artifacts,
                    final_directory,
                )
            _fsync_directory(destination)
            return self._prepared_from_existing(
                request,
                artifacts,
                final_directory,
            )
        finally:
            if temporary.exists():
                shutil.rmtree(temporary, ignore_errors=True)

    def _download_artifact(
        self,
        job_id: str,
        artifact: _Artifact,
        directory: Path,
        deadline: float,
    ) -> None:
        if artifact.name in {"audio.mp3", "transcript.txt"}:
            pass
        elif FRAME_NAME_PATTERN.fullmatch(artifact.name) is None:
            raise MediaWorkerProtocolError("unsafe_artifact_name")
        destination = directory / artifact.name
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        descriptor = os.open(destination, flags, 0o600)
        connection = self._connection(deadline)
        expiry = self._deadline_timer(connection, deadline)
        digest = hashlib.sha256()
        received = 0
        try:
            os.fchmod(descriptor, 0o600)
            connection.request(
                "GET",
                f"/v1/jobs/{job_id}/artifacts/{artifact.name}",
                headers={"Connection": "close"},
            )
            response = connection.getresponse()
            if response.status != 200:
                payload = self._read_response(
                    response,
                    maximum=MAX_JSON_BYTES,
                    connection=connection,
                    deadline=deadline,
                )
                self._decode_json_response(response.status, payload)
                raise MediaWorkerProtocolError("missing_artifact")
            if response.getheader("Content-Type") != artifact.content_type:
                raise MediaWorkerProtocolError("invalid_artifact_type")
            raw_length = response.getheader("Content-Length")
            if raw_length != str(artifact.length):
                raise MediaWorkerProtocolError("artifact_length_mismatch")
            while received < artifact.length:
                remaining = self._remaining_timeout(deadline)
                if connection.sock is not None:
                    connection.sock.settimeout(remaining)
                chunk = response.read1(
                    min(1024 * 1024, artifact.length - received)
                )
                if not chunk:
                    break
                if self._clock() >= deadline:
                    raise MediaWorkerUnavailable("processing_timeout")
                received += len(chunk)
                if received > self.max_output_bytes:
                    raise MediaWorkerProtocolError("output_too_large")
                digest.update(chunk)
                os.write(descriptor, chunk)
            if (
                received != artifact.length
                or digest.hexdigest() != artifact.sha256
                or response.read1(1)
            ):
                raise MediaWorkerProtocolError("artifact_digest_mismatch")
            os.fsync(descriptor)
        except MediaWorkerError:
            raise
        except (OSError, ssl.SSLError, http.client.HTTPException, TimeoutError):
            self._invalidate_resolution()
            raise MediaWorkerUnavailable(
                self._network_error_kind(deadline)
            ) from None
        finally:
            expiry.cancel()
            connection.close()
            os.close(descriptor)
        os.chmod(destination, 0o600)

    def _prepared_from_existing(
        self,
        request: _JobRequest,
        artifacts: Sequence[_Artifact],
        directory: Path,
    ) -> PreparedMedia:
        if directory.is_symlink():
            raise MediaWorkerProtocolError("unsafe_destination")
        try:
            status = directory.lstat()
        except OSError:
            raise MediaWorkerProtocolError("unsafe_destination") from None
        if (
            not stat.S_ISDIR(status.st_mode)
            or status.st_uid != os.getuid()
            or status.st_mode & 0o077
        ):
            raise MediaWorkerProtocolError("unsafe_destination")
        expected_names = {item.name for item in artifacts}
        try:
            actual_names = {item.name for item in directory.iterdir()}
        except OSError:
            raise MediaWorkerProtocolError("unsafe_destination") from None
        if actual_names != expected_names:
            raise MediaWorkerProtocolError("malformed_local_output")
        inputs: list[LocalInput] = []
        for artifact in artifacts:
            path = directory / artifact.name
            status = _owner_regular_file(path)
            if status.st_mode & 0o077 or status.st_size != artifact.length:
                raise MediaWorkerProtocolError("unsafe_artifact")
            length, digest = self._hash_local_artifact(path)
            if length != artifact.length or digest != artifact.sha256:
                raise MediaWorkerProtocolError("artifact_digest_mismatch")
            if artifact.name == "transcript.txt":
                continue
            if artifact.name == "audio.mp3":
                inputs.append(LocalInput("localAudio", str(path.resolve())))
            else:
                inputs.append(
                    LocalInput("localImage", str(path.resolve()), "low")
                )
        return PreparedMedia(
            kind=request.kind,
            duration_seconds=request.duration_seconds,
            inputs=tuple(inputs),
        )

    def transcribe(
        self,
        *,
        media_key: str,
        source_path: str | Path,
        duration_seconds: int,
        destination_directory: str | Path,
    ) -> str:
        source = Path(source_path)
        length, digest = _sha256_file(source, maximum=self.max_source_bytes)
        job_id = _canonical_job_id(
            kind="transcript",
            media_key=media_key,
            duration_seconds=duration_seconds,
            source_length=length,
            source_sha256=digest,
        )
        self.prepare(
            kind="transcript",
            media_key=media_key,
            source_path=source,
            duration_seconds=duration_seconds,
            destination_directory=destination_directory,
        )
        directory = self._validate_destination(destination_directory) / f"worker-{job_id}"
        path = directory / "transcript.txt"
        status = _owner_regular_file(path)
        if status.st_size > TRANSCRIPT_MAX_BYTES:
            raise MediaWorkerProtocolError("output_too_large")
        try:
            transcript = path.read_text(encoding="utf-8").strip()
        except (OSError, UnicodeError):
            raise MediaWorkerProtocolError("malformed_transcript") from None
        if not transcript or "\x00" in transcript:
            raise MediaWorkerProtocolError("malformed_transcript")
        return transcript

    def _validate_destination(self, value: str | Path) -> Path:
        path = Path(value).expanduser()
        if path.is_symlink():
            raise MediaProcessingError("unsafe_storage")
        try:
            status = path.lstat()
            resolved = path.resolve(strict=True)
        except (OSError, RuntimeError):
            raise MediaProcessingError("unsafe_storage") from None
        if (
            not stat.S_ISDIR(status.st_mode)
            or status.st_uid != os.getuid()
            or status.st_mode & 0o077
        ):
            raise MediaProcessingError("unsafe_storage")
        return resolved

    def _remaining_timeout(self, deadline: float) -> float:
        remaining = deadline - self._clock()
        if remaining <= 0:
            raise MediaWorkerUnavailable("processing_timeout")
        return min(self.request_timeout_seconds, remaining)

    def _deadline_timer(
        self,
        connection: _NamedServerHTTPSConnection,
        deadline: float,
    ) -> threading.Timer:
        timer = threading.Timer(
            max(0.0, deadline - self._clock()),
            self._abort_connection,
            args=(connection,),
        )
        timer.daemon = True
        timer.start()
        return timer

    @staticmethod
    def _abort_connection(
        connection: _NamedServerHTTPSConnection,
    ) -> None:
        active = connection.sock
        if active is not None:
            with contextlib.suppress(OSError):
                active.shutdown(socket.SHUT_RDWR)
            active.close()
        connection.close()

    def _network_error_kind(self, deadline: float) -> str:
        return (
            "processing_timeout"
            if self._clock() >= deadline
            else "worker_unavailable"
        )

    def _connection(
        self,
        deadline: float,
    ) -> _NamedServerHTTPSConnection:
        resolved_addresses = self._resolve_addresses(deadline)
        return _NamedServerHTTPSConnection(
            self._host,
            self._port,
            context=self.ssl_context,
            timeout=self._remaining_timeout(deadline),
            server_hostname=self.tls_server_name,
            resolved_addresses=resolved_addresses,
            deadline=deadline,
            clock=self._clock,
        )

    def _resolve_addresses(
        self,
        deadline: float,
    ) -> tuple[tuple[int, int, int, Any], ...]:
        try:
            literal = ipaddress.ip_address(self._host)
        except ValueError:
            literal = None
        if literal is not None:
            if literal.version == 4:
                return (
                    (
                        socket.AF_INET,
                        socket.SOCK_STREAM,
                        socket.IPPROTO_TCP,
                        (str(literal), self._port),
                    ),
                )
            return (
                (
                    socket.AF_INET6,
                    socket.SOCK_STREAM,
                    socket.IPPROTO_TCP,
                    (str(literal), self._port, 0, 0),
                ),
            )

        with self._resolver_lock:
            now = self._clock()
            if self._resolver_cache and now < self._resolver_cache_expires_at:
                return self._resolver_cache
            attempt = self._resolver_inflight
            if attempt is None:
                attempt = _ResolverAttempt(threading.Event())
                self._resolver_inflight = attempt
                thread = threading.Thread(
                    target=self._resolver_entry,
                    args=(attempt,),
                    name="media-worker-resolver",
                    daemon=True,
                )
                thread.start()
        remaining = deadline - self._clock()
        wait_timeout = min(self.request_timeout_seconds, remaining)
        if wait_timeout <= 0 or not attempt.event.wait(wait_timeout):
            raise MediaWorkerUnavailable(
                self._network_error_kind(deadline)
            )
        if attempt.error is not None:
            raise MediaWorkerUnavailable("worker_unavailable")
        if not attempt.addresses:
            raise MediaWorkerUnavailable("worker_unavailable")
        return attempt.addresses

    def _resolver_entry(self, attempt: _ResolverAttempt) -> None:
        try:
            values = socket.getaddrinfo(
                self._host,
                self._port,
                type=socket.SOCK_STREAM,
                proto=socket.IPPROTO_TCP,
            )
            addresses = tuple(
                (int(family), int(socket_type), int(protocol), address)
                for family, socket_type, protocol, _, address in values
            )
            if not addresses:
                raise OSError("media worker hostname has no addresses")
            attempt.addresses = addresses
        except BaseException as error:
            attempt.error = error
        finally:
            with self._resolver_lock:
                if (
                    self._resolver_inflight is attempt
                    and attempt.addresses
                    and attempt.error is None
                ):
                    self._resolver_cache = attempt.addresses
                    self._resolver_cache_expires_at = (
                        self._clock() + DNS_CACHE_SECONDS
                    )
                if self._resolver_inflight is attempt:
                    self._resolver_inflight = None
            attempt.event.set()

    def _invalidate_resolution(self) -> None:
        with self._resolver_lock:
            self._resolver_cache = ()
            self._resolver_cache_expires_at = 0.0

    def _read_response(
        self,
        response: http.client.HTTPResponse,
        *,
        maximum: int,
        connection: _NamedServerHTTPSConnection,
        deadline: float,
    ) -> bytes:
        raw_length = response.getheader("Content-Length")
        if raw_length is None or not re.fullmatch(r"0|[1-9][0-9]*", raw_length):
            raise MediaWorkerProtocolError("missing_content_length")
        length = int(raw_length)
        if length > maximum:
            raise MediaWorkerProtocolError("response_too_large")
        payload = bytearray()
        while len(payload) < length:
            remaining = self._remaining_timeout(deadline)
            if connection.sock is not None:
                connection.sock.settimeout(remaining)
            chunk = response.read1(
                min(64 * 1024, length - len(payload))
            )
            if not chunk:
                # A peer that closes after sending a valid Content-Length but
                # before the full body has produced a transport failure, not
                # an authenticated protocol conclusion.  Treat it like the
                # other retry/fallback-eligible connection failures so a
                # worker shutdown cannot take down the Pi media path.
                raise MediaWorkerUnavailable("worker_unavailable")
            payload.extend(chunk)
            if self._clock() >= deadline:
                raise MediaWorkerUnavailable("processing_timeout")
        return bytes(payload)

    @staticmethod
    def _hash_local_artifact(path: Path) -> tuple[int, str]:
        digest = hashlib.sha256()
        length = 0
        flags = os.O_RDONLY
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            descriptor = os.open(path, flags)
        except OSError:
            raise MediaWorkerProtocolError("unsafe_artifact") from None
        try:
            status = os.fstat(descriptor)
            if not stat.S_ISREG(status.st_mode) or status.st_uid != os.getuid():
                raise MediaWorkerProtocolError("unsafe_artifact")
            while True:
                chunk = os.read(descriptor, 1024 * 1024)
                if not chunk:
                    break
                length += len(chunk)
                digest.update(chunk)
        finally:
            os.close(descriptor)
        return length, digest.hexdigest()

    def _circuit_before_request(self) -> bool:
        with self._circuit_lock:
            now = self._clock()
            if self._circuit_state == "OPEN":
                if now - self._circuit_opened_at < self.circuit_recovery_seconds:
                    raise MediaWorkerUnavailable("circuit_open")
                self._circuit_state = "HALF_OPEN"
                self._half_open_in_flight = False
            if self._circuit_state == "HALF_OPEN":
                if self._half_open_in_flight:
                    raise MediaWorkerUnavailable("circuit_open")
                self._half_open_in_flight = True
                return True
            return False

    def _circuit_succeeded(self) -> None:
        with self._circuit_lock:
            self._circuit_state = "CLOSED"
            self._circuit_failures = 0
            self._half_open_in_flight = False

    def _circuit_failed(self, half_open: bool) -> None:
        with self._circuit_lock:
            self._half_open_in_flight = False
            if half_open:
                self._circuit_state = "OPEN"
                self._circuit_opened_at = self._clock()
                return
            self._circuit_failures += 1
            if self._circuit_failures >= self.circuit_failure_threshold:
                self._circuit_state = "OPEN"
                self._circuit_opened_at = self._clock()
