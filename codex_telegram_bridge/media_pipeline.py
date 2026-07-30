from __future__ import annotations

import logging
import os
import re
import ssl
import stat
from pathlib import Path
from typing import Protocol

from .config import MediaWorkerClientConfig
from .media import MediaProcessingError, MediaProcessor, PreparedMedia


LOGGER = logging.getLogger(__name__)
REMOTE_MEDIA_KINDS = frozenset({"voice", "video_note", "video"})
WORKER_OUTPUT_DIRECTORY_PATTERN = re.compile(r"worker-[0-9a-f]{64}")
MAX_WORKER_OUTPUT_BYTES = 32 * 1024 * 1024


class MediaWorker(Protocol):
    def prepare(
        self,
        *,
        kind: str,
        media_key: str,
        source_path: str | Path,
        duration_seconds: int,
        destination_directory: str | Path,
    ) -> PreparedMedia: ...


def build_media_worker_client(
    settings: MediaWorkerClientConfig | None,
) -> MediaWorker | None:
    if settings is None:
        return None
    for path in (
        settings.ca_certificate,
        settings.client_certificate,
        settings.client_key,
    ):
        try:
            status = path.lstat()
        except OSError:
            raise ValueError("media worker TLS material is unavailable") from None
        if (
            path.is_symlink()
            or not stat.S_ISREG(status.st_mode)
            or status.st_uid != os.getuid()
            or stat.S_IMODE(status.st_mode) & 0o077
        ):
            raise ValueError(
                "media worker TLS material must be owner-only regular files"
            )
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.check_hostname = True
    context.verify_mode = ssl.CERT_REQUIRED
    context.load_verify_locations(cafile=str(settings.ca_certificate))
    context.load_cert_chain(
        certfile=str(settings.client_certificate),
        keyfile=str(settings.client_key),
    )
    from .media_worker import MediaWorkerClient

    url_host = (
        f"[{settings.host}]" if ":" in settings.host else settings.host
    )
    return MediaWorkerClient(
        base_url=f"https://{url_host}:{settings.port}",
        ssl_context=context,
        tls_server_name=settings.server_name,
        request_timeout_seconds=settings.request_timeout_seconds,
        processing_timeout_seconds=settings.processing_timeout_seconds,
        circuit_failure_threshold=settings.failure_threshold,
        circuit_recovery_seconds=settings.cooldown_seconds,
    )


class HybridMediaProcessor:
    """Use an optional worker as an accelerator, never as source of truth."""

    def __init__(
        self,
        *,
        local: MediaProcessor,
        worker: MediaWorker | None,
    ) -> None:
        self.local = local
        self.worker = worker

    def prepare_voice(
        self,
        *,
        media_key: str,
        source_path: str | Path,
        duration_seconds: int,
    ) -> PreparedMedia:
        return self._prepare(
            kind="voice",
            media_key=media_key,
            source_path=source_path,
            duration_seconds=duration_seconds,
        )

    def prepare_video_note(
        self,
        *,
        media_key: str,
        source_path: str | Path,
        duration_seconds: int,
    ) -> PreparedMedia:
        return self._prepare(
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
        return self._prepare(
            kind="video",
            media_key=media_key,
            source_path=source_path,
            duration_seconds=duration_seconds,
        )

    def _prepare(
        self,
        *,
        kind: str,
        media_key: str,
        source_path: str | Path,
        duration_seconds: int,
    ) -> PreparedMedia:
        if kind not in REMOTE_MEDIA_KINDS:
            raise ValueError("unsupported remote media kind")
        worker = self.worker
        if worker is not None:
            destination = self.local.message_directory(media_key)
            try:
                prepared = worker.prepare(
                    kind=kind,
                    media_key=media_key,
                    source_path=source_path,
                    duration_seconds=duration_seconds,
                    destination_directory=destination,
                )
                self._validate_worker_result(
                    prepared,
                    expected_kind=kind,
                    expected_duration=duration_seconds,
                    destination_directory=destination,
                )
                return prepared
            except MediaProcessingError:
                # The authenticated worker reached a domain conclusion about
                # the media itself. Preserve the existing user-facing error
                # rather than wasting scarce Pi resources on the same input.
                raise
            except Exception as error:
                # Remote acceleration is deliberately non-critical. Never let
                # its transport, TLS material, queue, or protocol take down
                # Telegram processing or bridge readiness.
                LOGGER.warning(
                    "Optional media worker unavailable; kind=%s; reason=%s; "
                    "using local ffmpeg",
                    kind,
                    type(error).__name__,
                )
        return self._prepare_local(
            kind=kind,
            media_key=media_key,
            source_path=source_path,
            duration_seconds=duration_seconds,
        )

    def _prepare_local(
        self,
        *,
        kind: str,
        media_key: str,
        source_path: str | Path,
        duration_seconds: int,
    ) -> PreparedMedia:
        if kind == "voice":
            return self.local.prepare_voice(
                media_key=media_key,
                source_path=source_path,
                duration_seconds=duration_seconds,
            )
        if kind == "video_note":
            return self.local.prepare_video_note(
                media_key=media_key,
                source_path=source_path,
                duration_seconds=duration_seconds,
            )
        return self.local.prepare_video(
            media_key=media_key,
            source_path=source_path,
            duration_seconds=duration_seconds,
        )

    @staticmethod
    def _validate_worker_result(
        prepared: PreparedMedia,
        *,
        expected_kind: str,
        expected_duration: int,
        destination_directory: Path,
    ) -> None:
        if (
            not isinstance(prepared, PreparedMedia)
            or prepared.kind != expected_kind
            or prepared.duration_seconds != max(0, int(expected_duration))
        ):
            raise ValueError("media worker returned mismatched media metadata")
        inputs = prepared.inputs
        audio_count = sum(item.input_type == "localAudio" for item in inputs)
        image_count = sum(item.input_type == "localImage" for item in inputs)
        if (
            not inputs
            or len(inputs) > 4
            or audio_count > 1
            or image_count > 3
            or audio_count + image_count != len(inputs)
            or (expected_kind == "voice" and (audio_count, image_count) != (1, 0))
            or (expected_kind != "voice" and image_count == 0)
        ):
            raise ValueError("media worker returned an invalid artifact set")
        root = destination_directory.resolve(strict=True)
        artifact_parent: Path | None = None
        aggregate_size = 0
        for item in inputs:
            candidate = Path(item.path)
            try:
                resolved = candidate.resolve(strict=True)
                resolved.relative_to(root)
                status = candidate.lstat()
            except (FileNotFoundError, OSError, RuntimeError, ValueError):
                raise ValueError(
                    "media worker artifact is outside local media storage"
                ) from None
            parent = candidate.parent
            if parent == root:
                resolved_parent = root
            else:
                try:
                    parent_status = parent.lstat()
                    resolved_parent = parent.resolve(strict=True)
                except (OSError, RuntimeError):
                    raise ValueError(
                        "media worker artifact directory is unsafe"
                    ) from None
                if (
                    parent.parent != root
                    or resolved_parent.parent != root
                    or WORKER_OUTPUT_DIRECTORY_PATTERN.fullmatch(parent.name)
                    is None
                    or parent.is_symlink()
                    or not stat.S_ISDIR(parent_status.st_mode)
                    or parent_status.st_uid != os.getuid()
                    or stat.S_IMODE(parent_status.st_mode) & 0o077
                ):
                    raise ValueError(
                        "media worker artifact directory is unsafe"
                    )
            if artifact_parent is None:
                artifact_parent = resolved_parent
            elif artifact_parent != resolved_parent:
                raise ValueError(
                    "media worker artifacts span multiple directories"
                )
            if (
                not candidate.is_absolute()
                or resolved.parent != resolved_parent
                or candidate.is_symlink()
                or not stat.S_ISREG(status.st_mode)
                or status.st_uid != os.getuid()
                or status.st_size <= 0
                or stat.S_IMODE(status.st_mode) & 0o077
            ):
                raise ValueError(
                    "media worker artifact is not an owner-only regular file"
                )
            aggregate_size += int(status.st_size)
            if aggregate_size > MAX_WORKER_OUTPUT_BYTES:
                raise ValueError("media worker artifact set is too large")
