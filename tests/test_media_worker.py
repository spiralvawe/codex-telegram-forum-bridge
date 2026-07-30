from __future__ import annotations

import contextlib
import hashlib
import http.client
import os
import signal
import shutil
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from codex_telegram_bridge.input_types import LocalInput  # noqa: E402
from codex_telegram_bridge.media import (  # noqa: E402
    MediaProcessingError,
    PreparedMedia,
)
from codex_telegram_bridge.media_worker import (  # noqa: E402
    FFMPEG_PROCESS_GROUP_RSS_LIMIT_BYTES,
    FFmpegMediaWorkerProcessor,
    MAX_OUTPUT_BYTES,
    MediaWorkerBusy,
    MediaWorkerClient,
    MediaWorkerProtocolError,
    MediaWorkerServer,
    MediaWorkerUnavailable,
    _Artifact,
    _JobRequest,
    _canonical_job_id,
    _parse_status_manifest,
    _process_group_rss_bytes,
    _status_manifest,
)


OPENSSL = shutil.which("openssl")


class _FakeProcessor:
    def __init__(self) -> None:
        self.calls = 0

    def prepare_voice(
        self,
        *,
        media_key: str,
        source_path: str | Path,
        duration_seconds: int,
        output_directory: str | Path,
    ) -> PreparedMedia:
        del media_key, source_path
        self.calls += 1
        audio = Path(output_directory) / "result.mp3"
        audio.write_bytes(b"ID3fake-mp3")
        os.chmod(audio, 0o600)
        return PreparedMedia(
            kind="voice",
            duration_seconds=duration_seconds,
            inputs=(LocalInput("localAudio", str(audio)),),
        )

    def prepare_video(
        self,
        *,
        media_key: str,
        source_path: str | Path,
        duration_seconds: int,
        output_directory: str | Path,
    ) -> PreparedMedia:
        del media_key, source_path
        self.calls += 1
        output = Path(output_directory)
        audio = output / "sound.mp3"
        frame = output / "image.jpg"
        audio.write_bytes(b"ID3fake-mp3")
        frame.write_bytes(b"\xff\xd8\xfffake-jpeg\xff\xd9")
        os.chmod(audio, 0o600)
        os.chmod(frame, 0o600)
        return PreparedMedia(
            kind="video",
            duration_seconds=duration_seconds,
            inputs=(
                LocalInput("localAudio", str(audio)),
                LocalInput("localImage", str(frame), "low"),
            ),
        )

    def prepare_video_note(self, **kwargs: object) -> PreparedMedia:
        prepared = self.prepare_video(**kwargs)
        return PreparedMedia(
            kind="video_note",
            duration_seconds=prepared.duration_seconds,
            inputs=prepared.inputs,
        )


class _FailingProcessor(_FakeProcessor):
    def __init__(self, kind: str) -> None:
        super().__init__()
        self.kind = kind

    def prepare_voice(self, **kwargs: object) -> PreparedMedia:
        del kwargs
        self.calls += 1
        raise MediaProcessingError(self.kind)


class _BlockingProcessor(_FakeProcessor):
    def __init__(self) -> None:
        super().__init__()
        self.started = threading.Event()
        self.release = threading.Event()

    def prepare_voice(self, **kwargs: object) -> PreparedMedia:
        self.started.set()
        if not self.release.wait(10):
            raise MediaProcessingError("processing_failed")
        return super().prepare_voice(**kwargs)


class _UnsafeProcessor(_FakeProcessor):
    def prepare_voice(
        self,
        *,
        media_key: str,
        source_path: str | Path,
        duration_seconds: int,
        output_directory: str | Path,
    ) -> PreparedMedia:
        del media_key, source_path
        outside = Path(output_directory).parent / "outside.mp3"
        outside.write_bytes(b"ID3outside")
        os.chmod(outside, 0o600)
        return PreparedMedia(
            kind="voice",
            duration_seconds=duration_seconds,
            inputs=(LocalInput("localAudio", str(outside)),),
        )


def _make_source(directory: Path, name: str = "source.bin") -> Path:
    source = directory / name
    source.write_bytes(b"telegram-media-source")
    os.chmod(source, 0o600)
    return source


class ProtocolValidationTests(unittest.TestCase):
    def test_rejects_traversal_and_non_sequential_artifacts(self) -> None:
        with self.assertRaisesRegex(
            MediaWorkerProtocolError,
            "unsafe_artifact_name",
        ):
            _Artifact.from_dict(
                {
                    "name": "../audio.mp3",
                    "content_type": "audio/mpeg",
                    "length": 3,
                    "sha256": "a" * 64,
                }
            )

        request = _JobRequest(
            job_id="a" * 64,
            kind="video",
            media_key="b" * 32,
            duration_seconds=10,
            source_length=4,
            source_sha256="c" * 64,
        )
        manifest = _status_manifest(
            request,
            state="complete",
            artifacts=(
                _Artifact("frame-02.jpg", "image/jpeg", 4, "d" * 64),
            ),
        )
        with self.assertRaisesRegex(
            MediaWorkerProtocolError,
            "unordered_artifacts",
        ):
            _parse_status_manifest(
                manifest,
                expected=request,
                max_output_bytes=MAX_OUTPUT_BYTES,
            )

    def test_manifest_requires_exact_metadata_and_digest(self) -> None:
        request = _JobRequest(
            job_id="1" * 64,
            kind="voice",
            media_key="2" * 32,
            duration_seconds=1,
            source_length=10,
            source_sha256="3" * 64,
        )
        manifest = _status_manifest(request, state="queued")
        manifest["source_length"] = 11

        with self.assertRaisesRegex(MediaWorkerProtocolError, "status_mismatch"):
            _parse_status_manifest(
                manifest,
                expected=request,
                max_output_bytes=MAX_OUTPUT_BYTES,
            )

    def test_ffmpeg_keeps_output_suffix_and_maps_timeout(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            binary = Path("/usr/bin/true")
            source = _make_source(root)
            source.write_bytes(b"OggSsynthetic-audio")
            output = root / "output"
            output.mkdir(mode=0o700)
            processor = FFmpegMediaWorkerProcessor(binary, timeout_seconds=2)
            observed: list[str] = []

            class SuccessfulProcess:
                returncode = 0

                def __init__(
                    self,
                    command: list[str],
                    **kwargs: object,
                ) -> None:
                    del kwargs
                    observed.append(command[-1])
                    Path(command[-1]).write_bytes(b"ID3generated")

                def poll(self) -> int:
                    return self.returncode

                def wait(self, timeout: float | None = None) -> int:
                    del timeout
                    return self.returncode

                def kill(self) -> None:
                    self.returncode = -9

            with mock.patch(
                "codex_telegram_bridge.media_worker.subprocess.Popen",
                side_effect=SuccessfulProcess,
            ):
                prepared = processor.prepare_voice(
                    media_key="4" * 32,
                    source_path=source,
                    duration_seconds=1,
                    output_directory=output,
                )
            self.assertTrue(observed[0].endswith(".part.mp3"))
            self.assertTrue(Path(prepared.inputs[0].path).is_file())

            clock = iter((0.0, 0.0, 3.0))
            timeout_processor = FFmpegMediaWorkerProcessor(
                binary,
                timeout_seconds=2,
                _clock=lambda: next(clock),
            )

            class HangingProcess(SuccessfulProcess):
                def __init__(
                    self,
                    command: list[str],
                    **kwargs: object,
                ) -> None:
                    del command, kwargs
                    self.returncode = None

                def poll(self) -> int | None:
                    return self.returncode

                def wait(self, timeout: float | None = None) -> int:
                    if self.returncode is None:
                        raise subprocess.TimeoutExpired(["ffmpeg"], timeout)
                    return self.returncode

            with mock.patch(
                "codex_telegram_bridge.media_worker.subprocess.Popen",
                side_effect=HangingProcess,
            ):
                with self.assertRaisesRegex(
                    MediaProcessingError,
                    "ffmpeg_unavailable",
                ):
                    timeout_processor.prepare_voice(
                        media_key="4" * 32,
                        source_path=source,
                        duration_seconds=1,
                        output_directory=output,
                    )

    def test_ffmpeg_forces_magic_selected_demuxer_and_file_protocol(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            binary = Path("/usr/bin/true")
            source = root / "voice.bin"
            source.write_bytes(b"OggSsynthetic")
            source.chmod(0o600)
            output = root / "output"
            output.mkdir(mode=0o700)
            commands: list[list[str]] = []

            class SuccessfulProcess:
                returncode = 0

                def __init__(
                    self,
                    command: list[str],
                    **kwargs: object,
                ) -> None:
                    del kwargs
                    commands.append(command)
                    Path(command[-1]).write_bytes(b"ID3generated")

                def poll(self) -> int:
                    return self.returncode

                def wait(self, timeout: float | None = None) -> int:
                    del timeout
                    return self.returncode

                def kill(self) -> None:
                    self.returncode = -9

            processor = FFmpegMediaWorkerProcessor(binary)
            with mock.patch(
                "codex_telegram_bridge.media_worker.subprocess.Popen",
                side_effect=SuccessfulProcess,
            ) as launch:
                processor.prepare_voice(
                    media_key="4" * 32,
                    source_path=source,
                    duration_seconds=1,
                    output_directory=output,
                )

            selected = commands[0]
            input_index = selected.index("-i")
            self.assertEqual(
                selected[input_index - 4 : input_index],
                ["-protocol_whitelist", "file", "-f", "ogg"],
            )
            output_limit_index = selected.index("-fs")
            self.assertEqual(
                selected[output_limit_index : output_limit_index + 2],
                ["-fs", str(20 * 1024 * 1024 + 1)],
            )
            self.assertEqual(
                selected[selected.index("-max_alloc") :][:2],
                ["-max_alloc", str(128 * 1024 * 1024)],
            )
            self.assertEqual(
                selected[selected.index("-max_pixels") :][:2],
                ["-max_pixels", "16777216"],
            )
            self.assertEqual(selected.count("-threads"), 2)
            self.assertLess(selected.index("-max_pixels"), input_index)
            self.assertTrue(launch.call_args.kwargs["start_new_session"])

    def test_playlist_is_rejected_before_ffmpeg_or_network_access(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            binary = Path("/usr/bin/true")
            source = root / "voice.bin"
            source.write_bytes(
                b"#EXTM3U\nhttp://192.168.1.1/private\n"
            )
            source.chmod(0o600)
            output = root / "output"
            output.mkdir(mode=0o700)
            processor = FFmpegMediaWorkerProcessor(binary)

            with mock.patch(
                "codex_telegram_bridge.media_worker.subprocess.Popen"
            ) as run:
                with self.assertRaisesRegex(
                    MediaProcessingError,
                    "invalid_audio",
                ):
                    processor.prepare_voice(
                        media_key="4" * 32,
                        source_path=source,
                        duration_seconds=1,
                        output_directory=output,
                    )
            run.assert_not_called()

    def test_video_processing_uses_one_aggregate_deadline(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            binary = Path("/usr/bin/true")
            source = root / "video.bin"
            source.write_bytes(b"\x00\x00\x00\x18ftypisomsynthetic")
            source.chmod(0o600)
            output = root / "output"
            output.mkdir(mode=0o700)
            clock_values = iter((0.0, 0.0, 11.0))
            processor = FFmpegMediaWorkerProcessor(
                binary,
                timeout_seconds=10,
                _clock=lambda: next(clock_values),
            )

            class SuccessfulProcess:
                returncode = 0

                def __init__(
                    self,
                    command: list[str],
                    **kwargs: object,
                ) -> None:
                    del kwargs
                    Path(command[-1]).write_bytes(b"ID3generated")

                def poll(self) -> int:
                    return self.returncode

                def wait(self, timeout: float | None = None) -> int:
                    del timeout
                    return self.returncode

                def kill(self) -> None:
                    self.returncode = -9

            with mock.patch(
                "codex_telegram_bridge.media_worker.subprocess.Popen",
                side_effect=SuccessfulProcess,
            ) as run:
                with self.assertRaisesRegex(
                    MediaProcessingError,
                    "ffmpeg_unavailable",
                ):
                    processor.prepare_video(
                        media_key="4" * 32,
                        source_path=source,
                        duration_seconds=2,
                        output_directory=output,
                    )
            self.assertEqual(run.call_count, 1)

    def test_ffmpeg_output_is_killed_at_the_artifact_cap(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "voice.bin"
            source.write_bytes(b"OggSsynthetic")
            source.chmod(0o600)
            output = root / "output"
            output.mkdir(mode=0o700)
            processes: list[object] = []

            class OversizedProcess:
                def __init__(
                    self,
                    command: list[str],
                    **kwargs: object,
                ) -> None:
                    del kwargs
                    self.returncode: int | None = None
                    self.killed = False
                    with Path(command[-1]).open("wb") as stream:
                        stream.truncate(21 * 1024 * 1024)
                    processes.append(self)

                def poll(self) -> int | None:
                    return self.returncode

                def wait(self, timeout: float | None = None) -> int:
                    del timeout
                    if self.returncode is None:
                        raise subprocess.TimeoutExpired(["ffmpeg"], 0.05)
                    return self.returncode

                def kill(self) -> None:
                    self.killed = True
                    self.returncode = -9

            processor = FFmpegMediaWorkerProcessor("/usr/bin/true")
            with mock.patch(
                "codex_telegram_bridge.media_worker.subprocess.Popen",
                side_effect=OversizedProcess,
            ):
                with self.assertRaisesRegex(
                    MediaProcessingError,
                    "invalid_audio",
                ):
                    processor.prepare_voice(
                        media_key="4" * 32,
                        source_path=source,
                        duration_seconds=1,
                        output_directory=output,
                    )
            self.assertTrue(processes[0].killed)  # type: ignore[attr-defined]
            self.assertFalse(any(output.iterdir()))

    def test_server_output_limit_is_enforced_inside_ffmpeg(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "voice.bin"
            source.write_bytes(b"OggSsynthetic")
            source.chmod(0o600)
            output = root / "output"
            output.mkdir(mode=0o700)
            commands: list[list[str]] = []

            class OversizedProcess:
                def __init__(
                    self,
                    command: list[str],
                    **kwargs: object,
                ) -> None:
                    del kwargs
                    commands.append(command)
                    self.returncode: int | None = None
                    with Path(command[-1]).open("wb") as stream:
                        stream.truncate(1025)

                def poll(self) -> int | None:
                    return self.returncode

                def wait(self, timeout: float | None = None) -> int:
                    del timeout
                    if self.returncode is None:
                        raise subprocess.TimeoutExpired(["ffmpeg"], 0.05)
                    return self.returncode

                def kill(self) -> None:
                    self.returncode = -9

            processor = FFmpegMediaWorkerProcessor(
                "/usr/bin/true",
                max_output_bytes=1024,
            )
            with mock.patch(
                "codex_telegram_bridge.media_worker.subprocess.Popen",
                side_effect=OversizedProcess,
            ):
                with self.assertRaisesRegex(
                    MediaProcessingError,
                    "invalid_audio",
                ):
                    processor.prepare_voice(
                        media_key="4" * 32,
                        source_path=source,
                        duration_seconds=1,
                        output_directory=output,
                    )
            output_limit_index = commands[0].index("-fs")
            self.assertEqual(
                commands[0][output_limit_index : output_limit_index + 2],
                ["-fs", "1025"],
            )
            self.assertFalse(any(output.iterdir()))

    def test_ffmpeg_memory_budget_aborts_optional_video_stage_terminally(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "video.bin"
            source.write_bytes(b"\x00\x00\x00\x18ftypisomsynthetic")
            source.chmod(0o600)
            output = root / "output"
            output.mkdir(mode=0o700)
            processes: list[object] = []

            class MemoryHungryProcess:
                pid = 999_999

                def __init__(
                    self,
                    command: list[str],
                    **kwargs: object,
                ) -> None:
                    del kwargs
                    self.returncode: int | None = None
                    self.killed = False
                    Path(command[-1]).write_bytes(b"ID3partial")
                    processes.append(self)

                def poll(self) -> int | None:
                    return self.returncode

                def wait(self, timeout: float | None = None) -> int:
                    del timeout
                    if self.returncode is None:
                        raise subprocess.TimeoutExpired(["ffmpeg"], 0.01)
                    return self.returncode

                def kill(self) -> None:
                    self.killed = True
                    self.returncode = -9

            processor = FFmpegMediaWorkerProcessor("/usr/bin/true")
            with (
                mock.patch(
                    "codex_telegram_bridge.media_worker.subprocess.Popen",
                    side_effect=MemoryHungryProcess,
                ),
                mock.patch(
                    "codex_telegram_bridge.media_worker."
                    "_process_group_rss_bytes",
                    return_value=(
                        FFMPEG_PROCESS_GROUP_RSS_LIMIT_BYTES + 1
                    ),
                ),
                mock.patch(
                    "codex_telegram_bridge.media_worker.os.killpg",
                    side_effect=ProcessLookupError,
                ),
            ):
                with self.assertRaisesRegex(
                    MediaProcessingError,
                    "invalid_video",
                ):
                    processor.prepare_video(
                        media_key="4" * 32,
                        source_path=source,
                        duration_seconds=10,
                        output_directory=output,
                    )
            self.assertTrue(processes[0].killed)  # type: ignore[attr-defined]
            self.assertEqual(len(processes), 1)
            self.assertFalse(any(output.iterdir()))

    def test_macos_memory_monitor_failure_is_terminal(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "voice.bin"
            source.write_bytes(b"OggSsynthetic")
            source.chmod(0o600)
            output = root / "output"
            output.mkdir(mode=0o700)
            process = mock.Mock()
            process.pid = 999_998
            process.poll.return_value = None
            process.wait.side_effect = [
                subprocess.TimeoutExpired(["ffmpeg"], 0.01),
                subprocess.TimeoutExpired(["ffmpeg"], 0.01),
                -9,
            ]

            def kill_group(*args: object, **kwargs: object) -> None:
                del args, kwargs
                process.poll.return_value = -9
                process.returncode = -9

            processor = FFmpegMediaWorkerProcessor("/usr/bin/true")
            with (
                mock.patch(
                    "codex_telegram_bridge.media_worker.subprocess.Popen",
                    return_value=process,
                ),
                mock.patch(
                    "codex_telegram_bridge.media_worker."
                    "_process_group_rss_bytes",
                    return_value=None,
                ),
                mock.patch(
                    "codex_telegram_bridge.media_worker.sys.platform",
                    "darwin",
                ),
                mock.patch(
                    "codex_telegram_bridge.media_worker.os.killpg",
                    side_effect=kill_group,
                ) as terminate,
            ):
                with self.assertRaisesRegex(
                    MediaProcessingError,
                    "invalid_audio",
                ):
                    processor.prepare_voice(
                        media_key="4" * 32,
                        source_path=source,
                        duration_seconds=1,
                        output_directory=output,
                    )
            terminate.assert_called_once_with(
                process.pid,
                signal.SIGKILL,
            )
            self.assertFalse(any(output.iterdir()))

    @unittest.skipUnless(
        sys.platform == "darwin",
        "libproc physical-footprint smoke is macOS-only",
    )
    def test_macos_libproc_reads_an_isolated_process_group(self) -> None:
        process = subprocess.Popen(
            [
                sys.executable,
                "-c",
                "import time; time.sleep(2)",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        try:
            resident = None
            deadline = time.monotonic() + 1
            while resident is None and time.monotonic() < deadline:
                resident = _process_group_rss_bytes(process.pid)
                if resident is None:
                    time.sleep(0.01)
            self.assertIsNotNone(resident)
            self.assertGreater(resident, 0)
        finally:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=2)

    def test_runtime_rejects_worker_owned_ffmpeg(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            binary = root / "ffmpeg"
            binary.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
            binary.chmod(0o700)
            source = root / "voice.bin"
            source.write_bytes(b"OggSsynthetic")
            source.chmod(0o600)
            output = root / "output"
            output.mkdir(mode=0o700)
            processor = FFmpegMediaWorkerProcessor(binary)
            with mock.patch(
                "codex_telegram_bridge.media_worker.subprocess.Popen"
            ) as run:
                with self.assertRaisesRegex(
                    MediaProcessingError,
                    "ffmpeg_unavailable",
                ):
                    processor.prepare_voice(
                        media_key="4" * 32,
                        source_path=source,
                        duration_seconds=1,
                        output_directory=output,
                    )
            run.assert_not_called()


class ClientSafetyAndCircuitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.destination = self.root / "media"
        self.destination.mkdir(mode=0o700)
        os.chmod(self.destination, 0o700)
        self.source = _make_source(self.destination)
        self.clock_value = 0.0
        context = ssl.create_default_context()
        self.client = MediaWorkerClient(
            base_url="https://127.0.0.1:9",
            ssl_context=context,
            max_attempts=1,
            circuit_failure_threshold=2,
            circuit_recovery_seconds=10,
            _clock=lambda: self.clock_value,
            _sleep=lambda _: None,
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_protocol_failures_open_and_success_closes_circuit(self) -> None:
        calls = 0

        def fail(
            request: _JobRequest,
            source: Path,
            destination: Path,
            deadline: float,
        ) -> PreparedMedia:
            nonlocal calls
            del request, source, destination, deadline
            calls += 1
            raise MediaWorkerProtocolError("malformed_response")

        self.client._prepare_once = fail  # type: ignore[method-assign]
        for _ in range(2):
            with self.assertRaises(MediaWorkerProtocolError):
                self.client.prepare(
                    "voice",
                    "5" * 32,
                    self.source,
                    1,
                    self.destination,
                )
        self.assertEqual(self.client.circuit_state, "OPEN")
        with self.assertRaisesRegex(MediaWorkerUnavailable, "circuit_open"):
            self.client.prepare(
                "voice",
                "5" * 32,
                self.source,
                1,
                self.destination,
            )
        self.assertEqual(calls, 2)

        self.clock_value = 11

        def succeed(
            request: _JobRequest,
            source: Path,
            destination: Path,
            deadline: float,
        ) -> PreparedMedia:
            del source, destination, deadline
            return PreparedMedia(request.kind, request.duration_seconds, ())

        self.client._prepare_once = succeed  # type: ignore[method-assign]
        prepared = self.client.prepare(
            "voice",
            "5" * 32,
            self.source,
            1,
            self.destination,
        )
        self.assertEqual(prepared.kind, "voice")
        self.assertEqual(self.client.circuit_state, "CLOSED")

    def test_rejects_symlink_destination_before_network(self) -> None:
        real = self.root / "real"
        real.mkdir(mode=0o700)
        link = self.root / "link"
        link.symlink_to(real, target_is_directory=True)

        with self.assertRaisesRegex(MediaProcessingError, "unsafe_storage"):
            self.client.prepare(
                "voice",
                "6" * 32,
                self.source,
                1,
                link,
            )

    def test_rejects_existing_symlink_artifact(self) -> None:
        digest = hashlib.sha256(b"ID3safe").hexdigest()
        request = _JobRequest(
            job_id="7" * 64,
            kind="voice",
            media_key="8" * 32,
            duration_seconds=1,
            source_length=1,
            source_sha256="9" * 64,
        )
        result = self.destination / f"worker-{request.job_id}"
        result.mkdir(mode=0o700)
        target = self.root / "outside.mp3"
        target.write_bytes(b"ID3safe")
        os.chmod(target, 0o600)
        (result / "audio.mp3").symlink_to(target)

        with self.assertRaises(MediaWorkerProtocolError):
            self.client._prepared_from_existing(
                request,
                (_Artifact("audio.mp3", "audio/mpeg", 7, digest),),
                result,
            )

    def test_retries_share_one_processing_deadline(self) -> None:
        self.clock_value = 0.0
        client = MediaWorkerClient(
            base_url="https://127.0.0.1:9",
            ssl_context=ssl.create_default_context(),
            request_timeout_seconds=1,
            processing_timeout_seconds=5,
            max_attempts=2,
            retry_backoff_seconds=1,
            _clock=lambda: self.clock_value,
            _sleep=lambda value: setattr(
                self,
                "clock_value",
                self.clock_value + value,
            ),
        )
        deadlines: list[float] = []

        def fail(
            request: _JobRequest,
            source: Path,
            destination: Path,
            deadline: float,
        ) -> PreparedMedia:
            del request, source, destination
            deadlines.append(deadline)
            self.clock_value = deadline
            raise MediaWorkerUnavailable("worker_unavailable")

        client._prepare_once = fail  # type: ignore[method-assign]
        with self.assertRaisesRegex(
            MediaWorkerUnavailable,
            "processing_timeout",
        ):
            client.prepare(
                "voice",
                "5" * 32,
                self.source,
                1,
                self.destination,
            )
        self.assertEqual(deadlines, [5.0])

    def test_unexpected_half_open_failure_reopens_circuit(self) -> None:
        self.clock_value = 11.0
        self.client._circuit_state = "OPEN"
        self.client._circuit_opened_at = 0.0

        def crash(
            request: _JobRequest,
            source: Path,
            destination: Path,
            deadline: float,
        ) -> PreparedMedia:
            del request, source, destination, deadline
            raise RuntimeError("synthetic unexpected failure")

        self.client._prepare_once = crash  # type: ignore[method-assign]
        with self.assertRaisesRegex(RuntimeError, "synthetic"):
            self.client.prepare(
                "voice",
                "5" * 32,
                self.source,
                1,
                self.destination,
            )
        self.assertEqual(self.client.circuit_state, "OPEN")
        self.assertFalse(self.client._half_open_in_flight)

    def test_response_trickle_cannot_extend_absolute_deadline(self) -> None:
        class FakeSocket:
            def settimeout(self, value: float) -> None:
                del value

        class FakeConnection:
            sock = FakeSocket()

        class TrickleResponse:
            def getheader(self, name: str) -> str | None:
                return "3" if name == "Content-Length" else None

            def read1(self, length: int) -> bytes:
                del length
                self_clock[0] += 2.0
                return b"x"

        self_clock = [0.0]
        client = MediaWorkerClient(
            base_url="https://127.0.0.1:9",
            ssl_context=ssl.create_default_context(),
            request_timeout_seconds=5,
            processing_timeout_seconds=5,
            _clock=lambda: self_clock[0],
        )
        with self.assertRaisesRegex(
            MediaWorkerUnavailable,
            "processing_timeout",
        ):
            client._read_response(
                TrickleResponse(),  # type: ignore[arg-type]
                maximum=10,
                connection=FakeConnection(),  # type: ignore[arg-type]
                deadline=5.0,
            )

    def test_truncated_response_is_transport_failure(self) -> None:
        class FakeSocket:
            def settimeout(self, value: float) -> None:
                del value

        class FakeConnection:
            sock = FakeSocket()

        class TruncatedResponse:
            chunks = iter((b"x", b""))

            def getheader(self, name: str) -> str | None:
                return "2" if name == "Content-Length" else None

            def read1(self, length: int) -> bytes:
                del length
                return next(self.chunks)

        client = MediaWorkerClient(
            base_url="https://127.0.0.1:9",
            ssl_context=ssl.create_default_context(),
        )
        with self.assertRaisesRegex(
            MediaWorkerUnavailable,
            "worker_unavailable",
        ):
            client._read_response(
                TruncatedResponse(),  # type: ignore[arg-type]
                maximum=10,
                connection=FakeConnection(),  # type: ignore[arg-type]
                deadline=time.monotonic() + 5,
            )

    def test_worker_capability_limit_is_fallback_eligible(self) -> None:
        with self.assertRaisesRegex(
            MediaWorkerUnavailable,
            "unsupported_media",
        ):
            self.client.prepare(
                "voice",
                "5" * 32,
                self.source,
                60 * 60 + 1,
                self.destination,
            )

    def test_dns_resolution_cannot_extend_the_processing_deadline(self) -> None:
        client = MediaWorkerClient(
            base_url="https://codex-media-worker.invalid:9443",
            ssl_context=ssl.create_default_context(),
            request_timeout_seconds=1,
            processing_timeout_seconds=1,
            max_attempts=1,
        )

        def stalled_resolution(*args: object, **kwargs: object) -> object:
            del args, kwargs
            time.sleep(0.5)
            return []

        started = time.monotonic()
        with mock.patch(
            "codex_telegram_bridge.media_worker.socket.getaddrinfo",
            side_effect=stalled_resolution,
        ):
            with self.assertRaisesRegex(
                MediaWorkerUnavailable,
                "processing_timeout",
            ):
                client._connection(time.monotonic() + 0.1)
        self.assertLess(time.monotonic() - started, 0.35)

    def test_deadline_timer_closes_connection_during_header_wait(self) -> None:
        closed = threading.Event()

        class FakeSocket:
            def shutdown(self, how: int) -> None:
                del how
                closed.set()

            def close(self) -> None:
                closed.set()

        class FakeConnection:
            sock = FakeSocket()

            def close(self) -> None:
                closed.set()

        client = MediaWorkerClient(
            base_url="https://127.0.0.1:9",
            ssl_context=ssl.create_default_context(),
            _clock=time.monotonic,
        )
        timer = client._deadline_timer(  # type: ignore[arg-type]
            FakeConnection(),
            time.monotonic() + 0.05,
        )
        try:
            self.assertTrue(closed.wait(1))
        finally:
            timer.cancel()


@unittest.skipUnless(OPENSSL, "OpenSSL is required for local mTLS tests")
class MutualTLSIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.certificate_temp = tempfile.TemporaryDirectory()
        cls.certificates = Path(cls.certificate_temp.name)
        cls._create_certificates()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.certificate_temp.cleanup()

    @classmethod
    def _create_certificates(cls) -> None:
        def run(*arguments: str) -> None:
            subprocess.run(
                [str(OPENSSL), *arguments],
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )

        root = cls.certificates
        run(
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "2",
            "-subj",
            "/CN=Media Worker Test CA",
            "-addext",
            "basicConstraints=critical,CA:TRUE",
            "-addext",
            "keyUsage=critical,keyCertSign,cRLSign",
            "-keyout",
            str(root / "ca.key"),
            "-out",
            str(root / "ca.crt"),
        )
        for name, common_name, usage in (
            ("server", "localhost", "serverAuth"),
            ("client", "test-client", "clientAuth"),
        ):
            run(
                "req",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-subj",
                f"/CN={common_name}",
                "-keyout",
                str(root / f"{name}.key"),
                "-out",
                str(root / f"{name}.csr"),
            )
            extension = root / f"{name}.ext"
            extension.write_text(
                (
                    "subjectAltName=DNS:localhost,IP:127.0.0.1\n"
                    if name == "server"
                    else ""
                )
                + f"extendedKeyUsage={usage}\n",
                encoding="utf-8",
            )
            run(
                "x509",
                "-req",
                "-days",
                "2",
                "-in",
                str(root / f"{name}.csr"),
                "-CA",
                str(root / "ca.crt"),
                "-CAkey",
                str(root / "ca.key"),
                "-CAcreateserial",
                "-extfile",
                str(extension),
                "-out",
                str(root / f"{name}.crt"),
            )

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.destination = self.root / "media"
        self.destination.mkdir(mode=0o700)
        os.chmod(self.destination, 0o700)
        self.source = _make_source(self.destination)
        self.servers: list[
            tuple[MediaWorkerServer, threading.Thread, _BlockingProcessor | None]
        ] = []

    def tearDown(self) -> None:
        for server, thread, blocking in self.servers:
            if blocking is not None:
                blocking.release.set()
            server.shutdown()
            thread.join(timeout=10)
            self.assertFalse(thread.is_alive())
        self.temporary.cleanup()

    def _server_context(self) -> ssl.SSLContext:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(
            self.certificates / "server.crt",
            self.certificates / "server.key",
        )
        context.load_verify_locations(self.certificates / "ca.crt")
        context.verify_mode = ssl.CERT_REQUIRED
        return context

    def _client_context(self, *, with_certificate: bool = True) -> ssl.SSLContext:
        context = ssl.create_default_context(
            ssl.Purpose.SERVER_AUTH,
            cafile=self.certificates / "ca.crt",
        )
        if with_certificate:
            context.load_cert_chain(
                self.certificates / "client.crt",
                self.certificates / "client.key",
            )
        return context

    def _start_server(
        self,
        processor: _FakeProcessor,
        *,
        queue_capacity: int = 2,
        ttl_seconds: int = 3600,
        request_timeout_seconds: float = 3,
        shutdown_timeout_seconds: float = 3,
        spool_directory: Path | None = None,
    ) -> MediaWorkerServer:
        server = MediaWorkerServer(
            host="127.0.0.1",
            port=0,
            ssl_context=self._server_context(),
            spool_directory=(
                spool_directory
                or self.root / f"spool-{len(self.servers)}"
            ),
            processor=processor,
            queue_capacity=queue_capacity,
            ttl_seconds=ttl_seconds,
            request_timeout_seconds=request_timeout_seconds,
            shutdown_timeout_seconds=shutdown_timeout_seconds,
        )
        failures: list[BaseException] = []
        server._test_failures = failures  # type: ignore[attr-defined]

        def serve() -> None:
            try:
                server.serve_forever()
            except BaseException as error:
                failures.append(error)

        thread = threading.Thread(target=serve, daemon=True)
        thread.start()
        self.assertTrue(server._serving_event.wait(3))
        self.servers.append(
            (
                server,
                thread,
                processor if isinstance(processor, _BlockingProcessor) else None,
            )
        )
        return server

    def _client(
        self,
        server: MediaWorkerServer,
        *,
        with_certificate: bool = True,
        max_attempts: int = 1,
    ) -> MediaWorkerClient:
        _, port = server.server_address
        return MediaWorkerClient(
            base_url=f"https://127.0.0.1:{port}",
            ssl_context=self._client_context(
                with_certificate=with_certificate,
            ),
            request_timeout_seconds=3,
            processing_timeout_seconds=5,
            poll_interval_seconds=0.02,
            retry_backoff_seconds=0,
            max_attempts=max_attempts,
        )

    def _authenticated_socket(
        self,
        server: MediaWorkerServer,
    ) -> ssl.SSLSocket:
        host, port = server.server_address
        raw = socket.create_connection((host, port), timeout=3)
        return self._client_context().wrap_socket(
            raw,
            server_hostname="localhost",
        )

    @staticmethod
    def _job_headers(
        *,
        job_id: str,
        media_key: str,
        length: int,
        digest: str,
    ) -> bytes:
        return (
            f"POST /v1/jobs/{job_id} HTTP/1.0\r\n"
            "Host: localhost\r\n"
            "Content-Type: application/octet-stream\r\n"
            f"Content-Length: {length}\r\n"
            "X-Media-Worker-Version: 1\r\n"
            "X-Media-Kind: voice\r\n"
            f"X-Media-Key: {media_key}\r\n"
            "X-Media-Duration: 1\r\n"
            f"X-Content-SHA256: {digest}\r\n"
            "Connection: close\r\n\r\n"
        ).encode("ascii")

    def test_mtls_prepare_is_atomic_owner_only_and_idempotent(self) -> None:
        processor = _FakeProcessor()
        server = self._start_server(processor)
        client = self._client(server)

        first = client.prepare(
            "voice",
            "a" * 32,
            self.source,
            2,
            self.destination,
        )
        second = client.prepare(
            "voice",
            "a" * 32,
            self.source,
            2,
            self.destination,
        )

        self.assertEqual(first, second)
        self.assertEqual(processor.calls, 1)
        output = Path(first.inputs[0].path)
        self.assertTrue(output.is_file())
        self.assertEqual(output.stat().st_mode & 0o777, 0o600)
        self.assertEqual(output.parent.stat().st_mode & 0o777, 0o700)
        output.relative_to(self.destination.resolve())
        self.assertFalse(
            any(
                item.name.startswith(".worker-")
                for item in self.destination.iterdir()
            )
        )

    def test_server_requires_a_client_certificate(self) -> None:
        server = self._start_server(_FakeProcessor())
        client = self._client(server, with_certificate=False)

        with self.assertRaises(MediaWorkerUnavailable):
            client.prepare(
                "voice",
                "b" * 32,
                self.source,
                1,
                self.destination,
            )

    def test_client_deadline_aborts_a_slow_response_header(self) -> None:
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        server_context.load_cert_chain(
            self.certificates / "server.crt",
            self.certificates / "server.key",
        )
        finished = threading.Event()

        def drip() -> None:
            try:
                raw, _ = listener.accept()
                with server_context.wrap_socket(
                    raw,
                    server_side=True,
                ) as connection:
                    request = bytearray()
                    while b"\r\n\r\n" not in request:
                        chunk = connection.recv(4096)
                        if not chunk:
                            return
                        request.extend(chunk)
                    for byte in b"HTTP/1.0 200 OK\r\nContent-Length: 2\r\n":
                        connection.sendall(bytes((byte,)))
                        time.sleep(0.1)
            except (ConnectionError, OSError, ssl.SSLError):
                pass
            finally:
                listener.close()
                finished.set()

        thread = threading.Thread(target=drip, daemon=True)
        thread.start()
        _, port = listener.getsockname()
        client = MediaWorkerClient(
            base_url=f"https://127.0.0.1:{port}",
            ssl_context=self._client_context(),
            tls_server_name="localhost",
            request_timeout_seconds=1,
            processing_timeout_seconds=1,
            max_attempts=1,
        )
        started = time.monotonic()
        with self.assertRaisesRegex(
            MediaWorkerUnavailable,
            "processing_timeout|worker_unavailable",
        ):
            client.prepare(
                "voice",
                "b" * 32,
                self.source,
                1,
                self.destination,
            )
        self.assertLess(time.monotonic() - started, 1.8)
        self.assertTrue(finished.wait(2))

    def test_stalled_tls_handshake_is_bounded_before_authentication(self) -> None:
        processor = _FakeProcessor()
        server = self._start_server(
            processor,
            request_timeout_seconds=1,
        )
        host, port = server.server_address
        raw = socket.create_connection((host, port), timeout=2)
        raw.settimeout(3)
        started = time.monotonic()
        try:
            prepared = self._client(server).prepare(
                "voice",
                "0" * 32,
                self.source,
                1,
                self.destination,
            )
            self.assertEqual(prepared.kind, "voice")
            self.assertLess(time.monotonic() - started, 1.0)
            try:
                received = raw.recv(1)
            except (OSError, TimeoutError):
                received = b""
            self.assertEqual(received, b"")
        finally:
            raw.close()
        self.assertLess(time.monotonic() - started, 2.5)

    def test_authenticated_slow_body_has_one_wall_deadline(self) -> None:
        server = self._start_server(
            _FakeProcessor(),
            request_timeout_seconds=1,
        )
        body = b"abc"
        digest = hashlib.sha256(body).hexdigest()
        media_key = "9" * 32
        job_id = _canonical_job_id(
            kind="voice",
            media_key=media_key,
            duration_seconds=1,
            source_length=len(body),
            source_sha256=digest,
        )
        connection = self._authenticated_socket(server)
        connection.settimeout(3)
        started = time.monotonic()
        try:
            connection.sendall(
                self._job_headers(
                    job_id=job_id,
                    media_key=media_key,
                    length=len(body),
                    digest=digest,
                )
                + body[:1]
            )
            time.sleep(0.6)
            connection.sendall(body[1:2])
            time.sleep(0.6)
            try:
                received = connection.recv(1)
            except (OSError, TimeoutError):
                received = b""
            self.assertEqual(received, b"")
        finally:
            connection.close()
        self.assertLess(time.monotonic() - started, 2.0)

        prepared = self._client(server).prepare(
            "voice",
            "8" * 32,
            self.source,
            1,
            self.destination,
        )
        self.assertEqual(prepared.kind, "voice")

    def test_shutdown_closes_active_body_without_exceeding_budget(self) -> None:
        server = self._start_server(
            _FakeProcessor(),
            request_timeout_seconds=30,
            shutdown_timeout_seconds=1,
        )
        body = b"abc"
        digest = hashlib.sha256(body).hexdigest()
        media_key = "7" * 32
        job_id = _canonical_job_id(
            kind="voice",
            media_key=media_key,
            duration_seconds=1,
            source_length=len(body),
            source_sha256=digest,
        )
        connection = self._authenticated_socket(server)
        connection.sendall(
            self._job_headers(
                job_id=job_id,
                media_key=media_key,
                length=len(body),
                digest=digest,
            )
            + body[:1]
        )
        time.sleep(0.05)
        started = time.monotonic()
        server.shutdown()
        self.servers[-1][1].join(timeout=2)
        connection.close()
        self.assertLess(time.monotonic() - started, 1.5)
        self.assertFalse(self.servers[-1][1].is_alive())

    def test_busy_response_does_not_drain_an_untrusted_body(self) -> None:
        processor = _BlockingProcessor()
        server = self._start_server(processor, queue_capacity=0)
        first_client = self._client(server)
        first = threading.Thread(
            target=lambda: first_client.prepare(
                "voice",
                "6" * 32,
                self.source,
                1,
                self.destination,
            )
        )
        first.start()
        self.assertTrue(processor.started.wait(3))
        declared_length = 1024 * 1024
        digest = hashlib.sha256(b"not-sent").hexdigest()
        media_key = "5" * 32
        job_id = _canonical_job_id(
            kind="voice",
            media_key=media_key,
            duration_seconds=1,
            source_length=declared_length,
            source_sha256=digest,
        )
        connection = self._authenticated_socket(server)
        connection.settimeout(2)
        started = time.monotonic()
        try:
            connection.sendall(
                self._job_headers(
                    job_id=job_id,
                    media_key=media_key,
                    length=declared_length,
                    digest=digest,
                )
            )
            response = connection.recv(4096)
            self.assertIn(b" 429 ", response)
            self.assertLess(time.monotonic() - started, 1.0)
        finally:
            connection.close()
            processor.release.set()
            first.join(timeout=5)
        self.assertFalse(first.is_alive())

    def test_start_is_idempotent_and_worker_failure_stops_serving(self) -> None:
        running = self._start_server(_FakeProcessor())
        worker_threads = tuple(running._workers)
        running.start()
        self.assertEqual(tuple(running._workers), worker_threads)
        self.assertTrue(all(thread.is_alive() for thread in worker_threads))

        failed = MediaWorkerServer(
            host="127.0.0.1",
            port=0,
            ssl_context=self._server_context(),
            spool_directory=self.root / "spool-failed-worker",
            processor=_FakeProcessor(),
            shutdown_timeout_seconds=2,
        )

        def fail_loop() -> None:
            raise RuntimeError("synthetic worker failure")

        failed._worker_loop = fail_loop  # type: ignore[method-assign]
        failures: list[BaseException] = []

        def serve() -> None:
            try:
                failed.serve_forever()
            except BaseException as error:
                failures.append(error)

        thread = threading.Thread(target=serve, daemon=True)
        thread.start()
        thread.join(timeout=4)
        self.assertFalse(thread.is_alive())
        self.assertEqual(len(failures), 1)
        self.assertIsInstance(failures[0], RuntimeError)
        self.servers.append((failed, thread, None))

    def test_startup_removes_only_strict_stale_atomic_spool_directories(
        self,
    ) -> None:
        spool = self.root / "stale-spool"
        jobs = spool / "jobs"
        incoming = spool / "incoming"
        jobs.mkdir(parents=True, mode=0o700)
        incoming.mkdir(mode=0o700)
        stale = jobs / f".job-{'a' * 64}-{'b' * 32}.part"
        stale.mkdir(mode=0o700)
        (stale / "source.bin").write_bytes(b"partial")
        stale_upload = incoming / f".upload-{'c' * 32}.part"
        stale_upload.write_bytes(b"partial upload")
        os.chmod(stale_upload, 0o600)
        unrelated = jobs / ".job-not-a-protocol-job.part"
        unrelated.mkdir(mode=0o700)

        server = MediaWorkerServer(
            host="127.0.0.1",
            port=0,
            ssl_context=self._server_context(),
            spool_directory=spool,
            processor=_FakeProcessor(),
        )
        try:
            self.assertFalse(stale.exists())
            self.assertFalse(stale_upload.exists())
            self.assertTrue(unrelated.is_dir())
        finally:
            server.shutdown()

    def test_spool_lock_precedes_cleanup_and_bind(self) -> None:
        spool = self.root / "locked-spool"
        first = MediaWorkerServer(
            host="127.0.0.1",
            port=0,
            ssl_context=self._server_context(),
            spool_directory=spool,
            processor=_FakeProcessor(),
        )
        residue = (
            first.incoming_directory
            / f".upload-{'a' * 32}.part"
        )
        residue.write_bytes(b"live partial upload")
        residue.chmod(0o600)
        try:
            with self.assertRaisesRegex(
                MediaWorkerUnavailable,
                "spool_locked",
            ):
                MediaWorkerServer(
                    host="127.0.0.1",
                    port=0,
                    ssl_context=self._server_context(),
                    spool_directory=spool,
                    processor=_FakeProcessor(),
                )
            self.assertTrue(residue.is_file())
        finally:
            first.shutdown()

    def test_storage_quota_counts_inflight_reservations(self) -> None:
        server = MediaWorkerServer(
            host="127.0.0.1",
            port=0,
            ssl_context=self._server_context(),
            spool_directory=self.root / "reservation-spool",
            processor=_FakeProcessor(),
            processing_concurrency=1,
            queue_capacity=2,
            storage_limit_bytes=(
                20 * 1024 * 1024
                + MAX_OUTPUT_BYTES
                + 64 * 1024
            ),
        )

        def request(media_key: str, digest: str) -> _JobRequest:
            length = 20 * 1024 * 1024
            return _JobRequest(
                job_id=_canonical_job_id(
                    kind="voice",
                    media_key=media_key,
                    duration_seconds=1,
                    source_length=length,
                    source_sha256=digest,
                ),
                kind="voice",
                media_key=media_key,
                duration_seconds=1,
                source_length=length,
                source_sha256=digest,
            )

        first = request("a" * 32, "b" * 64)
        second = request("c" * 32, "d" * 64)
        try:
            self.assertIsNone(server._reserve_or_replay(first))
            partial = (
                server.incoming_directory
                / f".upload-{'e' * 32}.part"
            )
            partial.write_bytes(b"x")
            partial.chmod(0o600)
            with server._state_lock:
                server._cleanup_locked(required_bytes=0)
            with self.assertRaisesRegex(
                MediaWorkerUnavailable,
                "storage_full",
            ):
                server._reserve_or_replay(second)
        finally:
            server._release_reservation(first.job_id)
            server.shutdown()

    def test_duplicate_reservation_and_replay_skip_new_job_quota_cleanup(
        self,
    ) -> None:
        server = self._start_server(_FakeProcessor())
        body = self.source.read_bytes()
        digest = hashlib.sha256(body).hexdigest()
        media_key = "9" * 32
        request = _JobRequest(
            job_id=_canonical_job_id(
                kind="voice",
                media_key=media_key,
                duration_seconds=1,
                source_length=len(body),
                source_sha256=digest,
            ),
            kind="voice",
            media_key=media_key,
            duration_seconds=1,
            source_length=len(body),
            source_sha256=digest,
        )
        self.assertIsNone(server._reserve_or_replay(request))
        try:
            with mock.patch.object(
                server,
                "_cleanup_locked",
                side_effect=AssertionError("replay must not run admission cleanup"),
            ):
                with self.assertRaisesRegex(
                    MediaWorkerBusy,
                    "duplicate_in_progress",
                ):
                    server._reserve_or_replay(request)
        finally:
            server._release_reservation(request.job_id)

        self._client(server).prepare(
            "voice",
            media_key,
            self.source,
            1,
            self.destination,
        )
        with mock.patch.object(
            server,
            "_cleanup_locked",
            side_effect=AssertionError("replay must not run admission cleanup"),
        ):
            replay = server._reserve_or_replay(request)
        self.assertIsNotNone(replay)
        self.assertEqual(replay["state"], "complete")

    def test_infrastructure_failure_is_retried_after_service_restart(
        self,
    ) -> None:
        spool = self.root / "restart-spool"
        first = self._start_server(
            _FailingProcessor("ffmpeg_unavailable"),
            spool_directory=spool,
        )
        with self.assertRaises(MediaWorkerUnavailable):
            self._client(first).prepare(
                "voice",
                "4" * 32,
                self.source,
                1,
                self.destination,
            )
        first_thread = self.servers[-1][1]
        first_thread.join(timeout=5)
        self.assertFalse(first_thread.is_alive())
        job_directories = [
            child
            for child in (spool / "jobs").iterdir()
            if len(child.name) == 64
        ]
        self.assertEqual(len(job_directories), 1)
        status = (
            job_directories[0] / "status.json"
        ).read_text(encoding="utf-8")
        self.assertIn('"state":"running"', status)
        self.assertNotIn('"state":"failed"', status)

        recovered_processor = _FakeProcessor()
        recovered = self._start_server(
            recovered_processor,
            spool_directory=spool,
        )
        prepared = self._client(recovered).prepare(
            "voice",
            "4" * 32,
            self.source,
            1,
            self.destination,
        )
        self.assertEqual(prepared.kind, "voice")
        self.assertEqual(recovered_processor.calls, 1)

    def test_transient_job_read_stops_service_without_dropping_job(
        self,
    ) -> None:
        spool = self.root / "read-restart-spool"
        first = self._start_server(
            _FakeProcessor(),
            spool_directory=spool,
        )
        original_read = first._read_request
        failed = False

        def flaky_read(job_id: str) -> _JobRequest:
            nonlocal failed
            if (
                not failed
                and threading.current_thread().name.startswith(
                    "media-worker-"
                )
            ):
                failed = True
                raise OSError("synthetic transient read failure")
            return original_read(job_id)

        first._read_request = flaky_read  # type: ignore[method-assign]
        with self.assertRaises(MediaWorkerUnavailable):
            self._client(first).prepare(
                "voice",
                "0" * 32,
                self.source,
                1,
                self.destination,
            )
        self.servers[-1][1].join(timeout=5)
        self.assertFalse(self.servers[-1][1].is_alive())
        job = next((spool / "jobs").glob("[0-9a-f]" * 64))
        self.assertTrue(job.is_dir())

        second_processor = _FakeProcessor()
        second = self._start_server(
            second_processor,
            spool_directory=spool,
        )
        prepared = self._client(second).prepare(
            "voice",
            "0" * 32,
            self.source,
            1,
            self.destination,
        )
        self.assertEqual(prepared.kind, "voice")
        self.assertEqual(second_processor.calls, 1)

    def test_idle_housekeeping_prunes_expired_terminal_job(self) -> None:
        server = self._start_server(
            _FakeProcessor(),
            ttl_seconds=1,
        )
        client = self._client(server)
        client.prepare(
            "voice",
            "3" * 32,
            self.source,
            1,
            self.destination,
        )
        jobs = list(server.jobs_directory.glob("[0-9a-f]" * 64))
        self.assertEqual(len(jobs), 1)
        deadline = time.monotonic() + 3
        while jobs[0].exists() and time.monotonic() < deadline:
            time.sleep(0.05)
        self.assertFalse(jobs[0].exists())

    def test_startup_prunes_expired_terminal_job(self) -> None:
        spool = self.root / "startup-ttl-spool"
        first = self._start_server(
            _FakeProcessor(),
            ttl_seconds=3600,
            spool_directory=spool,
        )
        self._client(first).prepare(
            "voice",
            "2" * 32,
            self.source,
            1,
            self.destination,
        )
        job = next((spool / "jobs").glob("[0-9a-f]" * 64))
        first.shutdown()
        self.servers[-1][1].join(timeout=5)
        old = time.time() - 10
        os.utime(job, (old, old))

        second = MediaWorkerServer(
            host="127.0.0.1",
            port=0,
            ssl_context=self._server_context(),
            spool_directory=spool,
            processor=_FakeProcessor(),
            ttl_seconds=1,
        )
        try:
            self.assertFalse(job.exists())
        finally:
            second.shutdown()

    def test_ipv6_listener_is_reachable_when_available(self) -> None:
        if not socket.has_ipv6:
            self.skipTest("IPv6 is unavailable")
        try:
            server = MediaWorkerServer(
                host="::1",
                port=0,
                ssl_context=self._server_context(),
                spool_directory=self.root / "ipv6-spool",
                processor=_FakeProcessor(),
                request_timeout_seconds=3,
            )
        except OSError:
            self.skipTest("IPv6 loopback cannot be bound")
        failures: list[BaseException] = []

        def serve() -> None:
            try:
                server.serve_forever()
            except BaseException as error:
                failures.append(error)

        thread = threading.Thread(target=serve, daemon=True)
        thread.start()
        self.assertTrue(server._serving_event.wait(3))
        self.servers.append((server, thread, None))
        _, port = server.server_address
        client = MediaWorkerClient(
            base_url=f"https://[::1]:{port}",
            ssl_context=self._client_context(),
            tls_server_name="localhost",
            request_timeout_seconds=3,
            processing_timeout_seconds=5,
            poll_interval_seconds=0.02,
            max_attempts=1,
        )
        prepared = client.prepare(
            "voice",
            "1" * 32,
            self.source,
            1,
            self.destination,
        )
        self.assertEqual(prepared.kind, "voice")
        self.assertEqual(failures, [])

    def test_infrastructure_failure_is_unavailable_but_bad_media_is_terminal(
        self,
    ) -> None:
        infrastructure = self._start_server(
            _FailingProcessor("ffmpeg_unavailable")
        )
        with self.assertRaisesRegex(
            MediaWorkerUnavailable,
            "worker_unavailable",
        ):
            self._client(infrastructure).prepare(
                "voice",
                "c" * 32,
                self.source,
                1,
                self.destination,
            )

        terminal = self._start_server(_FailingProcessor("invalid_audio"))
        with self.assertRaisesRegex(MediaProcessingError, "invalid_audio"):
            self._client(terminal).prepare(
                "voice",
                "d" * 32,
                self.source,
                1,
                self.destination,
            )

    def test_bounded_queue_returns_busy(self) -> None:
        processor = _BlockingProcessor()
        server = self._start_server(processor, queue_capacity=0)
        first_client = self._client(server)
        first_result: list[PreparedMedia | BaseException] = []

        def first_request() -> None:
            try:
                first_result.append(
                    first_client.prepare(
                        "voice",
                        "e" * 32,
                        self.source,
                        1,
                        self.destination,
                    )
                )
            except BaseException as error:
                first_result.append(error)

        thread = threading.Thread(target=first_request)
        thread.start()
        self.assertTrue(processor.started.wait(3))
        second_source = _make_source(self.destination, "second.bin")
        try:
            with self.assertRaises(MediaWorkerBusy):
                self._client(server).prepare(
                    "voice",
                    "f" * 32,
                    second_source,
                    1,
                    self.destination,
                )
        finally:
            processor.release.set()
            thread.join(timeout=10)
        self.assertEqual(len(first_result), 1)
        self.assertIsInstance(first_result[0], PreparedMedia)

    def test_conflicting_replay_is_409_and_bad_job_binding_is_rejected(
        self,
    ) -> None:
        server = self._start_server(_FakeProcessor())
        body = b"same-body"
        digest = hashlib.sha256(body).hexdigest()
        media_key = "1" * 32
        job_id = _canonical_job_id(
            kind="voice",
            media_key=media_key,
            duration_seconds=1,
            source_length=len(body),
            source_sha256=digest,
        )
        first_status = self._raw_post(
            server,
            job_id=job_id,
            body=body,
            media_key=media_key,
            duration=1,
        )
        conflict_status = self._raw_post(
            server,
            job_id=job_id,
            body=body,
            media_key=media_key,
            duration=2,
        )
        bad_binding_status = self._raw_post(
            server,
            job_id="0" * 64,
            body=body,
            media_key="2" * 32,
            duration=1,
        )

        self.assertEqual(first_status, 202)
        self.assertEqual(conflict_status, 409)
        self.assertEqual(bad_binding_status, 500)

    def test_unsafe_processor_output_never_escapes_spool(self) -> None:
        server = self._start_server(_UnsafeProcessor())
        with self.assertRaisesRegex(
            MediaWorkerUnavailable,
            "worker_unavailable",
        ):
            self._client(server).prepare(
                "voice",
                "3" * 32,
                self.source,
                1,
                self.destination,
            )
        self.assertFalse(any(self.destination.glob("worker-*")))

    def _raw_post(
        self,
        server: MediaWorkerServer,
        *,
        job_id: str,
        body: bytes,
        media_key: str,
        duration: int,
    ) -> int:
        _, port = server.server_address
        connection = http.client.HTTPSConnection(
            "127.0.0.1",
            port,
            timeout=3,
            context=self._client_context(),
        )
        try:
            connection.request(
                "POST",
                f"/v1/jobs/{job_id}",
                body=body,
                headers={
                    "Content-Type": "application/octet-stream",
                    "Content-Length": str(len(body)),
                    "X-Media-Worker-Version": "1",
                    "X-Media-Kind": "voice",
                    "X-Media-Key": media_key,
                    "X-Media-Duration": str(duration),
                    "X-Content-SHA256": hashlib.sha256(body).hexdigest(),
                    "Connection": "close",
                },
            )
            response = connection.getresponse()
            response.read()
            return response.status
        finally:
            connection.close()
