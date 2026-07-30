from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from codex_telegram_bridge.input_types import LocalInput
from codex_telegram_bridge.media import MediaProcessingError, PreparedMedia
from codex_telegram_bridge.media_pipeline import HybridMediaProcessor


class HybridMediaProcessorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.media_directory = self.root / ("a" * 32)
        self.media_directory.mkdir(mode=0o700)
        self.source = self.media_directory / "source.bin"
        self.source.write_bytes(b"source")
        self.source.chmod(0o600)
        self.local = mock.Mock()
        self.local.message_directory.return_value = self.media_directory

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def make_audio_result(self) -> PreparedMedia:
        audio = self.media_directory / "audio.mp3"
        audio.write_bytes(b"ID3worker-result")
        audio.chmod(0o600)
        return PreparedMedia(
            kind="voice",
            duration_seconds=2,
            inputs=(LocalInput("localAudio", str(audio.resolve())),),
        )

    def make_nested_audio_result(self) -> PreparedMedia:
        output = self.media_directory / f"worker-{'b' * 64}"
        output.mkdir(mode=0o700)
        audio = output / "audio.mp3"
        audio.write_bytes(b"ID3worker-result")
        audio.chmod(0o600)
        return PreparedMedia(
            kind="voice",
            duration_seconds=2,
            inputs=(LocalInput("localAudio", str(audio.resolve())),),
        )

    def test_remote_success_never_invokes_local_ffmpeg(self) -> None:
        worker = mock.Mock()
        expected = self.make_audio_result()
        worker.prepare.return_value = expected
        pipeline = HybridMediaProcessor(local=self.local, worker=worker)

        result = pipeline.prepare_voice(
            media_key="a" * 32,
            source_path=self.source,
            duration_seconds=2,
        )

        self.assertEqual(result, expected)
        worker.prepare.assert_called_once_with(
            kind="voice",
            media_key="a" * 32,
            source_path=self.source,
            duration_seconds=2,
            destination_directory=self.media_directory,
        )
        self.local.prepare_voice.assert_not_called()

    def test_transport_failure_falls_back_to_local(self) -> None:
        worker = mock.Mock()
        worker.prepare.side_effect = ConnectionError("worker unavailable")
        expected = PreparedMedia(
            kind="voice",
            duration_seconds=2,
            inputs=(LocalInput("localAudio", str(self.source.resolve())),),
        )
        self.local.prepare_voice.return_value = expected
        pipeline = HybridMediaProcessor(local=self.local, worker=worker)

        result = pipeline.prepare_voice(
            media_key="a" * 32,
            source_path=self.source,
            duration_seconds=2,
        )

        self.assertEqual(result, expected)
        self.local.prepare_voice.assert_called_once()

    def test_atomic_nested_worker_output_is_accepted(self) -> None:
        worker = mock.Mock()
        expected = self.make_nested_audio_result()
        worker.prepare.return_value = expected
        pipeline = HybridMediaProcessor(local=self.local, worker=worker)

        result = pipeline.prepare_voice(
            media_key="a" * 32,
            source_path=self.source,
            duration_seconds=2,
        )

        self.assertEqual(result, expected)
        self.local.prepare_voice.assert_not_called()

    def test_terminal_media_error_does_not_repeat_work_on_pi(self) -> None:
        worker = mock.Mock()
        worker.prepare.side_effect = MediaProcessingError("invalid_video")
        pipeline = HybridMediaProcessor(local=self.local, worker=worker)

        with self.assertRaisesRegex(MediaProcessingError, "invalid_video"):
            pipeline.prepare_video(
                media_key="a" * 32,
                source_path=self.source,
                duration_seconds=2,
            )

        self.local.prepare_video.assert_not_called()

    def test_malformed_remote_result_is_rejected_before_local_input_use(
        self,
    ) -> None:
        outside = self.root / "outside.mp3"
        outside.write_bytes(b"private")
        os.chmod(outside, 0o600)
        worker = mock.Mock()
        worker.prepare.return_value = PreparedMedia(
            kind="voice",
            duration_seconds=2,
            inputs=(LocalInput("localAudio", str(outside.resolve())),),
        )
        expected = self.make_audio_result()
        self.local.prepare_voice.return_value = expected
        pipeline = HybridMediaProcessor(local=self.local, worker=worker)

        result = pipeline.prepare_voice(
            media_key="a" * 32,
            source_path=self.source,
            duration_seconds=2,
        )

        self.assertEqual(result, expected)
        self.local.prepare_voice.assert_called_once()

    def test_mismatched_remote_duration_falls_back_to_local(self) -> None:
        worker = mock.Mock()
        worker.prepare.return_value = PreparedMedia(
            kind="voice",
            duration_seconds=1,
            inputs=self.make_audio_result().inputs,
        )
        expected = self.make_audio_result()
        self.local.prepare_voice.return_value = expected
        pipeline = HybridMediaProcessor(local=self.local, worker=worker)

        result = pipeline.prepare_voice(
            media_key="a" * 32,
            source_path=self.source,
            duration_seconds=2,
        )

        self.assertEqual(result, expected)
        self.local.prepare_voice.assert_called_once()

    def test_disabled_worker_preserves_exact_local_path(self) -> None:
        expected = PreparedMedia(
            kind="video_note",
            duration_seconds=2,
            inputs=(LocalInput("localImage", str(self.source.resolve())),),
        )
        self.local.prepare_video_note.return_value = expected
        pipeline = HybridMediaProcessor(local=self.local, worker=None)

        result = pipeline.prepare_video_note(
            media_key="a" * 32,
            source_path=self.source,
            duration_seconds=2,
        )

        self.assertEqual(result, expected)
        self.local.prepare_video_note.assert_called_once_with(
            media_key="a" * 32,
            source_path=self.source,
            duration_seconds=2,
        )


if __name__ == "__main__":
    unittest.main()
