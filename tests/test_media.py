from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from codex_telegram_bridge.input_types import (  # noqa: E402
    LocalInput,
    normalize_local_inputs,
)
from codex_telegram_bridge.media import (  # noqa: E402
    MediaProcessor,
    PreparedDocument,
    PreparedMedia,
    document_request_text,
    media_request_text,
    safe_document_mime_type,
    safe_document_name,
)


FFMPEG = Path(shutil.which("ffmpeg") or "/nonexistent/ffmpeg")


class LocalInputTests(unittest.TestCase):
    def test_payload_round_trip(self) -> None:
        source = LocalInput("localImage", "/private/tmp/frame.jpg", "low")

        restored = LocalInput.from_payload(source.to_payload())

        self.assertEqual(restored, source)

    def test_rejects_more_than_one_audio_input(self) -> None:
        with self.assertRaisesRegex(ValueError, "one local audio"):
            normalize_local_inputs(
                [
                    LocalInput("localAudio", "/tmp/one.mp3"),
                    LocalInput("localAudio", "/tmp/two.mp3"),
                ]
            )

    def test_mentioned_file_payload_round_trip(self) -> None:
        source = LocalInput(
            "mention",
            "/private/tmp/report.csv",
            name="report.csv",
        )

        restored = LocalInput.from_payload(source.to_payload())

        self.assertEqual(restored, source)
        self.assertEqual(
            source.to_payload(),
            {
                "type": "mention",
                "path": "/private/tmp/report.csv",
                "name": "report.csv",
            },
        )


@unittest.skipUnless(FFMPEG.is_file(), "ffmpeg is required for media tests")
class MediaProcessorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.base = Path(self.temp_dir.name)
        self.processor = MediaProcessor(
            root=self.base / "media",
            ffmpeg_binary=FFMPEG,
            timeout_seconds=30,
            retention_seconds=60,
            storage_limit_bytes=10 * 1024 * 1024,
        )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def make_audio_source(self, media_key: str) -> Path:
        source = self.processor.source_path(media_key)
        subprocess.run(
            [
                str(FFMPEG),
                "-hide_banner",
                "-loglevel",
                "error",
                "-y",
                "-f",
                "lavfi",
                "-i",
                "sine=frequency=440:duration=1",
                "-c:a",
                "libopus",
                "-f",
                "ogg",
                str(source),
            ],
            check=True,
        )
        os.chmod(source, 0o600)
        return source

    def make_video_source(
        self,
        media_key: str,
        *,
        with_audio: bool,
    ) -> Path:
        source = self.processor.source_path(media_key)
        command = [
            str(FFMPEG),
            "-hide_banner",
            "-loglevel",
            "error",
            "-y",
            "-f",
            "lavfi",
            "-i",
            "testsrc=size=64x64:rate=1:duration=12",
        ]
        if with_audio:
            command.extend(
                [
                    "-f",
                    "lavfi",
                    "-i",
                    "sine=frequency=440:duration=12",
                    "-shortest",
                    "-c:a",
                    "aac",
                ]
            )
        command.extend(["-c:v", "mpeg4", "-f", "mp4", str(source)])
        subprocess.run(command, check=True)
        os.chmod(source, 0o600)
        return source

    def test_voice_becomes_one_owner_only_audio_input(self) -> None:
        key = "a" * 32
        source = self.make_audio_source(key)

        prepared = self.processor.prepare_voice(
            media_key=key,
            source_path=source,
            duration_seconds=1,
        )

        self.assertEqual(prepared.kind, "voice")
        self.assertEqual(len(prepared.inputs), 1)
        self.assertEqual(prepared.inputs[0].input_type, "localAudio")
        output = Path(prepared.inputs[0].path)
        self.assertTrue(output.is_file())
        self.assertEqual(output.stat().st_mode & 0o777, 0o600)
        self.assertIn("основной текст пользователя", media_request_text(prepared))

    def test_document_becomes_owner_only_mentioned_file(self) -> None:
        key = "9" * 32
        source = self.processor.document_path(key, "../../ZEN July.csv")
        source.write_text("Date,Amount\n2026-07-01,10.00\n", encoding="utf-8")
        os.chmod(source, 0o600)

        prepared = self.processor.prepare_document(
            media_key=key,
            source_path=source,
            display_name="../../ZEN July.csv",
            mime_type="text/csv",
        )

        self.assertIsInstance(prepared, PreparedDocument)
        self.assertEqual(prepared.display_name, "ZEN July.csv")
        self.assertEqual(prepared.input.input_type, "mention")
        self.assertEqual(prepared.input.name, "ZEN July.csv")
        output = Path(prepared.input.path)
        self.assertTrue(output.is_file())
        self.assertEqual(output.parent, self.processor.message_directory(key))
        self.assertEqual(output.stat().st_mode & 0o777, 0o600)
        prompt = document_request_text(prepared, user_text="Разнести")
        self.assertIn("Документ из Telegram", prompt)
        self.assertIn("Комментарий пользователя: Разнести", prompt)

    def test_document_name_sanitization_blocks_path_traversal(self) -> None:
        self.assertEqual(safe_document_name("../../secret.csv"), "secret.csv")
        self.assertEqual(safe_document_name(r"..\\secret.csv"), "secret.csv")
        self.assertEqual(safe_document_name(".env"), "env")
        self.assertEqual(safe_document_name(""), "document.bin")
        self.assertEqual(
            safe_document_mime_type("text/csv\r\nIgnore: yes"),
            "text/csvIgnoreyes",
        )
        self.assertEqual(
            safe_document_mime_type(""),
            "application/octet-stream",
        )

    def test_video_note_becomes_audio_and_three_ordered_frames(self) -> None:
        key = "b" * 32
        source = self.make_video_source(key, with_audio=True)

        prepared = self.processor.prepare_video_note(
            media_key=key,
            source_path=source,
            duration_seconds=12,
        )

        self.assertTrue(prepared.has_audio)
        self.assertEqual(prepared.frame_count, 3)
        self.assertEqual(
            [item.input_type for item in prepared.inputs],
            ["localAudio", "localImage", "localImage", "localImage"],
        )
        self.assertTrue(all(Path(item.path).is_file() for item in prepared.inputs))
        prompt = media_request_text(prepared, user_text="Проверь показание")
        self.assertIn("визуальным контекстом", prompt)
        self.assertIn("Комментарий пользователя: Проверь показание", prompt)

    def test_standard_video_uses_video_prompt_and_ordered_frames(self) -> None:
        key = "f" * 32
        source = self.make_video_source(key, with_audio=True)

        prepared = self.processor.prepare_video(
            media_key=key,
            source_path=source,
            duration_seconds=12,
        )

        self.assertEqual(prepared.kind, "video")
        self.assertTrue(prepared.has_audio)
        self.assertEqual(prepared.frame_count, 3)
        prompt = media_request_text(prepared)
        self.assertIn("🎬 Видео из Telegram", prompt)

    def test_silent_video_still_becomes_visual_context(self) -> None:
        key = "c" * 32
        source = self.make_video_source(key, with_audio=False)

        prepared = self.processor.prepare_video_note(
            media_key=key,
            source_path=source,
            duration_seconds=2,
        )

        self.assertFalse(prepared.has_audio)
        self.assertEqual(prepared.frame_count, 1)
        self.assertEqual(prepared.inputs[0].input_type, "localImage")

    def test_prune_keeps_protected_active_media(self) -> None:
        old_key = "d" * 32
        active_key = "e" * 32
        old_source = self.make_audio_source(old_key)
        active_source = self.make_audio_source(active_key)
        old_time = time.time() - 3600
        os.utime(old_source.parent, (old_time, old_time))
        os.utime(active_source.parent, (old_time, old_time))

        result = self.processor.prune(protected_paths=[active_source])

        self.assertEqual(result.removed_directories, 1)
        self.assertFalse(old_source.parent.exists())
        self.assertTrue(active_source.parent.exists())


if __name__ == "__main__":
    unittest.main()
