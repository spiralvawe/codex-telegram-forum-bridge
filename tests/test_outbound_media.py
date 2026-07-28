from __future__ import annotations

import base64
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from codex_telegram_bridge.outbound_media import (  # noqa: E402
    OutboundMediaError,
    OutboundMediaResolver,
)


class OutboundMediaResolverTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name) / "media"
        self.resolver = OutboundMediaResolver(
            root=self.root,
            max_bytes=1024 * 1024,
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_data_image_is_materialized_privately_and_reused(self) -> None:
        payload = b"\x89PNG\r\n\x1a\nvisible-image"
        data_url = (
            "data:image/png;base64,"
            + base64.b64encode(payload).decode("ascii")
        )

        first = self.resolver.resolve_user_input(
            {"type": "image", "url": data_url},
            index=1,
        )
        second = self.resolver.resolve_user_input(
            {"type": "image", "url": data_url},
            index=1,
        )

        self.assertIsNotNone(first)
        self.assertEqual(first, second)
        self.assertEqual(first.media_kind, "photo")
        self.assertEqual(first.path.read_bytes(), payload)
        self.assertEqual(first.path.stat().st_mode & 0o777, 0o600)
        self.assertEqual(first.path.parent.stat().st_mode & 0o777, 0o700)

    def test_local_video_mention_is_classified_as_video(self) -> None:
        video = Path(self.temporary.name) / "clip.mp4"
        video.write_bytes(b"video")

        attachment = self.resolver.resolve_user_input(
            {"type": "mention", "name": "clip.mp4", "path": str(video)},
            index=2,
        )

        self.assertIsNotNone(attachment)
        self.assertEqual(attachment.media_kind, "video")
        self.assertEqual(attachment.display_name, "clip.mp4")
        self.assertEqual(attachment.path, video.resolve())

    def test_secret_shaped_local_file_is_rejected(self) -> None:
        secret = Path(self.temporary.name) / "api-token.txt"
        secret.write_text("secret", encoding="utf-8")

        with self.assertRaisesRegex(OutboundMediaError, "unsafe_attachment"):
            self.resolver.resolve_path(secret)

    def test_credential_remote_url_is_rejected(self) -> None:
        with self.assertRaisesRegex(OutboundMediaError, "credential_url"):
            self.resolver.resolve_locator(
                "https://example.com/image.jpg?access_token=secret",
                display_name="image.jpg",
                preferred_kind="photo",
            )

    def test_public_remote_image_stays_remote(self) -> None:
        attachment = self.resolver.resolve_locator(
            "https://example.com/media/image.png",
            display_name="image.png",
            preferred_kind="photo",
        )

        self.assertEqual(attachment.media_kind, "photo")
        self.assertEqual(
            attachment.url,
            "https://example.com/media/image.png",
        )
        self.assertIsNone(attachment.path)

    def test_image_generation_uses_saved_path(self) -> None:
        image = Path(self.temporary.name) / "generated.webp"
        image.write_bytes(b"generated")

        attachments = self.resolver.resolve_thread_item(
            {
                "type": "imageGeneration",
                "savedPath": str(image),
                "result": "ignored",
            }
        )

        self.assertEqual(len(attachments), 1)
        self.assertEqual(attachments[0].media_kind, "photo")
        self.assertEqual(attachments[0].path, image.resolve())

    def test_oversized_data_url_is_rejected_before_write(self) -> None:
        resolver = OutboundMediaResolver(root=self.root, max_bytes=3)
        data_url = (
            "data:audio/mpeg;base64,"
            + base64.b64encode(b"too-large").decode("ascii")
        )

        with self.assertRaisesRegex(
            OutboundMediaError,
            "attachment_too_large",
        ):
            resolver.resolve_user_input(
                {"type": "audio", "url": data_url},
                index=1,
            )


if __name__ == "__main__":
    unittest.main()
