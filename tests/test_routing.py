from __future__ import annotations

import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from codex_telegram_bridge.service import (  # noqa: E402
    ThreadMode,
    approval_keyboard,
    clean_topic_title,
    parse_message_mode,
    redact_sensitive,
    strip_topic_mode_suffix,
    topic_title_with_mode,
)


class RoutingTests(unittest.TestCase):
    def test_plain_message(self) -> None:
        self.assertEqual(
            parse_message_mode("проверь датчик", "project_bridge_bot"),
            ("plain", "проверь датчик"),
        )

    def test_steer_command_with_bot_suffix(self) -> None:
        self.assertEqual(
            parse_message_mode(
                "/steer@project_bridge_bot проверь только логи",
                "project_bridge_bot",
            ),
            ("steer", "проверь только логи"),
        )

    def test_archive_command_with_bot_suffix(self) -> None:
        self.assertEqual(
            parse_message_mode(
                "/archive@project_bridge_bot", "project_bridge_bot"
            ),
            ("archive", ""),
        )

    def test_mode_command_with_bot_suffix(self) -> None:
        self.assertEqual(
            parse_message_mode(
                "/mode@project_bridge_bot", "project_bridge_bot"
            ),
            ("mode", ""),
        )

    def test_mention_means_steer(self) -> None:
        self.assertEqual(
            parse_message_mode(
                "не перезапускай @project_bridge_bot пока",
                "project_bridge_bot",
            ),
            ("steer", "не перезапускай  пока"),
        )

    def test_redacts_common_secret_shapes(self) -> None:
        source = (
            "TOKEN=secretvalue Authorization: Bearer abc.def "
            "https://user:pass@example.com/ 123456789:abcdefghijklmnopqrstuvwxyz_ABCD"
        )
        result = redact_sensitive(source)
        self.assertNotIn("secretvalue", result)
        self.assertNotIn("abc.def", result)
        self.assertNotIn("user:pass", result)
        self.assertNotIn("abcdefghijklmnopqrstuvwxyz_ABCD", result)

    def test_redacts_quoted_and_json_secret_values_completely(self) -> None:
        source = (
            'password="quoted value" '
            "api_key='single quoted value' "
            '{"token": "json value"} '
            '--password "command value" '
            "Authorization: Bearer abc/def+ghi=="
        )

        result = redact_sensitive(source)

        for secret in (
            "quoted value",
            "single quoted value",
            "json value",
            "command value",
            "abc/def+ghi==",
        ):
            self.assertNotIn(secret, result)
        self.assertEqual(result.count("[REDACTED]"), 5)

    def test_redacts_ha_and_infrastructure_credentials(self) -> None:
        source = "\n".join(
            [
                "Authorization: Basic dXNlcjpwYXNz",
                "OPENAI_API_KEY=sk-example",
                "AWS_SECRET_ACCESS_KEY=aws-example",
                'client_secret="oauth example"',
                "rtsp://camera:camera-pass@example.invalid/stream",
                "postgresql://db:db-pass@example.invalid/database",
                "-----BEGIN PRIVATE KEY-----",
                "private-material",
                "-----END PRIVATE KEY-----",
            ]
        )

        result = redact_sensitive(source)

        for secret in (
            "dXNlcjpwYXNz",
            "sk-example",
            "aws-example",
            "oauth example",
            "camera:camera-pass",
            "db:db-pass",
            "private-material",
        ):
            self.assertNotIn(secret, result)

    def test_topic_title_is_compact(self) -> None:
        title = clean_topic_title("  many\n spaces  ")
        self.assertEqual(title, "many spaces")
        self.assertLessEqual(len(clean_topic_title("x" * 300)), 128)

    def test_topic_title_mode_suffix_is_visible_compact_and_replaceable(
        self,
    ) -> None:
        standard = topic_title_with_mode(
            "Inspect the long-running bridge",
            ThreadMode(
                model="gpt-5.6-sol",
                effort="xhigh",
                service_tier="default",
            ),
        )
        fast = topic_title_with_mode(
            standard,
            ThreadMode(
                model="gpt-5.6-sol",
                effort="ultra",
                service_tier="priority",
            ),
        )

        self.assertEqual(
            standard,
            "Inspect the long-running bridge · 🧠XHigh · ⚡Standard",
        )
        self.assertEqual(
            fast,
            "Inspect the long-running bridge · 🧠Ultra · ⚡Fast",
        )
        self.assertEqual(
            strip_topic_mode_suffix(fast),
            "Inspect the long-running bridge",
        )
        self.assertLessEqual(
            len(
                topic_title_with_mode(
                    "x" * 300,
                    ThreadMode(effort="max", service_tier="priority"),
                )
            ),
            128,
        )

    def test_callback_payloads_fit_telegram_limit(self) -> None:
        keyboard = approval_keyboard("a" * 10)
        callbacks = [
            button["callback_data"]
            for row in keyboard["inline_keyboard"]
            for button in row
        ]
        self.assertTrue(all(len(value.encode("utf-8")) <= 64 for value in callbacks))


if __name__ == "__main__":
    unittest.main()
