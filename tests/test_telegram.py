from __future__ import annotations

import io
import http.client
import json
import os
import sys
import tempfile
import unittest
import urllib.error
from pathlib import Path
from unittest.mock import Mock, patch


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from codex_telegram_bridge.telegram import (  # noqa: E402
    TelegramAPI,
    TelegramError,
    exponential_backoff_delay,
    split_telegram_text,
)


class TelegramTextTests(unittest.TestCase):
    def test_short_text_stays_whole(self) -> None:
        self.assertEqual(split_telegram_text("hello"), ["hello"])

    def test_long_text_is_chunked_without_loss(self) -> None:
        source = "\n".join(f"line {index} " + ("x" * 80) for index in range(150))
        chunks = split_telegram_text(source, limit=500)
        self.assertGreater(len(chunks), 1)
        self.assertTrue(all(len(chunk) <= 500 for chunk in chunks))
        self.assertEqual("".join(chunks), source)

    def test_chunking_preserves_boundary_whitespace_and_indentation(self) -> None:
        text = ("A" * 3890) + "\n    " + ("B" * 100)

        chunks = split_telegram_text(text)

        self.assertEqual("".join(chunks), text)
        self.assertEqual(chunks[0][-1], "\n")
        self.assertTrue(chunks[1].startswith("    "))

    def test_send_message_uses_current_link_preview_option(self) -> None:
        telegram = object.__new__(TelegramAPI)
        telegram.call = Mock(return_value={"message_id": 1})

        telegram.send_message(chat_id=-1, text="https://example.invalid")

        method, params = telegram.call.call_args.args
        self.assertEqual(method, "sendMessage")
        self.assertEqual(
            params["link_preview_options"],
            {"is_disabled": True},
        )
        self.assertNotIn("disable_web_page_preview", params)

    def test_send_message_attaches_custom_emoji_only_to_first_chunk(self) -> None:
        telegram = object.__new__(TelegramAPI)
        telegram.call = Mock(return_value={"message_id": 1})
        entity = {
            "type": "custom_emoji",
            "offset": 0,
            "length": 2,
            "custom_emoji_id": "123456789",
        }

        telegram.send_message(
            chat_id=-1,
            text="💻 Codex\n\n" + ("x" * 4000),
            entities=[entity],
        )

        self.assertEqual(telegram.call.call_count, 2)
        first_params = telegram.call.call_args_list[0].args[1]
        second_params = telegram.call.call_args_list[1].args[1]
        self.assertEqual(first_params["entities"], [entity])
        self.assertNotIn("entities", second_params)

    def test_partial_multi_chunk_delivery_fails_closed(self) -> None:
        telegram = object.__new__(TelegramAPI)
        telegram.call = Mock(
            side_effect=[
                {"message_id": 1},
                TelegramError("definite second-chunk failure"),
            ]
        )

        with self.assertRaises(TelegramError) as raised:
            telegram.send_message(chat_id=-1, text="x" * 5000)

        self.assertEqual(telegram.call.call_count, 2)
        self.assertEqual(raised.exception.kind, "partial_delivery")
        self.assertTrue(raised.exception.outcome_ambiguous)
        self.assertFalse(raised.exception.retryable)

    def test_send_message_rejects_parse_mode_with_entities(self) -> None:
        telegram = object.__new__(TelegramAPI)

        with self.assertRaises(ValueError):
            telegram.send_message(
                chat_id=-1,
                text="💻 Codex",
                parse_mode="HTML",
                entities=[
                    {
                        "type": "custom_emoji",
                        "offset": 0,
                        "length": 2,
                        "custom_emoji_id": "123456789",
                    }
                ],
            )

    def test_send_document_targets_topic_and_reply(self) -> None:
        telegram = object.__new__(TelegramAPI)
        telegram.call_multipart = Mock(return_value={"message_id": 7})
        with tempfile.TemporaryDirectory() as temporary_directory:
            report = Path(temporary_directory) / "report.xlsx"
            report.write_bytes(b"spreadsheet")

            result = telegram.send_document(
                chat_id=-1,
                message_thread_id=50,
                reply_to_message_id=60,
                file_path=report,
                caption="Report",
            )

        self.assertEqual(result["message_id"], 7)
        method, params = telegram.call_multipart.call_args.args
        self.assertEqual(method, "sendDocument")
        self.assertEqual(params["message_thread_id"], 50)
        self.assertEqual(params["reply_parameters"]["message_id"], 60)
        self.assertEqual(params["caption"], "Report")
        self.assertEqual(
            telegram.call_multipart.call_args.kwargs,
            {
                "file_field": "document",
                "file_path": report,
            },
        )

    def test_send_photo_targets_topic_reply_and_keyboard(self) -> None:
        telegram = object.__new__(TelegramAPI)
        telegram.call_multipart = Mock(return_value={"message_id": 8})
        keyboard = {
            "inline_keyboard": [
                [{"text": "✅ Подходит", "callback_data": "prv:abc:yes"}]
            ]
        }
        with tempfile.TemporaryDirectory() as temporary_directory:
            photo = Path(temporary_directory) / "photo.jpg"
            photo.write_bytes(b"jpeg")

            result = telegram.send_photo(
                chat_id=-1,
                message_thread_id=50,
                reply_to_message_id=60,
                file_path=photo,
                caption="Фото 6",
                reply_markup=keyboard,
            )

        self.assertEqual(result["message_id"], 8)
        method, params = telegram.call_multipart.call_args.args
        self.assertEqual(method, "sendPhoto")
        self.assertEqual(params["message_thread_id"], 50)
        self.assertEqual(params["reply_parameters"]["message_id"], 60)
        self.assertEqual(params["caption"], "Фото 6")
        self.assertEqual(params["reply_markup"], keyboard)
        self.assertEqual(
            telegram.call_multipart.call_args.kwargs,
            {
                "file_field": "photo",
                "file_path": photo,
            },
        )

    def test_edit_message_caption_clears_keyboard(self) -> None:
        telegram = object.__new__(TelegramAPI)
        telegram.call = Mock(return_value=True)

        result = telegram.edit_message_caption(
            chat_id=-1,
            message_id=7,
            caption="Фото 6\n\nОтвет: ✅ Подходит",
            reply_markup={"inline_keyboard": []},
        )

        self.assertTrue(result)
        telegram.call.assert_called_once_with(
            "editMessageCaption",
            {
                "chat_id": -1,
                "message_id": 7,
                "caption": "Фото 6\n\nОтвет: ✅ Подходит",
                "reply_markup": {"inline_keyboard": []},
            },
        )

    def test_send_attachment_uses_native_video_method(self) -> None:
        telegram = object.__new__(TelegramAPI)
        telegram.call_multipart = Mock(return_value={"message_id": 8})
        with tempfile.TemporaryDirectory() as temporary_directory:
            video = Path(temporary_directory) / "clip.mp4"
            video.write_bytes(b"video")

            result = telegram.send_attachment(
                chat_id=-1,
                media_kind="video",
                message_thread_id=50,
                reply_to_message_id=60,
                file_path=video,
                caption="Clip",
            )

        self.assertEqual(result["message_id"], 8)
        method, params = telegram.call_multipart.call_args.args
        self.assertEqual(method, "sendVideo")
        self.assertTrue(params["supports_streaming"])
        self.assertEqual(params["message_thread_id"], 50)
        self.assertEqual(params["reply_parameters"]["message_id"], 60)
        self.assertEqual(
            telegram.call_multipart.call_args.kwargs,
            {"file_field": "video", "file_path": video},
        )

    def test_definite_native_rejection_falls_back_to_document(self) -> None:
        telegram = object.__new__(TelegramAPI)
        telegram.call_multipart = Mock(
            side_effect=[
                TelegramError(
                    "unsupported photo",
                    method="sendPhoto",
                    kind="api",
                ),
                {"message_id": 9},
            ]
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            image = Path(temporary_directory) / "image.heic"
            image.write_bytes(b"image")

            result = telegram.send_attachment(
                chat_id=-1,
                media_kind="photo",
                file_path=image,
            )

        self.assertEqual(result["message_id"], 9)
        self.assertEqual(
            [call.args[0] for call in telegram.call_multipart.call_args_list],
            ["sendPhoto", "sendDocument"],
        )

    def test_ambiguous_native_rejection_never_falls_back(self) -> None:
        telegram = object.__new__(TelegramAPI)
        telegram.call_multipart = Mock(
            side_effect=TelegramError(
                "response lost",
                method="sendPhoto",
                kind="network_timeout",
                retryable=True,
                outcome_ambiguous=True,
            )
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            image = Path(temporary_directory) / "image.jpg"
            image.write_bytes(b"image")

            with self.assertRaises(TelegramError):
                telegram.send_attachment(
                    chat_id=-1,
                    media_kind="photo",
                    file_path=image,
                )

        self.assertEqual(telegram.call_multipart.call_count, 1)

    def test_get_file_uses_read_only_telegram_method(self) -> None:
        telegram = object.__new__(TelegramAPI)
        telegram.call = Mock(
            return_value={
                "file_id": "opaque",
                "file_path": "voice/file.oga",
                "file_size": 4,
            }
        )

        result = telegram.get_file("opaque")

        self.assertEqual(result["file_path"], "voice/file.oga")
        telegram.call.assert_called_once_with(
            "getFile",
            {"file_id": "opaque"},
        )

    def test_download_file_streams_to_owner_only_destination(self) -> None:
        telegram = object.__new__(TelegramAPI)
        telegram._file_base_url = "https://example.invalid/file/"
        telegram._timeout_seconds = 2
        response = Mock()
        response.__enter__ = Mock(return_value=response)
        response.__exit__ = Mock(return_value=False)
        response.headers = {"Content-Length": "4"}
        response.read = Mock(side_effect=[b"data", b""])
        with tempfile.TemporaryDirectory() as temporary_directory:
            destination = Path(temporary_directory) / "source.bin"
            with patch(
                "codex_telegram_bridge.telegram.urllib.request.urlopen",
                return_value=response,
            ) as open_url:
                result = telegram.download_file(
                    file_path="voice/file.oga",
                    destination=destination,
                    max_bytes=10,
                )

            self.assertEqual(result, destination)
            self.assertEqual(destination.read_bytes(), b"data")
            self.assertEqual(destination.stat().st_mode & 0o777, 0o600)
            request = open_url.call_args.args[0]
            self.assertEqual(
                request.full_url,
                "https://example.invalid/file/voice/file.oga",
            )
            self.assertEqual(request.get_method(), "GET")

    def test_download_file_rejects_oversized_stream_without_partial_file(
        self,
    ) -> None:
        telegram = object.__new__(TelegramAPI)
        telegram._file_base_url = "https://example.invalid/file/"
        telegram._timeout_seconds = 2
        response = Mock()
        response.__enter__ = Mock(return_value=response)
        response.__exit__ = Mock(return_value=False)
        response.headers = {}
        response.read = Mock(side_effect=[b"12345", b""])
        with tempfile.TemporaryDirectory() as temporary_directory:
            destination = Path(temporary_directory) / "source.bin"
            with (
                patch(
                    "codex_telegram_bridge.telegram.urllib.request.urlopen",
                    return_value=response,
                ),
                self.assertRaises(TelegramError) as raised,
            ):
                telegram.download_file(
                    file_path="video/file.mp4",
                    destination=destination,
                    max_bytes=4,
                )

            self.assertEqual(raised.exception.kind, "file_too_large")
            self.assertFalse(destination.exists())
            self.assertEqual(
                [
                    path
                    for path in Path(temporary_directory).iterdir()
                    if ".part-" in path.name
                ],
                [],
            )

    def test_download_file_rejects_remote_path_traversal(self) -> None:
        telegram = object.__new__(TelegramAPI)
        telegram._file_base_url = "https://example.invalid/file/"
        telegram._timeout_seconds = 2
        with tempfile.TemporaryDirectory() as temporary_directory:
            destination = Path(temporary_directory) / "source.bin"

            with self.assertRaises(TelegramError) as raised:
                telegram.download_file(
                    file_path="../secret",
                    destination=destination,
                    max_bytes=4,
                )

        self.assertEqual(raised.exception.kind, "unsafe_file_path")

    def test_send_rich_message_targets_topic_and_reply(self) -> None:
        telegram = object.__new__(TelegramAPI)
        telegram.call = Mock(return_value={"message_id": 7})
        rich = {
            "blocks": [
                {"type": "paragraph", "text": "status"},
                {
                    "type": "details",
                    "summary": "details",
                    "blocks": [],
                    "is_open": True,
                },
            ]
        }

        result = telegram.send_rich_message(
            chat_id=-1,
            message_thread_id=50,
            reply_to_message_id=60,
            rich_message=rich,
        )

        self.assertEqual(result["message_id"], 7)
        method, params = telegram.call.call_args.args
        self.assertEqual(method, "sendRichMessage")
        self.assertEqual(params["rich_message"], rich)
        self.assertEqual(params["message_thread_id"], 50)
        self.assertEqual(params["reply_parameters"]["message_id"], 60)

    def test_edit_message_text_accepts_rich_content_without_plain_text(self) -> None:
        telegram = object.__new__(TelegramAPI)
        telegram.call = Mock(return_value={"message_id": 7})
        rich = {"blocks": [{"type": "paragraph", "text": "done"}]}

        telegram.edit_message_text(
            chat_id=-1,
            message_id=7,
            rich_message=rich,
        )

        method, params = telegram.call.call_args.args
        self.assertEqual(method, "editMessageText")
        self.assertEqual(params["rich_message"], rich)
        self.assertNotIn("text", params)
        self.assertNotIn("link_preview_options", params)

    def test_edit_message_text_requires_exactly_one_content_shape(self) -> None:
        telegram = object.__new__(TelegramAPI)
        telegram.call = Mock(return_value=True)

        with self.assertRaises(ValueError):
            telegram.edit_message_text(chat_id=-1, message_id=7)
        with self.assertRaises(ValueError):
            telegram.edit_message_text(
                chat_id=-1,
                message_id=7,
                text="plain",
                rich_message={"blocks": []},
            )

    def test_delete_forum_topic_uses_exact_topic_scope(self) -> None:
        telegram = object.__new__(TelegramAPI)
        telegram.call = Mock(return_value=True)

        self.assertTrue(telegram.delete_forum_topic(-100, 55))

        telegram.call.assert_called_once_with(
            "deleteForumTopic",
            {"chat_id": -100, "message_thread_id": 55},
        )

    def test_delete_message_uses_exact_chat_and_message(self) -> None:
        telegram = object.__new__(TelegramAPI)
        telegram.call = Mock(return_value=True)

        self.assertTrue(telegram.delete_message(-100, 77))

        telegram.call.assert_called_once_with(
            "deleteMessage",
            {"chat_id": -100, "message_id": 77},
        )

    def test_bot_commands_include_limits_archive_and_read_only_audit(
        self,
    ) -> None:
        telegram = object.__new__(TelegramAPI)
        telegram.call = Mock(return_value=True)

        telegram.set_commands()

        method, params = telegram.call.call_args.args
        self.assertEqual(method, "setMyCommands")
        commands = {
            entry["command"]: entry["description"]
            for entry in params["commands"]
        }
        self.assertIn("limits", commands)
        self.assertIn("недельного лимита Codex", commands["limits"])
        self.assertIn("archive", commands)
        self.assertIn("архивировать", commands["archive"])
        self.assertIn("mode", commands)
        self.assertIn("интеллект", commands["mode"])
        self.assertIn("скорость", commands["mode"])
        self.assertIn("audit", commands)
        self.assertIn("read-only", commands["audit"])


class TelegramErrorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.telegram = object.__new__(TelegramAPI)
        self.telegram._base_url = "https://example.invalid/"
        self.telegram._timeout_seconds = 1

    def test_message_only_error_constructor_remains_compatible(self) -> None:
        error = TelegramError("offline")

        self.assertEqual(str(error), "offline")
        self.assertFalse(error.retryable)
        self.assertFalse(error.outcome_ambiguous)
        self.assertFalse(error.ambiguous_outcome)

    def test_side_effecting_network_timeout_is_retryable_and_ambiguous(self) -> None:
        with (
            patch(
                "codex_telegram_bridge.telegram.urllib.request.urlopen",
                side_effect=TimeoutError(),
            ),
            self.assertRaises(TelegramError) as raised,
        ):
            self.telegram.call("createForumTopic", {})

        error = raised.exception
        self.assertEqual(error.kind, "network_timeout")
        self.assertTrue(error.retryable)
        self.assertTrue(error.outcome_ambiguous)
        self.assertEqual(error.method, "createForumTopic")

    def test_malformed_success_response_is_classified_by_method_safety(
        self,
    ) -> None:
        response = Mock()
        response.__enter__ = Mock(return_value=response)
        response.__exit__ = Mock(return_value=False)
        response.read.return_value = b"not-json"

        with patch(
            "codex_telegram_bridge.telegram.urllib.request.urlopen",
            return_value=response,
        ):
            with self.assertRaises(TelegramError) as send_error:
                self.telegram.call("sendMessage", {})
            with self.assertRaises(TelegramError) as read_error:
                self.telegram.call("getUpdates", {})

        self.assertEqual(send_error.exception.kind, "protocol")
        self.assertTrue(send_error.exception.outcome_ambiguous)
        self.assertEqual(read_error.exception.kind, "protocol")
        self.assertFalse(read_error.exception.outcome_ambiguous)

    def test_non_object_success_response_is_protocol_error(self) -> None:
        response = Mock()
        response.__enter__ = Mock(return_value=response)
        response.__exit__ = Mock(return_value=False)
        response.read.return_value = b"null"

        with (
            patch(
                "codex_telegram_bridge.telegram.urllib.request.urlopen",
                return_value=response,
            ),
            self.assertRaises(TelegramError) as raised,
        ):
            self.telegram.call("getUpdates", {})

        self.assertEqual(raised.exception.kind, "protocol")
        self.assertTrue(raised.exception.retryable)
        self.assertFalse(raised.exception.outcome_ambiguous)

    def test_get_updates_rejects_non_list_and_malformed_update(self) -> None:
        self.telegram.call = Mock(side_effect=[{}, [{"update_id": "bad"}]])

        with self.assertRaises(TelegramError) as non_list:
            self.telegram.get_updates(offset=None, timeout=0)
        with self.assertRaises(TelegramError) as bad_update:
            self.telegram.get_updates(offset=None, timeout=0)

        self.assertEqual(non_list.exception.kind, "protocol")
        self.assertEqual(bad_update.exception.kind, "protocol")

    def test_malformed_multipart_success_is_ambiguous_protocol_error(
        self,
    ) -> None:
        response = Mock()
        response.__enter__ = Mock(return_value=response)
        response.__exit__ = Mock(return_value=False)
        response.read.return_value = b"not-json"

        with tempfile.TemporaryDirectory() as temp_dir:
            sticker = Path(temp_dir) / "sticker.png"
            sticker.write_bytes(b"png")
            with (
                patch(
                    "codex_telegram_bridge.telegram.urllib.request.urlopen",
                    return_value=response,
                ),
                self.assertRaises(TelegramError) as raised,
            ):
                self.telegram.call_multipart(
                    "uploadStickerFile",
                    {"user_id": 1, "sticker_format": "static"},
                    file_field="sticker",
                    file_path=sticker,
                )

        self.assertEqual(raised.exception.kind, "protocol")
        self.assertTrue(raised.exception.outcome_ambiguous)
        self.assertEqual(raised.exception.method, "uploadStickerFile")

    def test_read_only_url_error_is_retryable_but_not_ambiguous(self) -> None:
        with (
            patch(
                "codex_telegram_bridge.telegram.urllib.request.urlopen",
                side_effect=urllib.error.URLError("offline"),
            ),
            self.assertRaises(TelegramError) as raised,
        ):
            self.telegram.call("getMe")

        error = raised.exception
        self.assertEqual(error.kind, "network_error")
        self.assertTrue(error.retryable)
        self.assertFalse(error.outcome_ambiguous)

    def test_remote_disconnect_is_a_retryable_network_error(self) -> None:
        with (
            patch(
                "codex_telegram_bridge.telegram.urllib.request.urlopen",
                side_effect=http.client.RemoteDisconnected("closed"),
            ),
            self.assertRaises(TelegramError) as raised,
        ):
            self.telegram.call("getUpdates", {})

        self.assertEqual(raised.exception.kind, "network_error")
        self.assertTrue(raised.exception.retryable)
        self.assertFalse(raised.exception.outcome_ambiguous)

    def test_side_effecting_http_5xx_is_retryable_and_ambiguous(self) -> None:
        http_error = urllib.error.HTTPError(
            "https://example.invalid/",
            502,
            "Bad Gateway",
            {},
            io.BytesIO(b'{"ok":false,"description":"Bad Gateway"}'),
        )
        with (
            patch(
                "codex_telegram_bridge.telegram.urllib.request.urlopen",
                side_effect=http_error,
            ),
            self.assertRaises(TelegramError) as raised,
        ):
            self.telegram.call("sendMessage", {})

        error = raised.exception
        self.assertEqual(error.kind, "http_5xx")
        self.assertEqual(error.http_status, 502)
        self.assertTrue(error.retryable)
        self.assertTrue(error.outcome_ambiguous)

    def test_read_only_http_5xx_is_not_ambiguous(self) -> None:
        http_error = urllib.error.HTTPError(
            "https://example.invalid/",
            503,
            "Service Unavailable",
            {},
            io.BytesIO(b"not-json"),
        )
        with (
            patch(
                "codex_telegram_bridge.telegram.urllib.request.urlopen",
                side_effect=http_error,
            ),
            self.assertRaises(TelegramError) as raised,
        ):
            self.telegram.call("getUpdates")

        error = raised.exception
        self.assertTrue(error.retryable)
        self.assertFalse(error.outcome_ambiguous)

    def test_flood_wait_retries_once_and_keeps_metadata(self) -> None:
        flood_payload = {
            "ok": False,
            "description": "Too Many Requests",
            "parameters": {"retry_after": 4},
        }
        response = Mock()
        response.__enter__ = Mock(return_value=response)
        response.__exit__ = Mock(return_value=False)
        response.read.return_value = json.dumps(flood_payload).encode("utf-8")

        with (
            patch(
                "codex_telegram_bridge.telegram.urllib.request.urlopen",
                return_value=response,
            ),
            patch("codex_telegram_bridge.telegram.time.sleep") as sleep,
            self.assertRaises(TelegramError) as raised,
        ):
            self.telegram.call("sendMessage", {})

        sleep.assert_called_once_with(5)
        error = raised.exception
        self.assertEqual(error.kind, "flood_wait")
        self.assertTrue(error.retryable)
        self.assertFalse(error.outcome_ambiguous)
        self.assertEqual(error.retry_after_seconds, 4)


class ExponentialBackoffTests(unittest.TestCase):
    def test_grows_exponentially_and_caps(self) -> None:
        delays = [
            exponential_backoff_delay(
                attempt,
                maximum_seconds=10,
                jitter_ratio=0,
            )
            for attempt in range(6)
        ]

        self.assertEqual(delays, [1, 2, 4, 8, 10, 10])

    def test_caller_supplied_jitter_is_deterministic(self) -> None:
        self.assertEqual(
            exponential_backoff_delay(2, jitter_ratio=0.25, jitter_sample=0),
            3,
        )
        self.assertEqual(
            exponential_backoff_delay(2, jitter_ratio=0.25, jitter_sample=0.5),
            4,
        )
        self.assertEqual(
            exponential_backoff_delay(2, jitter_ratio=0.25, jitter_sample=1),
            5,
        )

    def test_rejects_invalid_attempt_without_sleeping(self) -> None:
        with self.assertRaises(ValueError):
            exponential_backoff_delay(-1)


if __name__ == "__main__":
    unittest.main()
