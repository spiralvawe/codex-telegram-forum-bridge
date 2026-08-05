Warning: truncated output (original token count: 61500)
Total output lines: 6935

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import sys
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, Mock, patch


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from codex_telegram_bridge.codex import (  # noqa: E402
    CodexProtocolCompatibilityError,
    CodexProtocolError,
)
from codex_telegram_bridge.config import BridgeConfig  # noqa: E402
from codex_telegram_bridge.input_types import LocalInput  # noqa: E402
from codex_telegram_bridge.media import (  # noqa: E402
    MediaProcessingError,
    MediaPruneResult,
    PreparedMedia,
)
from codex_telegram_bridge.service import (  # noqa: E402
    BridgeService,
    CONTROL_PROMPT_TEXT,
    NEW_THREAD_PROMPT,
    PendingServerRequest,
    PROGRESS_COLLAPSE_RESET_SECONDS,
    TELEGRAM_UPDATE_BACKOFF_MAXIMUM_SECONDS,
    THREAD_SYNC_BACKOFF_MAXIMUM_SECONDS,
    ThreadMode,
    TopicCreationUnresolvedError,
    bootstrap_group,
    final_answer_attachments,
    format_limit_percent,
    format_token_count,
    parse_person_review_text,
    person_review_token,
    progress_summary,
    telegram_visible_text,
    weekly_codex_remaining_percent,
)
from codex_telegram_bridge.store import (  # noqa: E402
    BridgeStore,
    TopicBinding,
)
from codex_telegram_bridge.telegram import (  # noqa: E402
    TelegramError,
    TelegramIdentity,
)


class FakeTelegram:
    def __init__(
        self,
        *,
        updates: list[dict[str, Any]] | None = None,
        bot_member: dict[str, Any] | None = None,
        administrators: list[dict[str, Any]] | None = None,
    ) -> None:
        self.updates = list(updates or [])
        self.bot_member = dict(
            bot_member
            or {
                "status": "administrator",
                "can_manage_topics": True,
            }
        )
        self.administrators = list(administrators or [])
        self.commands_set = 0
        self.sent_messages: list[dict[str, Any]] = []
        self.sent_documents: list[dict[str, Any]] = []
        self.sent_attachments: list[dict[str, Any]] = []
        self.sent_photos: list[dict[str, Any]] = []
        self.sent_rich_messages: list[dict[str, Any]] = []
        self.callback_answers: list[tuple[str, str | None]] = []
        self.edited_markups: list[dict[str, Any]] = []
        self.edited_messages: list[dict[str, Any]] = []
        self.edited_captions: list[dict[str, Any]] = []
        self.chat_actions: list[dict[str, Any]] = []
        self.edited_topics: list[tuple[int, int, str]] = []
        self.created_topics: list[tuple[int, str]] = []
        self.deleted_topics: list[tuple[int, int]] = []
        self.update_requests: list[dict[str, Any]] = []
        self.file_requests: list[str] = []
        self.downloads: list[dict[str, Any]] = []

    def identity(self) -> TelegramIdentity:
        return TelegramIdentity(bot_id=700, username="project_bridge_bot")

    def get_updates(
        self, *, offset: int | None, timeout: int, limit: int = 100
    ) -> list[dict[str, Any]]:
        self.update_requests.append(
            {"offset": offset, "timeout": timeout, "limit": limit}
        )
        updates, self.updates = self.updates, []
        return updates

    def get_chat_member(self, chat_id: int, user_id: int) -> dict[str, Any]:
        return dict(self.bot_member)

    def get_chat_administrators(self, chat_id: int) -> list[dict[str, Any]]:
        return list(self.administrators)

    def set_commands(self) -> None:
        self.commands_set += 1

    def get_file(self, file_id: str) -> dict[str, Any]:
        self.file_requests.append(file_id)
        return {
            "file_id": file_id,
            "file_path": "voice/file.oga",
            "file_size": 1024,
        }

    def download_file(
        self,
        *,
        file_path: str,
        destination: str | Path,
        max_bytes: int,
    ) -> Path:
        output = Path(destination)
        output.write_bytes(b"downloaded")
        output.chmod(0o600)
        self.downloads.append(
            {
                "file_path": file_path,
                "destination": output,
                "max_bytes": max_bytes,
            }
        )
        return output

    def send_message(self, **kwargs: Any) -> list[dict[str, Any]]:
        self.sent_messages.append(dict(kwargs))
        return [{"message_id": 800 + len(self.sent_messages)}]

    def send_document(self, **kwargs: Any) -> dict[str, Any]:
        self.sent_documents.append(dict(kwargs))
        return {"message_id": 1000 + len(self.sent_documents)}

    def send_attachment(self, **kwargs: Any) -> dict[str, Any]:
        payload = dict(kwargs)
        self.sent_attachments.append(payload)
        if payload.get("media_kind") == "document":
            self.sent_documents.append(payload)
        return {"message_id": 1100 + len(self.sent_attachments)}

    def send_photo(self, **kwargs: Any) -> dict[str, Any]:
        self.sent_photos.append(dict(kwargs))
        return {"message_id": 1100 + len(self.sent_photos)}

    def send_rich_message(self, **kwargs: Any) -> dict[str, Any]:
        self.sent_rich_messages.append(dict(kwargs))
        return {"message_id": 900 + len(self.sent_rich_messages)}

    def answer_callback_query(
        self, callback_query_id: str, text: str | None = None
    ) -> bool:
        self.callback_answers.append((callback_query_id, text))
        return True

    def edit_reply_markup(self, **kwargs: Any) -> bool:
        self.edited_markups.append(dict(kwargs))
        return True

    def edit_message_text(self, **kwargs: Any) -> bool:
        self.edited_messages.append(dict(kwargs))
        return True

    def edit_message_caption(self, **kwargs: Any) -> bool:
        self.edited_captions.append(dict(kwargs))
        return True

    def send_chat_action(self, **kwargs: Any) -> bool:
        self.chat_actions.append(dict(kwargs))
        return True

    def edit_forum_topic(
        self, chat_id: int, message_thread_id: int, name: str
    ) -> bool:
        self.edited_topics.append((chat_id, message_thread_id, name))
        return True

    def create_forum_topic(self, chat_id: int, name: str) -> dict[str, Any]:
        self.created_topics.append((chat_id, name))
        return {"message_thread_id": 900 + len(self.created_topics)}

    def delete_forum_topic(
        self,
        chat_id: int,
        message_thread_id: int,
    ) -> bool:
        self.deleted_topics.append((chat_id, message_thread_id))
        return True


def connect_update(
    *,
    update_id: int = 10,
    sender_id: int = 100,
    chat_type: str = "supergroup",
    is_forum: bool = True,
) -> dict[str, Any]:
    return {
        "update_id": update_id,
        "message": {
            "message_id": 20,
            "message_thread_id": 1,
            "text": "/connect@project_bridge_bot",
            "from": {
                "id": sender_id,
                "is_bot": False,
            },
            "chat": {
                "id": -100500,
                "type": chat_type,
                "is_forum": is_forum,
                "title": "Private project",
            },
        },
    }


class BootstrapTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.store = BridgeStore(Path(self.temp_dir.name) / "state.sqlite3")

    def tearDown(self) -> None:
        self.store.close()
        self.temp_dir.cleanup()

    def test_connect_binds_only_the_human_admin_and_advances_offset(self) -> None:
        self.store.set_telegram_offset(5)
        telegram = FakeTelegram(
            updates=[connect_update(update_id=10)],
            administrators=[
                {"user": {"id": 100, "is_bot": False}},
                {"user": {"id": 700, "is_bot": True}},
            ],
        )

        result = bootstrap_group(store=self.store, telegram=telegram)

        binding = self.store.binding()
        self.assertTrue(result["ok"])
        self.assertIsNotNone(binding)
        self.assertEqual(binding.allowed_user_id, 100)
        self.assertEqual(binding.bot_username, "project_bridge_bot")
        self.assertEqual(self.store.telegram_offset(), 11)
        self.assertEqual(telegram.commands_set, 1)
        self.assertEqual(len(telegram.sent_messages), 1)
        self.assertEqual(telegram.sent_messages[0]["reply_to_message_id"], 20)

    def test_connect_rejects_human_who_is_not_a_group_admin(self) -> None:
        telegram = FakeTelegram(
            updates=[connect_update(sender_id=100)],
            administrators=[{"user": {"id": 101, "is_bot": False}}],
        )

        with self.assertRaisesRegex(RuntimeError, "human group administrator"):
            bootstrap_group(store=self.store, telegram=telegram)

        self.assertIsNone(self.store.binding())
        self.assertEqual(telegram.commands_set, 0)
        self.assertEqual(telegram.sent_messages, [])

    def test_connect_rejects_bot_without_manage_topics_permission(self) -> None:
        telegram = FakeTelegram(
            updates=[connect_update()],
            bot_member={
                "status": "administrator",
                "can_manage_topics": False,
            },
            administrators=[{"user": {"id": 100, "is_bot": False}}],
        )

        with self.assertRaisesRegex(RuntimeError, "Manage Topics"):
            bootstrap_group(store=self.store, telegram=telegram)

        self.assertIsNone(self.store.binding())
        self.assertEqual(telegram.commands_set, 0)

    def test_connect_ignores_a_non_forum_group(self) -> None:
        telegram = FakeTelegram(
            updates=[connect_update(chat_type="group", is_forum=False)],
            administrators=[{"user": {"id": 100, "is_bot": False}}],
        )

        result = bootstrap_group(store=self.store, telegram=telegram)

        self.assertFalse(result["ok"])
        self.assertTrue(result["needsConnectMessage"])
        self.assertIsNone(self.store.binding())


class ServiceRoutingTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        temp_path = Path(self.temp_dir.name)
        workspace = temp_path / "workspace"
        workspace.mkdir()
        self.store = BridgeStore(temp_path / "state.sqlite3")
        self.store.bind(
            chat_id=-100500,
            allowed_user_id=100,
            bot_id=700,
            bot_username="project_bridge_bot",
            chat_title="Private project",
        )
        self.store.upsert_topic(
            thread_id="thread-1",
            chat_id=-100500,
            topic_id=50,
            title="Test topic",
        )
        self.telegram = FakeTelegram()
        self.service = BridgeService(
            config=BridgeConfig(workspace=workspace, state_dir=temp_path),
            store=self.store,
            telegram=self.telegram,
        )
        self.codex = SimpleNamespace(
            respond=AsyncMock(),
            start_turn=AsyncMock(return_value={"id": "turn-new"}),
            steer_turn=AsyncMock(return_value="turn-active"),
            interrupt_turn=AsyncMock(),
            read_rate_limits=AsyncMock(),
            archive_thread=AsyncMock(return_value={}),
        )
        self.service.codex = self.codex

    async def asyncTearDown(self) -> None:
        self.store.close()
        self.temp_dir.cleanup()

    def test_bridge_service_wires_explicit_full_access(self) -> None:
        config = BridgeConfig(
            workspace=self.service.config.workspace,
            state_dir=self.service.config.state_dir,
            codex_full_access=True,
        )

        service = BridgeService(
            config=config,
            store=self.store,
            telegram=self.telegram,
        )

        self.assertTrue(service.codex.full_access)

    def test_person_review_text_parser_is_explicit(self) -> None:
        self.assertEqual(parse_person_review_text("подходит"), "yes")
        self.assertEqual(parse_person_review_text("Да, подходит!"), "yes")
        self.assertEqual(parse_person_review_text("не подходит"), "no")
        self.assertEqual(parse_person_review_text("Нет, не подходит."), "no")
        self.assertEqual(
            parse_person_review_text("Не подходит, поискать лучше"),
            "no",
        )
        self.assertEqual(parse_person_review_text("да"), "yes")
        self.assertEqual(parse_person_review_text("нет"), "no")

    def test_weekly_limit_prefers_default_codex_bucket(self) -> None:
        payload = {
            "rateLimits": {
                "primary": {
                    "usedPercent": 6,
                    "windowDurationMins": 10_080,
                },
            },
            "rateLimitsByLimitId": {
                "codex": {
                    "primary": {
                        "usedPercent": 6,
                        "windowDurationMins": 10_080,
                    },
                },
                "codex_other": {
                    "primary": {
                        "usedPercent": 0,
                        "windowDurationMins": 10_080,
                    },
                },
            },
        }

        self.assertEqual(weekly_codex_remaining_percent(payload), 94)

    def test_weekly_limit_ignores_short_window_and_invalid_percent(self) -> None:
        payload = {
            "rateLimits": {
                "primary": {
                    "usedPercent": 10,
                    "windowDurationMins": 300,
                },
                "secondary": {
                    "usedPercent": 101,
                    "windowDurationMins": 10_080,
                },
            }
        }

        self.assertIsNone(weekly_codex_remaining_percent(payload))
        self.assertEqual(format_limit_percent(94), "94")
        self.assertEqual(format_limit_percent(93.54), "93,5")

    def add_pending_approval(
        self,
        *,
        public_id: str = "approval1",
        method: str = "item/commandExecution/requestApproval",
        params: dict[str, Any] | None = None,
    ) -> PendingServerRequest:
        pending = PendingServerRequest(
            public_id=public_id,
            server_request_id="server-request-1",
            method=method,
            thread_id="thread-1",
            params=dict(params or {"command": ["safe-tool", "--check"]}),
            telegram_message_id=70,
        )
        self.service.pending_requests[public_id] = pending
        self.store.save_pending_request(
            public_id=public_id,
            thread_id="thread-1",
            request_kind="approval",
            metadata={"method": method},
            telegram_message_id=70,
        )
        return pending

    @staticmethod
    def topic_message(text: str, *, message_id: int = 90) -> dict[str, Any]:
        return {
            "update_id": 1,
            "message": {
                "message_id": message_id,
                "message_thread_id": 50,
                "text": text,
                "from": {"id": 100, "is_bot": False},
                "chat": {"id": -100500, "type": "supergroup"},
            },
        }

    @staticmethod
    def topic_media_message(
        kind: str,
        *,
        message_id: int = 90,
        file_size: int = 1024,
        reply_to_message: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        message: dict[str, Any] = {
            "message_id": message_id,
            "message_thread_id": 50,
            kind: {
                "file_id": f"{kind}-file",
                "file_unique_id": f"{kind}-unique",
                "duration": 12,
                "file_size": file_size,
            },
            "from": {"id": 100, "is_bot": False},
            "chat": {"id": -100500, "type": "supergroup"},
        }
        if kind == "document":
            message[kind].update(
                {
                    "file_name": "ZEN statement July.csv",
                    "mime_type": "text/csv",
                }
            )
        if reply_to_message is not None:
            message["reply_to_message"] = reply_to_message
        return {"update_id": 1, "message": message}

    def local_media_input(
        self,
        *,
        name: str = "audio.mp3",
        input_type: str = "localAudio",
    ) -> LocalInput:
        directory = self.service.config.media_directory / ("c" * 32)
        directory.mkdir(parents=True, mode=0o700, exist_ok=True)
        path = directory / name
        path.write_bytes(b"prepared media")
        path.chmod(0o600)
        return LocalInput(
            input_type,
            str(path.resolve()),
            detail="low" if input_type == "localImage" else None,
        )

    @staticmethod
    def queue_callback(
        queue_id: int,
        *,
        sender_id: int = 100,
        callback_id: str = "callback-1",
        topic_id: int = 50,
    ) -> dict[str, Any]:
        return {
            "id": callback_id,
            "data": f"stq:{queue_id}",
            "from": {"id": sender_id, "is_bot": False},
            "message": {
                "message_id": 91,
                "message_thread_id": topic_id,
                "chat": {"id": -100500, "type": "supergroup"},
            },
        }

    @staticmethod
    def progress_collapse_callback(
        message_id: int,
        *,
        revision: int = 0,
        sender_id: int = 100,
        callback_id: str = "progress-collapse-1",
        topic_id: int = 50,
    ) -> dict[str, Any]:
        return {
            "id": callback_id,
            "data": f"pgc:{revision}",
            "from": {"id": sender_id, "is_bot": False},
            "message": {
                "message_id": message_id,
                "message_thread_id": topic_id,
                "chat": {"id": -100500, "type": "supergroup"},
            },
        }

    async def test_plus_resolves_pending_approval_instead_of_starting_a_turn(
        self,
    ) -> None:
        self.add_pending_approval()

        await self.service.handle_telegram_update(self.topic_message("+"))

        self.codex.respond.assert_awaited_once_with(
            "server-request-1",
            result={"decision": "accept"},
        )
        self.codex.start_turn.assert_not_awaited()
        self.assertIsNone(self.store.next_queued("thread-1"))
        self.assertNotIn("approval1", self.service.pending_requests)
        status = self.store.connection.execute(
            "SELECT status FROM pending_requests WHERE public_id = ?",
            ("approval1",),
        ).fetchone()["status"]
        self.assertEqual(status, "once")

    async def test_no_text_denies_permissions_without_granting_any(self) -> None:
        self.add_pending_approval(
            method="item/permissions/requestApproval",
            params={
                "permissions": {
                    "network": True,
                    "fileSystem": {"read": ["/tmp/example"]},
                }
            },
        )

        consumed = await self.service._try_resolve_pending_text(
            "thread-1", "  НЕТ  "
        )

        self.assertTrue(consumed)
        self.codex.respond.assert_awaited_once_with(
            "server-request-1",
            result={"permissions": {}, "scope": "turn"},
        )
        self.codex.start_turn.assert_not_awaited()

    async def test_unrelated_text_does_not_accidentally_approve(self) -> None:
        self.add_pending_approval()

        consumed = await self.service._try_resolve_pending_text(
            "thread-1", "продолжай анализ"
        )

        self.assertFalse(consumed)
        self.codex.respond.assert_not_awaited()
        self.assertIn("approval1", self.service.pending_requests)

    async def test_ambiguous_plus_must_reply_to_one_approval_card(self) -> None:
        self.add_pending_approval(public_id="approval1")
        second = self.add_pending_approval(public_id="approval2")
        second.telegram_message_id = 71

        consumed = await self.service._try_resolve_pending_text(
            "thread-1",
            "+",
            source_message_id=90,
        )

        self.assertTrue(consumed)
        self.codex.respond.assert_not_awaited()
        self.assertEqual(
            set(self.service.pending_requests),
            {"approval1", "approval2"},
        )
        self.assertIn(
            "reply на нужную карточку",
            self.telegram.sent_messages[-1]["text"],
        )
        self.assertEqual(
            self.telegram.sent_messages[-1]["reply_to_message_id"],
            90,
        )

    async def test_reply_plus_resolves_exact_approval_card(self) -> None:
        first = self.add_pending_approval(public_id="approval1")
        first.server_request_id = "request-1"
        second = self.add_pending_approval(public_id="approval2")
        second.server_request_id = "request-2"
        second.telegram_message_id = 71

        consumed = await self.service._try_resolve_pending_text(
            "thread-1",
            "+",
            telegram_reply_to_message_id=71,
            source_message_id=90,
        )

        self.assertTrue(consumed)
        self.codex.respond.assert_awaited_once_with(
            "request-2",
            result={"decision": "accept"},
        )
        self.assertIn("approval1", self.service.pending_requests)
        self.assertNotIn("approval2", self.service.pending_requests)

    async def test_codex_success_is_not_retried_when_typing_action_fails(
        self,
    ) -> None:
        topic = self.store.topic_for_thread("thread-1")
        self.assertIsNotNone(topic)

        def fail_chat_action(**_: Any) -> bool:
            raise TelegramError("temporary network failure")

        self.telegram.send_chat_action = fail_chat_action  # type: ignore[method-assign]

        started = await self.service.start_turn(
            topic=topic,
            text="start exactly once",
            client_id="tg:exactly-once:90",
            reply_to=90,
        )

        self.assertTrue(started)
        self.codex.start_turn.assert_awaited_once()
        self.assertEqual(
            self.service.active_turns["thread-1"],
            "turn-new",
        )
        self.assertIsNone(self.store.next_queued("thread-1"))

    async def test_busy_plain_message_is_queued_with_a_steer_callback(self) -> None:
        self.service.busy_threads.add("thread-1")

        await self.service.handle_telegram_update(
            self.topic_message("проверь только журналы")
        )

        queued = self.store.next_queued("thread-1")
        self.assertIsNotNone(queued)
        self.assertEqual(queued.text, "проверь только журналы")
        self.codex.start_turn.assert_not_awaited()
        callback_data = self.telegram.sent_messages[-1]["reply_markup"][
            "inline_keyboard"
        ][0][0]["callback_data"]
        self.assertEqual(callback_data, f"stq:{queued.queue_id}")

    async def test_global_turn_limit_serializes_topics_until_terminal_event(
        self,
    ) -> None:
        self.service.config = replace(
            self.service.config,
            max_active_turns=1,
        )
        self.store.upsert_topic(
            thread_id="thread-2",
            chat_id=-100500,
            topic_id=51,
            title="Second topic",
        )
        first = self.store.enqueue(
            thread_id="thread-1",
            chat_id=-100500,
            topic_id=50,
            telegram_message_id=90,
            text="first task",
            client_id="tg:-100500:90",
        )
        second = self.store.enqueue(
            thread_id="thread-2",
            chat_id=-100500,
            topic_id=51,
            telegram_message_id=91,
            text="second task",
            client_id="tg:-100500:91",
        )
        self.codex.start_turn.side_effect = [
            {"id": "turn-first"},
            {"id": "turn-second"},
        ]

        await asyncio.gather(
            self.service.dispatch_queued_capacity(),
            self.service.dispatch_queued_capacity(),
        )

        self.assertEqual(self.codex.start_turn.await_count, 1)
        self.assertEqual(
            self.store.queued_message(first.queue_id).status,
            "sent",
        )
        self.assertEqual(
            self.store.queued_message(second.queue_id).status,
            "pending",
        )
        self.assertEqual(self.service.active_turn_count(), 1)

        await self.service.on_codex_notification(
            {
                "method": "turn/completed",
                "params": {
                    "threadId": "thread-1",
                    "turn": {
                        "id": "turn-first",
                        "status": "completed",
                    },
                },
            }
        )

        self.assertEqual(self.codex.start_turn.await_count, 2)
        self.assertEqual(
            self.codex.start_turn.await_args.kwargs["thread_id"],
            "thread-2",
        )
        self.assertEqual(
            self.store.queued_message(second.queue_id).status,
            "sent",
        )
        self.assertEqual(
            self.service.active_turns,
            {"thread-2": "turn-second"},
        )

    async def test_default_global_turn_limit_preserves_parallel_topics(
        self,
    ) -> None:
        self.assertEqual(self.service.config.max_active_turns, 0)
        self.store.upsert_topic(
            thread_id="thread-2",
            chat_id=-100500,
            topic_id=51,
            title="Second topic",
        )
        for thread_id, topic_id, message_id in (
            ("thread-1", 50, 90),
            ("thread-2", 51, 91),
        ):
            self.store.enqueue(
                thread_id=thread_id,
                chat_id=-100500,
                topic_id=topic_id,
                telegram_message_id=message_id,
                text=f"task for {thread_id}",
                client_id=f"tg:-100500:{message_id}",
            )
        self.codex.start_turn.side_effect = [
            {"id": "turn-first"},
            {"id": "turn-second"},
        ]

        await self.service.dispatch_queued_capacity()

        self.assertEqual(self.codex.start_turn.await_count, 2)
        self.assertEqual(self.service.active_turn_count(), 2)

    async def test_outcome_unknown_start_blocks_other_topic_capacity(
        self,
    ) -> None:
        self.service.config = replace(
            self.service.config,
            max_active_turns=1,
        )
        self.store.upsert_topic(
            thread_id="thread-2",
            chat_id=-100500,
            topic_id=51,
            title="Second topic",
        )
        uncertain = self.store.enqueue(
            thread_id="thread-1",
            chat_id=-100500,
            topic_id=50,
            telegram_message_id=90,
            text="possibly accepted",
            client_id="tg:-100500:90",
        )
        self.codex.start_turn.side_effect = RuntimeError(
            "response lost after request"
        )

        await self.service.dispatch_queued_capacity()

        self.assertTrue(self.service.codex_available)
        self.assertEqual(
            self.store.queued_message(uncertain.queue_id).status,
            "dispatching",
        )
        self.assertIn("thread-1", self.service.busy_threads)
        self.assertEqual(self.service.active_turn_count(), 1)

        restarted_telegram = FakeTelegram()
        restarted = BridgeService(
            config=self.service.config,
            store=self.store,
            telegram=restarted_telegram,
        )
        restarted.codex = SimpleNamespace(
            start_turn=AsyncMock(return_value={"id": "must-not-start"}),
        )
        self.assertEqual(restarted.active_turn_count(), 1)
        second_update = self.topic_message(
            "must wait for reconciliation",
            message_id=91,
        )
        second_update["message"]["message_thread_id"] = 51
        await restarted.handle_telegram_update(second_update)

        self.assertEqual(self.codex.start_turn.await_count, 1)
        restarted.codex.start_turn.assert_not_awaited()
        second = self.store.queued_message_for_client_id("tg:-100500:91")
        self.assertIsNotNone(second)
        self.assertEqual(second.status, "pending")
        self.assertEqual(
            self.store.dispatching_queue_thread_ids(),
            {"thread-1"},
        )

    async def test_interrupt_request_does_not_release_global_capacity(
        self,
    ) -> None:
        self.service.config = replace(
            self.service.config,
            max_active_turns=1,
        )
        self.store.upsert_topic(
            thread_id="thread-2",
            chat_id=-100500,
            topic_id=51,
            title="Second topic",
        )
        queued = self.store.enqueue(
            thread_id="thread-2",
            chat_id=-100500,
            topic_id=51,
            telegram_message_id=91,
            text="wait for interruption",
            client_id="tg:-100500:91",
        )
        self.service.active_turns["thread-1"] = "turn-running"
        self.service.busy_threads.add("thread-1")
        topic = self.store.topic_for_thread("thread-1")
        self.assertIsNotNone(topic)

        await self.service.cancel_turn(topic, reply_to=90)
        await self.service.dispatch_queued_capacity()

        self.codex.interrupt_turn.assert_awaited_once()
        self.codex.start_turn.assert_not_awaited()
        self.assertEqual(
            self.store.queued_message(queued.queue_id).status,
            "pending",
        )

        await self.service.on_codex_notification(
            {
                "method": "turn/completed",
                "params": {
                    "threadId": "thread-1",
                    "turn": {
                        "id": "turn-running",
                        "status": "interrupted",
                    },
                },
            }
        )

        self.codex.start_turn.assert_awaited_once_with(
            thread_id="thread-2",
            text="wait for interruption",
            client_id="tg:-100500:91",
        )
        self.assertEqual(
            self.store.queued_message(queued.queue_id).status,
            "sent",
        )

    async def test_stale_terminal_event_cannot_release_newer_observed_turn(
        self,
    ) -> None:
        self.service.config = replace(
            self.service.config,
            max_active_turns=1,
        )
        self.store.upsert_topic(
            thread_id="thread-2",
            chat_id=-100500,
            topic_id=51,
            title="Second topic",
        )
        self.store.enqueue(
            thread_id="thread-2",
            chat_id=-100500,
            topic_id=51,
            telegram_message_id=91,
            text="must keep waiting",
            client_id="tg:-100500:91",
        )
        self.service.busy_threads.add("thread-1")
        self.service.observed_turns["thread-1"] = "turn-newer"

        await self.service.on_codex_notification(
            {
                "method": "turn/completed",
                "params": {
                    "threadId": "thread-1",
                    "turn": {
                        "id": "turn-older",
                        "status": "completed",
                    },
                },
            }
        )

        self.codex.start_turn.assert_not_awaited()
        self.assertIn("thread-1", self.service.busy_threads)

        await self.service.on_codex_notification(
            {
                "method": "turn/completed",
                "params": {
                    "threadId": "thread-1",
                    "turn": {
                        "id": "turn-newer",
                        "status": "completed",
                    },
                },
            }
        )

        self.codex.start_turn.assert_awaited_once()

    async def test_sync_reconstructs_global_capacity_before_dispatching_queue(
        self,
    ) -> None:
        self.service.config = replace(
            self.service.config,
            max_active_turns=1,
        )
        self.store.upsert_topic(
            thread_id="thread-2",
            chat_id=-100500,
            topic_id=51,
            title="Second topic",
        )
        queued = self.store.enqueue(
            thread_id="thread-2",
            chat_id=-100500,
            topic_id=51,
            telegram_message_id=91,
            text="durable after restart",
            client_id="tg:-100500:91",
        )
        external_active = True
        summaries = [
            {
                "id": "thread-1",
                "name": "Test topic",
                "preview": "",
                "updatedAt": 101,
            },
            {
                "id": "thread-2",
                "name": "Second topic",
                "preview": "",
                "updatedAt": 102,
            },
        ]

        async def list_threads(*, archived: bool) -> list[dict[str, Any]]:
            return [] if archived else summaries

        async def read_thread(thread_id: str) -> dict[str, Any]:
            if thread_id == "thread-2":
                return {
                    "id": "thread-2",
                    "status": {"type": "idle"},
                    "updatedAt": 102,
                    "turns": [],
                }
            return {
                "id": "thread-1",
                "status": {
                    "type": "active" if external_active else "idle"
                },
                "updatedAt": 101,
                "turns": [
                    {
                        "id": "turn-external",
                        "status": (
                            "inProgress" if external_active else "completed"
                        ),
                        "completedAt": None if external_active else 103,
                        "items": [],
                    }
                ],
            }

        self.service.codex = SimpleNamespace(
            list_threads=AsyncMock(side_effect=list_threads),
            read_thread=AsyncMock(side_effect=read_thread),
            resume_thread=AsyncMock(return_value={}),
            start_turn=AsyncMock(return_value={"id": "turn-after-restart"}),
        )

        await self.service.sync_threads()

        self.service.codex.start_turn.assert_not_awaited()
        self.assertEqual(self.service.active_turn_count(), 1)
        self.assertEqual(
            self.store.queued_message(queued.queue_id).status,
            "pending",
        )

        external_active = False
        await self.service.sync_threads()

        self.service.codex.start_turn.assert_awaited_once_with(
            thread_id="thread-2",
            text="durable after restart",
            client_id="tg:-100500:91",
        )
        self.assertEqual(
            self.store.queued_message(queued.queue_id).status,
            "sent",
        )

    async def test_voice_message_starts_with_native_audio_input(self) -> None:
        audio = self.local_media_input()
        self.service._prepare_telegram_media = AsyncMock(  # type: ignore[method-assign]
            return_value=("🎙 Голосовой запрос", (audio,))
        )

        await self.service.handle_telegram_update(
            self.topic_media_message("voice")
        )

        self.codex.start_turn.assert_awaited_once_with(
            thread_id="thread-1",
            text="🎙 Голосовой запрос",
            client_id="tg:-100500:90",
            local_inputs=(audio,),
        )
        queued = self.store.queued_message_for_client_id("tg:-100500:90")
        self.assertIsNotNone(queued)
        self.assertEqual(queued.status, "sent")
        self.assertEqual(queued.local_inputs, (audio,))

    async def test_media_download_is_prepared_with_private_deterministic_path(
        self,
    ) -> None:
        audio = self.local_media_input(name="normalized.mp3")
        observed_keys: list[str] = []

        def source_path(media_key: str) -> Path:
            observed_keys.append(media_key)
            directory = self.service.config.media_directory / media_key
            directory.mkdir(parents=True, mode=0o700, exist_ok=True)
            return directory / "source.bin"

        def prepare_voice(**kwargs: Any) -> PreparedMedia:
            self.assertTrue(Path(kwargs["source_path"]).is_file())
            return PreparedMedia(
                kind="voice",
                duration_seconds=int(kwargs["duration_seconds"]),
                inputs=(audio,),
            )

        self.service.media = SimpleNamespace(
            prune=lambda **_: MediaPruneResult(0, 0),
            source_path=source_path,
            prepare_voice=prepare_voice,
        )
        update = self.topic_media_message("voice")

        text, inputs = await self.service._prepare_telegram_media(
            update["message"],
            client_id="tg:-100500:90",
            user_text="проверь",
        )

        self.assertEqual(self.telegram.file_requests, ["voice-file"])
        self.assertEqual(len(self.telegram.downloads), 1)
        self.assertEqual(len(observed_keys[0]), 32)
        self.assertNotIn("-100500", observed_keys[0])
        self.assertEqual(inputs, (audio,))
        self.assertIn("Комментарий пользователя: проверь", text)

    async def test_document_starts_with_native_mentioned_file_input(
        self,
    ) -> None:
        update = self.topic_media_message("document")
        update["message"]["caption"] = "Разнести эту выписку"

        await self.service.handle_telegram_update(update)

        self.codex.start_turn.assert_awaited_once()
        kwargs = self.codex.start_turn.await_args.kwargs
        self.assertIn("Документ из Telegram", kwargs["text"])
        self.assertIn(
            "Комментарий пользователя: Разнести эту выписку",
            kwargs["text"],
        )
        self.assertEqual(len(kwargs["local_inputs"]), 1)
        document = kwargs["local_inputs"][0]
        self.assertEqual(document.input_type, "mention")
        self.assertEqual(document.name, "ZEN statement July.csv")
        path = Path(document.path)
        self.assertTrue(path.is_file())
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        queued = self.store.queued_message_for_client_id("tg:-100500:90")
        self.assertIsNotNone(queued)
        self.assertEqual(queued.local_inputs, (document,))

    async def test_successful_local_stt_replaces_voice_input_with_transcript(
        self,
    ) -> None:
        audio = self.local_media_input(name="normalized.mp3")

        def source_path(media_key: str) -> Path:
            directory = self.service.config.media_directory / media_key
            directory.mkdir(parents=True, mode=0o700, exist_ok=True)
            return directory / "source.bin"

        self.service.media = SimpleNamespace(
            prune=lambda **_: MediaPruneResult(0, 0),
            source_path=source_path,
            prepare_voice=lambda **_: PreparedMedia(
                kind="voice",
                duration_seconds=7,
                inputs=(audio,),
            ),
        )
        self.service._local_stt_transcript = AsyncMock(  # type: ignore[method-assign]
            return_value="проверить бойлер"
        )

        text, inputs = await self.service._prepare_telegram_media(
            self.topic_media_message("voice")["message"],
            client_id="tg:-100500:90",
            user_text="срочно",
        )

        self.assertEqual(inputs, ())
        self.assertEqual(
            text,
            "срочно\n\n🎙 Расшифровка голосового сообщения:\n"
            "проверить бойлер",
        )

    async def test_remote_stt_replaces_voice_without_starting_local_stt(
        self,
    ) -> None:
        audio = self.local_media_input(name="normalized.mp3")
        self.service.media_worker = SimpleNamespace(
            transcribe=Mock(return_value="проверить бойлер"),
        )
        self.service._local_stt_transcript = AsyncMock(  # type: ignore[method-assign]
            return_value="local fallback must not run",
        )
        self.service.media.prepare_voice = Mock(  # type: ignore[method-assign]
            return_value=PreparedMedia(
                kind="voice",
                duration_seconds=7,
                inputs=(audio,),
            )
        )

        text, inputs = await self.service._prepare_telegram_media(
            self.topic_media_message("voice")["message"],
            client_id="tg:-100500:90",
            user_text="срочно",
        )

        self.service.media_worker.transcribe.assert_called_once()
        self.service._local_stt_transcript.assert_not_awaited()
        self.assertEqual(inputs, ())
        self.assertIn("проверить бойлер", text)

    async def test_configured_remote_stt_does_not_fallback_to_audio(
        self,
    ) -> None:
        audio = self.local_media_input(name="normalized.mp3")
        self.service.media_worker = SimpleNamespace(
            transcribe=Mock(side_effect=RuntimeError("offline")),
        )
        self.service.media.prepare_voice = Mock(  # type: ignore[method-assign]
            return_value=PreparedMedia(
                kind="voice",
                duration_seconds=7,
                inputs=(audio,),
            )
        )

        with self.assertRaisesRegex(
            MediaProcessingError,
            "transcription_unavailable",
        ):
            await self.service._prepare_telegram_media(
                self.topic_media_message("voice")["message"],
                client_id="tg:-100500:90",
                user_text="",
            )

    async def test_local_stt_is_not_admitted_while_a_turn_is_active(
        self,
    ) -> None:
        self.service.busy_threads.add("thread-1")
        self.service._linux_available_memory_bytes = (  # type: ignore[method-assign]
            lambda: 2 * 1024 * 1024 * 1024
        )

        self.assertEqual(
            self.service._local_stt_admission_reason(),
            "active_turn",
        )

    async def test_local_stt_is_not_admitted_under_memory_floor(
        self,
    ) -> None:
        self.service._linux_available_memory_bytes = (  # type: ignore[method-assign]
            lambda: 449 * 1024 * 1024
        )

        self.assertEqual(
            self.service._local_stt_admission_reason(),
            "low_memory",
        )

    async def test_local_stt_memory_floor_can_be_overridden(self) -> None:
        self.service._linux_available_memory_bytes = (  # type: ignore[method-assign]
            lambda: 128 * 1024 * 1024
        )
        with patch.dict(
            os.environ,
            {"CODEX_TELEGRAM_LOCAL_STT_MIN_AVAILABLE_MEMORY_MIB": "96"},
        ):
            self.assertIsNone(self.service._local_stt_admission_reason())

    async def test_document_without_caption_still_starts_a_turn(self) -> None:
        await self.service.handle_telegram_update(
            self.topic_media_message("document")
        )

        self.codex.start_turn.assert_awaited_once()
        kwargs = self.codex.start_turn.await_args.kwargs
        self.assertIn("Документ из Telegram", kwargs["text"])
        self.assertEqual(kwargs["local_inputs"][0].input_type, "mention")

    async def test_busy_document_queue_protects_downloaded_file(self) -> None:
        self.service.busy_threads.add("thread-1")

        await self.service.handle_telegram_update(
            self.topic_media_message("document")
        )

        queued = self.store.next_queued("thread-1")
        self.assertIsNotNone(queued)
        self.assertEqual(queued.local_inputs[0].input_type, "mention")
        document_path = Path(queued.local_inputs[0].path)
        self.assertIn(document_path, self.store.active_local_input_paths())
        self.codex.start_turn.assert_not_awaited()

    async def test_video_note_queue_keeps_audio_and_frames_for_steer(
        self,
    ) -> None:
        audio = self.local_media_input(name="circle.mp3")
        frame = self.local_media_input(
            name="frame-01.jpg",
            input_type="localImage",
        )
        inputs = (audio, frame)
        self.service._prepare_telegram_media = AsyncMock(  # type: ignore[method-assign]
            return_value=("⭕ Видеокружок", inputs)
        )
        self.service.busy_threads.add("thread-1")

        await self.service.handle_telegram_update(
            self.topic_media_message("video_note")
        )

        queued = self.store.next_queued("thread-1")
        self.assertIsNotNone(queued)
        self.assertEqual(queued.local_inputs, inputs)
        self.service.active_turns["thread-1"] = "turn-active"
        callback = self.queue_callback(queued.queue_id)
        callback["message"]["message_id"] = queued.status_message_id
        await self.service.handle_queue_steer(queued.queue_id, callback)

        self.codex.steer_turn.assert_awaited_once_with(
            thread_id="thread-1",
            turn_id="turn-active",
            text="⭕ Видеокружок",
            client_id="tg:-100500:90",
            local_inputs=inputs,
        )
        self.assertEqual(
            self.store.queued_message(queued.queue_id).status,
            "sent",
        )

    async def test_standard_video_starts_with_audio_and_frame_inputs(
        self,
    ) -> None:
        audio = self.local_media_input(name="video.mp3")
        frame = self.local_media_input(
            name="video-frame-01.jpg",
            input_type="localImage",
        )
        inputs = (audio, frame)
        self.service._prepare_telegram_media = AsyncMock(  # type: ignore[method-assign]
            return_value=("🎬 Видео", inputs)
        )

        await self.service.handle_telegram_update(
            self.topic_media_message("video")
        )

        self.codex.start_turn.assert_awaited_once_with(
            thread_id="thread-1",
            text="🎬 Видео",
            client_id="tg:-100500:90",
            local_inputs=inputs,
        )

    async def test_degraded_voice_message_is_durably_queued(self) -> None:
        audio = self.local_media_input()
        self.service._prepare_telegram_media = AsyncMock(  # type: ignore[method-assign]
            return_value=("🎙 Голосовой запрос", (audio,))
        )
        await self.service._enter_codex_degraded(
            "socket_unavailable",
            expire_prompts=False,
        )

        await self.service.handle_telegram_update(
            self.topic_media_message("voice")
        )

        queued = self.store.next_queued("thread-1")
        self.assertIsNotNone(queued)
        self.assertEqual(queued.local_inputs, (audio,))
        self.codex.start_turn.assert_not_awaited()
        self.assertIn("сохранено в очереди", self.telegram.sent_messages[-1]["text"])

    async def test_oversized_video_note_gets_clear_telegram_limit_message(
        self,
    ) -> None:
        await self.service.handle_telegram_update(
            self.topic_media_message(
                "video_note",
                file_size=self.service.config.telegram_media_max_bytes + 1,
            )
        )

        self.codex.start_turn.assert_not_awaited()
        self.assertIn("20 МБ", self.telegram.sent_messages[-1]["text"])

    async def test_degraded_plain_message_is_durably_queued_once(self) -> None:
        await self.service._enter_codex_degraded(
            "socket_unavailable",
            expire_prompts=False,
        )

        update = self.topic_message("сохрани до восстановления")
        await self.service.handle_telegram_update(update)
        await self.service.handle_telegram_update(update)

        queued = self.store.next_queued("thread-1")
        self.assertIsNotNone(queued)
        self.assertEqual(queued.text, "сохрани до восстановления")
        self.assertEqual(len(self.telegram.sent_messages), 1)
        self.assertIn("сохранено в очереди", self.telegram.sent_messages[0]["text"])
        self.assertEqual(
            self.telegram.sent_messages[0]["reply_markup"],
            {"inline_keyboard": []},
        )
        self.codex.start_turn.assert_not_awaited()
        self.codex.steer_turn.assert_not_awaited()

    async def test_mid_turn_socket_failure_queues_with_one_response(self) -> None:
        self.codex.start_turn.side_effect = CodexProtocolError(
            "private protocol detail"
        )

        await self.service.handle_telegram_update(
            self.topic_message("не потеряй этот запрос")
        )

        queued = self.store.queued_message_for_client_id("tg:-100500:90")
        self.assertIsNotNone(queued)
        self.assertEqual(queued.text, "не потеряй этот запрос")
        self.assertEqual(queued.status, "dispatching")
        self.assertFalse(self.service.codex_available)
        self.assertEqual(len(self.telegram.sent_messages), 1)
        self.assertNotIn(
            "private protocol detail",
            self.telegram.sent_messages[0]["text"],
        )
        self.assertEqual(
            self.telegram.sent_messages[0]["reply_markup"],
            {"inline_keyboard": []},
        )

    async def test_degraded_mutating_controls_fail_closed(self) -> None:
        pending = self.add_pending_approval(public_id="degraded-approval")
        await self.service._enter_codex_degraded(
            "split_brain",
            expire_prompts=False,
        )

        await self.service.handle_telegram_update(
            self.topic_message("/steer вмешайся", message_id=91)
        )
        await self.service.handle_telegram_update(
            self.topic_message("/new новый тред", message_id=92)
        )
        await self.service.handle_callback(
            {
                "id": "new-while-degraded",
                "data": "newthread",
                "from": {"id": 100, "is_bot": False},
                "message": {
                    "message_id": 93,
                    "message_thread_id": 50,
                    "chat": {"id": -100500, "type": "supergroup"},
                },
            }
        )
        await self.service.handle_callback(
            {
                "id": "approval-while-degraded",
                "data": f"apr:{pending.public_id}:once",
                "from": {"id": 100, "is_bot": False},
                "message": {
                    "message_id": 70,
                    "message_thread_id": 50,
                    "chat": {"id": -100500, "type": "supergroup"},
                },
            }
        )

        self.codex.start_turn.assert_not_awaited()
        self.codex.steer_turn.assert_not_awaited()
        self.codex.respond.assert_not_awaited()
        self.assertIsNone(self.store.next_queued("thread-1"))
        self.assertEqual(len(self.telegram.sent_messages), 2)
        self.assertTrue(
            all(
                "только очереди" in message["text"]
                for message in self.telegram.sent_messages
            )
        )
        self.assertIn(
            "временно недоступен",
            self.telegram.callback_answers[-1][1],
        )

    async def test_degraded_status_is_sanitized_and_has_no_unsafe_controls(
        self,
    ) -> None:
        self.service.codex_last_healthy_at = 1.0
        self.service.codex_last_healthy_version = "tested-version"
        self.service.set_codex_guard("raw secret should not be shown")

        await self.service.handle_telegram_update(
            self.topic_message("/status", message_id=94)
        )

        status = self.telegram.sent_messages[-1]
        self.assertIn("только очередь", status["text"])
        self.assertIn("проверка совместимости", status["text"])
        self.assertNotIn("raw secret", status["text"])
        callback_data = [
            button["callback_data"]
            for row in status["reply_markup"]["inline_keyboard"]
            for button in row
        ]
        self.assertEqual(callback_data, ["ctl:refresh"])

    async def test_edited_message_is_ignored_instead_of_becoming_a_new_turn(
        self,
    ) -> None:
        update = self.topic_message("исправленный текст")
        update["edited_message"] = update.pop("message")

        await self.service.handle_telegram_update(update)

        self.codex.start_turn.assert_not_awaited()
        self.codex.steer_turn.assert_not_awaited()
        self.assertIsNone(self.store.next_queued("thread-1"))
        self.assertEqual(self.telegram.sent_messages, [])

    async def test_new_without_payload_opens_force_reply_field(self) -> None:
        await self.service.handle_telegram_update(self.topic_message("/new"))

        prompt = self.telegram.sent_messages[-1]
        self.assertEqual(prompt["text"], NEW_THREAD_PROMPT)
        self.assertTrue(prompt["reply_markup"]["force_reply"])
        self.codex.start_turn.assert_not_awaited()

    async def test_limits_reports_remaining_weekly_codex_percent(self) -> None:
        self.codex.read_rate_limits.return_value = {
            "rateLimits": {
                "primary": {
                    "usedPercent": 6,
                    "windowDurationMins": 10_080,
                    "resetsAt": 1_800_000_000,
                },
            },
            "rateLimitsByLimitId": {
                "codex_other": {
                    "primary": {
                        "usedPercent": 0,
                        "windowDurationMins": 10_080,
                    },
                },
            },
        }

        await self.service.handle_telegram_update(
            self.topic_message(
                "/limits@project_bridge_bot",
                message_id=95,
            )
        )

        self.codex.read_rate_limits.assert_awaited_once_with()
        message = self.telegram.sent_messages[-1]
        self.assertEqual(
            message["text"],
            "📊 Недельный лимит Codex: осталось 94%.",
        )
        self.assertEqual(message["message_thread_id"], 50)
        self.assertEqual(message["reply_to_message_id"], 95)
        self.codex.start_turn.assert_not_awaited()

    async def test_limits_works_inside_archive_service_topic(self) -> None:
        self.store.set_archive_hub_topic_id(50)
        self.codex.read_rate_limits.return_value = {
            "rateLimits": {
                "primary": {
                    "usedPercent": 12.5,
                    "windowDurationMins": 10_080,
                },
            },
        }

        await self.service.handle_telegram_update(
            self.topic_message("/limits", message_id=96)
        )

        self.assertEqual(
            self.telegram.sent_messages[-1]["text"],
            "📊 Недельный лимит Codex: осталось 87,5%.",
        )

    async def test_limits_fails_closed_when_weekly_window_is_missing(
        self,
    ) -> None:
        self.codex.read_rate_limits.return_value = {
            "rateLimits": {
                "primary": {
                    "usedPercent": 20,
                    "windowDurationMins": 300,
                },
            },
        }

        await self.service.handle_telegram_update(
            self.topic_message("/limits", message_id=97)
        )

        self.assertIn(
            "не передал недельное окно",
            self.telegram.sent_messages[-1]["text"],
        )
        self.codex.start_turn.assert_not_awaited()

    async def test_limits_does_not_query_codex_while_degraded(self) -> None:
        self.service.set_codex_guard("protocol_incompatible")

        await self.service.handle_telegram_update(
            self.topic_message("/limits", message_id=98)
        )

        self.codex.read_rate_limits.assert_not_awaited()
        text = self.telegram.sent_messages[-1]["text"]
        self.assertIn("недоступен", text)
        self.assertIn("версия протокола Codex ещё не проверена", text)

    async def test_archive_command_calls_codex_and_never_starts_a_turn(
        self,
    ) -> None:
        await self.service.handle_telegram_update(
            self.topic_message("/archive")
        )

        self.codex.archive_thread.assert_awaited_once_with("thread-1")
        self.codex.start_turn.assert_not_awaited()
        self.assertIsNone(self.store.next_queued("thread-1"))
        self.assertTrue(self.service._sync_requested.is_set())
        self.assertEqual(self.telegram.sent_messages, [])

    async def test_archive_command_rejects_active_or_queued_work(self) -> None:
        self.service.busy_threads.add("thread-1")
        await self.service.handle_telegram_update(
            self.topic_message("/archive", message_id=95)
        )
        self.service.busy_threads.clear()
        self.store.enqueue(
            thread_id="thread-1",
            chat_id=-100500,
            topic_id=50,
            telegram_message_id=96,
            text="сначала обработай это",
            client_id="tg:-100500:96",
        )
        await self.service.handle_telegram_update(
            self.topic_message("/archive", message_id=97)
        )

        self.codex.archive_thread.assert_not_awaited()
        self.assertIn("активный ход", self.telegram.sent_messages[-2]["text"])
        self.assertIn("очереди", self.telegram.sent_messages[-1]["text"])

    async def test_archive_command_requires_exact_working_topic(self) -> None:
        update = self.topic_message("/archive", message_id=98)
        update["message"]["message_thread_id"] = 1

        await self.service.handle_telegram_update(update)

        self.codex.archive_thread.assert_not_awaited()
        self.assertIn(
            "только в связанном рабочем Topic",
            self.telegram.sent_messages[-1]["text"],
        )

    async def test_archive_command_rejects_arguments(self) -> None:
        await self.service.handle_telegram_update(
            self.topic_message("/archive сейчас", message_id=99)
        )

        self.codex.archive_thread.assert_not_awaited()
        self.assertEqual(
            self.telegram.sent_messages[-1]["text"],
            "Формат: /archive",
        )

    async def test_new_thread_button_opens_force_reply_field(self) -> None:
        self.store.save_control_status_card(
            thread_id="thread-1",
            chat_id=-100500,
            topic_id=50,
            telegram_message_id=91,
        )
        callback = {
            "id": "new-callback",
            "data": "newthread",
            "from": {"id": 100, "is_bot": False},
            "message": {
                "message_id": 91,
                "message_thread_id": 50,
                "chat": {"id": -100500, "type": "supergroup"},
            },
        }

        await self.service.handle_callback(callback)

        self.assertEqual(
            self.telegram.callback_answers[-1],
            ("new-callback", "Опишите новую задачу"),
        )
        prompt = self.telegram.sent_messages[-1]
        self.assertEqual(prompt["text"], NEW_THREAD_PROMPT)
        self.assertTrue(prompt["reply_markup"]["force_reply"])

    async def test_sta…31500 tokens truncated…       self,
    ) -> None:
        service = BridgeService(
            config=self.service.config,
            store=self.store,
            telegram=self.telegram,
        )
        service.codex = SimpleNamespace(stop=AsyncMock())
        service.expire_stale_prompt_cards = AsyncMock()
        ready = Mock()
        critical_started = asyncio.Event()
        never_finishes = asyncio.Event()

        async def block() -> None:
            critical_started.set()
            await never_finishes.wait()

        service.telegram_loop = block
        service.codex_connection_loop = block
        service.progress_heartbeat_loop = block
        service.telegram_setup_loop = block

        serve_task = asyncio.create_task(service.serve(on_ready=ready))
        await asyncio.wait_for(critical_started.wait(), timeout=1)

        service.expire_stale_prompt_cards.assert_awaited_once()
        ready.assert_called_once_with()
        service.stop()
        await asyncio.wait_for(serve_task, timeout=1)
        service.codex.stop.assert_awaited_once()

    async def test_default_initial_history_mirrors_every_visible_item(
        self,
    ) -> None:
        self.assertEqual(self.service.config.initial_history_messages, 0)
        thread = {
            "id": "thread-1",
            "turns": [
                {
                    "id": f"turn-full-{index}",
                    "status": "completed",
                    "items": [
                        {
                            "id": f"full-final-{index}",
                            "type": "agentMessage",
                            "text": f"Полная история {index}",
                            "phase": "final_answer",
                        }
                    ],
                }
                for index in range(7)
            ],
        }
        topic = self.store.topic_for_thread("thread-1")
        self.assertIsNotNone(topic)

        await self.service.sync_thread_history(thread, topic, initial=True)

        self.assertEqual(len(self.telegram.sent_messages), 7)
        self.assertTrue(
            all(
                any(
                    f"Полная история {index}" in message["text"]
                    for message in self.telegram.sent_messages
                )
                for index in range(7)
            )
        )

    async def test_interrupted_initial_history_backfill_is_bounded_and_idempotent(
        self,
    ) -> None:
        self.service.config = replace(
            self.service.config,
            initial_history_messages=3,
        )
        items = [
            {
                "id": f"item-{index}",
                "type": "agentMessage",
                "text": f"history body {index}",
                "phase": "final_answer",
            }
            for index in range(7)
        ]
        items.append(
            {
                "id": "reasoning-old",
                "type": "reasoning",
                "summary": ["old internal summary"],
            }
        )
        full_thread = {
            "id": "thread-1",
            "updatedAt": 123,
            "status": {"type": "idle"},
            "turns": [
                {
                    "id": "turn-history",
                    "status": "completed",
                    "items": items,
                }
            ],
        }
        summary = {
            "id": "thread-1",
            "name": "Test topic",
            "preview": "",
            "updatedAt": 123,
        }

        async def list_threads(*, archived: bool) -> list[dict[str, Any]]:
            return [] if archived else [summary]

        self.service.codex = SimpleNamespace(
            list_threads=AsyncMock(side_effect=list_threads),
            read_thread=AsyncMock(return_value=full_thread),
            resume_thread=AsyncMock(return_value={}),
        )
        # Simulate an interrupted earlier backfill: one old item and one item
        # inside the latest-three delivery window were already recorded.
        self.store.mark_mirrored_item(
            "thread-1",
            "item-1",
            "agentMessage",
            None,
        )
        self.store.mark_mirrored_item(
            "thread-1",
            "item-5",
            "agentMessage",
            805,
        )
        self.assertFalse(self.store.initial_history_complete("thread-1"))

        with patch.object(
            self.store,
            "mark_mirrored_items",
            wraps=self.store.mark_mirrored_items,
        ) as bulk_mark:
            await self.service.sync_threads()

        sent_bodies = [message["text"] for message in self.telegram.sent_messages]
        self.assertEqual(len(sent_bodies), 2)
        self.assertTrue(any("history body 4" in text for text in sent_bodies))
        self.assertTrue(any("history body 6" in text for text in sent_bodies))
        self.assertFalse(any("history body 0" in text for text in sent_bodies))
        self.assertFalse(any("history body 2" in text for text in sent_bodies))
        self.assertFalse(any("history body 3" in text for text in sent_bodies))

        bulk_mark.assert_called_once()
        bulk_ids = {entry[1] for entry in bulk_mark.call_args.args[0]}
        self.assertEqual(
            bulk_ids,
            {"item-0", "item-2", "item-3", "reasoning-old"},
        )
        mirrored_count = self.store.connection.execute(
            "SELECT COUNT(*) FROM mirrored_items WHERE thread_id = ?",
            ("thread-1",),
        ).fetchone()[0]
        self.assertEqual(mirrored_count, len(items))
        self.assertTrue(self.store.initial_history_complete("thread-1"))

        # A fresh service process performs one startup read, but the completion
        # marker plus per-item records must make it a no-op for Telegram.
        self.service._initial_sync = True
        await self.service.sync_threads()

        self.assertEqual(len(self.telegram.sent_messages), 2)
        self.assertEqual(self.service.codex.read_thread.await_count, 2)
        self.assertEqual(self.service.codex.resume_thread.await_count, 2)

    async def test_concurrent_mirror_item_sends_and_records_exactly_once(
        self,
    ) -> None:
        topic = self.store.topic_for_thread("thread-1")
        self.assertIsNotNone(topic)
        item = {
            "id": "concurrent-item",
            "type": "agentMessage",
            "text": "one visible answer",
            "phase": "final_answer",
        }

        await asyncio.gather(
            self.service.mirror_item(topic, item),
            self.service.mirror_item(topic, item),
        )

        self.assertEqual(len(self.telegram.sent_messages), 1)
        self.assertIn("one visible answer", self.telegram.sent_messages[0]["text"])
        rows = self.store.connection.execute(
            """
            SELECT item_type, telegram_message_id
            FROM mirrored_items
            WHERE thread_id = ? AND item_id = ?
            """,
            ("thread-1", "concurrent-item"),
        ).fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["item_type"], "agentMessage")
        self.assertIsNotNone(rows[0]["telegram_message_id"])

    async def test_notification_and_history_sync_race_sends_item_once(self) -> None:
        topic = self.store.topic_for_thread("thread-1")
        self.assertIsNotNone(topic)
        item = {
            "id": "notification-history-race",
            "type": "agentMessage",
            "text": "race-safe answer",
            "phase": "final_answer",
        }
        notification = {
            "method": "item/completed",
            "params": {
                "threadId": "thread-1",
                "item": item,
            },
        }
        thread = {
            "id": "thread-1",
            "turns": [
                {
                    "id": "turn-race",
                    "items": [item],
                }
            ],
        }

        await asyncio.gather(
            self.service.on_codex_notification(notification),
            self.service.sync_thread_history(
                thread,
                topic,
                initial=False,
            ),
        )

        self.assertEqual(len(self.telegram.sent_messages), 1)
        self.assertIn("race-safe answer", self.telegram.sent_messages[0]["text"])
        mirrored_count = self.store.connection.execute(
            """
            SELECT COUNT(*) FROM mirrored_items
            WHERE thread_id = ? AND item_id = ?
            """,
            ("thread-1", "notification-history-race"),
        ).fetchone()[0]
        self.assertEqual(mirrored_count, 1)

    async def test_live_final_waits_for_history_user_reply_context(self) -> None:
        topic = self.store.topic_for_thread("thread-1")
        self.assertIsNotNone(topic)
        notification_final = {
            "id": "notification-ordered-final",
            "type": "agentMessage",
            "phase": "final_answer",
            "text": "Финал после пользователя.",
        }

        await self.service.on_codex_notification(
            {
                "method": "item/completed",
                "params": {
                    "threadId": "thread-1",
                    "turnId": "turn-ordered",
                    "item": notification_final,
                },
            }
        )

        self.assertEqual(self.telegram.sent_messages, [])
        self.assertTrue(self.service._sync_requested.is_set())

        await self.service.sync_thread_history(
            {
                "id": "thread-1",
                "turns": [
                    {
                        "id": "turn-ordered",
                        "status": "completed",
                        "items": [
                            {
                                "id": "history-ordered-user",
                                "type": "userMessage",
                                "content": [
                                    {
                                        "type": "text",
                                        "text": "Сообщение из Codex App.",
                                    }
                                ],
                            },
                            {
                                **notification_final,
                                "id": "history-ordered-final",
                            },
                        ],
                    }
                ],
            },
            topic,
            initial=False,
        )

        self.assertEqual(len(self.telegram.sent_messages), 2)
        self.assertIn(
            "Сообщение из Codex App.",
            self.telegram.sent_messages[0]["text"],
        )
        self.assertIn(
            "Финал после пользователя.",
            self.telegram.sent_messages[1]["text"],
        )
        self.assertEqual(
            self.telegram.sent_messages[1]["reply_to_message_id"],
            801,
        )

    async def test_notification_and_history_different_ids_send_final_once(
        self,
    ) -> None:
        topic = self.store.topic_for_thread("thread-1")
        self.assertIsNotNone(topic)
        notification = {
            "method": "item/completed",
            "params": {
                "threadId": "thread-1",
                "turnId": "turn-final-alias",
                "item": {
                    "id": "notification-final-id",
                    "type": "agentMessage",
                    "text": "Один финальный ответ.",
                    "phase": "final_answer",
                },
            },
        }
        history = {
            "id": "thread-1",
            "turns": [
                {
                    "id": "turn-final-alias",
                    "status": "completed",
                    "items": [
                        {
                            "id": "history-final-id",
                            "type": "agentMessage",
                            "text": "Один финальный ответ.",
                            "phase": "final_answer",
                        }
                    ],
                }
            ],
        }
        self.store.upsert_turn_context(
            thread_id="thread-1",
            turn_id="turn-final-alias",
            source_message_id=90,
        )

        await self.service.on_codex_notification(notification)
        await self.service.on_codex_notification(
            {
                "method": "turn/completed",
                "params": {
                    "threadId": "thread-1",
                    "turn": {
                        "id": "turn-final-alias",
                        "status": "completed",
                    },
                },
            }
        )
        await self.service.sync_thread_history(
            history,
            topic,
            initial=False,
        )

        self.assertEqual(len(self.telegram.sent_messages), 1)
        self.assertIn(
            "Один финальный ответ.",
            self.telegram.sent_messages[0]["text"],
        )
        self.assertTrue(
            self.store.has_mirrored_item(
                "thread-1",
                "notification-final-id",
            )
        )
        self.assertTrue(
            self.store.has_mirrored_item(
                "thread-1",
                "history-final-id",
            )
        )
        delivery = self.store.connection.execute(
            """
            SELECT primary_item_id, counterpart_item_id
            FROM visible_item_deliveries
            WHERE thread_id = ? AND turn_id = ?
            """,
            ("thread-1", "turn-final-alias"),
        ).fetchone()
        self.assertEqual(delivery["primary_item_id"], "notification-final-id")
        self.assertEqual(delivery["counterpart_item_id"], "history-final-id")

    async def test_notification_and_history_send_attachment_once(
        self,
    ) -> None:
        topic = self.store.topic_for_thread("thread-1")
        self.assertIsNotNone(topic)
        output_directory = self.service.config.workspace / "outputs"
        output_directory.mkdir()
        report = output_directory / "deduplicated.xlsx"
        report.write_bytes(b"spreadsheet")
        body = (
            "Файл готов. "
            f":codex-file-citation{{path=\"{report}\" purpose=\"output\"}}"
        )
        self.store.upsert_turn_context(
            thread_id="thread-1",
            turn_id="turn-file-alias",
            source_message_id=90,
        )

        await self.service.mirror_item(
            topic,
            {
                "id": "notification-file-final",
                "type": "agentMessage",
                "phase": "final_answer",
                "text": body,
            },
            turn_id="turn-file-alias",
            item_origin="notification",
        )
        await self.service.mirror_item(
            topic,
            {
                "id": "history-file-final",
                "type": "agentMessage",
                "phase": "final_answer",
                "text": body,
            },
            turn_id="turn-file-alias",
            item_origin="history",
        )

        self.assertEqual(len(self.telegram.sent_messages), 1)
        self.assertEqual(len(self.telegram.sent_documents), 1)
        delivery = self.store.connection.execute(
            """
            SELECT primary_item_id, counterpart_item_id
            FROM visible_item_deliveries
            WHERE thread_id = ? AND turn_id = ?
              AND item_type = 'finalAttachment'
            """,
            ("thread-1", "turn-file-alias"),
        ).fetchone()
        self.assertIsNotNone(delivery)
        self.assertIn(
            "notification-file-final:attachment:",
            delivery["primary_item_id"],
        )
        self.assertIn(
            "history-file-final:attachment:",
            delivery["counterpart_item_id"],
        )

    async def test_ambiguous_final_send_is_not_replayed(self) -> None:
        topic = self.store.topic_for_thread("thread-1")
        self.assertIsNotNone(topic)

        def fail_ambiguously(**_: Any) -> list[dict[str, Any]]:
            raise TelegramError(
                "response lost",
                method="sendMessage",
                kind="network_timeout",
                retryable=True,
                outcome_ambiguous=True,
            )

        self.telegram.send_message = fail_ambiguously  # type: ignore[method-assign]
        with self.assertRaises(TelegramError):
            await self.service.mirror_item(
                topic,
                {
                    "id": "notification-ambiguous-final",
                    "type": "agentMessage",
                    "phase": "final_answer",
                    "text": "Не дублировать финал.",
                },
                turn_id="turn-ambiguous-final",
                item_origin="notification",
            )

        restarted_telegram = FakeTelegram()
        restarted = BridgeService(
            config=self.service.config,
            store=self.store,
            telegram=restarted_telegram,
        )
        restarted.codex = self.codex
        await restarted.mirror_item(
            topic,
            {
                "id": "history-ambiguous-final",
                "type": "agentMessage",
                "phase": "final_answer",
                "text": "Не дублировать финал.",
            },
            turn_id="turn-ambiguous-final",
            item_origin="history",
        )

        self.assertEqual(restarted_telegram.sent_messages, [])
        self.assertTrue(
            self.store.has_mirrored_item(
                "thread-1",
                "history-ambiguous-final",
            )
        )

    async def test_ambiguous_attachment_upload_is_not_replayed(self) -> None:
        topic = self.store.topic_for_thread("thread-1")
        self.assertIsNotNone(topic)
        output_directory = self.service.config.workspace / "outputs"
        output_directory.mkdir()
        report = output_directory / "ambiguous.xlsx"
        report.write_bytes(b"spreadsheet")
        body = (
            "Файл готов. "
            f":codex-file-citation{{path=\"{report}\" purpose=\"output\"}}"
        )

        def fail_ambiguously(**_: Any) -> dict[str, Any]:
            raise TelegramError(
                "response lost",
                method="sendDocument",
                kind="network_timeout",
                retryable=True,
                outcome_ambiguous=True,
            )

        self.telegram.send_attachment = (  # type: ignore[method-assign]
            fail_ambiguously
        )
        with self.assertRaises(TelegramError):
            await self.service.mirror_item(
                topic,
                {
                    "id": "notification-ambiguous-file",
                    "type": "agentMessage",
                    "phase": "final_answer",
                    "text": body,
                },
                turn_id="turn-ambiguous-file",
                item_origin="notification",
            )

        restarted_telegram = FakeTelegram()
        restarted = BridgeService(
            config=self.service.config,
            store=self.store,
            telegram=restarted_telegram,
        )
        restarted.codex = self.codex
        await restarted.mirror_item(
            topic,
            {
                "id": "history-ambiguous-file",
                "type": "agentMessage",
                "phase": "final_answer",
                "text": body,
            },
            turn_id="turn-ambiguous-file",
            item_origin="history",
        )

        self.assertEqual(restarted_telegram.sent_messages, [])
        self.assertEqual(restarted_telegram.sent_documents, [])
        health = self.store.delivery_uncertainty_health()
        self.assertEqual(health["finalAttachmentsOutcomeUnknown"], 1)
        self.assertTrue(
            self.store.has_mirrored_item(
                "thread-1",
                "history-ambiguous-file",
            )
        )

    async def test_history_then_notification_different_ids_send_user_once(
        self,
    ) -> None:
        topic = self.store.topic_for_thread("thread-1")
        self.assertIsNotNone(topic)
        history_item = {
            "id": "history-user-id",
            "type": "userMessage",
            "content": [{"type": "text", "text": "Сообщение из Codex App."}],
        }
        notification_item = {
            "id": "notification-user-id",
            "type": "userMessage",
            "content": [{"type": "text", "text": "Сообщение из Codex App."}],
        }
        history = {
            "id": "thread-1",
            "turns": [
                {
                    "id": "turn-user-alias",
                    "items": [history_item],
                }
            ],
        }

        await self.service.sync_thread_history(
            history,
            topic,
            initial=False,
        )
        await self.service.on_codex_notification(
            {
                "method": "item/completed",
                "params": {
                    "threadId": "thread-1",
                    "turnId": "turn-user-alias",
                    "item": notification_item,
                },
            }
        )

        self.assertEqual(len(self.telegram.sent_messages), 1)
        self.assertIn(
            "Сообщение из Codex App.",
            self.telegram.sent_messages[0]["text"],
        )
        delivery = self.store.connection.execute(
            """
            SELECT primary_item_id, counterpart_item_id
            FROM visible_item_deliveries
            WHERE thread_id = ? AND turn_id = ?
            """,
            ("thread-1", "turn-user-alias"),
        ).fetchone()
        self.assertEqual(delivery["primary_item_id"], "history-user-id")
        self.assertEqual(
            delivery["counterpart_item_id"],
            "notification-user-id",
        )

    async def test_history_reconciles_telegram_source_and_ambiguous_queue(
        self,
    ) -> None:
        topic = self.store.topic_for_thread("thread-1")
        self.assertIsNotNone(topic)
        queued = self.store.enqueue(
            thread_id="thread-1",
            chat_id=-100500,
            topic_id=50,
            telegram_message_id=90,
            text="Запустить один раз.",
            client_id="tg:-100500:90",
        )
        self.store.mark_queue(queued.queue_id, "dispatching")
        history = {
            "id": "thread-1",
            "turns": [
                {
                    "id": "turn-telegram-recovery",
                    "status": "completed",
                    "items": [
                        {
                            "id": "telegram-user-history",
                            "type": "userMessage",
                            "clientId": "tg:-100500:90",
                            "content": [
                                {
                                    "type": "text",
                                    "text": "Запустить один раз.",
                                }
                            ],
                        },
                        {
                            "id": "telegram-final-history",
                            "type": "agentMessage",
                            "phase": "final_answer",
                            "text": "Готово.",
                        },
                    ],
                }
            ],
        }

        await self.service.sync_thread_history(
            history,
            topic,
            initial=False,
        )

        self.assertEqual(
            self.store.queued_message(queued.queue_id).status,
            "sent",
        )
        context = self.store.turn_context(
            "thread-1",
            "turn-telegram-recovery",
        )
        self.assertIsNotNone(context)
        self.assertEqual(context.source_message_id, 90)
        finals = [
            message
            for message in self.telegram.sent_messages
            if "Готово." in message.get("text", "")
        ]
        self.assertEqual(len(finals), 1)
        self.assertEqual(finals[0]["reply_to_message_id"], 90)
        self.codex.start_turn.assert_not_awaited()

    async def test_queue_announcement_ambiguous_send_is_not_replayed(
        self,
    ) -> None:
        queued = self.store.enqueue(
            thread_id="thread-1",
            chat_id=-100500,
            topic_id=50,
            telegram_message_id=90,
            text="Один раз.",
            client_id="queue-card-ambiguous",
        )
        remote_messages: list[dict[str, Any]] = []

        def applied_but_response_lost(**kwargs: Any) -> list[dict[str, Any]]:
            remote_messages.append(dict(kwargs))
            raise TelegramError(
                "response lost SECRET_QUEUE_TEXT",
                method="sendMessage",
                kind="network_timeout",
                retryable=True,
                outcome_ambiguous=True,
            )

        self.telegram.send_message = applied_but_response_lost  # type: ignore[method-assign]
        with self.assertRaises(TelegramError):
            await self.service._announce_queued(
                queued,
                text="↪️ Сохранено.",
            )

        restarted_telegram = FakeTelegram()
        restarted = BridgeService(
            config=self.service.config,
            store=self.store,
            telegram=restarted_telegram,
        )
        await restarted._announce_queued(
            queued,
            text="↪️ Сохранено.",
        )

        self.assertEqual(len(remote_messages), 1)
        self.assertEqual(restarted_telegram.sent_messages, [])
        health = self.store.delivery_uncertainty_health()
        self.assertEqual(health["queueAnnouncementsOutcomeUnknown"], 1)

    async def test_queue_announcement_post_send_crash_is_not_replayed(
        self,
    ) -> None:
        queued = self.store.enqueue(
            thread_id="thread-1",
            chat_id=-100500,
            topic_id=50,
            telegram_message_id=90,
            text="Один раз при crash.",
            client_id="queue-card-crash",
        )
        original_complete = (
            self.store.complete_queue_announcement_delivery
        )

        def crash_before_commit(
            queue_id: int,
            telegram_message_id: int,
        ) -> None:
            raise RuntimeError("simulated process crash")

        self.store.complete_queue_announcement_delivery = crash_before_commit  # type: ignore[method-assign]
        with self.assertRaisesRegex(RuntimeError, "simulated process crash"):
            await self.service._announce_queued(
                queued,
                text="↪️ Сохранено.",
            )
        self.store.complete_queue_announcement_delivery = original_complete  # type: ignore[method-assign]

        restarted_telegram = FakeTelegram()
        restarted = BridgeService(
            config=self.service.config,
            store=self.store,
            telegram=restarted_telegram,
        )
        await restarted._announce_queued(
            queued,
            text="↪️ Сохранено.",
        )

        self.assertEqual(len(self.telegram.sent_messages), 1)
        self.assertEqual(restarted_telegram.sent_messages, [])
        health = self.store.delivery_uncertainty_health()
        self.assertEqual(health["queueAnnouncementsReserved"], 1)

    async def test_concurrent_queue_announcement_has_one_sender(self) -> None:
        queued = self.store.enqueue(
            thread_id="thread-1",
            chat_id=-100500,
            topic_id=50,
            telegram_message_id=90,
            text="Одна карточка.",
            client_id="queue-card-concurrent",
        )

        await asyncio.gather(
            self.service._announce_queued(queued, text="↪️ Сохранено."),
            self.service._announce_queued(queued, text="↪️ Сохранено."),
        )

        self.assertEqual(len(self.telegram.sent_messages), 1)

    async def test_dispatching_queue_waits_for_two_fresh_history_misses(
        self,
    ) -> None:
        topic = self.store.topic_for_thread("thread-1")
        self.assertIsNotNone(topic)
        queued = self.store.enqueue(
            thread_id="thread-1",
            chat_id=-100500,
            topic_id=50,
            telegram_message_id=90,
            text="Не дублировать ход.",
            client_id="dispatch-reconciliation",
        )
        self.store.mark_queue(queued.queue_id, "dispatching")
        self.store.connection.execute(
            """
            UPDATE queued_messages
            SET dispatch_started_at = ?
            WHERE id = ?
            """,
            ("2000-01-01T00:00:00+00:00", queued.queue_id),
        )
        self.store.connection.commit()
        empty_history = {"id": "thread-1", "turns": []}

        await self.service.sync_thread_history(
            empty_history,
            topic,
            initial=False,
        )
        self.assertEqual(
            self.store.queued_message(queued.queue_id).status,
            "dispatching",
        )

        await self.service.sync_thread_history(
            empty_history,
            topic,
            initial=False,
        )
        self.assertEqual(
            self.store.queued_message(queued.queue_id).status,
            "pending",
        )
        self.codex.start_turn.assert_not_awaited()

    async def test_sync_threads_keeps_reconciling_idle_dispatching_queue(
        self,
    ) -> None:
        queued = self.store.enqueue(
            thread_id="thread-1",
            chat_id=-100500,
            topic_id=50,
            telegram_message_id=90,
            text="Проверить историю повторно.",
            client_id="dispatch-sync-gate",
        )
        self.store.mark_queue(queued.queue_id, "dispatching")
        self.store.connection.execute(
            """
            UPDATE queued_messages
            SET dispatch_started_at = ?
            WHERE id = ?
            """,
            ("2000-01-01T00:00:00+00:00", queued.queue_id),
        )
        self.store.connection.commit()
        self.store.set_initial_history_complete("thread-1")
        self.service._initial_sync = False
        summary = {
            "id": "thread-1",
            "name": "Test topic",
            "preview": "",
            "updatedAt": 0,
        }

        async def list_threads(*, archived: bool) -> list[dict[str, Any]]:
            return [] if archived else [summary]

        self.service.codex = SimpleNamespace(
            list_threads=AsyncMock(side_effect=list_threads),
            read_thread=AsyncMock(
                return_value={
                    "id": "thread-1",
                    "status": {"type": "idle"},
                    "turns": [],
                }
            ),
            resume_thread=AsyncMock(return_value={}),
        )

        await self.service.sync_threads(dispatch_queue=False)
        self.assertEqual(
            self.store.queued_message(queued.queue_id).status,
            "dispatching",
        )
        await self.service.sync_threads(dispatch_queue=False)

        self.assertEqual(
            self.store.queued_message(queued.queue_id).status,
            "pending",
        )
        self.assertEqual(self.service.codex.read_thread.await_count, 2)

    def configure_live_mode(self) -> None:
        self.service.thread_modes["thread-1"] = ThreadMode(
            model="gpt-5.6-sol",
            effort="xhigh",
            service_tier="default",
        )
        self.service.model_catalog["gpt-5.6-sol"] = {
            "id": "gpt-5.6-sol",
            "displayName": "GPT-5.6-Sol",
            "supportedReasoningEfforts": [
                {"reasoningEffort": value}
                for value in (
                    "low",
                    "medium",
                    "high",
                    "xhigh",
                    "max",
                    "ultra",
                )
            ],
            "serviceTiers": [
                {
                    "id": "priority",
                    "name": "Fast",
                    "description": "1.5x speed, increased usage",
                }
            ],
        }

    async def test_mode_command_shows_native_effort_and_speed_picker(
        self,
    ) -> None:
        self.configure_live_mode()

        await self.service.handle_telegram_update(
            self.topic_message("/mode", message_id=89)
        )

        card = self.telegram.sent_messages[-1]
        self.assertIn("Модель: GPT-5.6-Sol", card["text"])
        self.assertIn("Интеллект: Extra High", card["text"])
        self.assertIn("Скорость: Standard", card["text"])
        self.assertIn("1,5× быстрее", card["text"])
        buttons = [
            button
            for row in card["reply_markup"]["inline_keyboard"]
            for button in row
        ]
        callbacks = {button["callback_data"] for button in buttons}
        self.assertIn("mode:e:low", callbacks)
        self.assertIn("mode:e:ultra", callbacks)
        self.assertIn("mode:s:default", callbacks)
        self.assertIn("mode:s:priority", callbacks)
        self.assertTrue(
            any(
                button["text"] == "✓ Extra High"
                for button in buttons
            )
        )
        self.assertTrue(
            any(button["text"] == "✓ Standard" for button in buttons)
        )

    async def test_mode_callback_updates_native_setting_and_topic_title(
        self,
    ) -> None:
        self.configure_live_mode()
        self.codex.update_thread_settings = AsyncMock(
            return_value={
                "model": "gpt-5.6-sol",
                "effort": "high",
                "serviceTier": "default",
            }
        )
        await self.service.handle_telegram_update(
            self.topic_message("/mode", message_id=89)
        )
        card_id = 800 + len(self.telegram.sent_messages)

        await self.service.handle_callback(
            {
                "id": "mode-high",
                "data": "mode:e:high",
                "from": {"id": 100, "is_bot": False},
                "message": {
                    "message_id": card_id,
                    "message_thread_id": 50,
                    "chat": {"id": -100500, "type": "supergroup"},
                },
            }
        )

        self.codex.update_thread_settings.assert_awaited_once_with(
            thread_id="thread-1",
            effort="high",
            update_effort=True,
        )
        self.assertEqual(
            self.telegram.edited_topics[-1],
            (-100500, 50, "Test topic · 🧠High · ⚡Standard"),
        )
        self.assertEqual(
            self.store.topic_for_thread("thread-1").title,
            "Test topic · 🧠High · ⚡Standard",
        )
        self.assertIn(
            "Интеллект: High",
            self.telegram.edited_messages[-1]["text"],
        )
        self.assertEqual(
            self.telegram.callback_answers[-1],
            ("mode-high", "Интеллект: High"),
        )

    async def test_mode_callback_clears_fast_to_native_standard_tier(
        self,
    ) -> None:
        self.configure_live_mode()
        self.service.thread_modes["thread-1"] = ThreadMode(
            model="gpt-5.6-sol",
            effort="xhigh",
            service_tier="priority",
        )
        self.codex.update_thread_settings = AsyncMock(
            return_value={
                "model": "gpt-5.6-sol",
                "effort": "xhigh",
                "serviceTier": "default",
            }
        )
        await self.service.handle_telegram_update(
            self.topic_message("/mode", message_id=89)
        )
        card_id = 800 + len(self.telegram.sent_messages)

        await self.service.handle_callback(
            {
                "id": "mode-standard",
                "data": "mode:s:default",
                "from": {"id": 100, "is_bot": False},
                "message": {
                    "message_id": card_id,
                    "message_thread_id": 50,
                    "chat": {"id": -100500, "type": "supergroup"},
                },
            }
        )

        self.codex.update_thread_settings.assert_awaited_once_with(
            thread_id="thread-1",
            service_tier=None,
            update_service_tier=True,
        )
        self.assertIn(
            "⚡Standard",
            self.store.topic_for_thread("thread-1").title,
        )

    async def test_desktop_thread_settings_notification_updates_topic_title(
        self,
    ) -> None:
        self.configure_live_mode()

        await self.service.on_codex_notification(
            {
                "method": "thread/settings/updated",
                "params": {
                    "threadId": "thread-1",
                    "threadSettings": {
                        "model": "gpt-5.6-sol",
                        "effort": "ultra",
                        "serviceTier": "priority",
                    },
                },
            }
        )

        self.assertEqual(
            self.telegram.edited_topics,
            [(-100500, 50, "Test topic · 🧠Ultra · ⚡Fast")],
        )
        self.assertEqual(
            self.service.thread_modes["thread-1"].effort,
            "ultra",
        )

    async def test_initial_sync_decorates_every_existing_working_topic(
        self,
    ) -> None:
        summary = {
            "id": "thread-1",
            "name": "Test topic",
            "preview": "",
            "updatedAt": 0,
        }

        async def list_threads(*, archived: bool) -> list[dict[str, Any]]:
            return [] if archived else [summary]

        self.service.codex = SimpleNamespace(
            list_threads=AsyncMock(side_effect=list_threads),
            resume_thread=AsyncMock(
                return_value={
                    "model": "gpt-5.6-sol",
                    "effort": "xhigh",
                    "serviceTier": "default",
                }
            ),
            read_thread=AsyncMock(
                return_value={
                    "id": "thread-1",
                    "status": {"type": "idle"},
                    "turns": [],
                }
            ),
        )

        await self.service.sync_threads(dispatch_queue=False)

        self.assertEqual(
            self.telegram.edited_topics,
            [(-100500, 50, "Test topic · 🧠XHigh · ⚡Standard")],
        )
        self.assertEqual(
            self.store.topic_for_thread("thread-1").title,
            "Test topic · 🧠XHigh · ⚡Standard",
        )

    async def test_mode_callback_requires_latest_exact_status_card(
        self,
    ) -> None:
        self.configure_live_mode()
        self.codex.update_thread_settings = AsyncMock(return_value={})
        await self.service.handle_telegram_update(
            self.topic_message("/mode", message_id=89)
        )
        old_card_id = 800 + len(self.telegram.sent_messages)
        await self.service.handle_telegram_update(
            self.topic_message("/mode", message_id=90)
        )

        await self.service.handle_callback(
            {
                "id": "old-mode-card",
                "data": "mode:e:high",
                "from": {"id": 100, "is_bot": False},
                "message": {
                    "message_id": old_card_id,
                    "message_thread_id": 50,
                    "chat": {"id": -100500, "type": "supergroup"},
                },
            }
        )

        self.codex.update_thread_settings.assert_not_awaited()
        self.assertIn(
            "недоступна",
            self.telegram.callback_answers[-1][1],
        )

    async def test_topic_status_has_health_queue_and_state_bound_controls(
        self,
    ) -> None:
        topic = self.store.topic_for_thread("thread-1")
        self.assertIsNotNone(topic)
        self.service.telegram_update_health.record_success()
        self.service.thread_sync_health.record_failure("codex_protocol")

        await self.service.send_status(topic, reply_to=90)

        idle_card = self.telegram.sent_messages[-1]
        idle_buttons = [
            button["text"]
            for row in idle_card["reply_markup"]["inline_keyboard"]
            for button in row
        ]
        self.assertIn("свободен", idle_card["text"])
        self.assertIn("Очередь этого треда: 0", idle_card["text"])
        self.assertIn("Telegram poll", idle_card["text"])
        self.assertIn("codex_protocol", idle_card["text"])
        self.assertEqual(idle_buttons, ["↻ Обновить", "➕ Новый тред"])

        self.service.busy_threads.add("thread-1")
        self.store.enqueue(
            thread_id="thread-1",
            chat_id=-100500,
            topic_id=50,
            telegram_message_id=91,
            text="private queued body",
            client_id="status-queue",
        )
        await self.service.send_status(topic, reply_to=91)

        busy_card = self.telegram.sent_messages[-1]
        busy_buttons = [
            button["text"]
            for row in busy_card["reply_markup"]["inline_keyboard"]
            for button in row
        ]
        self.assertIn("занят", busy_card["text"])
        self.assertIn("Очередь этого треда: 1", busy_card["text"])
        self.assertEqual(
            busy_buttons,
            [
                "⚡ В текущий ход",
                "↪️ Следующим ходом",
                "⏹ Стоп",
                "↻ Обновить",
                "➕ Новый тред",
            ],
        )
        self.assertNotIn("private queued body", busy_card["text"])

    async def test_status_ambiguous_send_is_not_replayed_and_is_sanitized(
        self,
    ) -> None:
        topic = self.store.topic_for_thread("thread-1")
        self.assertIsNotNone(topic)
        remote_messages: list[dict[str, Any]] = []

        def applied_but_response_lost(**kwargs: Any) -> list[dict[str, Any]]:
            remote_messages.append(dict(kwargs))
            raise TelegramError(
                "status response lost SECRET_STATUS_ERROR",
                method="sendMessage",
                kind="network_timeout",
                retryable=True,
                outcome_ambiguous=True,
            )

        self.telegram.send_message = applied_but_response_lost  # type: ignore[method-assign]
        with self.assertRaises(TelegramError):
            await self.service.send_status(topic, reply_to=90)

        restarted_telegram = FakeTelegram()
        restarted = BridgeService(
            config=self.service.config,
            store=self.store,
            telegram=restarted_telegram,
        )
        restarted.codex = self.codex
        await restarted.send_status(topic, reply_to=90)
        self.assertEqual(len(remote_messages), 1)
        self.assertEqual(restarted_telegram.sent_messages, [])

        await restarted.send_status(topic, reply_to=91)
        status_text = restarted_telegram.sent_messages[-1]["text"]
        self.assertIn("Неопределённая доставка", status_text)
        self.assertIn("статус=1", status_text)
        self.assertNotIn("SECRET_STATUS_ERROR", status_text)

    async def test_control_prompt_definite_send_failure_releases_action(
        self,
    ) -> None:
        topic = self.store.topic_for_thread("thread-1")
        self.assertIsNotNone(topic)
        self.service.busy_threads.add("thread-1")
        await self.service.send_status(topic, reply_to=90)
        status_card_id = 800 + len(self.telegram.sent_messages)
        callback = {
            "id": "queue-control-failure",
            "data": "ctl:queue",
            "from": {"id": 100, "is_bot": False},
            "message": {
                "message_id": status_card_id,
                "message_thread_id": 50,
                "chat": {"id": -100500, "type": "supergroup"},
            },
        }
        original_send = self.telegram.send_message

        def definite_failure(**_: Any) -> list[dict[str, Any]]:
            raise TelegramError(
                "definite failure",
                method="sendMessage",
                kind="api",
                outcome_ambiguous=False,
            )

        self.telegram.send_message = definite_failure  # type: ignore[method-assign]
        with self.assertRaises(TelegramError):
            await self.service.handle_callback(callback)
        self.telegram.send_message = original_send  # type: ignore[method-assign]
        callback["id"] = "queue-control-retry"
        await self.service.handle_callback(callback)

        prompts = [
            message
            for message in self.telegram.sent_messages
            if message.get("text") == CONTROL_PROMPT_TEXT["queue"]
        ]
        self.assertEqual(len(prompts), 1)
        self.assertEqual(
            self.store.delivery_uncertainty_health()[
                "controlActionsClaimed"
            ],
            0,
        )

    async def test_control_prompt_ambiguous_send_is_not_replayed(
        self,
    ) -> None:
        topic = self.store.topic_for_thread("thread-1")
        self.assertIsNotNone(topic)
        self.service.busy_threads.add("thread-1")
        await self.service.send_status(topic, reply_to=90)
        status_card_id = 800 + len(self.telegram.sent_messages)
        callback = {
            "id": "queue-control-ambiguous",
            "data": "ctl:queue",
            "from": {"id": 100, "is_bot": False},
            "message": {
                "message_id": status_card_id,
                "message_thread_id": 50,
                "chat": {"id": -100500, "type": "supergroup"},
            },
        }
        remote_prompts: list[dict[str, Any]] = []

        def applied_but_response_lost(**kwargs: Any) -> list[dict[str, Any]]:
            remote_prompts.append(dict(kwargs))
            raise TelegramError(
                "prompt response lost SECRET_ACTION_ERROR",
                method="sendMessage",
                kind="network_timeout",
                retryable=True,
                outcome_ambiguous=True,
            )

        original_send = self.telegram.send_message
        self.telegram.send_message = applied_but_response_lost  # type: ignore[method-assign]
        with self.assertRaises(TelegramError):
            await self.service.handle_callback(callback)
        self.telegram.send_message = original_send  # type: ignore[method-assign]
        callback["id"] = "queue-control-ambiguous-replay"
        await self.service.handle_callback(callback)

        self.assertEqual(len(remote_prompts), 1)
        self.assertIn(
            "доставка её действия не определена",
            self.telegram.callback_answers[-1][1],
        )
        await self.service.send_status(topic, reply_to=91)
        status_text = self.telegram.sent_messages[-1]["text"]
        self.assertIn("действия=1", status_text)
        self.assertNotIn("SECRET_ACTION_ERROR", status_text)

    async def test_control_force_reply_survives_restart_and_is_single_use(
        self,
    ) -> None:
        topic = self.store.topic_for_thread("thread-1")
        self.assertIsNotNone(topic)
        self.service.busy_threads.add("thread-1")
        await self.service.send_status(topic, reply_to=90)
        status_card_id = self.telegram.sent_messages[-1]["reply_to_message_id"]
        status_card_id = 800 + len(self.telegram.sent_messages)
        callback = {
            "id": "steer-control",
            "data": "ctl:steer",
            "from": {"id": 100, "is_bot": False},
            "message": {
                "message_id": status_card_id,
                "message_thread_id": 50,
                "chat": {"id": -100500, "type": "supergroup"},
            },
        }
        await self.service.handle_callback(callback)
        prompt = self.telegram.sent_messages[-1]
        prompt_id = 800 + len(self.telegram.sent_messages)
        self.assertEqual(prompt["text"], CONTROL_PROMPT_TEXT["steer"])
        self.assertTrue(prompt["reply_markup"]["force_reply"])

        restarted_telegram = FakeTelegram()
        restarted = BridgeService(
            config=self.service.config,
            store=self.store,
            telegram=restarted_telegram,
        )
        restarted.codex = self.codex
        restarted.busy_threads.add("thread-1")
        restarted.observed_turns["thread-1"] = "turn-active"
        reply = self.topic_message("Добавь эту проверку", message_id=95)
        reply["message"]["reply_to_message"] = {
            "message_id": prompt_id,
            "text": CONTROL_PROMPT_TEXT["steer"],
            "from": {"id": 700, "is_bot": True},
        }

        await restarted.handle_telegram_update(reply)
        await restarted.handle_telegram_update(reply)

        self.codex.steer_turn.assert_awaited_once()
        self.codex.start_turn.assert_not_awaited()
        self.assertIn(
            "уже использована",
            restarted_telegram.sent_messages[-1]["text"],
        )

    async def test_control_buttons_require_latest_exact_status_card(
        self,
    ) -> None:
        topic = self.store.topic_for_thread("thread-1")
        self.assertIsNotNone(topic)
        self.service.busy_threads.add("thread-1")
        await self.service.send_status(topic, reply_to=90)
        old_card_id = 800 + len(self.telegram.sent_messages)
        await self.service.send_status(topic, reply_to=91)
        new_card_id = 800 + len(self.telegram.sent_messages)

        def callback(message_id: int, callback_id: str) -> dict[str, Any]:
            return {
                "id": callback_id,
                "data": "ctl:queue",
                "from": {"id": 100, "is_bot": False},
                "message": {
                    "message_id": message_id,
                    "message_thread_id": 50,
                    "chat": {"id": -100500, "type": "supergroup"},
                },
            }

        await self.service.handle_callback(callback(old_card_id, "old-card"))
        self.assertIn("недоступна", self.telegram.callback_answers[-1][1])
        prompt_count = len(self.telegram.sent_messages)
        await self.service.handle_callback(callback(new_card_id, "new-card"))
        self.assertEqual(len(self.telegram.sent_messages), prompt_count + 1)
        self.assertEqual(
            self.telegram.sent_messages[-1]["text"],
            CONTROL_PROMPT_TEXT["queue"],
        )
        await self.service.handle_callback(
            callback(new_card_id, "new-card-replay")
        )
        self.assertEqual(len(self.telegram.sent_messages), prompt_count + 1)
        self.assertIn(
            "уже использована",
            self.telegram.callback_answers[-1][1],
        )

    async def test_queue_force_reply_is_mode_bound_and_wrong_card_fails_closed(
        self,
    ) -> None:
        topic = self.store.topic_for_thread("thread-1")
        self.assertIsNotNone(topic)
        self.service.busy_threads.add("thread-1")
        await self.service.send_status(topic, reply_to=90)
        status_card_id = 800 + len(self.telegram.sent_messages)
        await self.service.handle_callback(
            {
                "id": "queue-control",
                "data": "ctl:queue",
                "from": {"id": 100, "is_bot": False},
                "message": {
                    "message_id": status_card_id,
                    "message_thread_id": 50,
                    "chat": {"id": -100500, "type": "supergroup"},
                },
            }
        )
        prompt_id = 800 + len(self.telegram.sent_messages)

        wrong = self.topic_message("Не принимать", message_id=96)
        wrong["message"]["reply_to_message"] = {
            "message_id": prompt_id + 100,
            "text": CONTROL_PROMPT_TEXT["queue"],
            "from": {"id": 700, "is_bot": True},
        }
        await self.service.handle_telegram_update(wrong)
        self.assertIsNone(self.store.next_queued("thread-1"))
        self.codex.steer_turn.assert_not_awaited()
        self.codex.start_turn.assert_not_awaited()

        correct = self.topic_message("Следующий ход", message_id=97)
        correct["message"]["reply_to_message"] = {
            "message_id": prompt_id,
            "text": CONTROL_PROMPT_TEXT["queue"],
            "from": {"id": 700, "is_bot": True},
        }
        await self.service.handle_telegram_update(correct)

        queued = self.store.next_queued("thread-1")
        self.assertIsNotNone(queued)
        self.assertEqual(queued.text, "Следующий ход")
        self.codex.steer_turn.assert_not_awaited()
        self.codex.start_turn.assert_not_awaited()

    async def test_stop_confirmation_is_exact_durable_and_interrupts_once(
        self,
    ) -> None:
        topic = self.store.topic_for_thread("thread-1")
        self.assertIsNotNone(topic)
        self.service.busy_threads.add("thread-1")
        self.service.active_turns["thread-1"] = "turn-active"
        await self.service.send_status(topic, reply_to=90)
        status_card_id = 800 + len(self.telegram.sent_messages)
        await self.service.handle_callback(
            {
                "id": "stop-control",
                "data": "ctl:stop",
                "from": {"id": 100, "is_bot": False},
                "message": {
                    "message_id": status_card_id,
                    "message_thread_id": 50,
                    "chat": {"id": -100500, "type": "supergroup"},
                },
            }
        )
        confirmation = self.telegram.sent_messages[-1]
        confirmation_id = 800 + len(self.telegram.sent_messages)
        confirmation_data = confirmation["reply_markup"]["inline_keyboard"][0][
            0
        ]["callback_data"]

        restarted_telegram = FakeTelegram()
        restarted = BridgeService(
            config=self.service.config,
            store=self.store,
            telegram=restarted_telegram,
        )
        restarted.codex = self.codex
        restarted.active_turns["thread-1"] = "turn-active"

        def stop_callback(
            *,
            message_id: int,
            sender_id: int = 100,
            callback_id: str,
        ) -> dict[str, Any]:
            return {
                "id": callback_id,
                "data": confirmation_data,
                "from": {"id": sender_id, "is_bot": False},
                "message": {
                    "message_id": message_id,
                    "message_thread_id": 50,
                    "chat": {"id": -100500, "type": "supergroup"},
                },
            }

        await restarted.handle_callback(
            stop_callback(
                message_id=confirmation_id,
                sender_id=999,
                callback_id="wrong-user",
            )
        )
        await restarted.handle_callback(
            stop_callback(
                message_id=confirmation_id + 1,
                callback_id="wrong-card",
            )
        )
        self.codex.interrupt_turn.assert_not_awaited()

        def edit_fails(**_: Any) -> bool:
            raise TelegramError(
                "edit unavailable",
                method="editMessageText",
                kind="network_error",
            )

        restarted_telegram.edit_message_text = edit_fails  # type: ignore[method-assign]
        await restarted.handle_callback(
            stop_callback(
                message_id=confirmation_id,
                callback_id="correct-card",
            )
        )
        await restarted.handle_callback(
            stop_callback(
                message_id=confirmation_id,
                callback_id="replay-card",
            )
        )

        self.codex.interrupt_turn.assert_awaited_once_with(
            thread_id="thread-1",
            turn_id="turn-active",
        )

    async def test_audit_is_general_only_sanitized_and_excludes_archive_hub(
        self,
    ) -> None:
        self.store.upsert_topic(
            thread_id="thread-stale",
            chat_id=-100500,
            topic_id=51,
            title="Private stale title",
        )
        self.store.observe_topic(-100500, 60, "Архивные треды")
        self.store.set_archive_hub_topic_id(60)
        self.store.observe_topic(-100500, 61, "Unmapped private title")
        self.store.reserve_topic_creation(
            thread_id="thread-unresolved",
            chat_id=-100500,
            title="Private unresolved title",
        )
        self.store.enqueue(
            thread_id="thread-1",
            chat_id=-100500,
            topic_id=50,
            telegram_message_id=91,
            text="secret queue content",
            client_id="audit-queue",
        )
        list_threads = AsyncMock(
            return_value=[
                {"id": "thread-1"},
                {"id": "thread-missing"},
            ]
        )
        self.service.codex = SimpleNamespace(
            list_threads=list_threads,
            is_connected=True,
            server_version="test-version",
        )
        update = self.topic_message("/audit", message_id=99)
        update["message"]["message_thread_id"] = 0

        await self.service.handle_telegram_update(update)

        text = self.telegram.sent_messages[-1]["text"]
        list_threads.assert_awaited_once_with(archived=False)
        self.assertIn(
            "Активных задач Codex в рабочей папке: 2",
            text,
        )
        self.assertIn("Открытых привязанных Topic: 2", text)
        self.assertIn(
            "Известных мосту открытых Topic Telegram: 5 "
            "(рабочих=2; служебных=2; непривязанных=1)",
            text,
        )
        self.assertIn(
            "Полный список старых неизвестных Topic Bot API не перечисляет",
            text,
        )
        self.assertIn("Пропущенных привязок: 1", text)
        self.assertIn("Устаревших привязок: 1", text)
        self.assertIn("Непривязанных наблюдаемых Topic: 1", text)
        self.assertIn("Незавершённых созданий Topic: 1", text)
        self.assertIn("Целостность SQLite: ok", text)
        self.assertIn("ожидает=1", text)
        self.assertIn("подключён=да; версия=test-version", text)
        for private_value in (
            "thread-",
            "Private",
            "secret queue content",
            "-100500",
        ):
            self.assertNotIn(private_value, text)

    async def test_audit_in_working_topic_is_denied_without_codex_read(
        self,
    ) -> None:
        list_threads = AsyncMock()
        self.service.codex = SimpleNamespace(list_threads=list_threads)

        await self.service.handle_telegram_update(
            self.topic_message("/audit", message_id=98)
        )

        list_threads.assert_not_awaited()
        self.assertIn(
            "только в General",
            self.telegram.sent_messages[-1]["text"],
        )


if __name__ == "__main__":
    unittest.main()
