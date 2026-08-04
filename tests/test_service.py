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

    async def test_stale_new_thread_button_is_rejected(self) -> None:
        callback = {
            "id": "stale-new-callback",
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
            (
                "stale-new-callback",
                "Эта кнопка недоступна в данном Topic",
            ),
        )
        self.assertEqual(self.telegram.sent_messages, [])

    async def test_force_reply_creates_new_thread_from_plain_text(self) -> None:
        update = self.topic_message("Проверь ночные ошибки")
        update["message"]["reply_to_message"] = {
            "message_id": 89,
            "text": NEW_THREAD_PROMPT,
            "from": {"id": 700, "is_bot": True},
        }
        self.service.create_thread_from_general = AsyncMock()

        await self.service.handle_telegram_update(update)

        self.service.create_thread_from_general.assert_awaited_once_with(
            "Проверь ночные ошибки",
            source_message_id=90,
        )
        self.codex.start_turn.assert_not_awaited()

    async def test_force_reply_voice_defers_title_until_codex_transcribes(
        self,
    ) -> None:
        audio = self.local_media_input()
        update = self.topic_media_message(
            "voice",
            reply_to_message={
                "message_id": 89,
                "text": NEW_THREAD_PROMPT,
                "from": {"id": 700, "is_bot": True},
            },
        )
        self.service._prepare_telegram_media = AsyncMock(  # type: ignore[method-assign]
            return_value=("🎙 Голосовой запрос", (audio,))
        )
        self.service.create_thread_from_general = AsyncMock()

        await self.service.handle_telegram_update(update)

        self.service.create_thread_from_general.assert_awaited_once_with(
            "🎙 Голосовой запрос",
            source_message_id=90,
            local_inputs=(audio,),
            defer_title_to_codex=True,
        )

    async def test_new_voice_thread_does_not_preempt_native_codex_title(
        self,
    ) -> None:
        audio = self.local_media_input()
        new_topic = TopicBinding(
            thread_id="thread-voice",
            chat_id=-100500,
            topic_id=60,
            title="Распознаётся голосовая задача",
            archived=False,
            last_updated_at=0,
        )
        self.codex.start_thread = AsyncMock(
            return_value={"id": "thread-voice", "updatedAt": 123}
        )
        self.codex.set_thread_name = AsyncMock()
        self.service._refresh_thread_mode = AsyncMock(return_value=None)
        self.service._create_topic_for_thread = AsyncMock(
            return_value=new_topic
        )
        self.service.start_turn = AsyncMock(return_value=True)

        await self.service.create_thread_from_general(
            "🎙 Голосовой запрос",
            source_message_id=100,
            local_inputs=(audio,),
            defer_title_to_codex=True,
        )

        self.codex.set_thread_name.assert_not_awaited()
        self.service._create_topic_for_thread.assert_awaited_once_with(
            thread_id="thread-voice",
            title="Распознаётся голосовая задача",
            updated_at=123,
        )
        self.service.start_turn.assert_awaited_once_with(
            topic=new_topic,
            text=(
                "🎙 Голосовой запрос\n\n"
                "Название: 2–5 слов, телеграфно; объект + "
                "действие/состояние; привычные сокращения."
            ),
            client_id="tg:-100500:100",
            reply_to=801,
            local_inputs=(audio,),
        )
        self.assertEqual(
            self.telegram.sent_messages[-1]["text"],
            "👤 Telegram\n\n🎙 Голосовой запрос",
        )

    async def test_new_thread_replay_does_not_duplicate_thread_or_turn(
        self,
    ) -> None:
        new_topic = TopicBinding(
            thread_id="thread-new",
            chat_id=-100500,
            topic_id=60,
            title="New task",
            archived=False,
            last_updated_at=0,
        )
        self.codex.start_thread = AsyncMock(
            return_value={"id": "thread-new", "updatedAt": 123}
        )
        self.codex.set_thread_name = AsyncMock()
        self.service._create_topic_for_thread = AsyncMock(
            return_value=new_topic
        )
        self.service.start_turn = AsyncMock(return_value=True)

        await self.service.create_thread_from_general(
            "New task",
            source_message_id=100,
        )
        await self.service.create_thread_from_general(
            "New task",
            source_message_id=100,
        )

        self.codex.start_thread.assert_awaited_once()
        self.service._create_topic_for_thread.assert_awaited_once()
        self.service.start_turn.assert_awaited_once()
        echoed = [
            message
            for message in self.telegram.sent_messages
            if message.get("message_thread_id") == 60
        ]
        self.assertEqual(len(echoed), 1)
        request = self.store.new_thread_request(-100500, 100)
        self.assertIsNotNone(request)
        self.assertEqual(request.status, "turn_started")
        self.assertEqual(request.thread_id, "thread-new")
        self.assertIsNotNone(request.echo_message_id)

    async def test_new_thread_replay_resumes_after_thread_creation(self) -> None:
        new_topic = TopicBinding(
            thread_id="thread-existing",
            chat_id=-100500,
            topic_id=61,
            title="Resume task",
            archived=False,
            last_updated_at=0,
        )
        prompt = "Resume task"
        self.store.reserve_new_thread_request(
            chat_id=-100500,
            message_id=101,
            prompt_hash=hashlib.sha256(prompt.encode("utf-8")).hexdigest(),
        )
        self.store.update_new_thread_request(
            chat_id=-100500,
            message_id=101,
            status="thread_created",
            thread_id="thread-existing",
        )
        self.codex.start_thread = AsyncMock()
        self.service._create_topic_for_thread = AsyncMock(
            return_value=new_topic
        )
        self.service.start_turn = AsyncMock(return_value=True)

        await self.service.create_thread_from_general(
            prompt,
            source_message_id=101,
        )

        self.codex.start_thread.assert_not_awaited()
        self.service._create_topic_for_thread.assert_awaited_once()
        self.service.start_turn.assert_awaited_once()

    async def test_new_thread_waiting_for_global_capacity_replies_to_topic_echo(
        self,
    ) -> None:
        self.service.config = replace(
            self.service.config,
            max_active_turns=1,
        )
        self.service.busy_threads.add("thread-1")
        self.service.active_turns["thread-1"] = "turn-running"
        new_topic = TopicBinding(
            thread_id="thread-new",
            chat_id=-100500,
            topic_id=60,
            title="Queued new task",
            archived=False,
            last_updated_at=0,
        )
        self.codex.start_thread = AsyncMock(
            return_value={"id": "thread-new", "updatedAt": 123}
        )
        self.codex.set_thread_name = AsyncMock()
        self.service._create_topic_for_thread = AsyncMock(
            return_value=new_topic
        )

        await self.service.create_thread_from_general(
            "Queued new task",
            source_message_id=100,
        )

        request = self.store.new_thread_request(-100500, 100)
        self.assertIsNotNone(request)
        self.assertIsNotNone(request.echo_message_id)
        queued = self.store.queued_message_for_client_id("tg:-100500:100")
        self.assertIsNotNone(queued)
        self.assertEqual(queued.status, "pending")
        self.assertEqual(
            queued.telegram_message_id,
            request.echo_message_id,
        )
        queue_card = self.telegram.sent_messages[-1]
        self.assertEqual(queue_card["message_thread_id"], 60)
        self.assertEqual(
            queue_card["reply_to_message_id"],
            request.echo_message_id,
        )
        self.codex.start_turn.assert_not_awaited()

    async def test_new_thread_history_and_notification_keep_topic_echo_reply(
        self,
    ) -> None:
        for index, user_origin in enumerate(("history", "notification")):
            with self.subTest(user_origin=user_origin):
                thread_id = f"thread-new-{user_origin}"
                turn_id = f"turn-new-{user_origin}"
                topic_id = 60 + index
                general_message_id = 100 + index
                echo_message_id = 900 + index
                client_id = f"tg:-100500:{general_message_id}"
                self.store.upsert_topic(
                    thread_id=thread_id,
                    chat_id=-100500,
                    topic_id=topic_id,
                    title=f"New {user_origin}",
                )
                self.store.enqueue(
                    thread_id=thread_id,
                    chat_id=-100500,
                    topic_id=topic_id,
                    telegram_message_id=echo_message_id,
                    text=f"New task from {user_origin}",
                    client_id=client_id,
                )
                topic = self.store.topic_for_thread(thread_id)
                self.assertIsNotNone(topic)
                user_item = {
                    "id": f"user-{user_origin}",
                    "type": "userMessage",
                    "clientId": client_id,
                    "content": [
                        {
                            "type": "text",
                            "text": f"New task from {user_origin}",
                        }
                    ],
                }
                if user_origin == "history":
                    await self.service.sync_thread_history(
                        {
                            "id": thread_id,
                            "turns": [
                                {
                                    "id": turn_id,
                                    "status": "inProgress",
                                    "items": [user_item],
                                }
                            ],
                        },
                        topic,
                        initial=False,
                    )
                else:
                    await self.service.on_codex_notification(
                        {
                            "method": "item/completed",
                            "params": {
                                "threadId": thread_id,
                                "turnId": turn_id,
                                "item": user_item,
                            },
                        }
                    )

                context = self.store.turn_context(thread_id, turn_id)
                self.assertIsNotNone(context)
                self.assertEqual(
                    context.source_message_id,
                    echo_message_id,
                )
                rich_before = len(self.telegram.sent_rich_messages)
                final_before = len(self.telegram.sent_messages)
                await self.service.on_codex_notification(
                    {
                        "method": "item/completed",
                        "params": {
                            "threadId": thread_id,
                            "turnId": turn_id,
                            "item": {
                                "id": f"progress-{user_origin}",
                                "type": "agentMessage",
                                "phase": "commentary",
                                "text": f"Progress from {user_origin}",
                            },
                        },
                    }
                )
                await self.service.on_codex_notification(
                    {
                        "method": "item/completed",
                        "params": {
                            "threadId": thread_id,
                            "turnId": turn_id,
                            "item": {
                                "id": f"final-{user_origin}",
                                "type": "agentMessage",
                                "phase": "final_answer",
                                "text": f"Final from {user_origin}",
                            },
                        },
                    }
                )
                await self.service.on_codex_notification(
                    {
                        "method": "turn/completed",
                        "params": {
                            "threadId": thread_id,
                            "turn": {"id": turn_id, "status": "completed"},
                        },
                    }
                )

                self.assertEqual(
                    self.telegram.sent_rich_messages[rich_before][
                        "reply_to_message_id"
                    ],
                    echo_message_id,
                )
                self.assertEqual(
                    self.telegram.sent_messages[final_before][
                        "reply_to_message_id"
                    ],
                    echo_message_id,
                )

    async def test_new_thread_ambiguous_start_fails_closed_on_replay(
        self,
    ) -> None:
        self.codex.start_thread = AsyncMock(
            side_effect=CodexProtocolError("connection lost after request")
        )

        with self.assertRaises(CodexProtocolError):
            await self.service.create_thread_from_general(
                "Do not duplicate",
                source_message_id=102,
            )
        await self.service.create_thread_from_general(
            "Do not duplicate",
            source_message_id=102,
        )

        self.codex.start_thread.assert_awaited_once()
        request = self.store.new_thread_request(-100500, 102)
        self.assertIsNotNone(request)
        self.assertEqual(
            request.status,
            "thread_start_outcome_unknown",
        )
        self.assertIn(
            "Автоматический повтор отключён",
            self.telegram.sent_messages[-1]["text"],
        )

    async def test_manual_topic_ambiguous_start_fails_closed_on_replay(
        self,
    ) -> None:
        self.codex.start_thread = AsyncMock(
            side_effect=CodexProtocolError("connection lost after request")
        )

        with self.assertRaises(CodexProtocolError):
            await self.service.bind_manual_topic(
                77,
                "Do not duplicate",
                source_message_id=103,
            )
        replayed = await self.service.bind_manual_topic(
            77,
            "Do not duplicate",
            source_message_id=103,
        )

        self.assertIsNone(replayed)
        self.codex.start_thread.assert_awaited_once()
        intent = self.store.manual_topic_thread_intent(
            chat_id=-100500,
            topic_id=77,
        )
        self.assertIsNotNone(intent)
        self.assertEqual(intent.status, "outcome_unknown")
        self.assertIn(
            "Автоматический повтор отключён",
            self.telegram.sent_messages[-1]["text"],
        )

    async def test_topic_creation_completes_durable_intent(self) -> None:
        topic = await self.service._create_topic_for_thread(
            thread_id="thread-topic-success",
            title="Durable success",
            updated_at=123,
        )

        intent = self.store.topic_creation_intent("thread-topic-success")
        self.assertEqual(
            self.telegram.created_topics,
            [(-100500, "Durable success")],
        )
        self.assertIsNotNone(intent)
        self.assertEqual(intent.status, "completed")
        self.assertEqual(intent.topic_id, topic.topic_id)
        self.assertEqual(
            self.store.topic_for_thread("thread-topic-success"),
            topic,
        )
        self.assertEqual(self.store.unresolved_topic_creations(), [])

    async def test_definite_topic_failure_is_retryable(self) -> None:
        successful_create = self.telegram.create_forum_topic
        attempts = 0

        def fail_once(chat_id: int, name: str) -> dict[str, Any]:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise TelegramError(
                    "definite rejection",
                    kind="api",
                    outcome_ambiguous=False,
                )
            return successful_create(chat_id, name)

        self.telegram.create_forum_topic = fail_once  # type: ignore[method-assign]

        with self.assertRaises(TelegramError):
            await self.service._create_topic_for_thread(
                thread_id="thread-topic-retry",
                title="Retryable topic",
                updated_at=124,
            )

        self.assertIsNone(
            self.store.topic_creation_intent("thread-topic-retry")
        )
        topic = await self.service._create_topic_for_thread(
            thread_id="thread-topic-retry",
            title="Retryable topic",
            updated_at=124,
        )

        self.assertEqual(attempts, 2)
        self.assertEqual(
            self.store.topic_creation_intent(
                "thread-topic-retry"
            ).status,
            "completed",
        )
        self.assertEqual(
            self.store.topic_for_thread("thread-topic-retry"),
            topic,
        )

    async def test_ambiguous_topic_failure_blocks_create_after_restart(
        self,
    ) -> None:
        attempts = 0

        def fail_ambiguously(
            chat_id: int,
            name: str,
        ) -> dict[str, Any]:
            nonlocal attempts
            attempts += 1
            raise TelegramError(
                "response lost",
                kind="network_timeout",
                retryable=True,
                outcome_ambiguous=True,
            )

        self.telegram.create_forum_topic = (  # type: ignore[method-assign]
            fail_ambiguously
        )
        with self.assertRaises(TopicCreationUnresolvedError):
            await self.service._create_topic_for_thread(
                thread_id="thread-topic-ambiguous",
                title="Do not duplicate",
                updated_at=125,
            )

        intent = self.store.topic_creation_intent(
            "thread-topic-ambiguous"
        )
        self.assertIsNotNone(intent)
        self.assertEqual(intent.status, "outcome_unknown")

        reopened_store = BridgeStore(self.store.path)
        restarted_telegram = FakeTelegram()
        restarted = BridgeService(
            config=self.service.config,
            store=reopened_store,
            telegram=restarted_telegram,
        )

        async def list_threads(
            *,
            archived: bool,
        ) -> list[dict[str, Any]]:
            if archived:
                return []
            return [
                {
                    "id": "thread-topic-ambiguous",
                    "name": "Do not duplicate",
                    "updatedAt": 125,
                }
            ]

        restarted.codex = SimpleNamespace(
            list_threads=AsyncMock(side_effect=list_threads),
            resume_thread=AsyncMock(return_value={}),
        )
        try:
            with patch(
                "codex_telegram_bridge.service.LOGGER.error"
            ) as log_error:
                await restarted.sync_threads()
        finally:
            reopened_store.close()

        self.assertEqual(attempts, 1)
        self.assertEqual(restarted_telegram.created_topics, [])
        log_error.assert_called_once()
        logged = repr(log_error.call_args.args)
        self.assertIn("unresolved_count=%d", logged)
        self.assertNotIn("thread-topic-ambiguous", logged)
        self.assertNotIn("Do not duplicate", logged)

    async def test_reserved_topic_intent_resumes_remote_create(self) -> None:
        self.store.reserve_topic_creation(
            thread_id="thread-topic-reserved",
            chat_id=-100500,
            title="Reserved crash",
        )

        topic = await self.service._create_topic_for_thread(
            thread_id="thread-topic-reserved",
            title="Reserved crash",
            updated_at=126,
        )

        self.assertEqual(
            self.telegram.created_topics,
            [(-100500, "Reserved crash")],
        )
        intent = self.store.topic_creation_intent("thread-topic-reserved")
        self.assertIsNotNone(intent)
        self.assertEqual(intent.status, "completed")
        self.assertEqual(intent.topic_id, topic.topic_id)

    async def test_ambiguous_topic_intent_adopts_one_observed_topic(self) -> None:
        self.store.reserve_topic_creation(
            thread_id="thread-topic-observed",
            chat_id=-100500,
            title="Observed recovery",
        )
        self.store.mark_topic_creation_outcome_unknown(
            "thread-topic-observed"
        )
        self.store.observe_topic(-100500, 777, "Observed recovery")

        topic = await self.service._create_topic_for_thread(
            thread_id="thread-topic-observed",
            title="Observed recovery",
            updated_at=130,
        )

        self.assertEqual(self.telegram.created_topics, [])
        self.assertEqual(topic.topic_id, 777)
        self.assertEqual(
            self.store.topic_creation_intent(
                "thread-topic-observed"
            ).status,
            "completed",
        )

    async def test_ambiguous_topic_intent_rejects_stale_observed_topic(
        self,
    ) -> None:
        self.store.reserve_topic_creation(
            thread_id="thread-topic-stale",
            chat_id=-100500,
            title="Stale recovery",
        )
        self.store.mark_topic_creation_outcome_unknown(
            "thread-topic-stale"
        )
        self.store.observe_topic(-100500, 778, "Stale recovery")

        def fail_probe(**_: Any) -> bool:
            raise TelegramError(
                "sendChatAction: Bad Request: TOPIC_ID_INVALID",
                method="sendChatAction",
                kind="api",
            )

        self.telegram.send_chat_action = fail_probe  # type: ignore[method-assign]

        with self.assertRaises(TopicCreationUnresolvedError):
            await self.service._create_topic_for_thread(
                thread_id="thread-topic-stale",
                title="Stale recovery",
                updated_at=131,
            )

        self.assertIsNone(
            self.store.topic_for_thread("thread-topic-stale")
        )
        self.assertIsNone(
            self.store.observed_topic_title(-100500, 778)
        )
        self.assertEqual(
            self.store.topic_creation_intent(
                "thread-topic-stale"
            ).status,
            "outcome_unknown",
        )

    async def test_recorded_topic_result_completes_without_remote_create(
        self,
    ) -> None:
        self.store.reserve_topic_creation(
            thread_id="thread-topic-recorded",
            chat_id=-100500,
            title="Recorded result",
        )
        self.store.mark_topic_creation_outcome_unknown(
            "thread-topic-recorded"
        )
        self.store.record_created_topic("thread-topic-recorded", 905)

        topic = await self.service._create_topic_for_thread(
            thread_id="thread-topic-recorded",
            title="Recorded result",
            updated_at=127,
        )

        self.assertEqual(self.telegram.created_topics, [])
        self.assertEqual(topic.topic_id, 905)
        self.assertEqual(
            self.store.topic_creation_intent(
                "thread-topic-recorded"
            ).status,
            "completed",
        )
        self.assertEqual(
            self.store.topic_for_thread("thread-topic-recorded"),
            topic,
        )

    async def test_existing_topic_mapping_never_reserves_or_creates(
        self,
    ) -> None:
        existing = self.store.topic_for_thread("thread-1")

        topic = await self.service._create_topic_for_thread(
            thread_id="thread-1",
            title="Ignored replacement",
            updated_at=128,
        )

        self.assertEqual(topic, existing)
        self.assertEqual(self.telegram.created_topics, [])
        self.assertIsNone(self.store.topic_creation_intent("thread-1"))

    async def test_explicit_queue_card_is_finalized_after_dispatch(self) -> None:
        await self.service.handle_telegram_update(
            self.topic_message("/queue следующий ход")
        )

        row = self.store.connection.execute(
            """
            SELECT status, status_message_id
            FROM queued_messages
            ORDER BY id DESC LIMIT 1
            """
        ).fetchone()
        self.assertEqual(row["status"], "sent")
        self.assertIsNotNone(row["status_message_id"])
        self.assertEqual(len(self.telegram.edited_messages), 1)
        self.assertEqual(
            self.telegram.edited_messages[0]["text"],
            "✅ Отправлено в Codex следующим ходом.",
        )

    async def test_queued_steer_callback_is_single_use(self) -> None:
        queued = self.store.enqueue(
            thread_id="thread-1",
            chat_id=-100500,
            topic_id=50,
            telegram_message_id=90,
            text="учти новое условие",
            client_id="tg:message:90",
        )
        self.service.busy_threads.add("thread-1")
        self.service.observed_turns["thread-1"] = "turn-active"
        callback = self.queue_callback(queued.queue_id)

        await self.service.handle_callback(callback)
        await self.service.handle_callback(
            self.queue_callback(
                queued.queue_id,
                callback_id="callback-repeat",
            )
        )

        self.assertEqual(self.store.queued_message(queued.queue_id).status, "sent")
        self.codex.steer_turn.assert_awaited_once_with(
            thread_id="thread-1",
            turn_id="turn-active",
            text="учти новое условие",
            client_id="tg:message:90",
        )
        self.assertEqual(len(self.telegram.edited_messages), 1)
        self.assertEqual(
            self.telegram.edited_messages[0]["text"],
            "⚡ Передано в Codex.",
        )
        self.assertIn("уже обработано", self.telegram.callback_answers[-1][1])

    async def test_failed_queued_steer_waits_for_history_reconciliation(self) -> None:
        queued = self.store.enqueue(
            thread_id="thread-1",
            chat_id=-100500,
            topic_id=50,
            telegram_message_id=90,
            text="не перезапускай",
            client_id="tg:message:90",
        )
        self.service.busy_threads.add("thread-1")
        self.service.observed_turns["thread-1"] = "turn-observed"
        self.codex.steer_turn.side_effect = CodexProtocolError("not owned")

        await self.service.handle_callback(self.queue_callback(queued.queue_id))

        self.assertEqual(
            self.store.queued_message(queued.queue_id).status,
            "dispatching",
        )
        self.assertEqual(self.telegram.edited_markups, [])
        self.assertIn("истории Codex", self.telegram.callback_answers[-1][1])

    async def test_new_steer_waits_behind_outcome_unknown_steer(self) -> None:
        uncertain = self.store.enqueue(
            thread_id="thread-1",
            chat_id=-100500,
            topic_id=50,
            telegram_message_id=90,
            text="possibly steered",
            client_id="tg:-100500:90",
        )
        self.assertTrue(self.store.claim_queue(uncertain.queue_id))
        self.service.busy_threads.add("thread-1")
        self.service.observed_turns["thread-1"] = "turn-active"

        await self.service.handle_telegram_update(
            self.topic_message("/steer wait behind it", message_id=91)
        )

        self.codex.steer_turn.assert_not_awaited()
        queued = self.store.queued_message_for_client_id("tg:-100500:91")
        self.assertIsNotNone(queued)
        self.assertEqual(queued.status, "pending")
        self.assertIn(
            "сверяется с историей Codex",
            self.telegram.sent_messages[-1]["text"],
        )

    async def test_idle_queued_steer_waits_for_history_reconciliation(
        self,
    ) -> None:
        queued = self.store.enqueue(
            thread_id="thread-1",
            chat_id=-100500,
            topic_id=50,
            telegram_message_id=90,
            text="следующий ход",
            client_id="tg:message:90",
        )
        self.service.start_turn = AsyncMock(return_value=False)

        await self.service.handle_callback(self.queue_callback(queued.queue_id))

        self.assertEqual(
            self.store.queued_message(queued.queue_id).status,
            "dispatching",
        )
        self.service.start_turn.assert_awaited_once()
        self.assertEqual(self.telegram.edited_markups, [])
        self.assertIn("был ли ход принят", self.telegram.callback_answers[-1][1])

    async def test_unauthorized_queue_callback_cannot_mutate_queue(self) -> None:
        queued = self.store.enqueue(
            thread_id="thread-1",
            chat_id=-100500,
            topic_id=50,
            telegram_message_id=90,
            text="очередь",
            client_id="tg:message:90",
        )
        self.service.busy_threads.add("thread-1")
        self.service.observed_turns["thread-1"] = "turn-active"

        await self.service.handle_callback(
            self.queue_callback(queued.queue_id, sender_id=999)
        )

        self.assertEqual(
            self.store.queued_message(queued.queue_id).status,
            "pending",
        )
        self.codex.steer_turn.assert_not_awaited()
        self.assertEqual(self.telegram.callback_answers[-1][1], "Нет доступа")

    async def test_queue_callback_from_wrong_topic_cannot_mutate_queue(
        self,
    ) -> None:
        queued = self.store.enqueue(
            thread_id="thread-1",
            chat_id=-100500,
            topic_id=50,
            telegram_message_id=90,
            text="очередь",
            client_id="tg:message:wrong-topic",
        )

        await self.service.handle_callback(
            self.queue_callback(queued.queue_id, topic_id=999)
        )

        self.assertEqual(
            self.store.queued_message(queued.queue_id).status,
            "pending",
        )
        self.codex.steer_turn.assert_not_awaited()
        self.assertIn(
            "другой очереди",
            self.telegram.callback_answers[-1][1],
        )

    async def test_approval_callback_must_match_exact_card(self) -> None:
        self.add_pending_approval()
        callback = {
            "id": "approval-wrong-card",
            "data": "apr:approval1:once",
            "from": {"id": 100, "is_bot": False},
            "message": {
                "message_id": 999,
                "message_thread_id": 50,
                "chat": {"id": -100500, "type": "supergroup"},
            },
        }

        await self.service.handle_callback(callback)

        self.codex.respond.assert_not_awaited()
        self.assertIn("approval1", self.service.pending_requests)
        self.assertIn(
            "не относится",
            self.telegram.callback_answers[-1][1],
        )

    async def test_thread_name_notification_uses_thread_name_field(self) -> None:
        await self.service.on_codex_notification(
            {
                "method": "thread/name/updated",
                "params": {
                    "threadId": "thread-1",
                    "threadName": "Updated from Codex",
                },
            }
        )

        self.assertEqual(
            self.telegram.edited_topics,
            [(-100500, 50, "Updated from Codex")],
        )
        self.assertEqual(
            self.store.topic_for_thread("thread-1").title,
            "Updated from Codex",
        )

    async def test_archive_notification_clears_live_turn_state(self) -> None:
        self.service.active_turns["thread-1"] = "turn-active"
        self.service.observed_turns["thread-1"] = "turn-active"
        self.service.busy_threads.add("thread-1")
        self.codex.forget_thread = unittest.mock.Mock()

        await self.service.on_codex_notification(
            {
                "method": "thread/archived",
                "params": {"threadId": "thread-1"},
            }
        )

        self.codex.forget_thread.assert_called_once_with("thread-1")
        self.assertNotIn("thread-1", self.service.active_turns)
        self.assertNotIn("thread-1", self.service.observed_turns)
        self.assertNotIn("thread-1", self.service.busy_threads)
        self.assertTrue(self.service._sync_requested.is_set())

    async def test_deleted_thread_removes_orphan_topic(self) -> None:
        self.codex.forget_thread = unittest.mock.Mock()

        await self.service.on_codex_notification(
            {
                "method": "thread/deleted",
                "params": {"threadId": "thread-1"},
            }
        )

        self.codex.forget_thread.assert_called_once_with("thread-1")
        self.assertEqual(self.telegram.deleted_topics, [(-100500, 50)])
        self.assertTrue(self.store.topic_for_thread("thread-1").archived)

    async def test_failed_thread_name_notification_keeps_retryable_title(
        self,
    ) -> None:
        def fail_rename(
            chat_id: int,
            message_thread_id: int,
            name: str,
        ) -> bool:
            raise TelegramError("temporary rename failure")

        self.telegram.edit_forum_topic = fail_rename  # type: ignore[method-assign]

        await self.service.on_codex_notification(
            {
                "method": "thread/name/updated",
                "params": {
                    "threadId": "thread-1",
                    "threadName": "Retry this title",
                },
            }
        )

        self.assertEqual(
            self.store.topic_for_thread("thread-1").title,
            "Test topic",
        )

    async def test_secret_user_input_is_left_for_trusted_client(
        self,
    ) -> None:
        await self.service.on_codex_server_request(
            {
                "id": "secret-request",
                "method": "item/tool/requestUserInput",
                "params": {
                    "threadId": "thread-1",
                    "questions": [
                        {
                            "id": "credential",
                            "header": "Credential",
                            "question": "Enter the credential",
                            "isSecret": True,
                        }
                    ],
                },
            }
        )

        self.codex.respond.assert_not_awaited()
        self.assertEqual(self.service.pending_requests, {})
        pending_count = self.store.connection.execute(
            "SELECT COUNT(*) FROM pending_requests"
        ).fetchone()[0]
        self.assertEqual(pending_count, 0)
        self.assertEqual(self.telegram.sent_messages, [])

    async def test_unsupported_server_request_does_not_break_other_client(
        self,
    ) -> None:
        await self.service.on_codex_server_request(
            {
                "id": "dynamic-tool-request",
                "method": "item/tool/call",
                "params": {"threadId": "thread-1"},
            }
        )

        self.codex.respond.assert_not_awaited()
        self.assertEqual(self.service.pending_requests, {})
        self.assertEqual(self.telegram.sent_messages, [])

    async def test_failed_approval_delivery_clears_memory_pending(self) -> None:
        def fail_send(**_: Any) -> list[dict[str, Any]]:
            raise TelegramError("offline")

        self.telegram.send_message = fail_send  # type: ignore[method-assign]

        with self.assertRaises(TelegramError):
            await self.service.on_codex_server_request(
                {
                    "id": "approval-delivery-failure",
                    "method": "item/commandExecution/requestApproval",
                    "params": {
                        "threadId": "thread-1",
                        "command": ["safe-tool", "--check"],
                    },
                }
            )

        self.assertEqual(self.service.pending_requests, {})
        row = self.store.connection.execute(
            """
            SELECT status FROM pending_requests
            WHERE public_id = (
                SELECT public_id FROM pending_requests
                ORDER BY created_at DESC LIMIT 1
            )
            """
        ).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row["status"], "telegram_delivery_failed")

    async def test_ambiguous_approval_delivery_remains_usable_and_visible(
        self,
    ) -> None:
        original_send = self.telegram.send_message

        def applied_but_response_lost(**_: Any) -> list[dict[str, Any]]:
            raise TelegramError(
                "response lost SECRET_APPROVAL_DELIVERY",
                method="sendMessage",
                kind="network_timeout",
                retryable=True,
                outcome_ambiguous=True,
            )

        self.telegram.send_message = applied_but_response_lost  # type: ignore[method-assign]
        with self.assertRaises(TelegramError):
            await self.service.on_codex_server_request(
                {
                    "id": "approval-ambiguous-delivery",
                    "method": "item/commandExecution/requestApproval",
                    "params": {
                        "threadId": "thread-1",
                        "command": ["safe-tool", "--check"],
                    },
                }
            )

        self.telegram.send_message = original_send  # type: ignore[method-assign]
        self.assertEqual(len(self.service.pending_requests), 1)
        public_id = next(iter(self.service.pending_requests))
        row = self.store.connection.execute(
            """
            SELECT status FROM pending_requests WHERE public_id = ?
            """,
            (public_id,),
        ).fetchone()
        self.assertEqual(row["status"], "delivery_outcome_unknown")
        health = self.store.delivery_uncertainty_health()
        self.assertEqual(health["approvalPromptsOutcomeUnknown"], 1)
        self.assertNotIn("SECRET_APPROVAL_DELIVERY", repr(health))

        self.add_pending_approval(public_id="approval-other")
        handled = await self.service._try_resolve_pending_text(
            "thread-1",
            "+",
        )
        self.assertTrue(handled)
        self.codex.respond.assert_not_awaited()

        await self.service.handle_callback(
            {
                "id": "ambiguous-approval-button",
                "data": f"apr:{public_id}:once",
                "from": {"id": 100, "is_bot": False},
                "message": {
                    "message_id": 501,
                    "message_thread_id": 50,
                    "chat": {"id": -100500, "type": "supergroup"},
                },
            }
        )
        self.codex.respond.assert_awaited_once_with(
            "approval-ambiguous-delivery",
            result={"decision": "accept"},
        )
        self.assertIn("approval-other", self.service.pending_requests)
        self.assertEqual(
            self.store.delivery_uncertainty_health()[
                "approvalPromptsOutcomeUnknown"
            ],
            0,
        )

    async def test_telegram_loop_backs_off_and_resets_after_success(self) -> None:
        self.service._retry_jitter = lambda: 0.5
        self.service._tg = AsyncMock(
            side_effect=[
                TelegramError(
                    "network failure",
                    kind="network_error",
                    retryable=True,
                ),
                [],
                TelegramError(
                    "gateway failure",
                    kind="http_5xx",
                    retryable=True,
                ),
                asyncio.CancelledError(),
            ]
        )

        with patch(
            "codex_telegram_bridge.service.asyncio.sleep",
            new=AsyncMock(),
        ) as sleep:
            with self.assertRaises(asyncio.CancelledError):
                await self.service.telegram_loop()

        self.assertEqual(
            [call.args[0] for call in sleep.await_args_list],
            [1.0, 1.0],
        )
        self.assertEqual(
            self.service.telegram_update_health.last_error_kind,
            "http_5xx",
        )
        self.assertEqual(
            self.service.telegram_update_health.consecutive_failures,
            1,
        )
        self.assertIsNotNone(
            self.service.telegram_update_health.last_success_at,
        )
        self.assertIsNotNone(
            self.service.telegram_update_health.last_error_at,
        )

    async def test_transient_command_setup_failure_does_not_exit(self) -> None:
        self.service._retry_jitter = lambda: 0.5
        self.service._tg = AsyncMock(
            side_effect=[
                TelegramError(
                    "gateway detail",
                    kind="http_5xx",
                    retryable=True,
                ),
                None,
            ]
        )

        with patch(
            "codex_telegram_bridge.service.asyncio.sleep",
            new=AsyncMock(),
        ) as sleep:
            await self.service.telegram_setup_loop()

        self.assertEqual(self.service._tg.await_count, 2)
        sleep.assert_awaited_once_with(1.0)

    async def test_telegram_loop_backoff_is_bounded(self) -> None:
        self.service._retry_jitter = lambda: 0.5
        failures = [
            TelegramError(
                "network failure",
                kind="network_error",
                retryable=True,
            )
            for _ in range(8)
        ]
        self.service._tg = AsyncMock(
            side_effect=[*failures, asyncio.CancelledError()]
        )

        with patch(
            "codex_telegram_bridge.service.asyncio.sleep",
            new=AsyncMock(),
        ) as sleep:
            with self.assertRaises(asyncio.CancelledError):
                await self.service.telegram_loop()

        delays = [call.args[0] for call in sleep.await_args_list]
        self.assertEqual(delays[:6], [1.0, 2.0, 4.0, 8.0, 16.0, 30.0])
        self.assertTrue(
            all(
                delay <= TELEGRAM_UPDATE_BACKOFF_MAXIMUM_SECONDS
                for delay in delays
            )
        )

    async def test_telegram_loop_uses_short_poll_after_repeated_failures(self) -> None:
        self.service._retry_jitter = lambda: 0.5
        self.service._tg = AsyncMock(
            side_effect=[
                RuntimeError("broken long poll"),
                RuntimeError("broken long poll"),
                RuntimeError("broken long poll"),
                [],
                asyncio.CancelledError(),
            ]
        )

        with patch(
            "codex_telegram_bridge.service.asyncio.sleep",
            new=AsyncMock(),
        ):
            with self.assertRaises(asyncio.CancelledError):
                await self.service.telegram_loop()

        timeouts = [call.kwargs["timeout"] for call in self.service._tg.await_args_list]
        self.assertEqual(timeouts[:4], [25, 25, 25, 0])
        self.assertEqual(self.service.telegram_update_health.consecutive_failures, 0)

    def test_watchdog_rejects_only_stale_repeated_local_fault(self) -> None:
        self.service.telegram_update_health.record_failure("network_error")
        self.service.telegram_update_health.consecutive_failures = 4
        self.service._telegram_update_started_at = 1.0
        with patch("codex_telegram_bridge.service.time.time", return_value=1000.0):
            self.assertTrue(self.service.local_watchdog_healthy())

        self.service.telegram_update_health.last_error_kind = "unexpected"
        with patch("codex_telegram_bridge.service.time.time", return_value=1000.0):
            self.assertFalse(self.service.local_watchdog_healthy())

    async def test_protocol_sync_failure_requests_reconnect(
        self,
    ) -> None:
        self.service.sync_threads = AsyncMock(
            side_effect=CodexProtocolError("protocol failure")
        )

        with patch(
            "codex_telegram_bridge.service.asyncio.sleep",
            new=AsyncMock(),
        ) as sleep:
            await self.service.thread_sync_loop()

        sleep.assert_not_awaited()
        self.assertEqual(
            self.service.thread_sync_health.last_error_kind,
            "codex_protocol",
        )
        self.assertEqual(
            self.service.thread_sync_health.consecutive_failures,
            1,
        )
        self.assertTrue(self.service._codex_reconnect_requested.is_set())
        self.assertFalse(self.service.codex_available)

    async def test_unknown_codex_version_enters_degraded_mode(self) -> None:
        self.service.codex = SimpleNamespace(
            start=AsyncMock(
                side_effect=[
                    CodexProtocolCompatibilityError("future version detail"),
                    asyncio.CancelledError(),
                ]
            ),
            stop=AsyncMock(),
        )
        self.service.sync_threads = AsyncMock()
        self.service._wait_for_codex_retry = AsyncMock()

        with self.assertRaises(asyncio.CancelledError):
            await self.service.codex_connection_loop()

        self.assertFalse(self.service.codex_available)
        self.assertEqual(
            self.service.codex_degraded_reason,
            "protocol_incompatible",
        )
        self.assertTrue(self.service._recovery_notice_pending)
        self.service.sync_threads.assert_not_awaited()

    async def test_reconnect_resumes_durable_queue_and_notifies_once(
        self,
    ) -> None:
        await self.service._enter_codex_degraded(
            "socket_unavailable",
            expire_prompts=False,
        )
        await self.service.handle_telegram_update(
            self.topic_message("выполни после reconnect", message_id=95)
        )
        started = asyncio.Event()
        monitor_release = asyncio.Event()

        async def start_queued_turn(**_: Any) -> dict[str, str]:
            started.set()
            return {"id": "turn-after-reconnect"}

        async def wait_closed() -> None:
            await monitor_release.wait()

        reconnect_codex = SimpleNamespace(
            server_version="0.146.0-alpha.3.1",
            start=AsyncMock(),
            stop=AsyncMock(),
            wait_closed=wait_closed,
            start_turn=AsyncMock(side_effect=start_queued_turn),
        )
        self.service.codex = reconnect_codex
        self.service.sync_threads = AsyncMock()

        async def sync_forever() -> None:
            await monitor_release.wait()

        self.service.thread_sync_loop = sync_forever
        connection_task = asyncio.create_task(
            self.service.codex_connection_loop()
        )
        try:
            await asyncio.wait_for(started.wait(), timeout=1)
            queue_id = self.store.connection.execute(
                "SELECT id FROM queued_messages LIMIT 1"
            ).fetchone()["id"]
            queued = self.store.queued_message(queue_id)
            for _ in range(100):
                if queued is not None and queued.status == "sent":
                    break
                await asyncio.sleep(0)
                queued = self.store.queued_message(queue_id)
            self.assertIsNotNone(queued)
            self.assertEqual(queued.status, "sent")
            recovery_notices = [
                message
                for message in self.telegram.sent_messages
                if "Связь с Codex восстановлена" in message["text"]
            ]
            self.assertEqual(len(recovery_notices), 1)

            await self.service._mark_codex_healthy()
            recovery_notices = [
                message
                for message in self.telegram.sent_messages
                if "Связь с Codex восстановлена" in message["text"]
            ]
            self.assertEqual(len(recovery_notices), 1)
        finally:
            connection_task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await connection_task

    async def test_sync_request_wakes_thread_sync_loop_immediately(self) -> None:
        first_sync_finished = asyncio.Event()
        sync_count = 0

        async def sync_threads() -> None:
            nonlocal sync_count
            sync_count += 1
            if sync_count == 1:
                first_sync_finished.set()
                return
            raise asyncio.CancelledError()

        self.service.config = replace(
            self.service.config,
            thread_poll_seconds=60,
        )
        self.service.sync_threads = AsyncMock(side_effect=sync_threads)
        task = asyncio.create_task(self.service.thread_sync_loop())

        await first_sync_finished.wait()
        self.service._sync_requested.set()

        with self.assertRaises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=1)
        self.assertEqual(sync_count, 2)

    async def test_thread_sync_loop_backoff_is_bounded(self) -> None:
        self.service._retry_jitter = lambda: 0.5
        self.service.sync_threads = AsyncMock(
            side_effect=[
                *[RuntimeError("offline") for _ in range(7)],
                asyncio.CancelledError(),
            ]
        )

        with patch(
            "codex_telegram_bridge.service.asyncio.sleep",
            new=AsyncMock(),
        ) as sleep:
            with self.assertRaises(asyncio.CancelledError):
                await self.service.thread_sync_loop()

        delays = [call.args[0] for call in sleep.await_args_list]
        self.assertEqual(delays[:6], [2.0, 4.0, 8.0, 16.0, 32.0, 60.0])
        self.assertTrue(
            all(delay <= THREAD_SYNC_BACKOFF_MAXIMUM_SECONDS for delay in delays)
        )

    async def test_loop_cancellation_does_not_sleep(self) -> None:
        self.service._tg = AsyncMock(side_effect=asyncio.CancelledError())
        self.service.sync_threads = AsyncMock(
            side_effect=asyncio.CancelledError()
        )

        with patch(
            "codex_telegram_bridge.service.asyncio.sleep",
            new=AsyncMock(),
        ) as sleep:
            with self.assertRaises(asyncio.CancelledError):
                await self.service.telegram_loop()
            with self.assertRaises(asyncio.CancelledError):
                await self.service.thread_sync_loop()

        sleep.assert_not_awaited()

    async def test_stale_approval_card_is_disabled_on_restart(self) -> None:
        self.add_pending_approval()
        self.service.pending_requests.clear()

        await self.service.expire_stale_prompt_cards()

        row = self.store.connection.execute(
            """
            SELECT status FROM pending_requests
            WHERE public_id = 'approval1'
            """
        ).fetchone()
        self.assertEqual(row["status"], "expired_restart")
        self.assertEqual(len(self.telegram.edited_messages), 1)
        self.assertIn("истёк", self.telegram.edited_messages[0]["text"])
        self.assertEqual(
            self.telegram.edited_messages[0]["reply_markup"],
            {"inline_keyboard": []},
        )

    async def test_identical_commentary_does_not_create_duplicate_card(
        self,
    ) -> None:
        topic = self.store.topic_for_thread("thread-1")
        self.assertIsNotNone(topic)

        def unchanged(**_: Any) -> bool:
            raise TelegramError("Bad Request: message is not modified")

        await self.service.mirror_item(
            topic,
            {
                "id": "same-commentary-1",
                "type": "agentMessage",
                "text": "Одинаковый статус.",
                "phase": "commentary",
            },
            turn_id="turn-same",
        )
        self.telegram.edit_message_text = unchanged  # type: ignore[method-assign]
        await self.service.mirror_item(
            topic,
            {
                "id": "same-commentary-2",
                "type": "agentMessage",
                "text": "Одинаковый статус.",
                "phase": "commentary",
            },
            turn_id="turn-same",
        )

        self.assertEqual(len(self.telegram.sent_rich_messages), 1)
        self.assertEqual(len(self.telegram.sent_messages), 0)
        first_id = self.store.connection.execute(
            """
            SELECT telegram_message_id FROM mirrored_items
            WHERE item_id = 'same-commentary-1'
            """
        ).fetchone()[0]
        second_id = self.store.connection.execute(
            """
            SELECT telegram_message_id FROM mirrored_items
            WHERE item_id = 'same-commentary-2'
            """
        ).fetchone()[0]
        self.assertEqual(first_id, second_id)

    async def test_notification_and_history_alias_render_progress_once(
        self,
    ) -> None:
        topic = self.store.topic_for_thread("thread-1")
        self.assertIsNotNone(topic)
        notification = {
            "method": "item/completed",
            "params": {
                "threadId": "thread-1",
                "turnId": "turn-progress-alias",
                "item": {
                    "id": "transient-progress-id",
                    "type": "agentMessage",
                    "text": "Один видимый статус.",
                    "phase": "commentary",
                },
            },
        }
        history = {
            "id": "thread-1",
            "turns": [
                {
                    "id": "turn-progress-alias",
                    "items": [
                        {
                            "id": "durable-progress-id",
                            "type": "agentMessage",
                            "text": "Один видимый статус.",
                            "phase": "commentary",
                        }
                    ],
                }
            ],
        }
        self.store.upsert_turn_context(
            thread_id="thread-1",
            turn_id="turn-progress-alias",
            source_message_id=90,
        )

        await self.service.on_codex_notification(notification)
        await self.service.sync_thread_history(
            history,
            topic,
            initial=False,
        )

        entries = self.store.progress_entries(
            "thread-1",
            "turn-progress-alias",
        )
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0].text, "Один видимый статус.")
        self.assertEqual(len(self.telegram.sent_rich_messages), 1)
        self.assertEqual(len(self.telegram.edited_messages), 0)
        self.assertTrue(
            self.store.has_mirrored_item(
                "thread-1",
                "transient-progress-id",
            )
        )
        self.assertTrue(
            self.store.has_mirrored_item(
                "thread-1",
                "durable-progress-id",
            )
        )

    async def test_completed_progress_card_is_not_edited_on_every_sync(
        self,
    ) -> None:
        topic = self.store.topic_for_thread("thread-1")
        self.assertIsNotNone(topic)
        item = {
            "id": "closed-progress-item",
            "type": "agentMessage",
            "text": "Готовлю итог.",
            "phase": "commentary",
        }
        await self.service.mirror_item(
            topic,
            item,
            turn_id="turn-closed-once",
        )
        await self.service._finalize_progress_card(
            topic,
            turn_id="turn-closed-once",
            outcome="completed",
        )
        closed_context = self.store.turn_context(
            "thread-1",
            "turn-closed-once",
        )
        self.assertIsNotNone(closed_context)
        self.assertTrue(closed_context.progress_closed)
        closed_updated_at = self.store.connection.execute(
            """
            SELECT updated_at FROM turn_contexts
            WHERE thread_id = ? AND turn_id = ?
            """,
            ("thread-1", "turn-closed-once"),
        ).fetchone()[0]
        self.telegram.edited_messages.clear()
        completed_history = {
            "id": "thread-1",
            "turns": [
                {
                    "id": "turn-closed-once",
                    "status": "completed",
                    "items": [item],
                }
            ],
        }

        await self.service.sync_thread_history(
            completed_history,
            topic,
            initial=False,
        )
        await self.service.sync_thread_history(
            completed_history,
            topic,
            initial=False,
        )

        self.assertEqual(self.telegram.edited_messages, [])
        self.assertEqual(
            self.store.connection.execute(
                """
                SELECT updated_at FROM turn_contexts
                WHERE thread_id = ? AND turn_id = ?
                """,
                ("thread-1", "turn-closed-once"),
            ).fetchone()[0],
            closed_updated_at,
        )

    async def test_changed_terminal_outcome_updates_closed_progress_card(
        self,
    ) -> None:
        topic = self.store.topic_for_thread("thread-1")
        self.assertIsNotNone(topic)
        await self.service.mirror_item(
            topic,
            {
                "id": "changed-outcome-progress",
                "type": "agentMessage",
                "text": "Почти готово.",
                "phase": "commentary",
            },
            turn_id="turn-changed-outcome",
        )
        await self.service._finalize_progress_card(
            topic,
            turn_id="turn-changed-outcome",
            outcome="completed",
        )
        self.telegram.edited_messages.clear()

        await self.service._finalize_progress_card(
            topic,
            turn_id="turn-changed-outcome",
            outcome="interrupted",
        )

        self.assertEqual(len(self.telegram.edited_messages), 1)
        context = self.store.turn_context(
            "thread-1",
            "turn-changed-outcome",
        )
        self.assertIsNotNone(context)
        self.assertEqual(context.progress_outcome, "interrupted")

    async def test_concurrent_progress_finalizers_edit_once(self) -> None:
        topic = self.store.topic_for_thread("thread-1")
        self.assertIsNotNone(topic)
        await self.service.mirror_item(
            topic,
            {
                "id": "concurrent-finalize-progress",
                "type": "agentMessage",
                "text": "Закрываю карточку.",
                "phase": "commentary",
            },
            turn_id="turn-concurrent-finalize",
        )

        await asyncio.gather(
            self.service._finalize_progress_card(
                topic,
                turn_id="turn-concurrent-finalize",
                outcome="completed",
            ),
            self.service._finalize_progress_card(
                topic,
                turn_id="turn-concurrent-finalize",
                outcome="completed",
            ),
        )

        # Closing a native Rich Details card is intentionally two-phase: a
        # compact text reset clears Telegram's client-local open state, then
        # the same message is restored as a closed details block. Concurrent
        # finalizers must still perform only this one logical close.
        self.assertEqual(len(self.telegram.edited_messages), 2)
        self.assertIn("text", self.telegram.edited_messages[0])
        self.assertIn("rich_message", self.telegram.edited_messages[1])

    async def test_failed_progress_finalization_remains_retryable(self) -> None:
        topic = self.store.topic_for_thread("thread-1")
        self.assertIsNotNone(topic)
        await self.service.mirror_item(
            topic,
            {
                "id": "retry-finalize-progress",
                "type": "agentMessage",
                "text": "Проверяю повтор.",
                "phase": "commentary",
            },
            turn_id="turn-retry-finalize",
        )
        original_edit = self.telegram.edit_message_text

        def ambiguous_failure(**_: Any) -> bool:
            raise TelegramError(
                "editMessageText: Telegram network error",
                method="editMessageText",
                kind="network_timeout",
                retryable=True,
                outcome_ambiguous=True,
            )

        self.telegram.edit_message_text = ambiguous_failure  # type: ignore[method-assign]
        with self.assertRaises(TelegramError):
            await self.service._finalize_progress_card(
                topic,
                turn_id="turn-retry-finalize",
                outcome="completed",
            )
        failed_context = self.store.turn_context(
            "thread-1",
            "turn-retry-finalize",
        )
        self.assertIsNotNone(failed_context)
        self.assertFalse(failed_context.progress_closed)

        self.telegram.edit_message_text = original_edit  # type: ignore[method-assign]
        await self.service._finalize_progress_card(
            topic,
            turn_id="turn-retry-finalize",
            outcome="completed",
        )
        retried_context = self.store.turn_context(
            "thread-1",
            "turn-retry-finalize",
        )
        self.assertIsNotNone(retried_context)
        self.assertTrue(retried_context.progress_closed)
        self.assertEqual(retried_context.progress_outcome, "completed")

    def test_user_input_card_redacts_unflagged_secret_shape(self) -> None:
        pending = PendingServerRequest(
            public_id="redact-input",
            server_request_id="request-redact",
            method="item/tool/requestUserInput",
            thread_id="thread-1",
            params={
                "questions": [
                    {
                        "id": "q",
                        "header": "Check",
                        "question": "TOKEN=secretvalue",
                        "isSecret": False,
                    }
                ]
            },
        )

        text, _ = self.service._render_user_input_request(pending)

        self.assertNotIn("secretvalue", text)
        self.assertIn("[REDACTED]", text)

    async def test_multiple_user_inputs_accept_visible_numeric_labels(
        self,
    ) -> None:
        pending = PendingServerRequest(
            public_id="multi-input",
            server_request_id="request-multi",
            method="item/tool/requestUserInput",
            thread_id="thread-1",
            params={
                "questions": [
                    {"id": "first_internal", "question": "First?"},
                    {"id": "second_internal", "question": "Second?"},
                ]
            },
            telegram_message_id=70,
        )
        self.service.pending_requests[pending.public_id] = pending
        self.store.save_pending_request(
            public_id=pending.public_id,
            thread_id="thread-1",
            request_kind="user_input",
            metadata={"method": pending.method},
            telegram_message_id=70,
        )

        await self.service.resolve_user_input_text(
            pending.public_id,
            "1: alpha\n2: beta",
        )

        self.codex.respond.assert_awaited_once_with(
            "request-multi",
            result={
                "answers": {
                    "first_internal": {"answers": ["alpha"]},
                    "second_internal": {"answers": ["beta"]},
                }
            },
        )

    async def test_approval_buttons_respect_available_decisions(self) -> None:
        await self.service.on_codex_server_request(
            {
                "id": "limited-approval",
                "method": "item/commandExecution/requestApproval",
                "params": {
                    "threadId": "thread-1",
                    "command": ["safe-tool"],
                    "availableDecisions": ["accept", "decline"],
                },
            }
        )

        buttons = [
            button["text"]
            for row in self.telegram.sent_messages[-1]["reply_markup"][
                "inline_keyboard"
            ]
            for button in row
        ]
        self.assertFalse(any("сессии" in text for text in buttons))
        pending = next(iter(self.service.pending_requests.values()))
        await self.service.resolve_approval(
            pending.public_id,
            "session",
            callback_query_id="limited-callback",
        )
        self.codex.respond.assert_not_awaited()
        self.assertIn(
            "недоступно",
            self.telegram.callback_answers[-1][1],
        )

    async def test_empty_available_decisions_fail_closed(self) -> None:
        await self.service.on_codex_server_request(
            {
                "id": "empty-decisions",
                "method": "item/commandExecution/requestApproval",
                "params": {
                    "threadId": "thread-1",
                    "command": ["safe-tool"],
                    "availableDecisions": [],
                },
            }
        )

        # The bridge is one of several clients subscribed to the shared
        # server. Unsafe-to-render requests are left for Codex Desktop instead
        # of winning the first-response race with a bridge error.
        self.codex.respond.assert_not_awaited()
        self.assertEqual(self.telegram.sent_messages, [])
        self.assertEqual(self.service.pending_requests, {})

    async def test_file_change_without_exact_diff_fails_closed(self) -> None:
        await self.service.on_codex_server_request(
            {
                "id": "unseen-file-change",
                "method": "item/fileChange/requestApproval",
                "params": {
                    "threadId": "thread-1",
                    "itemId": "opaque-item",
                    "grantRoot": "/workspace",
                    "reason": "Apply changes",
                    "availableDecisions": ["accept", "decline"],
                },
            }
        )

        self.codex.respond.assert_not_awaited()
        self.assertEqual(self.telegram.sent_messages, [])
        self.assertEqual(self.service.pending_requests, {})

    async def test_file_change_approval_uses_started_item_scope(self) -> None:
        await self.service.on_codex_notification(
            {
                "method": "item/started",
                "params": {
                    "threadId": "thread-1",
                    "turnId": "turn-file-approval",
                    "item": {
                        "id": "file-change-item",
                        "type": "fileChange",
                        "changes": [
                            {
                                "path": "configuration.yaml",
                                "kind": "update",
                            }
                        ],
                    },
                },
            }
        )

        await self.service.on_codex_server_request(
            {
                "id": "file-change-request",
                "method": "item/fileChange/requestApproval",
                "params": {
                    "threadId": "thread-1",
                    "turnId": "turn-file-approval",
                    "itemId": "file-change-item",
                    "availableDecisions": ["accept", "decline"],
                },
            }
        )

        self.codex.respond.assert_not_awaited()
        self.assertEqual(len(self.telegram.sent_messages), 1)
        self.assertIn(
            "configuration.yaml",
            self.telegram.sent_messages[0]["text"],
        )
        self.assertEqual(len(self.service.pending_requests), 1)

    async def test_commentary_updates_one_card_and_final_replies_to_source(
        self,
    ) -> None:
        topic = self.store.topic_for_thread("thread-1")
        self.assertIsNotNone(topic)
        self.store.upsert_turn_context(
            thread_id="thread-1",
            turn_id="turn-compact",
            source_message_id=90,
        )

        await self.service.mirror_item(
            topic,
            {
                "id": "commentary-1",
                "type": "agentMessage",
                "text": "Смотрю конфигурацию.",
                "phase": "commentary",
            },
            turn_id="turn-compact",
        )
        await self.service.mirror_item(
            topic,
            {
                "id": "commentary-2",
                "type": "agentMessage",
                "text": "Проверяю тесты.",
                "phase": "commentary",
            },
            turn_id="turn-compact",
        )
        await self.service.mirror_item(
            topic,
            {
                "id": "final-1",
                "type": "agentMessage",
                "text": "Готово.",
                "phase": "final_answer",
            },
            turn_id="turn-compact",
        )

        self.assertEqual(len(self.telegram.edited_messages), 3)
        open_message = self.telegram.edited_messages[0]["rich_message"]
        open_details = open_message["blocks"][1]
        self.assertIn("text", self.telegram.edited_messages[1])
        closed_message = self.telegram.edited_messages[2]["rich_message"]
        closed_details = closed_message["blocks"][1]
        self.assertTrue(open_details["is_open"])
        self.assertNotIn("is_open", closed_details)
        self.assertEqual(
            open_message["blocks"][0]["text"],
            "🟡 Codex работает.",
        )
        self.assertEqual(
            closed_details["summary"],
            "Ход работы Codex",
        )
        collapse_button = self.telegram.edited_messages[2]["reply_markup"][
            "inline_keyboard"
        ][0][0]
        self.assertEqual(collapse_button["text"], "▲ Свернуть")
        self.assertEqual(collapse_button["callback_data"], "pgc:0")
        rendered_progress = "\n".join(
            block.get("text", "")
            for block in open_details["blocks"]
        )
        self.assertIn("Смотрю конфигурацию.", rendered_progress)
        self.assertIn("Проверяю тесты.", rendered_progress)
        self.assertIn("Шаг 1\nСмотрю конфигурацию.", rendered_progress)
        self.assertIn("Шаг 2\nПроверяю тесты.", rendered_progress)
        self.assertEqual(len(open_details["blocks"]), 1)
        self.assertEqual(len(self.telegram.sent_rich_messages), 1)
        self.assertEqual(len(self.telegram.sent_messages), 1)
        self.assertEqual(
            self.telegram.sent_rich_messages[0]["reply_to_message_id"],
            90,
        )
        self.assertEqual(
            self.telegram.sent_messages[0]["reply_to_message_id"],
            90,
        )
        self.assertIn("🟢 Codex", self.telegram.sent_messages[0]["text"])

    def test_progress_elapsed_minutes_use_russian_grammar(self) -> None:
        self.assertEqual(
            progress_summary(
                closed=False,
                outcome="completed",
                elapsed_minutes=0,
            ),
            "🟡 Codex работает.",
        )
        expected = {
            1: "1 минуту",
            2: "2 минуты",
            5: "5 минут",
            11: "11 минут",
            21: "21 минуту",
            22: "22 минуты",
            25: "25 минут",
        }
        for minutes, phrase in expected.items():
            with self.subTest(minutes=minutes):
                self.assertEqual(
                    progress_summary(
                        closed=False,
                        outcome="completed",
                        elapsed_minutes=minutes,
                    ),
                    f"🟡 Codex работает {phrase}.",
                )

    async def test_active_progress_card_heartbeats_once_per_elapsed_minute(
        self,
    ) -> None:
        topic = self.store.topic_for_thread("thread-1")
        self.assertIsNotNone(topic)
        turn_id = "turn-heartbeat"
        await self.service.mirror_item(
            topic,
            {
                "id": "heartbeat-progress",
                "type": "agentMessage",
                "text": "Жду завершения долгой операции.",
                "phase": "commentary",
            },
            turn_id=turn_id,
        )
        self.store.connection.execute(
            """
            UPDATE turn_contexts
            SET created_at = ?
            WHERE thread_id = ? AND turn_id = ?
            """,
            ("1970-01-01T00:00:00+00:00", "thread-1", turn_id),
        )
        self.store.connection.commit()
        self.service.busy_threads.add("thread-1")
        self.service.observed_turns["thread-1"] = turn_id
        self.telegram.edited_messages.clear()

        with patch(
            "codex_telegram_bridge.service.time.time",
            return_value=130,
        ):
            await self.service._refresh_progress_heartbeats()
            await self.service._refresh_progress_heartbeats()

        self.assertEqual(len(self.telegram.edited_messages), 1)
        two_minutes = self.telegram.edited_messages[0]["rich_message"][
            "blocks"
        ][0]
        self.assertEqual(
            two_minutes["text"],
            "🟡 Codex работает 2 минуты.",
        )

        with patch(
            "codex_telegram_bridge.service.time.time",
            return_value=190,
        ):
            await self.service._refresh_progress_heartbeats()

        self.assertEqual(len(self.telegram.edited_messages), 2)
        three_minutes = self.telegram.edited_messages[1]["rich_message"][
            "blocks"
        ][0]
        self.assertEqual(
            three_minutes["text"],
            "🟡 Codex работает 3 минуты.",
        )

        self.store.update_turn_progress_state(
            thread_id="thread-1",
            turn_id=turn_id,
            closed=True,
            outcome="completed",
        )
        with patch(
            "codex_telegram_bridge.service.time.time",
            return_value=250,
        ):
            await self.service._refresh_progress_heartbeats()
        self.assertEqual(len(self.telegram.edited_messages), 2)

    async def test_final_answer_uses_green_state_marker(self) -> None:
        topic = self.store.topic_for_thread("thread-1")
        self.assertIsNotNone(topic)

        await self.service.mirror_item(
            topic,
            {
                "id": "custom-emoji-final",
                "type": "agentMessage",
                "text": "Готово.",
                "phase": "final_answer",
            },
            turn_id="turn-custom-emoji",
        )

        sent = self.telegram.sent_messages[-1]
        self.assertTrue(sent["text"].startswith("🟢 Codex\n\n"))
        self.assertIsNone(sent["entities"])

    async def test_token_footer_deduplicates_notification_and_history(self) -> None:
        topic = self.store.topic_for_thread("thread-1")
        self.assertIsNotNone(topic)
        self.store.upsert_turn_context(
            thread_id="thread-1",
            turn_id="turn-usage",
            source_message_id=77,
        )
        await self.service.on_codex_notification(
            {
                "method": "item/completed",
                "params": {
                    "threadId": "thread-1",
                    "turnId": "turn-usage",
                    "item": {
                        "id": "live-final",
                        "type": "agentMessage",
                        "text": "Готово.",
                        "phase": "final_answer",
                    },
                },
            }
        )
        await self.service.on_codex_notification(
            {
                "method": "thread/tokenUsage/updated",
                "params": {
                    "threadId": "thread-1",
                    "turnId": "turn-usage",
                    "tokenUsage": {
                        "last": {"totalTokens": 1_200},
                        "total": {"totalTokens": 223_000},
                    },
                },
            }
        )
        await self.service.on_codex_notification(
            {
                "method": "turn/completed",
                "params": {
                    "threadId": "thread-1",
                    "turn": {"id": "turn-usage", "status": "completed"},
                },
            }
        )
        self.assertEqual(len(self.telegram.sent_messages), 1)
        self.assertTrue(
            self.telegram.sent_messages[0]["text"].endswith(
                "\n\n(1,2k / 223k tkn)"
            )
        )

        await self.service.mirror_item(
            topic,
            {
                "id": "history-final",
                "type": "agentMessage",
                "text": "Готово.",
                "phase": "final_answer",
            },
            turn_id="turn-usage",
            item_origin="history",
        )
        self.assertEqual(len(self.telegram.sent_messages), 1)
        self.assertTrue(
            self.store.has_mirrored_item("thread-1", "history-final")
        )

    async def test_bottom_button_rerenders_completed_progress_as_collapsed(
        self,
    ) -> None:
        topic = self.store.topic_for_thread("thread-1")
        self.assertIsNotNone(topic)
        await self.service.mirror_item(
            topic,
            {
                "id": "collapse-progress",
                "type": "agentMessage",
                "text": "Подробный видимый статус.",
                "phase": "commentary",
            },
            turn_id="turn-collapse",
        )
        await self.service._finalize_progress_card(
            topic,
            turn_id="turn-collapse",
            outcome="completed",
        )
        context = self.store.turn_context("thread-1", "turn-collapse")
        self.assertIsNotNone(context)
        self.assertIsNotNone(context.status_message_id)
        self.telegram.edited_messages.clear()

        with patch(
            "codex_telegram_bridge.service.asyncio.sleep",
            new=AsyncMock(),
        ) as sleep:
            await self.service.handle_callback(
                self.progress_collapse_callback(context.status_message_id)
            )

        self.assertEqual(len(self.telegram.edited_messages), 2)
        reset = self.telegram.edited_messages[0]
        self.assertEqual(
            reset["text"],
            "Codex закончил работу.",
        )
        self.assertNotIn("rich_message", reset)
        sleep.assert_awaited_once_with(PROGRESS_COLLAPSE_RESET_SECONDS)
        collapsed = self.telegram.edited_messages[1]
        details = collapsed["rich_message"]["blocks"][1]
        self.assertNotIn("is_open", details)
        self.assertEqual(
            details["summary"],
            "Ход работы Codex",
        )
        self.assertEqual(
            collapsed["reply_markup"]["inline_keyboard"][0][0][
                "callback_data"
            ],
            "pgc:1",
        )
        self.assertEqual(
            self.telegram.callback_answers[-1],
            ("progress-collapse-1", "Свёрнуто"),
        )

    async def test_bottom_collapse_button_must_match_exact_progress_card(
        self,
    ) -> None:
        await self.service.handle_callback(
            self.progress_collapse_callback(9999)
        )

        self.assertEqual(self.telegram.edited_messages, [])
        self.assertEqual(
            self.telegram.callback_answers[-1],
            ("progress-collapse-1", "Карточка уже недоступна"),
        )

    def test_telegram_visible_text_rewrites_only_local_markdown_links(
        self,
    ) -> None:
        workspace = self.service.config.workspace
        local_target = workspace / "docs" / "My Report.md"
        source = (
            f"[отчёт](<{local_target}:12>)\n"
            "[сайт](https://example.com/path)\n"
            "https://example.com/bare\n"
            f"`[inline](<{local_target}>)`\n"
            f"```\n[fenced](<{local_target}>)\n```\n"
            f"![image](<{local_target}>)\n"
            f"\\[escaped](<{local_target}>)"
        )

        rendered = telegram_visible_text(source, workspace)

        self.assertIn("отчёт — docs/My Report.md:12", rendered)
        self.assertIn("[сайт](https://example.com/path)", rendered)
        self.assertIn("https://example.com/bare", rendered)
        self.assertIn(f"`[inline](<{local_target}>)`", rendered)
        self.assertIn(f"[fenced](<{local_target}>)", rendered)
        self.assertIn("image — docs/My Report.md", rendered)
        self.assertIn(f"\\[escaped](<{local_target}>)", rendered)

    def test_final_answer_attachments_are_explicit_and_workspace_scoped(
        self,
    ) -> None:
        workspace = self.service.config.workspace
        output_directory = workspace / "outputs"
        output_directory.mkdir()
        report = output_directory / "report.xlsx"
        report.write_bytes(b"spreadsheet")
        docs_directory = workspace / "docs"
        docs_directory.mkdir()
        reference = docs_directory / "reference.pdf"
        reference.write_bytes(b"reference")
        handoff = docs_directory / "handoff.md"
        handoff.write_text("# Handoff\n", encoding="utf-8")
        secret = workspace / ".env"
        secret.write_text("TOKEN=do-not-send", encoding="utf-8")
        outside = workspace.parent / "outside.pdf"
        outside.write_bytes(b"outside")
        source = (
            f":codex-file-citation{{path=\"{reference}\" purpose=\"output\"}}\n"
            f"[отчёт](<{report}>)\n"
            f"[справка](<{reference}>)\n"
            f"[инструкция](<{handoff}>)\n"
            f":codex-file-citation{{path=\"{secret}\" purpose=\"output\"}}\n"
            f":codex-file-citation{{path=\"{outside}\" purpose=\"output\"}}\n"
            f"`[код](<{report}>)`\n"
            f"```\n:codex-file-citation{{path=\"{report}\" "
            'purpose="output"}}\n```'
        )

        attachments = final_answer_attachments(source, workspace)

        self.assertEqual(
            attachments,
            [report.resolve()],
        )

    async def test_final_answer_does_not_upload_technical_workspace_link(
        self,
    ) -> None:
        topic = self.store.topic_for_thread("thread-1")
        self.assertIsNotNone(topic)
        docs_directory = self.service.config.workspace / "docs"
        docs_directory.mkdir()
        handoff = docs_directory / "handoff.md"
        handoff.write_text("# Handoff\n", encoding="utf-8")
        self.store.upsert_turn_context(
            thread_id="thread-1",
            turn_id="turn-workspace-file",
            source_message_id=91,
        )

        await self.service.mirror_item(
            topic,
            {
                "id": "workspace-file-final",
                "type": "agentMessage",
                "phase": "final_answer",
                "text": f"Готово. [Инструкция](<{handoff}>)",
            },
            turn_id="turn-workspace-file",
            item_origin="notification",
        )

        self.assertEqual(len(self.telegram.sent_messages), 1)
        self.assertEqual(self.telegram.sent_documents, [])
        self.assertIn(
            "Инструкция — docs/handoff.md",
            self.telegram.sent_messages[0]["text"],
        )

    async def test_final_answer_uploads_explicit_output_file(self) -> None:
        topic = self.store.topic_for_thread("thread-1")
        self.assertIsNotNone(topic)
        output_directory = self.service.config.workspace / "outputs"
        output_directory.mkdir()
        report = output_directory / "result.xlsx"
        report.write_bytes(b"spreadsheet")
        self.store.upsert_turn_context(
            thread_id="thread-1",
            turn_id="turn-file",
            source_message_id=90,
        )

        await self.service.mirror_item(
            topic,
            {
                "id": "file-final",
                "type": "agentMessage",
                "phase": "final_answer",
                "text": (
                    "Готово. "
                    f":codex-file-citation{{path=\"{report}\" "
                    'purpose="output"}'
                ),
            },
            turn_id="turn-file",
            item_origin="notification",
        )

        self.assertEqual(len(self.telegram.sent_messages), 1)
        self.assertEqual(len(self.telegram.sent_documents), 1)
        document = self.telegram.sent_documents[0]
        self.assertEqual(document["chat_id"], -100500)
        self.assertEqual(document["message_thread_id"], 50)
        self.assertEqual(document["reply_to_message_id"], 801)
        self.assertEqual(document["file_path"], report.resolve())
        self.assertEqual(document["caption"], "📎 result.xlsx")
        self.assertIn(
            "result.xlsx — outputs/result.xlsx",
            self.telegram.sent_messages[0]["text"],
        )
        self.assertNotIn(
            ":codex-file-citation",
            self.telegram.sent_messages[0]["text"],
        )

    async def test_codex_app_user_message_uploads_photo_and_video(self) -> None:
        topic = self.store.topic_for_thread("thread-1")
        self.assertIsNotNone(topic)
        video = self.service.config.workspace / "clip.mp4"
        video.write_bytes(b"video")
        data_url = (
            "data:image/png;base64,"
            + base64.b64encode(b"\x89PNG\r\n\x1a\nimage").decode("ascii")
        )

        await self.service.mirror_item(
            topic,
            {
                "id": "desktop-media-message",
                "type": "userMessage",
                "content": [
                    {"type": "text", "text": "Посмотри вложения."},
                    {"type": "image", "url": data_url},
                    {
                        "type": "mention",
                        "name": "clip.mp4",
                        "path": str(video),
                    },
                ],
            },
            turn_id="turn-desktop-media",
            item_origin="history",
        )

        self.assertEqual(len(self.telegram.sent_messages), 1)
        self.assertEqual(len(self.telegram.sent_attachments), 2)
        self.assertEqual(
            [
                attachment["media_kind"]
                for attachment in self.telegram.sent_attachments
            ],
            ["photo", "video"],
        )
        self.assertTrue(
            all(
                attachment["reply_to_message_id"] == 801
                for attachment in self.telegram.sent_attachments
            )
        )
        materialized = self.telegram.sent_attachments[0]["file_path"]
        self.assertEqual(materialized.stat().st_mode & 0o777, 0o600)
        self.assertEqual(
            self.store.turn_context(
                "thread-1",
                "turn-desktop-media",
            ).source_message_id,
            801,
        )

    async def test_final_media_outputs_use_native_telegram_types(self) -> None:
        topic = self.store.topic_for_thread("thread-1")
        self.assertIsNotNone(topic)
        output_directory = self.service.config.workspace / "outputs"
        output_directory.mkdir()
        image = output_directory / "render.png"
        video = output_directory / "demo.mp4"
        image.write_bytes(b"image")
        video.write_bytes(b"video")

        await self.service.mirror_item(
            topic,
            {
                "id": "native-media-final",
                "type": "agentMessage",
                "phase": "final_answer",
                "text": (
                    f"![Результат](<{image}>)\n"
                    f"[Видео](<{video}>)"
                ),
            },
            turn_id="turn-native-media",
            item_origin="history",
        )

        self.assertEqual(
            [
                attachment["media_kind"]
                for attachment in self.telegram.sent_attachments
            ],
            ["photo", "video"],
        )
        self.assertIn(
            "Результат — outputs/render.png",
            self.telegram.sent_messages[0]["text"],
        )

    async def test_existing_user_message_backfills_media_without_text_replay(
        self,
    ) -> None:
        data_url = (
            "data:image/png;base64,"
            + base64.b64encode(b"\x89PNG\r\n\x1a\nbackfill").decode("ascii")
        )
        item = {
            "id": "existing-desktop-media",
            "type": "userMessage",
            "content": [
                {"type": "text", "text": "Уже отправленный текст."},
                {"type": "image", "url": data_url},
            ],
        }
        thread = {
            "id": "thread-1",
            "turns": [
                {
                    "id": "turn-existing-media",
                    "status": "completed",
                    "items": [item],
                }
            ],
        }
        self.store.mark_mirrored_item(
            "thread-1",
            "existing-desktop-media",
            "userMessage",
            777,
        )

        topic = self.store.topic_for_thread("thread-1")
        self.assertIsNotNone(topic)
        await self.service.sync_thread_history(thread, topic, initial=False)
        await self.service.sync_thread_history(thread, topic, initial=False)

        self.assertEqual(self.telegram.sent_messages, [])
        self.assertEqual(len(self.telegram.sent_attachments), 1)
        self.assertEqual(
            self.telegram.sent_attachments[0]["reply_to_message_id"],
            777,
        )

    async def test_existing_null_image_view_backfills_native_photo(self) -> None:
        image = self.service.config.workspace / "view.png"
        image.write_bytes(b"image")
        item = {
            "id": "existing-image-view",
            "type": "imageView",
            "path": str(image),
        }
        thread = {
            "id": "thread-1",
            "turns": [
                {
                    "id": "turn-image-view",
                    "status": "completed",
                    "items": [item],
                }
            ],
        }
        self.store.mark_mirrored_item(
            "thread-1",
            "existing-image-view",
            "imageView",
            None,
        )
        self.store.upsert_turn_context(
            thread_id="thread-1",
            turn_id="turn-image-view",
            source_message_id=70,
        )

        topic = self.store.topic_for_thread("thread-1")
        self.assertIsNotNone(topic)
        await self.service.sync_thread_history(thread, topic, initial=False)

        self.assertEqual(len(self.telegram.sent_attachments), 1)
        self.assertEqual(
            self.telegram.sent_attachments[0]["media_kind"],
            "photo",
        )
        mirrored = self.store.mirrored_item_state(
            "thread-1",
            "existing-image-view",
        )
        self.assertEqual(mirrored, (True, 1101))

    async def test_person_review_image_uses_photo_buttons_and_saves_answer(
        self,
    ) -> None:
        topic = self.store.topic_for_thread("thread-1")
        self.assertIsNotNone(topic)
        review_directory = (
            self.service.config.workspace / "outputs" / "person-review"
        )
        review_directory.mkdir(parents=True)
        photo = review_directory / "Фото 6 — 20 июля 13-26.jpg"
        photo.write_bytes(b"jpeg")
        relative_path = photo.relative_to(self.service.config.workspace)
        token = person_review_token(relative_path)
        self.assertIsNotNone(token)
        self.store.upsert_turn_context(
            thread_id="thread-1",
            turn_id="turn-review",
            source_message_id=90,
        )

        await self.service.mirror_item(
            topic,
            {
                "id": "review-final",
                "type": "agentMessage",
                "phase": "final_answer",
                "text": f"[Фото 6](<{photo}>)",
            },
            turn_id="turn-review",
            item_origin="notification",
        )

        self.assertEqual(self.telegram.sent_documents, [])
        self.assertEqual(len(self.telegram.sent_photos), 1)
        sent = self.telegram.sent_photos[0]
        self.assertEqual(sent["chat_id"], -100500)
        self.assertEqual(sent["message_thread_id"], 50)
        self.assertEqual(sent["reply_to_message_id"], 801)
        self.assertEqual(sent["file_path"], photo.resolve())
        self.assertEqual(sent["caption"], photo.stem)
        yes_button = sent["reply_markup"]["inline_keyboard"][0][0]
        no_button = sent["reply_markup"]["inline_keyboard"][0][1]
        self.assertEqual(yes_button["callback_data"], f"prv:{token}:yes")
        self.assertEqual(no_button["callback_data"], f"prv:{token}:no")

        callback = {
            "id": "review-callback",
            "data": yes_button["callback_data"],
            "from": {"id": 100, "is_bot": False},
            "message": {
                "message_id": 1101,
                "message_thread_id": 50,
                "caption": photo.stem,
                "chat": {"id": -100500, "type": "supergroup"},
            },
        }
        await self.service.handle_callback(callback)

        record = json.loads(
            self.store.get_setting(f"telegram_person_review:{token}") or "{}"
        )
        self.assertEqual(record["decision"], "yes")
        self.assertEqual(record["message_id"], 1101)
        self.assertEqual(
            self.telegram.edited_captions[-1],
            {
                "chat_id": -100500,
                "message_id": 1101,
                "caption": f"{photo.stem}\n\nОтвет: ✅ Подходит",
                "reply_markup": {"inline_keyboard": []},
            },
        )
        self.assertEqual(
            self.telegram.callback_answers[-1],
            ("review-callback", "Сохранено: подходит"),
        )

        callback["id"] = "review-callback-repeat"
        callback["data"] = no_button["callback_data"]
        await self.service.handle_callback(callback)
        self.assertEqual(
            self.telegram.callback_answers[-1],
            ("review-callback-repeat", "Уже отмечено: подходит"),
        )
        record = json.loads(
            self.store.get_setting(f"telegram_person_review:{token}") or "{}"
        )
        self.assertEqual(record["decision"], "yes")

    async def test_person_review_text_reply_uses_exact_replied_photo(
        self,
    ) -> None:
        topic = self.store.topic_for_thread("thread-1")
        self.assertIsNotNone(topic)
        review_directory = (
            self.service.config.workspace / "outputs" / "person-review"
        )
        review_directory.mkdir(parents=True)
        photo = review_directory / "Фото 15 — возвращение после обеда.jpg"
        photo.write_bytes(b"jpeg")
        relative_path = photo.relative_to(self.service.config.workspace)
        token = person_review_token(relative_path)
        self.assertIsNotNone(token)
        self.store.upsert_turn_context(
            thread_id="thread-1",
            turn_id="turn-review-reply",
            source_message_id=90,
        )
        await self.service.mirror_item(
            topic,
            {
                "id": "review-reply-final",
                "type": "agentMessage",
                "phase": "final_answer",
                "text": f"[Фото 15](<{photo}>)",
            },
            turn_id="turn-review-reply",
            item_origin="notification",
        )

        update = self.topic_message("подходит", message_id=91)
        update["message"]["reply_to_message"] = {
            "message_id": 1101,
            "message_thread_id": 50,
            "caption": photo.stem,
            "from": {"id": 700, "is_bot": True},
            "chat": {"id": -100500, "type": "supergroup"},
            "photo": [{"file_id": "review-photo"}],
        }
        await self.service.handle_telegram_update(update)

        record = json.loads(
            self.store.get_setting(f"telegram_person_review:{token}") or "{}"
        )
        self.assertEqual(record["decision"], "yes")
        self.assertEqual(
            self.telegram.edited_captions[-1],
            {
                "chat_id": -100500,
                "message_id": 1101,
                "caption": f"{photo.stem}\n\nОтвет: ✅ Подходит",
                "reply_markup": {"inline_keyboard": []},
            },
        )
        self.assertEqual(
            self.telegram.sent_messages[-1]["reply_to_message_id"],
            91,
        )
        self.assertIn(photo.stem, self.telegram.sent_messages[-1]["text"])
        self.codex.start_turn.assert_not_awaited()
        self.assertIsNone(
            self.store.queued_message_for_client_id("tg:-100500:91")
        )

    async def test_person_review_negative_button_starts_better_search(
        self,
    ) -> None:
        topic = self.store.topic_for_thread("thread-1")
        self.assertIsNotNone(topic)
        review_directory = (
            self.service.config.workspace / "outputs" / "person-review"
        )
        review_directory.mkdir(parents=True)
        photo = review_directory / "27 июля — дневной отчёт.jpg"
        photo.write_bytes(b"jpeg")
        relative_path = photo.relative_to(self.service.config.workspace)
        token = person_review_token(relative_path)
        self.assertIsNotNone(token)
        self.store.upsert_turn_context(
            thread_id="thread-1",
            turn_id="turn-review-reject",
            source_message_id=90,
        )
        await self.service.mirror_item(
            topic,
            {
                "id": "review-reject-final",
                "type": "agentMessage",
                "phase": "final_answer",
                "text": f"[27 июля](<{photo}>)",
            },
            turn_id="turn-review-reject",
            item_origin="notification",
        )
        callback = {
            "id": "review-reject-callback",
            "data": f"prv:{token}:no",
            "from": {"id": 100, "is_bot": False},
            "message": {
                "message_id": 1101,
                "message_thread_id": 50,
                "caption": photo.stem,
                "chat": {"id": -100500, "type": "supergroup"},
            },
        }

        await self.service.handle_callback(callback)

        record = json.loads(
            self.store.get_setting(f"telegram_person_review:{token}") or "{}"
        )
        self.assertEqual(record["decision"], "no")
        self.assertEqual(
            self.telegram.callback_answers[-1],
            (
                "review-reject-callback",
                "Сохранено: не подходит; ищу лучше",
            ),
        )
        self.codex.start_turn.assert_awaited_once()
        started_text = self.codex.start_turn.await_args.kwargs["text"]
        self.assertIn(photo.stem, started_text)
        self.assertIn("Подбери более подходящий вариант", started_text)

    async def test_regular_telegram_reply_preserves_bot_message_context(
        self,
    ) -> None:
        update = self.topic_message("Проверь это", message_id=91)
        update["message"]["reply_to_message"] = {
            "message_id": 777,
            "message_thread_id": 50,
            "caption": "Фото 21 — 27 июля 10-39 — возможный приход",
            "from": {"id": 700, "is_bot": True},
            "chat": {"id": -100500, "type": "supergroup"},
            "photo": [{"file_id": "ordinary-photo"}],
        }

        await self.service.handle_telegram_update(update)

        self.codex.start_turn.assert_awaited_once_with(
            thread_id="thread-1",
            text=(
                "Контекст Telegram reply: пользователь ответил на сообщение "
                "Codex «Фото 21 — 27 июля 10-39 — возможный приход».\n\n"
                "Проверь это"
            ),
            client_id="tg:-100500:91",
        )

    async def test_final_and_progress_use_phone_readable_local_links(
        self,
    ) -> None:
        topic = self.store.topic_for_thread("thread-1")
        self.assertIsNotNone(topic)
        target = self.service.config.workspace / "docs" / "bridge.md"

        await self.service.mirror_item(
            topic,
            {
                "id": "local-link-progress",
                "type": "agentMessage",
                "phase": "commentary",
                "text": f"Сверяю [документ](<{target}>).",
            },
            turn_id="turn-local-link",
        )
        await self.service.mirror_item(
            topic,
            {
                "id": "local-link-final",
                "type": "agentMessage",
                "phase": "final_answer",
                "text": f"Готово: [документ](<{target}>).",
            },
            turn_id="turn-local-link",
        )

        progress = self.telegram.sent_rich_messages[0]["rich_message"]
        progress_text = "\n".join(
            block.get("text", "")
            for block in progress["blocks"][1]["blocks"]
        )
        self.assertIn("документ — docs/bridge.md", progress_text)
        self.assertIn(
            "документ — docs/bridge.md",
            self.telegram.sent_messages[-1]["text"],
        )
        self.assertNotIn(str(self.service.config.workspace), progress_text)
        self.assertNotIn(
            str(self.service.config.workspace),
            self.telegram.sent_messages[-1]["text"],
        )

    async def test_progress_accumulates_only_visible_safe_entry_kinds(self) -> None:
        topic = self.store.topic_for_thread("thread-1")
        self.assertIsNotNone(topic)
        self.store.upsert_turn_context(
            thread_id="thread-1",
            turn_id="turn-progress-kinds",
            source_message_id=90,
        )
        items = [
            {
                "id": "visible-commentary",
                "type": "agentMessage",
                "phase": "commentary",
                "text": "Проверяю TOKEN=super-secret-value.",
            },
            {
                "id": "visible-plan",
                "type": "plan",
                "text": "Сначала проверка, затем тесты.",
            },
            {
                "id": "visible-tool-status",
                "type": "toolStatus",
                "summary": "Тесты выполняются.",
                "output": "RAW OUTPUT MUST NOT BE STORED",
            },
        ]

        for item in items:
            await self.service.mirror_item(
                topic,
                item,
                turn_id="turn-progress-kinds",
            )

        entries = self.store.progress_entries(
            "thread-1",
            "turn-progress-kinds",
        )
        self.assertEqual(
            [entry.entry_kind for entry in entries],
            ["commentary", "plan", "tool_status"],
        )
        stored = "\n".join(entry.text for entry in entries)
        self.assertIn("TOKEN=[REDACTED]", stored)
        self.assertNotIn("super-secret-value", stored)
        self.assertNotIn("RAW OUTPUT", stored)
        self.assertEqual(len(self.telegram.sent_rich_messages), 1)
        self.assertEqual(len(self.telegram.edited_messages), 2)

    async def test_reasoning_and_raw_command_items_never_enter_progress(self) -> None:
        topic = self.store.topic_for_thread("thread-1")
        self.assertIsNotNone(topic)
        self.service.config = replace(
            self.service.config,
            reasoning_mode="summary",
        )

        await self.service.mirror_item(
            topic,
            {
                "id": "hidden-reasoning",
                "type": "reasoning",
                "summary": ["private chain of thought"],
            },
            turn_id="turn-private",
        )
        await self.service.mirror_item(
            topic,
            {
                "id": "raw-command",
                "type": "commandExecution",
                "output": "credential-bearing raw output",
                "text": "must not be mirrored",
            },
            turn_id="turn-private",
        )

        self.assertEqual(
            self.store.progress_entries("thread-1", "turn-private"),
            [],
        )
        self.assertEqual(self.telegram.sent_rich_messages, [])
        self.assertEqual(self.telegram.sent_messages, [])
        self.assertTrue(
            self.store.has_mirrored_item("thread-1", "hidden-reasoning")
        )
        self.assertTrue(self.store.has_mirrored_item("thread-1", "raw-command"))

    async def test_restart_replay_reuses_durable_progress_card(self) -> None:
        self.store.upsert_turn_context(
            thread_id="thread-1",
            turn_id="turn-restart",
            source_message_id=90,
        )
        self.store.append_progress_entry(
            thread_id="thread-1",
            turn_id="turn-restart",
            item_id="restart-commentary",
            entry_kind="commentary",
            sanitized_text="Сохранённый видимый прогресс.",
        )
        self.store.update_turn_progress_state(
            thread_id="thread-1",
            turn_id="turn-restart",
            status_message_id=777,
            render_mode="rich_details",
            closed=False,
        )
        restarted_telegram = FakeTelegram()
        restarted = BridgeService(
            config=self.service.config,
            store=self.store,
            telegram=restarted_telegram,
        )
        restarted.codex = self.codex
        topic = self.store.topic_for_thread("thread-1")
        self.assertIsNotNone(topic)

        await restarted.mirror_item(
            topic,
            {
                "id": "restart-commentary",
                "type": "agentMessage",
                "phase": "commentary",
                "text": "Сохранённый видимый прогресс.",
            },
            turn_id="turn-restart",
        )

        self.assertEqual(len(restarted_telegram.sent_rich_messages), 0)
        self.assertEqual(len(restarted_telegram.edited_messages), 0)
        self.assertEqual(
            len(self.store.progress_entries("thread-1", "turn-restart")),
            1,
        )
        mirrored_id = self.store.connection.execute(
            """
            SELECT telegram_message_id FROM mirrored_items
            WHERE thread_id = ? AND item_id = ?
            """,
            ("thread-1", "restart-commentary"),
        ).fetchone()[0]
        self.assertEqual(mirrored_id, 777)

    async def test_rich_api_rejection_falls_back_and_stays_on_html(self) -> None:
        topic = self.store.topic_for_thread("thread-1")
        self.assertIsNotNone(topic)
        self.store.upsert_turn_context(
            thread_id="thread-1",
            turn_id="turn-fallback",
            source_message_id=90,
        )

        def unsupported(**_: Any) -> dict[str, Any]:
            raise TelegramError(
                "sendRichMessage: method is unavailable",
                method="sendRichMessage",
                kind="api",
            )

        self.telegram.send_rich_message = unsupported  # type: ignore[method-assign]
        await self.service.mirror_item(
            topic,
            {
                "id": "fallback-commentary-1",
                "type": "agentMessage",
                "phase": "commentary",
                "text": "Первая проверка.",
            },
            turn_id="turn-fallback",
        )
        await self.service.mirror_item(
            topic,
            {
                "id": "fallback-commentary-2",
                "type": "agentMessage",
                "phase": "commentary",
                "text": "Вторая проверка.",
            },
            turn_id="turn-fallback",
        )
        await self.service.mirror_item(
            topic,
            {
                "id": "fallback-final",
                "type": "agentMessage",
                "phase": "final_answer",
                "text": "Готово.",
            },
            turn_id="turn-fallback",
        )

        status_send = self.telegram.sent_messages[0]
        self.assertEqual(status_send["parse_mode"], "HTML")
        self.assertIn("<blockquote>", status_send["text"])
        self.assertNotIn("expandable", status_send["text"])
        self.assertEqual(
            self.store.turn_context(
                "thread-1",
                "turn-fallback",
            ).progress_render_mode,
            "expandable_quote",
        )
        self.assertIn(
            "<blockquote expandable>",
            self.telegram.edited_messages[-1]["text"],
        )
        self.assertEqual(self.telegram.edited_messages[-1]["parse_mode"], "HTML")
        self.assertIn("🟢 Codex", self.telegram.sent_messages[-1]["text"])
        context = self.store.turn_context("thread-1", "turn-fallback")
        self.assertIsNotNone(context)
        self.assertIsNotNone(context.status_message_id)
        self.telegram.edited_messages.clear()

        with patch(
            "codex_telegram_bridge.service.asyncio.sleep",
            new=AsyncMock(),
        ):
            await self.service.handle_callback(
                self.progress_collapse_callback(context.status_message_id)
            )

        self.assertEqual(len(self.telegram.edited_messages), 2)
        self.assertNotIn(
            "<blockquote",
            self.telegram.edited_messages[0]["text"],
        )
        restored = self.telegram.edited_messages[1]
        self.assertIn("<blockquote expandable>", restored["text"])
        self.assertEqual(restored["parse_mode"], "HTML")
        self.assertEqual(
            self.telegram.callback_answers[-1],
            ("progress-collapse-1", "Свёрнуто"),
        )

    async def test_ambiguous_rich_send_failure_never_attempts_fallback(self) -> None:
        topic = self.store.topic_for_thread("thread-1")
        self.assertIsNotNone(topic)

        def timed_out(**_: Any) -> dict[str, Any]:
            raise TelegramError(
                "sendRichMessage: Telegram network error",
                method="sendRichMessage",
                kind="network_timeout",
                retryable=True,
                outcome_ambiguous=True,
            )

        self.telegram.send_rich_message = timed_out  # type: ignore[method-assign]
        with self.assertRaises(TelegramError):
            await self.service.mirror_item(
                topic,
                {
                    "id": "ambiguous-rich-progress",
                    "type": "agentMessage",
                    "phase": "commentary",
                    "text": "Сохранить, но не дублировать отправку.",
                },
                turn_id="turn-ambiguous-rich",
            )

        self.assertEqual(self.telegram.sent_messages, [])
        self.assertEqual(self.telegram.sent_rich_messages, [])
        self.assertEqual(
            len(
                self.store.progress_entries(
                    "thread-1",
                    "turn-ambiguous-rich",
                )
            ),
            1,
        )
        self.assertFalse(
            self.store.has_mirrored_item(
                "thread-1",
                "ambiguous-rich-progress",
            )
        )
        context = self.store.turn_context(
            "thread-1",
            "turn-ambiguous-rich",
        )
        self.assertIsNotNone(context)
        self.assertTrue(context.progress_send_outcome_unknown)

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
                "id": "ambiguous-rich-progress",
                "type": "agentMessage",
                "phase": "commentary",
                "text": "Сохранить, но не дублировать отправку.",
            },
            turn_id="turn-ambiguous-rich",
        )

        self.assertEqual(restarted_telegram.sent_rich_messages, [])
        self.assertEqual(restarted_telegram.sent_messages, [])
        self.assertTrue(
            self.store.has_mirrored_item(
                "thread-1",
                "ambiguous-rich-progress",
            )
        )

    async def test_progress_render_is_bounded_but_durable_entries_remain(self) -> None:
        topic = self.store.topic_for_thread("thread-1")
        self.assertIsNotNone(topic)
        turn_id = "turn-large-progress"
        self.store.upsert_turn_context(
            thread_id="thread-1",
            turn_id=turn_id,
            source_message_id=90,
        )
        for index in range(120):
            self.store.append_progress_entry(
                thread_id="thread-1",
                turn_id=turn_id,
                item_id=f"large-{index}",
                entry_kind="commentary",
                sanitized_text=("данные & <safe> " * 260),
            )

        await self.service._render_progress_card(
            topic,
            turn_id=turn_id,
            closed=False,
        )

        rich = self.telegram.sent_rich_messages[0]["rich_message"]

        def rich_text_strings(value: Any) -> list[str]:
            if isinstance(value, list):
                return [
                    text
                    for item in value
                    for text in rich_text_strings(item)
                ]
            if isinstance(value, dict):
                result: list[str] = []
                for key, item in value.items():
                    if key in {"text", "summary"} and isinstance(item, str):
                        result.append(item)
                    else:
                        result.extend(rich_text_strings(item))
                return result
            return []

        rendered_bytes = sum(
            len(value.encode("utf-8"))
            for value in rich_text_strings(rich)
        )
        self.assertLessEqual(rendered_bytes, 30_000)
        self.assertEqual(
            len(self.store.progress_entries("thread-1", turn_id)),
            120,
        )
        details = rich["blocks"][1]
        self.assertLessEqual(len(details["blocks"]), 101)
        self.assertIn("сохранено локально", details["blocks"][-1]["text"])

    async def test_interrupted_turn_closes_existing_progress_card(self) -> None:
        topic = self.store.topic_for_thread("thread-1")
        self.assertIsNotNone(topic)
        await self.service.mirror_item(
            topic,
            {
                "id": "interrupt-commentary",
                "type": "agentMessage",
                "phase": "commentary",
                "text": "Проверка перед остановкой.",
            },
            turn_id="turn-interrupted",
        )
        self.service.active_turns["thread-1"] = "turn-interrupted"

        await self.service.on_codex_notification(
            {
                "method": "turn/completed",
                "params": {
                    "threadId": "thread-1",
                    "turn": {
                        "id": "turn-interrupted",
                        "status": "interrupted",
                    },
                },
            }
        )

        closed = self.telegram.edited_messages[-1]["rich_message"]["blocks"]
        self.assertEqual(
            closed[0]["text"],
            "⏹ Ход работы Codex остановлен",
        )
        self.assertNotIn("is_open", closed[1])
        self.assertTrue(
            self.store.turn_context(
                "thread-1",
                "turn-interrupted",
            ).progress_closed
        )

    async def test_offline_codex_startup_keeps_telegram_polling_alive(
        self,
    ) -> None:
        telegram_started = asyncio.Event()
        never_finishes = asyncio.Event()
        self.service.codex = SimpleNamespace(stop=AsyncMock())

        async def telegram_loop() -> None:
            telegram_started.set()
            await never_finishes.wait()

        async def offline_codex_loop() -> None:
            await self.service._enter_codex_degraded(
                "socket_unavailable",
                expire_prompts=False,
            )
            await never_finishes.wait()

        async def telegram_setup_loop() -> None:
            await never_finishes.wait()

        self.service.telegram_loop = telegram_loop
        self.service.codex_connection_loop = offline_codex_loop
        self.service.telegram_setup_loop = telegram_setup_loop

        serve_task = asyncio.create_task(self.service.serve())
        await asyncio.wait_for(telegram_started.wait(), timeout=1)
        self.assertFalse(serve_task.done())
        self.assertFalse(self.service.codex_available)

        self.service.stop()
        await asyncio.wait_for(serve_task, timeout=1)
        self.service.codex.stop.assert_awaited_once()

    async def test_serve_propagates_every_critical_worker_failure(
        self,
    ) -> None:
        class InjectedWorkerFailure(RuntimeError):
            pass

        workers = {
            "telegram_loop": "telegram-loop",
            "codex_connection_loop": "codex-connection-loop",
            "progress_heartbeat_loop": "progress-heartbeat-loop",
        }
        for attribute, task_name in workers.items():
            with self.subTest(worker=task_name):
                service = BridgeService(
                    config=self.service.config,
                    store=self.store,
                    telegram=self.telegram,
                )
                service.codex = SimpleNamespace(stop=AsyncMock())
                never_finishes = asyncio.Event()

                async def block() -> None:
                    await never_finishes.wait()

                async def fail(
                    current_name: str = task_name,
                ) -> None:
                    raise InjectedWorkerFailure(current_name)

                service.telegram_loop = block
                service.codex_connection_loop = block
                service.progress_heartbeat_loop = block
                service.telegram_setup_loop = block
                setattr(service, attribute, fail)

                with self.assertRaises(InjectedWorkerFailure) as raised:
                    await asyncio.wait_for(service.serve(), timeout=1)

                self.assertEqual(str(raised.exception), task_name)
                service.codex.stop.assert_awaited_once()

    async def test_serve_rejects_every_critical_worker_early_return(
        self,
    ) -> None:
        workers = {
            "telegram_loop": "telegram-loop",
            "codex_connection_loop": "codex-connection-loop",
            "progress_heartbeat_loop": "progress-heartbeat-loop",
        }
        for attribute, task_name in workers.items():
            with self.subTest(worker=task_name):
                service = BridgeService(
                    config=self.service.config,
                    store=self.store,
                    telegram=self.telegram,
                )
                service.codex = SimpleNamespace(stop=AsyncMock())
                never_finishes = asyncio.Event()

                async def block() -> None:
                    await never_finishes.wait()

                async def finish() -> None:
                    return None

                service.telegram_loop = block
                service.codex_connection_loop = block
                service.progress_heartbeat_loop = block
                service.telegram_setup_loop = block
                setattr(service, attribute, finish)

                with self.assertRaises(RuntimeError) as raised:
                    await asyncio.wait_for(service.serve(), timeout=1)

                self.assertEqual(
                    str(raised.exception),
                    f"Critical bridge worker {task_name} exited unexpectedly",
                )
                service.codex.stop.assert_awaited_once()

    async def test_setup_worker_return_does_not_block_clean_stop(
        self,
    ) -> None:
        service = BridgeService(
            config=self.service.config,
            store=self.store,
            telegram=self.telegram,
        )
        service.codex = SimpleNamespace(stop=AsyncMock())
        critical_started = asyncio.Event()
        never_finishes = asyncio.Event()

        async def block() -> None:
            critical_started.set()
            await never_finishes.wait()

        async def setup_finishes() -> None:
            return None

        service.telegram_loop = block
        service.codex_connection_loop = block
        service.progress_heartbeat_loop = block
        service.telegram_setup_loop = setup_finishes

        serve_task = asyncio.create_task(service.serve())
        await asyncio.wait_for(critical_started.wait(), timeout=1)
        await asyncio.sleep(0)
        self.assertFalse(serve_task.done())

        service.stop()
        await asyncio.wait_for(serve_task, timeout=1)
        service.codex.stop.assert_awaited_once()

    async def test_serve_reports_ready_only_after_initialization(
        self,
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
