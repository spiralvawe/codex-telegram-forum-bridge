from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from codex_telegram_bridge.codex import CodexProtocolError  # noqa: E402
from codex_telegram_bridge.config import BridgeConfig  # noqa: E402
from codex_telegram_bridge.service import BridgeService  # noqa: E402
from codex_telegram_bridge.service import (  # noqa: E402
    ARCHIVE_DELETE_CONFIRM_ATTEMPTS,
    archive_card_payload,
    build_archive_summary,
)
from codex_telegram_bridge.store import BridgeStore  # noqa: E402
from codex_telegram_bridge.telegram import TelegramError  # noqa: E402


class ArchiveTelegram:
    def __init__(self) -> None:
        self.created_topics: list[tuple[int, str, int]] = []
        self.deleted_topics: list[tuple[int, int]] = []
        self.sent_messages: list[dict[str, Any]] = []
        self.edited_messages: list[dict[str, Any]] = []
        self.edited_topics: list[tuple[int, int, str]] = []
        self.callback_answers: list[tuple[str, str | None]] = []
        self.delete_error: TelegramError | None = None
        self.delete_successes_before_absent = 1
        self.delete_calls: dict[tuple[int, int], int] = {}
        self.chat_action_error: TelegramError | None = None
        self.send_error_at_call: int | None = None
        self.send_error: TelegramError | None = None

    def create_forum_topic(self, chat_id: int, name: str) -> dict[str, Any]:
        topic_id = 900 + len(self.created_topics)
        self.created_topics.append((chat_id, name, topic_id))
        return {"message_thread_id": topic_id}

    def delete_forum_topic(
        self,
        chat_id: int,
        message_thread_id: int,
    ) -> bool:
        if self.delete_error is not None:
            raise self.delete_error
        key = (chat_id, message_thread_id)
        self.delete_calls[key] = self.delete_calls.get(key, 0) + 1
        if self.delete_calls[key] > self.delete_successes_before_absent:
            raise TelegramError(
                "deleteForumTopic: Bad Request: TOPIC_ID_INVALID",
                method="deleteForumTopic",
                kind="api",
            )
        self.deleted_topics.append((chat_id, message_thread_id))
        return True

    def send_message(self, **kwargs: Any) -> list[dict[str, Any]]:
        if (
            self.send_error is not None
            and self.send_error_at_call == len(self.sent_messages) + 1
        ):
            raise self.send_error
        self.sent_messages.append(dict(kwargs))
        return [{"message_id": 1000 + len(self.sent_messages)}]

    def edit_message_text(self, **kwargs: Any) -> bool:
        self.edited_messages.append(dict(kwargs))
        return True

    def edit_forum_topic(
        self,
        chat_id: int,
        message_thread_id: int,
        name: str,
    ) -> bool:
        self.edited_topics.append((chat_id, message_thread_id, name))
        return True

    def answer_callback_query(
        self,
        callback_query_id: str,
        text: str | None = None,
    ) -> bool:
        self.callback_answers.append((callback_query_id, text))
        return True

    def send_chat_action(self, **_: Any) -> bool:
        if self.chat_action_error is not None:
            raise self.chat_action_error
        return True


class ArchiveStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.store = BridgeStore(Path(self.temp_dir.name) / "state.sqlite3")

    def tearDown(self) -> None:
        self.store.close()
        self.temp_dir.cleanup()

    def test_archive_lifecycle_is_replay_safe_and_supports_rearchive(self) -> None:
        first = self.store.reserve_archived_thread(
            thread_id="thread-1",
            restore_token="token-first",
            title="First title",
            thread_created_at=100,
            source_topic_id=50,
        )
        replay = self.store.reserve_archived_thread(
            thread_id="thread-1",
            restore_token="token-ignored",
            title="Updated title",
            thread_created_at=100,
            source_topic_id=50,
        )

        self.assertEqual(first.status, "detected")
        self.assertEqual(replay.restore_token, "token-first")
        self.assertEqual(replay.archived_at, first.archived_at)
        self.assertEqual(replay.title, "Updated title")

        self.store.set_archived_thread_status("thread-1", "archived")
        self.store.set_archived_thread_status("thread-1", "restoring")
        self.store.mark_archive_replacement_outcome_unknown("thread-1")
        replacement = self.store.record_archive_replacement_topic(
            "thread-1",
            60,
        )
        self.assertEqual(replacement.replacement_topic_id, 60)
        self.assertFalse(replacement.replacement_outcome_unknown)

        self.store.set_archived_thread_status("thread-1", "restored")
        self.assertEqual(self.store.list_current_archived_threads(), [])

        rearchived = self.store.reserve_archived_thread(
            thread_id="thread-1",
            restore_token="token-second",
            title="Second archive",
            thread_created_at=100,
            source_topic_id=70,
        )
        self.assertEqual(rearchived.status, "detected")
        self.assertEqual(rearchived.restore_token, "token-second")
        self.assertIsNone(rearchived.replacement_topic_id)

    def test_unchanged_archive_reservation_is_a_database_noop(self) -> None:
        first = self.store.reserve_archived_thread(
            thread_id="thread-noop",
            restore_token="token-noop",
            title="Stable title",
            thread_created_at=100,
            source_topic_id=50,
        )
        before = self.store.connection.execute(
            """
            SELECT updated_at FROM archived_threads
            WHERE thread_id = ?
            """,
            ("thread-noop",),
        ).fetchone()["updated_at"]

        replay = self.store.reserve_archived_thread(
            thread_id="thread-noop",
            restore_token="ignored-token",
            title="Stable title",
            thread_created_at=100,
            source_topic_id=50,
        )
        after = self.store.connection.execute(
            """
            SELECT updated_at FROM archived_threads
            WHERE thread_id = ?
            """,
            ("thread-noop",),
        ).fetchone()["updated_at"]

        self.assertEqual(replay, first)
        self.assertEqual(after, before)

    def test_archive_summary_and_card_escape_runtime_content(self) -> None:
        summary = build_archive_summary(
            {
                "name": "Card <test>",
                "turns": [
                    {
                        "items": [
                            {
                                "type": "userMessage",
                                "content": [
                                    {
                                        "type": "text",
                                        "text": "Проверить <карточку>",
                                    }
                                ],
                            },
                            {
                                "type": "agentMessage",
                                "phase": "final_answer",
                                "text": "Готово & проверено",
                            },
                        ]
                    }
                ],
            },
            workspace=Path(self.temp_dir.name),
        )
        record = self.store.reserve_archived_thread(
            thread_id="thread-<1>",
            restore_token="token-card",
            title="Card <test>",
            thread_created_at=100,
            source_topic_id=50,
            summary=summary,
        )

        text, keyboard = archive_card_payload(record)

        self.assertIn("Card &lt;test&gt;", text)
        self.assertIn("thread-&lt;1&gt;", text)
        self.assertIn("Проверить &lt;карточку&gt;", text)
        self.assertIn("Готово &amp; проверено", text)
        self.assertIn("<blockquote expandable>", text)
        self.assertEqual(
            keyboard["inline_keyboard"][0][0]["text"],
            "♻️ Восстановить",
        )


class ArchiveServiceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        temp_path = Path(self.temp_dir.name)
        workspace = temp_path / "workspace"
        workspace.mkdir()
        self.database_path = temp_path / "state.sqlite3"
        self.store = BridgeStore(self.database_path)
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
            title="Archived task",
        )
        self.telegram = ArchiveTelegram()
        self.service = BridgeService(
            config=BridgeConfig(workspace=workspace, state_dir=temp_path),
            store=self.store,
            telegram=self.telegram,
        )
        self.archived_summary = {
            "id": "thread-1",
            "name": "Archived task",
            "preview": "",
            "createdAt": 1_700_000_000,
            "updatedAt": 1_700_000_100,
        }
        self.service.codex = SimpleNamespace(
            list_threads=AsyncMock(
                side_effect=lambda *, archived: (
                    [self.archived_summary] if archived else []
                )
            ),
            read_thread=AsyncMock(
                side_effect=lambda thread_id: {
                    **self.archived_summary,
                    "id": thread_id,
                    "turns": [
                        {
                            "id": f"turn-{thread_id}",
                            "items": [
                                {
                                    "id": f"user-{thread_id}",
                                    "type": "userMessage",
                                    "content": [
                                        {
                                            "type": "text",
                                            "text": "Проверить архивный UX",
                                        }
                                    ],
                                },
                                {
                                    "id": f"final-{thread_id}",
                                    "type": "agentMessage",
                                    "phase": "final_answer",
                                    "text": (
                                        "Проверили поведение и подготовили "
                                        "безопасное изменение."
                                    ),
                                },
                            ],
                        }
                    ],
                }
            ),
            start_turn=AsyncMock(return_value={"id": "turn-telegram-archive"}),
            archive_thread=AsyncMock(return_value={}),
        )

    async def asyncTearDown(self) -> None:
        self.store.close()
        self.temp_dir.cleanup()

    async def test_new_archive_card_precedes_topic_deletion_and_is_idempotent(
        self,
    ) -> None:
        await self.service.sync_threads()
        await self.service.sync_threads()

        topic = self.store.topic_for_thread("thread-1")
        record = self.store.archived_thread("thread-1")
        self.assertIsNotNone(topic)
        self.assertTrue(topic.archived)
        self.assertIsNotNone(record)
        self.assertEqual(record.status, "archived")
        self.assertEqual(
            self.telegram.deleted_topics,
            [(-100500, 50)],
        )
        self.assertEqual(len(self.telegram.created_topics), 1)
        self.assertEqual(self.telegram.created_topics[0][1], "Архивные треды")
        self.assertEqual(len(self.telegram.sent_messages), 2)
        rendered = self.telegram.sent_messages[-1]
        self.assertIn("Archived task", rendered["text"])
        self.assertIn("Создан:", rendered["text"])
        self.assertIn("Архивирован:", rendered["text"])
        self.assertIn("Codex thread ID:", rendered["text"])
        self.assertIn("thread-1", rendered["text"])
        self.assertIn("<blockquote expandable>", rendered["text"])
        self.assertIn("О чём: Проверить архивный UX", rendered["text"])
        self.assertIn("Что сделали:", rendered["text"])
        self.assertEqual(rendered["parse_mode"], "HTML")
        callback = rendered["reply_markup"]["inline_keyboard"][0][0][
            "callback_data"
        ]
        self.assertTrue(callback.startswith("arcun:"))
        self.assertEqual(
            rendered["reply_markup"]["inline_keyboard"][0][0]["text"],
            "♻️ Восстановить",
        )
        self.assertEqual(record.presentation, "card")
        self.assertEqual(record.archive_message_state, "sent")
        self.assertEqual(record.archive_message_id, 1002)

    async def test_migrated_legacy_archive_stays_in_shared_index(self) -> None:
        self.store.reserve_archived_thread(
            thread_id="thread-1",
            restore_token="legacy-token",
            title="Archived task",
            thread_created_at=1_700_000_000,
            source_topic_id=50,
        )
        self.store.connection.execute(
            """
            UPDATE archived_threads
            SET
                presentation = 'legacy_index',
                archive_message_state = 'sent',
                archive_message_id = NULL
            WHERE thread_id = 'thread-1'
            """
        )
        self.store.connection.commit()

        await self.service.sync_threads()

        record = self.store.archived_thread("thread-1")
        self.assertEqual(record.presentation, "legacy_index")
        self.assertEqual(len(self.telegram.sent_messages), 1)
        self.assertTrue(self.telegram.edited_messages)
        self.assertIn(
            "Archived task",
            self.telegram.edited_messages[-1]["text"],
        )

    async def test_ambiguous_archive_card_send_never_duplicates_or_deletes(
        self,
    ) -> None:
        self.telegram.send_error_at_call = 2
        self.telegram.send_error = TelegramError(
            "sendMessage: network outcome unknown",
            method="sendMessage",
            kind="network_error",
            outcome_ambiguous=True,
        )

        await self.service.sync_threads()
        await self.service.sync_threads()

        record = self.store.archived_thread("thread-1")
        self.assertEqual(record.archive_message_state, "outcome_unknown")
        self.assertIsNone(record.archive_message_id)
        self.assertFalse(self.store.topic_for_thread("thread-1").archived)
        self.assertEqual(self.telegram.deleted_topics, [])
        self.assertEqual(len(self.telegram.sent_messages), 1)

    async def test_telegram_archive_request_uses_same_card_path(self) -> None:
        await self.service.handle_telegram_update(
            {
                "message": {
                    "message_id": 321,
                    "message_thread_id": 50,
                    "chat": {"id": -100500, "type": "supergroup"},
                    "from": {"id": 100, "is_bot": False},
                    "text": "/archive",
                }
            }
        )

        self.service.codex.archive_thread.assert_awaited_once_with("thread-1")
        self.service.codex.start_turn.assert_not_awaited()
        self.assertIsNone(
            self.store.queued_message_for_client_id("tg:-100500:321")
        )
        self.service.codex.read_thread = AsyncMock(
            return_value={
                **self.archived_summary,
                "turns": [
                    {
                        "id": "turn-original",
                        "items": [
                            {
                                "id": "user-original",
                                "type": "userMessage",
                                "content": [
                                    {
                                        "type": "text",
                                        "text": "Настроить архивные карточки",
                                    }
                                ],
                            },
                            {
                                "id": "final-original",
                                "type": "agentMessage",
                                "phase": "final_answer",
                                "text": "Карточки настроены и проверены.",
                            },
                        ],
                    },
                ],
            }
        )

        await self.service.sync_threads()

        card = self.telegram.sent_messages[-1]
        self.assertIn(
            "О чём: Настроить архивные карточки",
            card["text"],
        )
        self.assertIn(
            "Что сделали: Карточки настроены и проверены.",
            card["text"],
        )
        self.assertTrue(self.store.topic_for_thread("thread-1").archived)

    async def test_archive_hub_reserved_pre_call_state_resumes_create(
        self,
    ) -> None:
        self.store.set_setting(
            "telegram_archive_hub_creation_unknown",
            "reserved",
        )

        hub = await self.service._ensure_archive_hub()

        self.assertIsNotNone(hub)
        self.assertEqual(len(self.telegram.created_topics), 1)
        self.assertIsNone(
            self.store.get_setting(
                "telegram_archive_hub_creation_unknown"
            )
        )

    async def test_archive_hub_rejects_stale_observed_candidate(self) -> None:
        self.store.observe_topic(
            -100500,
            777,
            "Архивные треды",
        )
        self.store.set_setting(
            "telegram_archive_hub_creation_unknown",
            "outcome_unknown",
        )
        self.telegram.chat_action_error = TelegramError(
            "sendChatAction: Bad Request: TOPIC_ID_INVALID",
            method="sendChatAction",
            kind="api",
        )

        hub = await self.service._ensure_archive_hub()

        self.assertIsNone(hub)
        self.assertIsNone(self.store.archive_hub_topic_id())
        self.assertIsNone(self.store.observed_topic_title(-100500, 777))
        self.assertEqual(self.telegram.created_topics, [])

    async def test_desktop_archive_race_skips_stale_active_resume(self) -> None:
        self.service.codex.list_threads = AsyncMock(
            side_effect=lambda *, archived: [self.archived_summary]
        )
        self.service.codex.resume_thread = AsyncMock(
            side_effect=CodexProtocolError(
                "thread/resume: session is archived; unarchive it first"
            )
        )

        await self.service.sync_threads()

        self.service.codex.resume_thread.assert_awaited_once_with("thread-1")
        self.assertTrue(self.store.topic_for_thread("thread-1").archived)
        self.assertEqual(
            self.store.archived_thread("thread-1").status,
            "archived",
        )
        self.assertEqual(self.telegram.deleted_topics, [(-100500, 50)])

    async def test_failed_delete_keeps_working_topic_and_retries(self) -> None:
        self.telegram.delete_error = TelegramError(
            "deleteForumTopic: temporary failure",
            retryable=True,
        )

        await self.service.sync_threads()

        self.assertFalse(self.store.topic_for_thread("thread-1").archived)
        self.assertEqual(
            self.store.archived_thread("thread-1").status,
            "detected",
        )

        self.telegram.delete_error = None
        await self.service.sync_threads()

        self.assertTrue(self.store.topic_for_thread("thread-1").archived)
        self.assertEqual(
            self.store.archived_thread("thread-1").status,
            "archived",
        )
        self.assertEqual(self.telegram.deleted_topics, [(-100500, 50)])

    async def test_topic_id_invalid_delete_is_idempotent_success(self) -> None:
        self.telegram.delete_error = TelegramError(
            "deleteForumTopic: Bad Request: TOPIC_ID_INVALID",
            method="deleteForumTopic",
            kind="api",
        )
        self.store.observe_topic(-100500, 50, "Archived task")

        await self.service.sync_threads()

        self.assertTrue(self.store.topic_for_thread("thread-1").archived)
        self.assertEqual(
            self.store.archived_thread("thread-1").status,
            "archived",
        )
        self.assertIsNone(
            self.store.observed_topic_title(-100500, 50)
        )

    async def test_archive_waits_for_independent_absence_confirmation(
        self,
    ) -> None:
        self.telegram.delete_successes_before_absent = (
            ARCHIVE_DELETE_CONFIRM_ATTEMPTS
        )

        await self.service.sync_threads()

        self.assertFalse(self.store.topic_for_thread("thread-1").archived)
        self.assertEqual(
            self.store.archived_thread("thread-1").status,
            "detected",
        )
        self.assertEqual(
            self.telegram.delete_calls[(-100500, 50)],
            ARCHIVE_DELETE_CONFIRM_ATTEMPTS,
        )

        await self.service.sync_threads()

        self.assertTrue(self.store.topic_for_thread("thread-1").archived)
        self.assertEqual(
            self.store.archived_thread("thread-1").status,
            "archived",
        )

    async def test_pending_archive_deletion_survives_process_restart(
        self,
    ) -> None:
        self.telegram.delete_successes_before_absent = (
            ARCHIVE_DELETE_CONFIRM_ATTEMPTS
        )

        await self.service.sync_threads()

        self.assertFalse(self.store.topic_for_thread("thread-1").archived)
        self.assertEqual(
            self.store.archived_thread("thread-1").status,
            "detected",
        )

        self.store.close()
        self.store = BridgeStore(self.database_path)
        restarted = BridgeService(
            config=self.service.config,
            store=self.store,
            telegram=self.telegram,
        )
        restarted.codex = self.service.codex

        await restarted.sync_threads()

        self.assertTrue(self.store.topic_for_thread("thread-1").archived)
        self.assertEqual(
            self.store.archived_thread("thread-1").status,
            "archived",
        )

    async def test_message_from_archived_topic_reconfirms_deletion(
        self,
    ) -> None:
        await self.service.sync_threads()
        self.telegram.delete_calls.clear()
        self.telegram.deleted_topics.clear()

        await self.service.handle_telegram_update(
            {
                "update_id": 1,
                "message": {
                    "message_id": 77,
                    "message_thread_id": 50,
                    "chat": {"id": -100500},
                    "from": {"id": 100, "is_bot": False},
                    "voice": {
                        "file_id": "archived-topic-proof",
                        "duration": 3,
                    },
                },
            }
        )

        self.assertEqual(
            self.telegram.delete_calls[(-100500, 50)],
            2,
        )
        self.assertEqual(
            self.telegram.deleted_topics,
            [(-100500, 50)],
        )
        self.assertTrue(self.store.topic_for_thread("thread-1").archived)
        self.assertEqual(
            self.store.archived_thread("thread-1").status,
            "archived",
        )

    async def test_archived_topic_redelete_retries_after_transient_failure(
        self,
    ) -> None:
        await self.service.sync_threads()
        self.telegram.delete_calls.clear()
        self.telegram.deleted_topics.clear()
        self.telegram.delete_error = TelegramError(
            "temporary transport failure",
            method="deleteForumTopic",
            kind="transport",
        )

        await self.service.handle_telegram_update(
            {
                "update_id": 2,
                "message": {
                    "message_id": 78,
                    "message_thread_id": 50,
                    "chat": {"id": -100500},
                    "from": {"id": 100, "is_bot": False},
                    "text": "отправь этот топик в архив",
                },
            }
        )

        self.assertTrue(self.store.topic_for_thread("thread-1").archived)
        self.assertEqual(
            self.store.archived_thread("thread-1").status,
            "detected",
        )

        self.store.close()
        self.store = BridgeStore(self.database_path)
        restarted = BridgeService(
            config=self.service.config,
            store=self.store,
            telegram=self.telegram,
        )
        restarted.codex = self.service.codex
        self.telegram.delete_error = None
        await restarted.sync_threads()

        self.assertEqual(
            self.telegram.delete_calls[(-100500, 50)],
            2,
        )
        self.assertTrue(self.store.topic_for_thread("thread-1").archived)
        self.assertEqual(
            self.store.archived_thread("thread-1").status,
            "archived",
        )

    async def test_mass_archive_uses_one_registry_batch(self) -> None:
        summaries = [self.archived_summary]
        for index in range(2, 9):
            thread_id = f"thread-{index}"
            self.store.upsert_topic(
                thread_id=thread_id,
                chat_id=-100500,
                topic_id=49 + index,
                title=f"Archived task {index}",
            )
            summaries.append(
                {
                    **self.archived_summary,
                    "id": thread_id,
                    "name": f"Archived task {index}",
                }
            )
        self.service.codex.list_threads = AsyncMock(
            side_effect=lambda *, archived: summaries if archived else []
        )

        await self.service.sync_threads()

        self.assertEqual(len(self.telegram.deleted_topics), 8)
        self.assertEqual(
            sum(not topic.archived for topic in self.store.list_topics()),
            0,
        )
        self.assertEqual(len(self.store.list_current_archived_threads()), 8)
        self.assertTrue(
            all(
                record.status == "archived"
                for record in self.store.list_current_archived_threads()
            )
        )
        self.assertEqual(len(self.telegram.created_topics), 1)
        self.assertEqual(len(self.telegram.sent_messages), 9)
        self.assertEqual(len(self.telegram.edited_messages), 0)

        await self.service.sync_threads()

        self.assertEqual(len(self.telegram.deleted_topics), 8)
        self.assertEqual(len(self.telegram.sent_messages), 9)
        self.assertEqual(len(self.telegram.edited_messages), 0)

    async def test_restore_button_unarchives_once_and_creates_new_topic(
        self,
    ) -> None:
        await self.service.sync_threads()
        record = self.store.archived_thread("thread-1")
        self.assertIsNotNone(record)
        self.service.codex.unarchive_thread = AsyncMock(
            return_value={
                **self.archived_summary,
                "updatedAt": 1_700_000_200,
            }
        )
        callback = {
            "id": "restore-1",
            "data": f"arcun:{record.restore_token}",
            "from": {"id": 100, "is_bot": False},
            "message": {
                "message_id": record.archive_message_id,
                "message_thread_id": self.store.archive_hub_topic_id(),
                "chat": {"id": -100500, "type": "supergroup"},
            },
        }

        await self.service.handle_callback(callback)
        await self.service.handle_callback({**callback, "id": "restore-2"})

        self.service.codex.unarchive_thread.assert_awaited_once_with("thread-1")
        topic = self.store.topic_for_thread("thread-1")
        self.assertFalse(topic.archived)
        self.assertNotEqual(topic.topic_id, 50)
        self.assertEqual(
            self.store.archived_thread("thread-1").status,
            "restored",
        )
        self.assertEqual(self.store.list_current_archived_threads(), [])
        self.assertEqual(
            self.telegram.callback_answers[-1],
            ("restore-2", "Тред уже восстановлен"),
        )
        restored_card = self.telegram.edited_messages[-1]
        self.assertIn("Статус: ✅ восстановлен", restored_card["text"])
        self.assertEqual(
            restored_card["reply_markup"],
            {"inline_keyboard": []},
        )

    async def test_ambiguous_replacement_adopts_observed_topic_without_duplicate(
        self,
    ) -> None:
        await self.service.sync_threads()
        self.store.set_archived_thread_status("thread-1", "restoring")
        self.store.mark_archive_replacement_outcome_unknown("thread-1")
        self.store.observe_topic(-100500, 777, "Archived task")
        create_count = len(self.telegram.created_topics)

        topic, created = await self.service._ensure_active_archive_topic(
            self.archived_summary,
            title="Archived task",
            updated_at=1_700_000_200,
        )

        self.assertTrue(created)
        self.assertEqual(topic.topic_id, 777)
        self.assertEqual(len(self.telegram.created_topics), create_count)
        self.assertEqual(
            self.store.archived_thread("thread-1").status,
            "restored",
        )

    async def test_ambiguous_replacement_uses_durable_title_then_renames(
        self,
    ) -> None:
        await self.service.sync_threads()
        self.store.begin_archive_restore(
            "thread-1",
            replacement_title="Archived task",
        )
        self.store.mark_archive_replacement_outcome_unknown("thread-1")
        self.store.observe_topic(-100500, 777, "Archived task")
        create_count = len(self.telegram.created_topics)

        topic, created = await self.service._ensure_active_archive_topic(
            {**self.archived_summary, "name": "Renamed task"},
            title="Renamed task",
            updated_at=1_700_000_200,
        )

        self.assertTrue(created)
        self.assertEqual(topic.topic_id, 777)
        self.assertEqual(topic.title, "Renamed task")
        self.assertEqual(len(self.telegram.created_topics), create_count)
        self.assertEqual(
            self.telegram.edited_topics,
            [(-100500, 777, "Renamed task")],
        )

    async def test_ambiguous_replacement_rejects_stale_observed_topic(
        self,
    ) -> None:
        await self.service.sync_threads()
        self.store.set_archived_thread_status("thread-1", "restoring")
        self.store.mark_archive_replacement_outcome_unknown("thread-1")
        self.store.observe_topic(-100500, 777, "Archived task")
        self.telegram.chat_action_error = TelegramError(
            "sendChatAction: Bad Request: TOPIC_ID_INVALID",
            method="sendChatAction",
            kind="api",
        )

        topic, created = await self.service._ensure_active_archive_topic(
            self.archived_summary,
            title="Archived task",
            updated_at=1_700_000_200,
        )

        self.assertIsNone(topic)
        self.assertFalse(created)
        self.assertIsNone(self.store.observed_topic_title(-100500, 777))
        self.assertTrue(
            self.store.archived_thread(
                "thread-1"
            ).replacement_outcome_unknown
        )


if __name__ == "__main__":
    unittest.main()
