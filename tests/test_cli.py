from __future__ import annotations

import contextlib
import io
import sys
import tempfile
import unittest
from argparse import Namespace
from pathlib import Path
from types import SimpleNamespace
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from codex_telegram_bridge.cli import (  # noqa: E402
    create_runtime,
    detect_desktop_shared_socket,
    doctor_database_health,
    doctor_media_health,
    doctor_topic_health,
    main,
    mapping_health,
    queue_health_is_acceptable,
    requirements_allow_full_access,
    setup_final_icon,
    thread_contract_is_valid,
)
from codex_telegram_bridge.config import BridgeConfig  # noqa: E402
from codex_telegram_bridge.service import (  # noqa: E402
    FINAL_ANSWER_CUSTOM_EMOJI_SETTING,
)
from codex_telegram_bridge.store import (  # noqa: E402
    BridgeStore,
    CURRENT_SCHEMA_VERSION,
)
from codex_telegram_bridge.telegram import TelegramError  # noqa: E402


class DoctorDesktopDetectionTests(unittest.TestCase):
    socket_path = Path("/tmp/shared-codex.sock")

    def test_full_access_requirements_fail_closed(self) -> None:
        self.assertTrue(requirements_allow_full_access(None))
        self.assertTrue(
            requirements_allow_full_access(
                {
                    "allowedApprovalPolicies": ["never", "on-request"],
                    "allowedSandboxModes": [
                        "danger-full-access",
                        "workspace-write",
                    ],
                }
            )
        )
        self.assertFalse(
            requirements_allow_full_access(
                {
                    "allowedApprovalPolicies": ["on-request"],
                    "allowedSandboxModes": ["danger-full-access"],
                }
            )
        )
        self.assertFalse(
            requirements_allow_full_access(
                {
                    "allowedApprovalPolicies": ["never"],
                    "allowedSandboxModes": ["workspace-write"],
                }
            )
        )
        self.assertFalse(requirements_allow_full_access("invalid"))  # type: ignore[arg-type]

    def test_running_desktop_must_have_positive_socket_peer(self) -> None:
        running, shared = detect_desktop_shared_socket(
            process_lines=[
                "/Applications/ChatGPT.app/Contents/MacOS/ChatGPT",
            ],
            lsof_lines=[
                (
                    "codex 10 user 3u unix 0xserver 0t0 "
                    f"{self.socket_path}"
                ),
            ],
            socket_path=self.socket_path,
        )

        self.assertTrue(running)
        self.assertFalse(shared)

    def test_mapping_health_requires_exact_open_mapping_set(self) -> None:
        topics = [
            SimpleNamespace(thread_id="a", archived=False),
            SimpleNamespace(thread_id="old", archived=False),
            SimpleNamespace(thread_id="archived", archived=True),
        ]
        result = mapping_health(
            active_threads=[{"id": "a"}, {"id": "missing"}],
            topics=topics,
        )
        self.assertFalse(result["exact"])
        self.assertEqual(1, result["missingMappings"])
        self.assertEqual(1, result["staleMappings"])

    def test_queue_health_rejects_only_stale_active_work(self) -> None:
        self.assertTrue(
            queue_health_is_acceptable(
                {
                    "pending": 1,
                    "dispatching": 0,
                    "oldestActiveAgeSeconds": 30,
                }
            )
        )
        self.assertFalse(
            queue_health_is_acceptable(
                {
                    "pending": 0,
                    "dispatching": 1,
                    "oldestActiveAgeSeconds": 301,
                }
            )
        )

    def test_doctor_exposes_schema_and_sanitized_delivery_uncertainty(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = BridgeStore(Path(directory) / "bridge.sqlite3")
            try:
                store.reserve_outbound_delivery(
                    kind="status_card",
                    source_key="sensitive-status-key",
                    thread_id="sensitive-thread-id",
                    chat_id=-1001,
                    topic_id=50,
                    reply_to_message_id=81,
                )
                store.mark_outbound_delivery_outcome_unknown(
                    "status_card",
                    "sensitive-status-key",
                )
                store.reserve_archived_thread(
                    thread_id="archive-pending",
                    restore_token="opaque-token",
                    title="Pending archive",
                    thread_created_at=1,
                    source_topic_id=50,
                )

                result = doctor_database_health(store)
            finally:
                store.close()

        self.assertEqual(
            result["databaseSchemaVersion"],
            CURRENT_SCHEMA_VERSION,
        )
        self.assertEqual(
            result["expectedDatabaseSchemaVersion"],
            CURRENT_SCHEMA_VERSION,
        )
        self.assertEqual(
            result["deliveryUncertainty"]["totalUncertain"],
            1,
        )
        self.assertEqual(
            result["deliveryUncertainty"][
                "statusCardsOutcomeUnknown"
            ],
            1,
        )
        self.assertEqual(result["pendingArchiveDeletions"], 1)
        self.assertNotIn("sensitive", repr(result))

    def test_doctor_separates_working_service_and_observed_topics(
        self,
    ) -> None:
        store = mock.Mock()
        store.list_topics.return_value = [
            SimpleNamespace(archived=False),
            SimpleNamespace(archived=False),
            SimpleNamespace(archived=True),
        ]
        store.archive_hub_topic_id.return_value = 70
        store.observed_unmapped_count.return_value = 1

        result = doctor_topic_health(store, chat_id=-1001)

        self.assertEqual(result["openMappedTopics"], 2)
        self.assertEqual(result["openServiceTopics"], 2)
        self.assertEqual(result["openObservedUnmappedTopics"], 1)
        self.assertEqual(result["openKnownTelegramTopics"], 5)
        self.assertFalse(result["telegramTopicEnumerationAvailable"])
        store.observed_unmapped_count.assert_called_once_with(
            chat_id=-1001,
            excluding_topic_ids=(70,),
        )

    def test_thread_contract_requires_id_and_turn_list(self) -> None:
        self.assertTrue(thread_contract_is_valid({"id": "thread", "turns": []}))
        self.assertFalse(thread_contract_is_valid({"id": "", "turns": []}))
        self.assertFalse(
            thread_contract_is_valid({"id": "thread", "turns": {}})
        )

    def test_doctor_requires_ffmpeg_and_owner_only_media_storage(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "state"
            workspace = Path(directory) / "workspace"
            state.mkdir(mode=0o700)
            workspace.mkdir()
            config = BridgeConfig(
                workspace=workspace,
                state_dir=state,
                ffmpeg_binary="/bin/sh",
            )

            ready = doctor_media_health(config)
            state.chmod(0o755)
            unsafe = doctor_media_health(config)

        self.assertTrue(ready["mediaInputReady"])
        self.assertFalse(unsafe["mediaStorageOwnerOnly"])
        self.assertFalse(unsafe["mediaInputReady"])

    def test_read_only_runtime_does_not_change_database(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory)
            database = state / "bridge.sqlite3"
            writable = BridgeStore(database)
            writable.close()
            before = database.stat().st_mtime_ns
            args = Namespace(
                workspace=str(ROOT),
                state_dir=str(state),
                keychain_service="test-service",
            )
            with mock.patch(
                "codex_telegram_bridge.cli.read_bot_token",
                return_value="123456:abcdefghijklmnopqrstuv",
            ):
                _, read_only, _ = create_runtime(args, read_only=True)
            try:
                self.assertTrue(read_only.read_only)
                self.assertEqual(before, database.stat().st_mtime_ns)
            finally:
                read_only.close()

    def test_runtime_refuses_serve_during_interrupted_install(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory)
            journal = state / "install-transactions" / "active.json"
            journal.parent.mkdir()
            journal.write_text('{"version":1}\n', encoding="utf-8")
            errors = io.StringIO()

            with contextlib.redirect_stderr(errors):
                result = main(
                    [
                        "--workspace",
                        str(ROOT),
                        "--state-dir",
                        str(state),
                        "serve",
                    ]
                )

            self.assertEqual(result, 75)
            self.assertIn("recovery is required", errors.getvalue())
            self.assertFalse((state / "bridge.sqlite3").exists())

    def test_main_accepts_default_state_directory(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config = SimpleNamespace(state_dir=Path(directory))
            store = mock.Mock()
            telegram = mock.Mock()
            output = io.StringIO()
            with (
                mock.patch(
                    "codex_telegram_bridge.cli.create_config",
                    return_value=config,
                ),
                mock.patch(
                    "codex_telegram_bridge.cli.create_runtime",
                    return_value=(config, store, telegram),
                ),
                mock.patch(
                    "codex_telegram_bridge.cli.run_doctor",
                    new=mock.AsyncMock(return_value={"ok": True}),
                ),
                contextlib.redirect_stdout(output),
            ):
                result = main(["doctor"])

            self.assertEqual(0, result)
            self.assertIn('"ok": true', output.getvalue())
            store.close.assert_called_once()

    def test_desktop_peer_to_shared_listener_is_verified(self) -> None:
        running, shared = detect_desktop_shared_socket(
            process_lines=[
                "/Applications/ChatGPT.app/Contents/MacOS/ChatGPT",
            ],
            lsof_lines=[
                (
                    "codex 10 user 3u unix 0xserver 0t0 "
                    f"{self.socket_path}"
                ),
                "ChatGPT 20 user 9u unix 0xclient 0t0 ->0xserver",
            ],
            socket_path=self.socket_path,
        )

        self.assertTrue(running)
        self.assertTrue(shared)

    def test_private_server_overrides_socket_heuristic(self) -> None:
        running, shared = detect_desktop_shared_socket(
            process_lines=[
                "/Applications/ChatGPT.app/Contents/MacOS/ChatGPT",
                (
                    "/Applications/ChatGPT.app/Contents/Resources/codex "
                    "-c features.code_mode_host=true app-server "
                    "--analytics-default-enabled"
                ),
            ],
            lsof_lines=[
                (
                    "codex 10 user 3u unix 0xserver 0t0 "
                    f"{self.socket_path}"
                ),
                "ChatGPT 20 user 9u unix 0xclient 0t0 ->0xserver",
            ],
            socket_path=self.socket_path,
        )

        self.assertTrue(running)
        self.assertFalse(shared)


class FinalIconSetupTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        temp_path = Path(self.temp_dir.name)
        self.image = temp_path / "emoji.png"
        self.image.write_bytes(b"png")
        self.store = BridgeStore(temp_path / "state.sqlite3")
        self.store.bind(
            chat_id=-100500,
            allowed_user_id=100,
            bot_id=700,
            bot_username="project_bridge_bot",
            chat_title="Private project",
        )
        self.config = SimpleNamespace(workspace=temp_path)

    def tearDown(self) -> None:
        self.store.close()
        self.temp_dir.cleanup()

    def test_existing_set_is_reused_without_upload(self) -> None:
        telegram = SimpleNamespace(
            get_sticker_set=lambda _: {
                "stickers": [{"custom_emoji_id": "123456789"}]
            }
        )

        result = setup_final_icon(
            config=self.config,
            store=self.store,
            telegram=telegram,
            image_path=self.image,
        )

        self.assertEqual(
            self.store.get_setting(FINAL_ANSWER_CUSTOM_EMOJI_SETTING),
            "123456789",
        )
        self.assertFalse(result["stickerSetCreated"])
        self.assertNotIn("customEmojiId", result)

    def test_missing_set_is_created_then_resolved(self) -> None:
        class FakeTelegram:
            def __init__(self) -> None:
                self.lookups = 0
                self.uploaded = False
                self.created = False

            def get_sticker_set(self, _: str) -> dict:
                self.lookups += 1
                if self.lookups == 1:
                    raise TelegramError(
                        "getStickerSet: Bad Request: STICKERSET_INVALID"
                    )
                return {
                    "stickers": [{"custom_emoji_id": "987654321"}]
                }

            def upload_static_sticker(self, **_: object) -> dict:
                self.uploaded = True
                return {"file_id": "uploaded-file"}

            def create_custom_emoji_set(self, **_: object) -> bool:
                self.created = True
                return True

        telegram = FakeTelegram()
        result = setup_final_icon(
            config=self.config,
            store=self.store,
            telegram=telegram,
            image_path=self.image,
        )

        self.assertTrue(telegram.uploaded)
        self.assertTrue(telegram.created)
        self.assertTrue(result["stickerSetCreated"])
        self.assertEqual(
            self.store.get_setting(FINAL_ANSWER_CUSTOM_EMOJI_SETTING),
            "987654321",
        )


if __name__ == "__main__":
    unittest.main()
