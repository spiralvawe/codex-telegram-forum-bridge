from __future__ import annotations

import hashlib
import shutil
import sqlite3
import sys
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from codex_telegram_bridge.store import (  # noqa: E402
    CURRENT_SCHEMA_VERSION,
    MIGRATION_BACKUP_RETENTION,
    OPERATIONAL_BACKUP_LOCK_NAME,
    OPERATIONAL_BACKUP_MAX_RETENTION,
    OPERATIONAL_BACKUP_NAME,
    BridgeStore,
    SchemaVersionError,
)
from codex_telegram_bridge.input_types import LocalInput  # noqa: E402


class StoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.store = BridgeStore(Path(self.temp_dir.name) / "state.sqlite3")

    def tearDown(self) -> None:
        self.store.close()
        self.temp_dir.cleanup()

    def test_binding_round_trip(self) -> None:
        self.store.bind(
            chat_id=-1001,
            allowed_user_id=42,
            bot_id=7,
            bot_username="example_bot",
            chat_title="Private project",
        )
        binding = self.store.binding()
        self.assertIsNotNone(binding)
        self.assertEqual(binding.chat_id, -1001)
        self.assertEqual(binding.allowed_user_id, 42)
        self.assertEqual(binding.bot_username, "example_bot")

    def test_quarantined_update_is_processed_and_recorded(self) -> None:
        self.store.quarantine_telegram_update(
            100,
            update_kind="message",
            error_type="AttributeError",
        )

        self.assertTrue(self.store.telegram_update_processed(100))
        self.assertEqual(
            self.store.telegram_update_quarantine_health(),
            {"quarantined": 1},
        )
        row = self.store.connection.execute(
            "SELECT update_kind, error_type FROM telegram_update_quarantine"
        ).fetchone()
        self.assertEqual(tuple(row), ("message", "AttributeError"))

    def test_topic_mapping_is_bidirectional(self) -> None:
        self.store.upsert_topic(
            thread_id="thread-1",
            chat_id=-1001,
            topic_id=10,
            title="Test",
            last_updated_at=12,
        )
        by_thread = self.store.topic_for_thread("thread-1")
        by_topic = self.store.topic_for_telegram(-1001, 10)
        self.assertEqual(by_thread, by_topic)
        self.assertEqual(by_thread.title, "Test")

    def test_topic_creation_intent_is_idempotently_reserved(self) -> None:
        first = self.store.reserve_topic_creation(
            thread_id="thread-create",
            chat_id=-1001,
            title="Create once",
        )
        replay = self.store.reserve_topic_creation(
            thread_id="thread-create",
            chat_id=-1001,
            title="Create once",
        )

        self.assertEqual(first, replay)
        self.assertEqual(first.status, "reserved")
        self.assertIsNone(first.topic_id)
        count = self.store.connection.execute(
            """
            SELECT COUNT(*) FROM topic_creation_intents
            WHERE thread_id = ?
            """,
            ("thread-create",),
        ).fetchone()[0]
        self.assertEqual(count, 1)

    def test_topic_creation_replay_rejects_different_parameters(self) -> None:
        self.store.reserve_topic_creation(
            thread_id="thread-create",
            chat_id=-1001,
            title="Original",
        )

        with self.assertRaisesRegex(RuntimeError, "different parameters"):
            self.store.reserve_topic_creation(
                thread_id="thread-create",
                chat_id=-1002,
                title="Original",
            )
        with self.assertRaisesRegex(RuntimeError, "different parameters"):
            self.store.reserve_topic_creation(
                thread_id="thread-create",
                chat_id=-1001,
                title="Changed",
            )

    def test_topic_creation_state_machine_completes_mapping_atomically(
        self,
    ) -> None:
        reserved = self.store.reserve_topic_creation(
            thread_id="thread-create",
            chat_id=-1001,
            title="Durable topic",
        )
        unknown = self.store.mark_topic_creation_outcome_unknown(
            "thread-create"
        )
        created = self.store.record_created_topic("thread-create", 55)
        topic = self.store.complete_topic_creation(
            "thread-create",
            last_updated_at=123,
        )

        self.assertEqual(reserved.status, "reserved")
        self.assertEqual(unknown.status, "outcome_unknown")
        self.assertEqual(created.status, "created")
        self.assertEqual(created.topic_id, 55)
        self.assertEqual(topic.thread_id, "thread-create")
        self.assertEqual(topic.topic_id, 55)
        self.assertEqual(topic.title, "Durable topic")
        self.assertEqual(topic.last_updated_at, 123)
        completed = self.store.topic_creation_intent("thread-create")
        self.assertIsNotNone(completed)
        self.assertEqual(completed.status, "completed")
        self.assertEqual(self.store.unresolved_topic_creations(), [])

    def test_reserved_topic_creation_is_unresolved_after_reopen(self) -> None:
        self.store.reserve_topic_creation(
            thread_id="thread-reserved",
            chat_id=-1001,
            title="Reserved",
        )

        reopened = BridgeStore(self.store.path)
        try:
            unresolved = reopened.unresolved_topic_creations()
            self.assertEqual(len(unresolved), 1)
            self.assertEqual(unresolved[0].status, "reserved")
            self.assertIsNone(
                reopened.topic_for_thread("thread-reserved")
            )
        finally:
            reopened.close()

    def test_unknown_topic_creation_is_unresolved_after_reopen(self) -> None:
        self.store.reserve_topic_creation(
            thread_id="thread-unknown",
            chat_id=-1001,
            title="Unknown",
        )
        self.store.mark_topic_creation_outcome_unknown("thread-unknown")

        reopened = BridgeStore(self.store.path)
        try:
            unresolved = reopened.unresolved_topic_creations()
            self.assertEqual(len(unresolved), 1)
            self.assertEqual(unresolved[0].status, "outcome_unknown")
            self.assertIsNone(
                reopened.topic_for_thread("thread-unknown")
            )
        finally:
            reopened.close()

    def test_recorded_topic_creation_is_unresolved_after_reopen(self) -> None:
        self.store.reserve_topic_creation(
            thread_id="thread-created",
            chat_id=-1001,
            title="Created",
        )
        self.store.mark_topic_creation_outcome_unknown("thread-created")
        self.store.record_created_topic("thread-created", 56)

        reopened = BridgeStore(self.store.path)
        try:
            unresolved = reopened.unresolved_topic_creations()
            self.assertEqual(len(unresolved), 1)
            self.assertEqual(unresolved[0].status, "created")
            self.assertEqual(unresolved[0].topic_id, 56)
            self.assertIsNone(
                reopened.topic_for_thread("thread-created")
            )
        finally:
            reopened.close()

    def test_completed_topic_creation_stays_complete_after_reopen(self) -> None:
        self.store.reserve_topic_creation(
            thread_id="thread-completed",
            chat_id=-1001,
            title="Completed",
        )
        self.store.mark_topic_creation_outcome_unknown("thread-completed")
        self.store.record_created_topic("thread-completed", 57)
        self.store.complete_topic_creation(
            "thread-completed",
            archived=True,
            last_updated_at=456,
        )

        reopened = BridgeStore(self.store.path)
        try:
            self.assertEqual(reopened.unresolved_topic_creations(), [])
            intent = reopened.topic_creation_intent("thread-completed")
            topic = reopened.topic_for_thread("thread-completed")
            self.assertIsNotNone(intent)
            self.assertIsNotNone(topic)
            self.assertEqual(intent.status, "completed")
            self.assertEqual(topic.topic_id, intent.topic_id)
            self.assertTrue(topic.archived)
            self.assertEqual(topic.last_updated_at, 456)
        finally:
            reopened.close()

    def test_topic_creation_transitions_are_replay_safe(self) -> None:
        self.store.reserve_topic_creation(
            thread_id="thread-replay",
            chat_id=-1001,
            title="Replay safe",
        )
        first_unknown = self.store.mark_topic_creation_outcome_unknown(
            "thread-replay"
        )
        replayed_unknown = self.store.mark_topic_creation_outcome_unknown(
            "thread-replay"
        )
        first_created = self.store.record_created_topic(
            "thread-replay",
            58,
        )
        replayed_created = self.store.record_created_topic(
            "thread-replay",
            58,
        )
        first_topic = self.store.complete_topic_creation("thread-replay")
        replayed_topic = self.store.complete_topic_creation("thread-replay")

        self.assertEqual(first_unknown, replayed_unknown)
        self.assertEqual(first_created, replayed_created)
        self.assertEqual(first_topic, replayed_topic)
        self.assertEqual(
            self.store.mark_topic_creation_outcome_unknown(
                "thread-replay"
            ).status,
            "completed",
        )

    def test_topic_creation_rejects_invalid_transition_and_result(self) -> None:
        self.store.reserve_topic_creation(
            thread_id="thread-invalid",
            chat_id=-1001,
            title="Invalid",
        )

        with self.assertRaisesRegex(RuntimeError, "not marked unknown"):
            self.store.record_created_topic("thread-invalid", 59)
        with self.assertRaisesRegex(RuntimeError, "result was not recorded"):
            self.store.complete_topic_creation("thread-invalid")

        self.store.mark_topic_creation_outcome_unknown("thread-invalid")
        self.store.record_created_topic("thread-invalid", 59)
        with self.assertRaisesRegex(RuntimeError, "different result"):
            self.store.record_created_topic("thread-invalid", 60)

    def test_topic_creation_methods_require_reservation(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "not reserved"):
            self.store.mark_topic_creation_outcome_unknown("missing")
        with self.assertRaisesRegex(RuntimeError, "not reserved"):
            self.store.record_created_topic("missing", 61)
        with self.assertRaisesRegex(RuntimeError, "not reserved"):
            self.store.complete_topic_creation("missing")

    def test_topic_creation_schema_rejects_inconsistent_state(self) -> None:
        with self.assertRaises(sqlite3.IntegrityError):
            self.store.connection.execute(
                """
                INSERT INTO topic_creation_intents(
                    thread_id, chat_id, title, topic_id, status,
                    created_at, updated_at
                )
                VALUES (?, ?, ?, ?, 'created', ?, ?)
                """,
                (
                    "thread-corrupt",
                    -1001,
                    "Corrupt",
                    None,
                    "now",
                    "now",
                ),
            )
        self.store.connection.rollback()

    def test_definite_failure_clear_allows_safe_new_reservation(self) -> None:
        self.store.reserve_topic_creation(
            thread_id="thread-definite-failure",
            chat_id=-1001,
            title="Retry safely",
        )
        self.store.mark_topic_creation_outcome_unknown(
            "thread-definite-failure"
        )

        cleared = self.store.clear_topic_creation_after_definite_failure(
            "thread-definite-failure"
        )
        replacement = self.store.reserve_topic_creation(
            thread_id="thread-definite-failure",
            chat_id=-1001,
            title="Retry safely",
        )

        self.assertTrue(cleared)
        self.assertEqual(replacement.status, "reserved")

    def test_definite_failure_clear_cannot_discard_known_result(self) -> None:
        self.store.reserve_topic_creation(
            thread_id="thread-known-result",
            chat_id=-1001,
            title="Known result",
        )
        with self.assertRaisesRegex(RuntimeError, "can be cleared"):
            self.store.clear_topic_creation_after_definite_failure(
                "thread-known-result"
            )

        self.store.mark_topic_creation_outcome_unknown("thread-known-result")
        self.store.record_created_topic("thread-known-result", 63)
        with self.assertRaisesRegex(RuntimeError, "can be cleared"):
            self.store.clear_topic_creation_after_definite_failure(
                "thread-known-result"
            )

    def test_topic_creation_mapping_conflict_rolls_back_completion(
        self,
    ) -> None:
        self.store.upsert_topic(
            thread_id="existing-thread",
            chat_id=-1001,
            topic_id=62,
            title="Existing",
        )
        self.store.reserve_topic_creation(
            thread_id="new-thread",
            chat_id=-1001,
            title="Conflicting",
        )
        self.store.mark_topic_creation_outcome_unknown("new-thread")
        self.store.record_created_topic("new-thread", 62)

        with self.assertRaises(sqlite3.IntegrityError):
            self.store.complete_topic_creation("new-thread")

        intent = self.store.topic_creation_intent("new-thread")
        self.assertIsNotNone(intent)
        self.assertEqual(intent.status, "created")
        self.assertIsNone(self.store.topic_for_thread("new-thread"))
        self.assertIsNotNone(self.store.topic_for_thread("existing-thread"))

    def test_topic_creation_migrates_into_existing_database(self) -> None:
        legacy_path = Path(self.temp_dir.name) / "topic-legacy.sqlite3"
        connection = sqlite3.connect(legacy_path)
        connection.executescript(
            """
            CREATE TABLE settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            INSERT INTO settings(key, value, updated_at)
            VALUES ('legacy', 'preserved', 'before-migration');
            """
        )
        connection.close()

        legacy = BridgeStore(legacy_path)
        try:
            intent = legacy.reserve_topic_creation(
                thread_id="thread-migrated",
                chat_id=-1001,
                title="Migrated",
            )
            self.assertEqual(intent.status, "reserved")
            self.assertEqual(legacy.get_setting("legacy"), "preserved")
            self.assertEqual(legacy_path.stat().st_mode & 0o777, 0o600)
        finally:
            legacy.close()

    def test_new_database_has_explicit_current_schema_version(self) -> None:
        version = self.store.connection.execute(
            "PRAGMA user_version"
        ).fetchone()[0]

        self.assertEqual(version, CURRENT_SCHEMA_VERSION)

    def test_newer_database_schema_is_refused_without_mutation(self) -> None:
        future_path = Path(self.temp_dir.name) / "future.sqlite3"
        connection = sqlite3.connect(future_path)
        connection.execute(
            f"PRAGMA user_version = {CURRENT_SCHEMA_VERSION + 1}"
        )
        connection.commit()
        connection.close()
        before = future_path.read_bytes()

        with self.assertRaisesRegex(SchemaVersionError, "newer than supported"):
            BridgeStore(future_path)
        with self.assertRaisesRegex(SchemaVersionError, "newer than supported"):
            BridgeStore(future_path, read_only=True)

        self.assertEqual(future_path.read_bytes(), before)
        self.assertFalse((future_path.parent / "backups").exists())

    def test_read_only_open_never_migrates_legacy_database(self) -> None:
        legacy_path = Path(self.temp_dir.name) / "read-only-legacy.sqlite3"
        connection = sqlite3.connect(legacy_path)
        connection.executescript(
            """
            CREATE TABLE settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            INSERT INTO settings(key, value, updated_at)
            VALUES ('legacy', 'preserved', 'before-migration');
            """
        )
        connection.close()
        before_bytes = legacy_path.read_bytes()
        before_mtime = legacy_path.stat().st_mtime_ns

        with self.assertRaisesRegex(
            SchemaVersionError,
            "read-only open cannot migrate",
        ):
            BridgeStore(legacy_path, read_only=True)

        self.assertEqual(legacy_path.read_bytes(), before_bytes)
        self.assertEqual(legacy_path.stat().st_mtime_ns, before_mtime)
        connection = sqlite3.connect(legacy_path)
        try:
            version = connection.execute("PRAGMA user_version").fetchone()[0]
            tables = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                )
            }
        finally:
            connection.close()
        self.assertEqual(version, 0)
        self.assertEqual(tables, {"settings"})

    def test_read_only_current_database_rejects_writes_and_stays_unchanged(
        self,
    ) -> None:
        current_path = Path(self.temp_dir.name) / "read-only-current.sqlite3"
        writable = BridgeStore(current_path)
        writable.set_setting("preserved", "yes")
        writable.close()
        before_bytes = current_path.read_bytes()
        before_mtime = current_path.stat().st_mtime_ns

        read_only = BridgeStore(current_path, read_only=True)
        try:
            self.assertEqual(read_only.get_setting("preserved"), "yes")
            self.assertEqual(read_only.integrity_check(), "ok")
            with self.assertRaises(sqlite3.OperationalError):
                read_only.set_setting("must-not-write", "no")
        finally:
            read_only.close()

        self.assertEqual(current_path.read_bytes(), before_bytes)
        self.assertEqual(current_path.stat().st_mtime_ns, before_mtime)
        connection = sqlite3.connect(current_path)
        try:
            version = connection.execute("PRAGMA user_version").fetchone()[0]
            missing = connection.execute(
                "SELECT value FROM settings WHERE key = 'must-not-write'"
            ).fetchone()
        finally:
            connection.close()
        self.assertEqual(version, CURRENT_SCHEMA_VERSION)
        self.assertIsNone(missing)

    def test_current_schema_missing_delivery_intents_is_rejected(self) -> None:
        path = self.store.path
        self.store.connection.execute(
            "DROP TABLE visible_item_delivery_intents"
        )
        self.store.connection.commit()
        with self.assertRaisesRegex(
            SchemaVersionError,
            "visible_item_delivery_intents",
        ):
            BridgeStore(path, read_only=True)

    def test_current_schema_missing_archive_column_is_rejected(self) -> None:
        path = self.store.path
        self.store.connection.execute(
            "ALTER TABLE archived_threads DROP COLUMN replacement_title"
        )
        self.store.connection.commit()
        with self.assertRaisesRegex(
            SchemaVersionError,
            "replacement_title",
        ):
            BridgeStore(path, read_only=True)

    def test_current_schema_missing_unique_index_is_rejected(self) -> None:
        path = self.store.path
        self.store.connection.execute("DROP INDEX queued_messages_client_id")
        self.store.connection.commit()

        with self.assertRaisesRegex(
            SchemaVersionError,
            "queued_messages.*queued_messages_client_id",
        ):
            BridgeStore(path, read_only=True)

    def test_current_schema_non_unique_replacement_index_is_rejected(self) -> None:
        path = self.store.path
        self.store.connection.executescript(
            """
            DROP INDEX turn_progress_entries_counterpart;
            CREATE INDEX turn_progress_entries_counterpart
            ON turn_progress_entries(thread_id, turn_id, counterpart_item_id);
            """
        )

        with self.assertRaisesRegex(
            SchemaVersionError,
            "turn_progress_entries.*turn_progress_entries_counterpart",
        ):
            BridgeStore(path, read_only=True)

    def test_current_schema_wrong_unique_index_columns_are_rejected(self) -> None:
        path = self.store.path
        self.store.connection.executescript(
            """
            DROP INDEX queued_messages_client_id;
            CREATE UNIQUE INDEX queued_messages_client_id
            ON queued_messages(id);
            """
        )

        with self.assertRaisesRegex(
            SchemaVersionError,
            "queued_messages.*queued_messages_client_id",
        ):
            BridgeStore(path, read_only=True)

    def test_current_schema_outbound_delivery_key_must_be_unique(self) -> None:
        path = self.store.path
        self.store.connection.executescript(
            """
            DROP INDEX outbound_deliveries_state;
            ALTER TABLE outbound_deliveries RENAME TO outbound_deliveries_old;
            CREATE TABLE outbound_deliveries (
                kind TEXT NOT NULL,
                source_key TEXT NOT NULL,
                thread_id TEXT,
                chat_id INTEGER NOT NULL,
                topic_id INTEGER NOT NULL,
                reply_to_message_id INTEGER NOT NULL,
                state TEXT NOT NULL,
                telegram_message_id INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            DROP TABLE outbound_deliveries_old;
            CREATE INDEX outbound_deliveries_state
                ON outbound_deliveries(kind, state, created_at);
            """
        )

        with self.assertRaisesRegex(
            SchemaVersionError,
            "outbound_deliveries.*uniquely key",
        ):
            BridgeStore(path, read_only=True)

    def test_current_schema_wrong_partial_index_predicate_is_rejected(self) -> None:
        path = self.store.path
        self.store.connection.executescript(
            """
            DROP INDEX turn_progress_entries_counterpart;
            CREATE UNIQUE INDEX turn_progress_entries_counterpart
            ON turn_progress_entries(
                thread_id, turn_id, counterpart_item_id
            )
            WHERE counterpart_item_id IS NULL;
            """
        )

        with self.assertRaisesRegex(
            SchemaVersionError,
            "turn_progress_entries.*turn_progress_entries_counterpart",
        ):
            BridgeStore(path, read_only=True)

    def test_current_schema_extra_partial_predicate_is_rejected(self) -> None:
        path = self.store.path
        self.store.connection.executescript(
            """
            DROP INDEX turn_progress_entries_counterpart;
            CREATE UNIQUE INDEX turn_progress_entries_counterpart
            ON turn_progress_entries(
                thread_id, turn_id, counterpart_item_id
            )
            WHERE counterpart_item_id IS NOT NULL AND thread_id = 'narrow';
            """
        )

        with self.assertRaisesRegex(
            SchemaVersionError,
            "turn_progress_entries.*turn_progress_entries_counterpart",
        ):
            BridgeStore(path, read_only=True)

    def test_database_symlink_is_rejected_without_changing_target_mode(self) -> None:
        target = self.store.path
        target_mode = target.stat().st_mode & 0o777
        alias = target.with_name("database-alias.sqlite3")
        alias.symlink_to(target)

        with self.assertRaisesRegex(SchemaVersionError, "must not be a symlink"):
            BridgeStore(alias)

        self.assertEqual(target_mode, target.stat().st_mode & 0o777)

    def test_migration_failure_rolls_back_and_keeps_consistent_backup(
        self,
    ) -> None:
        legacy_path = Path(self.temp_dir.name) / "rollback.sqlite3"
        connection = sqlite3.connect(legacy_path)
        connection.executescript(
            """
            CREATE TABLE settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            INSERT INTO settings(key, value, updated_at)
            VALUES ('legacy', 'preserved', 'before-migration');
            """
        )
        connection.close()

        def fail_actual_migration(
            store: BridgeStore,
            connection: sqlite3.Connection,
            target_version: int,
            *,
            validating: bool,
        ) -> None:
            if not validating:
                connection.execute(
                    "CREATE TABLE fault_injected_partial(id INTEGER)"
                )
                raise RuntimeError("fault after migration step")

        with mock.patch.object(
            BridgeStore,
            "_after_migration_step",
            fail_actual_migration,
        ):
            with self.assertRaisesRegex(
                RuntimeError,
                "fault after migration step",
            ):
                BridgeStore(legacy_path)

        connection = sqlite3.connect(legacy_path)
        try:
            version = connection.execute("PRAGMA user_version").fetchone()[0]
            legacy_value = connection.execute(
                "SELECT value FROM settings WHERE key = 'legacy'"
            ).fetchone()[0]
            partial = connection.execute(
                """
                SELECT 1 FROM sqlite_master
                WHERE type = 'table' AND name = 'fault_injected_partial'
                """
            ).fetchone()
            migrated = connection.execute(
                """
                SELECT 1 FROM sqlite_master
                WHERE type = 'table' AND name = 'thread_topics'
                """
            ).fetchone()
        finally:
            connection.close()
        self.assertEqual(version, 0)
        self.assertEqual(legacy_value, "preserved")
        self.assertIsNone(partial)
        self.assertIsNone(migrated)

        backups = list(
            (legacy_path.parent / "backups").glob(
                f"bridge-schema-v0-to-v{CURRENT_SCHEMA_VERSION}-*.sqlite3"
            )
        )
        self.assertEqual(len(backups), 1)
        self.assertEqual(backups[0].stat().st_mode & 0o777, 0o600)
        backup_connection = sqlite3.connect(backups[0])
        try:
            self.assertEqual(
                backup_connection.execute(
                    "PRAGMA integrity_check"
                ).fetchone()[0],
                "ok",
            )
            self.assertEqual(
                backup_connection.execute(
                    "SELECT value FROM settings WHERE key = 'legacy'"
                ).fetchone()[0],
                "preserved",
            )
        finally:
            backup_connection.close()

    def test_failed_validation_copy_never_touches_source_or_keeps_backup(
        self,
    ) -> None:
        legacy_path = Path(self.temp_dir.name) / "validation-failure.sqlite3"
        connection = sqlite3.connect(legacy_path)
        connection.execute(
            """
            CREATE TABLE settings(
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        connection.commit()
        connection.close()
        before = legacy_path.read_bytes()

        def fail_validation(
            store: BridgeStore,
            connection: sqlite3.Connection,
            target_version: int,
            *,
            validating: bool,
        ) -> None:
            if validating:
                raise RuntimeError("validation fault")

        with mock.patch.object(
            BridgeStore,
            "_after_migration_step",
            fail_validation,
        ):
            with self.assertRaisesRegex(RuntimeError, "validation fault"):
                BridgeStore(legacy_path)

        self.assertEqual(legacy_path.read_bytes(), before)
        backup_directory = legacy_path.parent / "backups"
        self.assertFalse(backup_directory.exists())

    def test_automatic_migration_backup_can_restore_legacy_data(self) -> None:
        legacy_path = Path(self.temp_dir.name) / "backup-source.sqlite3"
        connection = sqlite3.connect(legacy_path)
        connection.executescript(
            """
            CREATE TABLE settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            INSERT INTO settings(key, value, updated_at)
            VALUES ('restore-me', 'intact', 'before-migration');
            """
        )
        connection.close()

        migrated = BridgeStore(legacy_path)
        migrated.close()
        backups = list(
            (legacy_path.parent / "backups").glob(
                f"bridge-schema-v0-to-v{CURRENT_SCHEMA_VERSION}-*.sqlite3"
            )
        )
        self.assertEqual(len(backups), 1)

        restored_path = Path(self.temp_dir.name) / "restored.sqlite3"
        shutil.copyfile(backups[0], restored_path)
        restored = BridgeStore(restored_path)
        try:
            self.assertEqual(restored.integrity_check(), "ok")
            self.assertEqual(restored.get_setting("restore-me"), "intact")
            self.assertEqual(
                restored.connection.execute(
                    "PRAGMA user_version"
                ).fetchone()[0],
                CURRENT_SCHEMA_VERSION,
            )
        finally:
            restored.close()

    def test_v1_database_migrates_new_crash_safety_state(self) -> None:
        legacy_path = Path(self.temp_dir.name) / "v1.sqlite3"
        current = BridgeStore(legacy_path)
        current.set_setting("v1-marker", "preserved")
        current.close()
        connection = sqlite3.connect(legacy_path)
        connection.executescript(
            """
            DROP TABLE manual_topic_threads;
            DROP TABLE visible_item_delivery_intents;
            ALTER TABLE archived_threads DROP COLUMN replacement_title;
            ALTER TABLE archived_threads DROP COLUMN replacement_started_at;
            PRAGMA user_version = 1;
            """
        )
        connection.close()

        migrated = BridgeStore(legacy_path)
        try:
            version = migrated.connection.execute(
                "PRAGMA user_version"
            ).fetchone()[0]
            tables = {
                str(row[0])
                for row in migrated.connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                )
            }
            archive_columns = {
                str(row["name"])
                for row in migrated.connection.execute(
                    "PRAGMA table_info(archived_threads)"
                )
            }
            marker = migrated.get_setting("v1-marker")
        finally:
            migrated.close()

        self.assertEqual(version, CURRENT_SCHEMA_VERSION)
        self.assertEqual(marker, "preserved")
        self.assertIn("manual_topic_threads", tables)
        self.assertIn("visible_item_delivery_intents", tables)
        self.assertIn("replacement_title", archive_columns)
        self.assertIn("replacement_started_at", archive_columns)
        read_only = BridgeStore(legacy_path, read_only=True)
        read_only.close()

    def test_frozen_v2_database_migrates_to_current_without_reopening_risks(
        self,
    ) -> None:
        legacy_path = Path(self.temp_dir.name) / "frozen-v2.sqlite3"
        connection = sqlite3.connect(legacy_path)
        connection.row_factory = sqlite3.Row
        # This builder is the retained v2 schema builder. The fingerprint
        # freezes its exact sqlite_master representation so a future edit
        # cannot silently weaken this migration fixture.
        frozen_builder = object.__new__(BridgeStore)
        frozen_builder.connection = connection
        frozen_builder._migrate_schema_v1_current_connection()  # noqa: SLF001
        schema_rows = connection.execute(
            """
            SELECT type, name, tbl_name, sql
            FROM sqlite_master
            WHERE name NOT LIKE 'sqlite_%'
            ORDER BY type, name
            """
        ).fetchall()
        frozen_schema = "\n".join(
            "|".join("" if value is None else str(value) for value in row)
            for row in schema_rows
        )
        self.assertEqual(
            hashlib.sha256(frozen_schema.encode()).hexdigest(),
            "5f710442db84fa457ca551db35953d6f88eb33c2ccf32a2b8d02698a01194d42",
        )
        connection.execute(
            """
            INSERT INTO settings(key, value, updated_at)
            VALUES ('v2-marker', 'preserved', '2026-01-01T00:00:00+00:00')
            """
        )
        connection.execute(
            """
            INSERT INTO queued_messages(
                thread_id, chat_id, topic_id, telegram_message_id,
                text, client_id, status, status_message_id,
                created_at, updated_at
            )
            VALUES (
                'thread-v2', -1001, 50, 80,
                'queued', 'tg:-1001:80', 'dispatching', NULL,
                '2026-01-01T00:00:00+00:00',
                '2026-01-01T00:00:01+00:00'
            )
            """
        )
        connection.execute(
            """
            INSERT INTO visible_item_delivery_intents(
                thread_id, turn_id, item_type, item_origin,
                content_fingerprint, primary_item_id,
                counterpart_item_id, created_at
            )
            VALUES (
                'thread-v2', 'turn-v2', 'agentMessage', 'history',
                'fingerprint', 'final-v2', NULL,
                '2026-01-01T00:00:02+00:00'
            )
            """
        )
        connection.execute(
            """
            INSERT INTO control_status_cards(
                thread_id, chat_id, topic_id, telegram_message_id,
                status, created_at, updated_at
            )
            VALUES (
                'thread-v2', -1001, 50, 90, 'active',
                '2026-01-01T00:00:03+00:00',
                '2026-01-01T00:00:03+00:00'
            )
            """
        )
        connection.execute(
            """
            INSERT INTO control_status_actions(
                thread_id, chat_id, topic_id, telegram_message_id,
                action, created_at
            )
            VALUES (
                'thread-v2', -1001, 50, 90, 'steer',
                '2026-01-01T00:00:04+00:00'
            )
            """
        )
        connection.execute("PRAGMA user_version = 2")
        connection.commit()
        connection.close()

        migrated = BridgeStore(legacy_path)
        try:
            self.assertEqual(
                migrated.connection.execute(
                    "PRAGMA user_version"
                ).fetchone()[0],
                CURRENT_SCHEMA_VERSION,
            )
            self.assertEqual(migrated.get_setting("v2-marker"), "preserved")
            tables = {
                str(row[0])
                for row in migrated.connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                )
            }
            self.assertIn("outbound_deliveries", tables)
            queued = migrated.queued_message(1)
            self.assertIsNotNone(queued)
            self.assertEqual(queued.status, "dispatching")
            self.assertEqual(
                queued.dispatch_started_at,
                "2026-01-01T00:00:01+00:00",
            )
            self.assertEqual(queued.dispatch_history_miss_count, 0)
            self.assertEqual(queued.local_inputs, ())
            intent_state = migrated.connection.execute(
                """
                SELECT state FROM visible_item_delivery_intents
                WHERE primary_item_id = 'final-v2'
                """
            ).fetchone()[0]
            action = migrated.connection.execute(
                """
                SELECT state, updated_at FROM control_status_actions
                WHERE telegram_message_id = 90 AND action = 'steer'
                """
            ).fetchone()
            self.assertEqual(intent_state, "outcome_unknown")
            self.assertEqual(action["state"], "completed")
            self.assertEqual(
                action["updated_at"],
                "2026-01-01T00:00:04+00:00",
            )
        finally:
            migrated.close()

    def test_v4_archive_rows_migrate_as_legacy_index_entries(self) -> None:
        legacy_path = Path(self.temp_dir.name) / "archive-v4.sqlite3"
        connection = sqlite3.connect(legacy_path)
        connection.row_factory = sqlite3.Row
        builder = object.__new__(BridgeStore)
        builder.connection = connection
        builder._migrate_schema_v1_current_connection()  # noqa: SLF001
        builder._migrate_schema_v3(connection)  # noqa: SLF001
        builder._migrate_schema_v4(connection)  # noqa: SLF001
        connection.execute(
            """
            INSERT INTO archived_threads(
                thread_id, restore_token, title, thread_created_at,
                archived_at, source_topic_id, status, restored_at,
                replacement_topic_id, replacement_outcome_unknown,
                replacement_title, replacement_started_at,
                created_at, updated_at
            )
            VALUES (
                'legacy-archive', 'legacy-token', 'Legacy archive', 100,
                '2026-01-01T00:00:00+00:00', 50, 'archived', NULL,
                NULL, 0, NULL, NULL,
                '2026-01-01T00:00:00+00:00',
                '2026-01-01T00:00:00+00:00'
            )
            """
        )
        connection.execute("PRAGMA user_version = 4")
        connection.commit()
        connection.close()

        migrated = BridgeStore(legacy_path)
        try:
            record = migrated.archived_thread("legacy-archive")
            self.assertIsNotNone(record)
            self.assertEqual(record.presentation, "legacy_index")
            self.assertEqual(record.summary, "")
            self.assertEqual(record.archive_message_state, "sent")
            self.assertIsNone(record.archive_message_id)
        finally:
            migrated.close()

    def test_migration_backup_retention_is_bounded(self) -> None:
        for _ in range(MIGRATION_BACKUP_RETENTION + 2):
            self.store._create_migration_backup(  # noqa: SLF001
                CURRENT_SCHEMA_VERSION,
                CURRENT_SCHEMA_VERSION + 1,
            )

        backups = list(
            (self.store.path.parent / "backups").glob(
                "bridge-schema-v*-to-v*-*.sqlite3"
            )
        )
        self.assertEqual(len(backups), MIGRATION_BACKUP_RETENTION)
        self.assertTrue(
            all(path.stat().st_mode & 0o777 == 0o600 for path in backups)
        )

    def test_operational_backup_is_verified_readable_and_owner_only(
        self,
    ) -> None:
        self.store.set_setting("operational-backup-marker", "preserved")

        read_only_source = BridgeStore(self.store.path, read_only=True)
        try:
            result = read_only_source.create_operational_backup()
        finally:
            read_only_source.close()

        self.assertTrue(result.path.is_file())
        self.assertIsNotNone(OPERATIONAL_BACKUP_NAME.fullmatch(result.path.name))
        self.assertEqual(result.schema_version, CURRENT_SCHEMA_VERSION)
        self.assertGreater(result.size_bytes, 0)
        self.assertEqual(result.retained_backups, 1)
        self.assertEqual(result.pruned_backups, 0)
        self.assertEqual(result.path.stat().st_mode & 0o777, 0o600)
        self.assertEqual(result.path.parent.stat().st_mode & 0o777, 0o700)

        backup = sqlite3.connect(
            f"{result.path.resolve().as_uri()}?mode=ro",
            uri=True,
        )
        try:
            self.assertEqual(
                backup.execute("PRAGMA integrity_check").fetchone()[0],
                "ok",
            )
            self.assertEqual(
                backup.execute(
                    "SELECT value FROM settings WHERE key = ?",
                    ("operational-backup-marker",),
                ).fetchone()[0],
                "preserved",
            )
            self.assertEqual(
                backup.execute("PRAGMA user_version").fetchone()[0],
                CURRENT_SCHEMA_VERSION,
            )
        finally:
            backup.close()

        self.store.set_setting("source-remains-online", "yes")
        self.assertEqual(self.store.get_setting("source-remains-online"), "yes")

    def test_operational_backup_retention_prunes_only_exact_safe_names(
        self,
    ) -> None:
        migration = self.store._create_migration_backup(  # noqa: SLF001
            CURRENT_SCHEMA_VERSION,
            CURRENT_SCHEMA_VERSION + 1,
        )
        backup_directory = migration.parent
        unrelated = backup_directory / "bridge-operational-manual.sqlite3"
        unrelated.write_text("keep", encoding="utf-8")
        symlink = backup_directory / (
            "bridge-operational-v5-20000101T000000000000Z-deadbeef.sqlite3"
        )
        symlink.symlink_to(unrelated)

        results = [
            self.store.create_operational_backup(retention=2)
            for _ in range(4)
        ]

        retained = [
            path
            for path in backup_directory.iterdir()
            if OPERATIONAL_BACKUP_NAME.fullmatch(path.name)
            and not path.is_symlink()
        ]
        self.assertEqual(len(retained), 2)
        self.assertTrue(
            all(path.stat().st_mode & 0o777 == 0o600 for path in retained)
        )
        self.assertEqual(results[-1].retained_backups, 2)
        self.assertEqual(results[-1].pruned_backups, 1)
        self.assertTrue(all(not result.path.exists() for result in results[:2]))
        self.assertTrue(all(result.path.exists() for result in results[2:]))
        self.assertTrue(migration.exists())
        self.assertTrue(unrelated.exists())
        self.assertTrue(symlink.is_symlink())

    def test_operational_backup_serializes_concurrent_creators(self) -> None:
        barrier = Barrier(32)

        def create(_: int):
            source = BridgeStore(self.store.path, read_only=True)
            try:
                barrier.wait(timeout=10)
                return source.create_operational_backup(retention=1)
            finally:
                source.close()

        with ThreadPoolExecutor(max_workers=32) as executor:
            results = list(executor.map(create, range(32)))

        retained = [
            path
            for path in (self.store.path.parent / "backups").iterdir()
            if OPERATIONAL_BACKUP_NAME.fullmatch(path.name)
            and not path.is_symlink()
        ]
        self.assertEqual(len(results), 32)
        self.assertEqual(len(retained), 1)
        self.assertEqual(retained[0].stat().st_mode & 0o777, 0o600)

    def test_operational_backup_cleans_only_owned_stale_temps(self) -> None:
        backup_directory = self.store.path.parent / "backups"
        self.store._ensure_private_backup_directory(  # noqa: SLF001
            backup_directory
        )
        stale_names = (
            ".bridge-operational-deadbeef.tmp",
            ".bridge-operational-deadbeef.tmp-journal",
            ".bridge-operational-deadbeef.tmp-wal",
            ".bridge-operational-deadbeef.tmp-shm",
        )
        for name in stale_names:
            path = backup_directory / name
            path.write_bytes(b"interrupted")
            path.chmod(0o600)
        unrelated = backup_directory / ".bridge-operational-deadbeef.tmp-extra"
        unrelated.write_bytes(b"keep")
        symlink_target = backup_directory / "unrelated"
        symlink_target.write_bytes(b"keep")
        symlink = backup_directory / ".bridge-operational-feedface.tmp"
        symlink.symlink_to(symlink_target)

        self.store.create_operational_backup()

        self.assertTrue(
            all(not (backup_directory / name).exists() for name in stale_names)
        )
        self.assertTrue(unrelated.exists())
        self.assertTrue(symlink.is_symlink())
        self.assertTrue(symlink_target.exists())

    def test_operational_backup_validation_failure_cleans_private_temp(
        self,
    ) -> None:
        self.store.set_setting("source-marker", "unchanged")

        with mock.patch.object(
            BridgeStore,
            "_validate_backup_file",
            side_effect=sqlite3.DatabaseError("injected validation failure"),
        ):
            with self.assertRaisesRegex(
                sqlite3.DatabaseError,
                "injected validation failure",
            ):
                self.store.create_operational_backup()

        backup_directory = self.store.path.parent / "backups"
        self.assertTrue(backup_directory.is_dir())
        self.assertEqual(
            [path.name for path in backup_directory.iterdir()],
            [OPERATIONAL_BACKUP_LOCK_NAME],
        )
        self.assertEqual(
            (backup_directory / OPERATIONAL_BACKUP_LOCK_NAME).stat().st_mode
            & 0o777,
            0o600,
        )
        self.assertEqual(self.store.get_setting("source-marker"), "unchanged")
        self.assertEqual(self.store.integrity_check(), "ok")

    def test_operational_backup_retention_rejects_unbounded_values(
        self,
    ) -> None:
        for invalid in (True, 0, -1, OPERATIONAL_BACKUP_MAX_RETENTION + 1):
            with self.subTest(retention=invalid):
                with self.assertRaises(ValueError):
                    self.store.create_operational_backup(retention=invalid)

    def test_queue_lifecycle(self) -> None:
        queued = self.store.enqueue(
            thread_id="thread-1",
            chat_id=-1001,
            topic_id=10,
            telegram_message_id=99,
            text="next",
            client_id="tg:-1001:99",
        )
        self.assertEqual(self.store.next_queued("thread-1"), queued)
        self.store.mark_queue(queued.queue_id, "sent")
        self.assertIsNone(self.store.next_queued("thread-1"))

    def test_pending_queue_threads_are_global_fifo_and_block_uncertain_thread(
        self,
    ) -> None:
        first = self.store.enqueue(
            thread_id="thread-first",
            chat_id=-1001,
            topic_id=10,
            telegram_message_id=99,
            text="first",
            client_id="tg:-1001:99",
        )
        self.store.enqueue(
            thread_id="thread-second",
            chat_id=-1001,
            topic_id=11,
            telegram_message_id=100,
            text="second",
            client_id="tg:-1001:100",
        )
        self.store.enqueue(
            thread_id="thread-first",
            chat_id=-1001,
            topic_id=10,
            telegram_message_id=101,
            text="third",
            client_id="tg:-1001:101",
        )

        self.assertEqual(
            self.store.pending_queue_thread_ids(),
            ["thread-first", "thread-second"],
        )
        self.assertTrue(self.store.claim_queue(first.queue_id))
        self.assertEqual(
            self.store.pending_queue_thread_ids(),
            ["thread-second"],
        )
        self.assertEqual(
            self.store.dispatching_queue_thread_ids(),
            {"thread-first"},
        )
        self.store.mark_queue(first.queue_id, "sent")
        self.assertEqual(self.store.dispatching_queue_thread_ids(), set())

    def test_queue_preserves_native_local_inputs(self) -> None:
        media_directory = Path(self.temp_dir.name) / "media" / ("a" * 32)
        media_directory.mkdir(parents=True, mode=0o700)
        audio = media_directory / "audio.mp3"
        frame = media_directory / "frame-01.jpg"
        audio.write_bytes(b"audio")
        frame.write_bytes(b"frame")
        audio.chmod(0o600)
        frame.chmod(0o600)
        inputs = (
            LocalInput("localAudio", str(audio.resolve())),
            LocalInput("localImage", str(frame.resolve()), detail="low"),
        )

        queued = self.store.enqueue(
            thread_id="thread-media",
            chat_id=-1001,
            topic_id=10,
            telegram_message_id=100,
            text="video note",
            client_id="tg:-1001:100",
            local_inputs=inputs,
        )

        self.assertEqual(queued.local_inputs, inputs)
        self.assertEqual(
            self.store.active_local_input_paths(),
            {audio.resolve(), frame.resolve()},
        )
        self.store.mark_queue(queued.queue_id, "sent")
        self.assertEqual(self.store.active_local_input_paths(), set())

    def test_queue_rejects_local_input_outside_private_media_root(self) -> None:
        outside = Path(self.temp_dir.name) / "outside.mp3"
        outside.write_bytes(b"audio")

        with self.assertRaisesRegex(ValueError, "bridge media file"):
            self.store.enqueue(
                thread_id="thread-media",
                chat_id=-1001,
                topic_id=10,
                telegram_message_id=101,
                text="voice",
                client_id="tg:-1001:101",
                local_inputs=(
                    LocalInput("localAudio", str(outside.resolve())),
                ),
            )

    def test_queue_client_id_rejects_changed_local_inputs(self) -> None:
        media_directory = Path(self.temp_dir.name) / "media" / ("b" * 32)
        media_directory.mkdir(parents=True, mode=0o700)
        first_audio = media_directory / "first.mp3"
        second_audio = media_directory / "second.mp3"
        first_audio.write_bytes(b"first")
        second_audio.write_bytes(b"second")
        first = (LocalInput("localAudio", str(first_audio.resolve())),)
        second = (LocalInput("localAudio", str(second_audio.resolve())),)
        arguments = {
            "thread_id": "thread-media",
            "chat_id": -1001,
            "topic_id": 10,
            "telegram_message_id": 102,
            "text": "voice",
            "client_id": "tg:-1001:102",
        }
        self.store.enqueue(**arguments, local_inputs=first)

        with self.assertRaisesRegex(RuntimeError, "different content"):
            self.store.enqueue(**arguments, local_inputs=second)

    def test_queue_announcement_delivery_is_reserved_before_send_and_durable(
        self,
    ) -> None:
        queued = self.store.enqueue(
            thread_id="thread-queue-card",
            chat_id=-1001,
            topic_id=50,
            telegram_message_id=80,
            text="later",
            client_id="tg:-1001:80",
        )
        reserved = self.store.reserve_queue_announcement_delivery(
            queued.queue_id
        )
        replay = self.store.reserve_queue_announcement_delivery(
            queued.queue_id
        )
        self.assertEqual(reserved, replay)
        self.assertTrue(reserved.newly_reserved)
        self.assertFalse(replay.newly_reserved)
        self.assertEqual(reserved.state, "reserved")
        self.assertIsNone(reserved.telegram_message_id)

        reopened = BridgeStore(self.store.path)
        try:
            unknown = reopened.mark_queue_announcement_outcome_unknown(
                queued.queue_id
            )
            self.assertEqual(unknown.state, "outcome_unknown")
            self.assertEqual(
                reopened.delivery_uncertainty_health()[
                    "queueAnnouncementsOutcomeUnknown"
                ],
                1,
            )
            delivered = reopened.complete_queue_announcement_delivery(
                queued.queue_id,
                101,
            )
            current = reopened.queued_message(queued.queue_id)
        finally:
            reopened.close()

        self.assertEqual(delivered.state, "delivered")
        self.assertEqual(delivered.telegram_message_id, 101)
        self.assertIsNotNone(current)
        self.assertEqual(current.status_message_id, 101)
        self.assertEqual(
            self.store.complete_queue_announcement_delivery(
                queued.queue_id,
                101,
            ),
            delivered,
        )
        with self.assertRaisesRegex(RuntimeError, "different result"):
            self.store.complete_queue_announcement_delivery(
                queued.queue_id,
                102,
            )
        with self.assertRaisesRegex(RuntimeError, "cannot be cleared"):
            self.store.clear_queue_announcement_delivery_after_definite_failure(
                queued.queue_id
            )

    def test_queue_announcement_definite_failure_can_be_cleared_and_retried(
        self,
    ) -> None:
        queued = self.store.enqueue(
            thread_id="thread-queue-retry",
            chat_id=-1001,
            topic_id=50,
            telegram_message_id=81,
            text="retry",
            client_id="tg:-1001:81",
        )
        self.store.reserve_queue_announcement_delivery(queued.queue_id)
        self.store.mark_queue_announcement_outcome_unknown(queued.queue_id)

        self.assertTrue(
            self.store.clear_queue_announcement_delivery_after_definite_failure(
                queued.queue_id
            )
        )
        self.assertFalse(
            self.store.clear_queue_announcement_delivery_after_definite_failure(
                queued.queue_id
            )
        )
        retried = self.store.reserve_queue_announcement_delivery(
            queued.queue_id
        )
        self.assertEqual(retried.state, "reserved")
        self.assertTrue(retried.newly_reserved)

    def test_dispatch_history_requires_fresh_bounded_misses_and_grace(
        self,
    ) -> None:
        queued = self.store.enqueue(
            thread_id="thread-dispatch",
            chat_id=-1001,
            topic_id=50,
            telegram_message_id=82,
            text="dispatch once",
            client_id="tg:-1001:82",
        )
        self.assertTrue(self.store.claim_queue(queued.queue_id))
        self.store.connection.execute(
            """
            UPDATE queued_messages
            SET dispatch_started_at = '2026-01-01T00:00:00+00:00'
            WHERE id = ?
            """,
            (queued.queue_id,),
        )
        self.store.connection.commit()

        self.assertFalse(
            self.store.record_queue_dispatch_history_miss(
                queued.queue_id,
                observed_at="2026-01-01T00:00:10+00:00",
            )
        )
        # Reusing the same snapshot timestamp is not a fresh miss.
        self.assertFalse(
            self.store.record_queue_dispatch_history_miss(
                queued.queue_id,
                observed_at="2026-01-01T00:00:10+00:00",
            )
        )
        after_one = self.store.queued_message(queued.queue_id)
        self.assertIsNotNone(after_one)
        self.assertEqual(after_one.dispatch_history_miss_count, 1)

        reopened = BridgeStore(self.store.path)
        try:
            eligible = reopened.record_queue_dispatch_history_miss(
                queued.queue_id,
                observed_at="2026-01-01T00:00:11+00:00",
            )
            persisted = reopened.queued_message(queued.queue_id)
        finally:
            reopened.close()

        self.assertTrue(eligible)
        self.assertIsNotNone(persisted)
        self.assertEqual(persisted.dispatch_history_miss_count, 2)
        self.assertEqual(persisted.status, "dispatching")
        self.store.mark_queue(queued.queue_id, "pending")
        cleared = self.store.queued_message(queued.queue_id)
        self.assertIsNotNone(cleared)
        self.assertEqual(cleared.dispatch_history_miss_count, 0)
        self.assertIsNone(cleared.dispatch_started_at)
        self.assertIsNone(cleared.dispatch_last_miss_at)

    def test_dispatch_miss_threshold_does_not_bypass_grace(self) -> None:
        queued = self.store.enqueue(
            thread_id="thread-dispatch-grace",
            chat_id=-1001,
            topic_id=50,
            telegram_message_id=83,
            text="wait",
            client_id="tg:-1001:83",
        )
        self.assertTrue(self.store.claim_queue(queued.queue_id))
        self.store.connection.execute(
            """
            UPDATE queued_messages
            SET dispatch_started_at = '2026-01-01T00:00:00+00:00'
            WHERE id = ?
            """,
            (queued.queue_id,),
        )
        self.store.connection.commit()

        for observed_at in (
            "2026-01-01T00:00:01+00:00",
            "2026-01-01T00:00:02+00:00",
        ):
            self.assertFalse(
                self.store.record_queue_dispatch_history_miss(
                    queued.queue_id,
                    grace_seconds=5,
                    observed_at=observed_at,
                )
            )
        self.assertTrue(
            self.store.record_queue_dispatch_history_miss(
                queued.queue_id,
                grace_seconds=5,
                observed_at="2026-01-01T00:00:05+00:00",
            )
        )
        current = self.store.queued_message(queued.queue_id)
        self.assertIsNotNone(current)
        self.assertEqual(current.dispatch_history_miss_count, 2)

    def test_read_only_open_does_not_recover_live_dispatching_item(self) -> None:
        queued = self.store.enqueue(
            thread_id="thread-1",
            chat_id=-1001,
            topic_id=10,
            telegram_message_id=99,
            text="recover me",
            client_id="tg:recovery:99",
        )
        self.store.mark_queue(queued.queue_id, "dispatching")
        database_path = self.store.path
        self.store.close()

        self.store = BridgeStore(database_path)

        self.assertIsNone(self.store.next_queued("thread-1"))
        current = self.store.queued_message(queued.queue_id)
        self.assertIsNotNone(current)
        self.assertEqual(current.status, "dispatching")

    def test_service_owner_preserves_ambiguous_dispatching_items(self) -> None:
        queued = self.store.enqueue(
            thread_id="thread-1",
            chat_id=-1001,
            topic_id=10,
            telegram_message_id=99,
            text="recover me",
            client_id="tg:recovery:owner",
        )
        self.store.mark_queue(queued.queue_id, "dispatching")
        self.store.acquire_service_lock()

        recovered_count = self.store.recover_dispatching_queue()

        self.assertEqual(recovered_count, 0)
        self.assertIsNone(self.store.next_queued("thread-1"))
        preserved = self.store.queued_message(queued.queue_id)
        self.assertIsNotNone(preserved)
        self.assertEqual(preserved.status, "dispatching")

    def test_second_service_owner_is_rejected(self) -> None:
        second_store = BridgeStore(self.store.path)
        try:
            self.store.acquire_service_lock()
            with self.assertRaisesRegex(RuntimeError, "already running"):
                second_store.acquire_service_lock()
        finally:
            second_store.close()

    def test_enqueue_client_id_is_idempotent_across_connections(self) -> None:
        first = self.store.enqueue(
            thread_id="thread-1",
            chat_id=-1001,
            topic_id=10,
            telegram_message_id=99,
            text="once",
            client_id="tg:idempotent:99",
        )
        second_store = BridgeStore(self.store.path)
        try:
            second = second_store.enqueue(
                thread_id="thread-1",
                chat_id=-1001,
                topic_id=10,
                telegram_message_id=99,
                text="once",
                client_id="tg:idempotent:99",
            )
        finally:
            second_store.close()

        self.assertEqual(first.queue_id, second.queue_id)
        count = self.store.connection.execute(
            "SELECT COUNT(*) FROM queued_messages WHERE client_id = ?",
            ("tg:idempotent:99",),
        ).fetchone()[0]
        self.assertEqual(count, 1)

    def test_turn_context_preserves_source_and_status(self) -> None:
        self.store.upsert_turn_context(
            thread_id="thread-1",
            turn_id="turn-1",
            source_message_id=80,
        )
        context = self.store.upsert_turn_context(
            thread_id="thread-1",
            turn_id="turn-1",
            status_message_id=81,
        )

        self.assertEqual(context.source_message_id, 80)
        self.assertEqual(context.status_message_id, 81)
        self.assertIsNone(context.final_message_id)
        self.assertIsNone(context.progress_render_mode)
        self.assertFalse(context.progress_closed)

    def test_progress_entries_and_card_state_are_durable_and_idempotent(
        self,
    ) -> None:
        inserted = self.store.append_progress_entry(
            thread_id="thread-1",
            turn_id="turn-progress",
            item_id="visible-1",
            entry_kind="commentary",
            sanitized_text="safe visible progress",
        )
        replayed = self.store.append_progress_entry(
            thread_id="thread-1",
            turn_id="turn-progress",
            item_id="visible-1",
            entry_kind="commentary",
            sanitized_text="different replay must not replace the first value",
        )
        self.store.update_turn_progress_state(
            thread_id="thread-1",
            turn_id="turn-progress",
            status_message_id=91,
            render_mode="rich_details",
            closed=True,
            outcome="completed",
        )

        reopened = BridgeStore(self.store.path)
        try:
            entries = reopened.progress_entries("thread-1", "turn-progress")
            context = reopened.turn_context("thread-1", "turn-progress")
        finally:
            reopened.close()

        self.assertTrue(inserted)
        self.assertFalse(replayed)
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0].text, "safe visible progress")
        self.assertIsNotNone(context)
        self.assertEqual(context.status_message_id, 91)
        self.assertEqual(context.progress_render_mode, "rich_details")
        self.assertTrue(context.progress_closed)
        self.assertEqual(context.progress_outcome, "completed")

    def test_progress_entry_reconciles_notification_and_history_ids(
        self,
    ) -> None:
        notification_inserted = self.store.append_progress_entry(
            thread_id="thread-1",
            turn_id="turn-progress-alias",
            item_id="transient-notification-id",
            entry_kind="commentary",
            sanitized_text="same visible progress",
            item_origin="notification",
        )
        history_changed_content = self.store.append_progress_entry(
            thread_id="thread-1",
            turn_id="turn-progress-alias",
            item_id="durable-history-id",
            entry_kind="commentary",
            sanitized_text="same visible progress",
            item_origin="history",
        )

        rows = self.store.connection.execute(
            """
            SELECT item_id, item_origin, counterpart_item_id, text
            FROM turn_progress_entries
            WHERE thread_id = ? AND turn_id = ?
            """,
            ("thread-1", "turn-progress-alias"),
        ).fetchall()

        self.assertTrue(notification_inserted)
        self.assertFalse(history_changed_content)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["item_id"], "transient-notification-id")
        self.assertEqual(rows[0]["item_origin"], "notification")
        self.assertEqual(
            rows[0]["counterpart_item_id"],
            "durable-history-id",
        )

    def test_identical_history_progress_entries_remain_distinct(self) -> None:
        for item_id in ("history-1", "history-2"):
            self.assertTrue(
                self.store.append_progress_entry(
                    thread_id="thread-1",
                    turn_id="turn-repeated-history",
                    item_id=item_id,
                    entry_kind="commentary",
                    sanitized_text="intentionally repeated",
                    item_origin="history",
                )
            )

        self.assertEqual(
            len(
                self.store.progress_entries(
                    "thread-1",
                    "turn-repeated-history",
                )
            ),
            2,
        )

    def test_progress_pairing_is_one_to_one_when_history_arrives_first(
        self,
    ) -> None:
        for item_id in ("history-1", "history-2"):
            self.assertTrue(
                self.store.append_progress_entry(
                    thread_id="thread-1",
                    turn_id="turn-history-first",
                    item_id=item_id,
                    entry_kind="commentary",
                    sanitized_text="intentionally repeated",
                    item_origin="history",
                )
            )
        for item_id in ("notification-1", "notification-2"):
            self.assertFalse(
                self.store.append_progress_entry(
                    thread_id="thread-1",
                    turn_id="turn-history-first",
                    item_id=item_id,
                    entry_kind="commentary",
                    sanitized_text="intentionally repeated",
                    item_origin="notification",
                )
            )

        rows = self.store.connection.execute(
            """
            SELECT counterpart_item_id
            FROM turn_progress_entries
            WHERE thread_id = ? AND turn_id = ?
            ORDER BY id
            """,
            ("thread-1", "turn-history-first"),
        ).fetchall()
        self.assertEqual(len(rows), 2)
        self.assertEqual(
            [row["counterpart_item_id"] for row in rows],
            ["notification-1", "notification-2"],
        )

    def test_reconcile_existing_progress_alias_rows(self) -> None:
        for item_id, item_origin in (
            ("provisional-1", "notification"),
            ("canonical-1", "history"),
            ("provisional-2", "notification"),
            ("canonical-2", "history"),
        ):
            self.store.connection.execute(
                """
                INSERT INTO turn_progress_entries(
                    thread_id, turn_id, item_id, item_origin,
                    entry_kind, text, created_at
                )
                VALUES (?, ?, ?, ?, 'commentary', ?, ?)
                """,
                (
                    "thread-1",
                    "turn-reconcile",
                    item_id,
                    item_origin,
                    "same visible text",
                    "2026-07-27T00:00:00+00:00",
                ),
            )
        self.store.connection.commit()

        changed = self.store.reconcile_progress_entries_with_history(
            thread_id="thread-1",
            turn_id="turn-reconcile",
            canonical_entries=[
                ("canonical-1", "commentary", "same visible text"),
                ("canonical-2", "commentary", "same visible text"),
            ],
        )
        rows = self.store.connection.execute(
            """
            SELECT item_id, counterpart_item_id
            FROM turn_progress_entries
            WHERE thread_id = ? AND turn_id = ?
            ORDER BY id
            """,
            ("thread-1", "turn-reconcile"),
        ).fetchall()

        self.assertTrue(changed)
        self.assertEqual(len(rows), 2)
        self.assertEqual(
            {row["item_id"] for row in rows},
            {"canonical-1", "canonical-2"},
        )
        self.assertEqual(
            {row["counterpart_item_id"] for row in rows},
            {"provisional-1", "provisional-2"},
        )

    def test_visible_item_delivery_pairs_notification_and_history(self) -> None:
        self.store.record_visible_item_delivery(
            thread_id="thread-1",
            turn_id="turn-final",
            item_id="notification-final",
            item_type="agentMessage",
            item_origin="notification",
            content_fingerprint="same-final",
            telegram_message_id=501,
        )

        matched = self.store.match_visible_item_delivery(
            thread_id="thread-1",
            turn_id="turn-final",
            item_id="history-final",
            item_type="agentMessage",
            item_origin="history",
            content_fingerprint="same-final",
        )
        replayed = self.store.match_visible_item_delivery(
            thread_id="thread-1",
            turn_id="turn-final",
            item_id="history-final",
            item_type="agentMessage",
            item_origin="history",
            content_fingerprint="same-final",
        )

        self.assertEqual(matched, 501)
        self.assertEqual(replayed, 501)
        row = self.store.connection.execute(
            """
            SELECT primary_item_id, counterpart_item_id
            FROM visible_item_deliveries
            WHERE thread_id = ? AND turn_id = ?
            """,
            ("thread-1", "turn-final"),
        ).fetchone()
        self.assertEqual(row["primary_item_id"], "notification-final")
        self.assertEqual(row["counterpart_item_id"], "history-final")

    def test_visible_item_delivery_pairing_is_one_to_one(self) -> None:
        for index in (1, 2):
            self.store.record_visible_item_delivery(
                thread_id="thread-1",
                turn_id="turn-repeated-final",
                item_id=f"history-{index}",
                item_type="agentMessage",
                item_origin="history",
                content_fingerprint="intentionally-identical",
                telegram_message_id=600 + index,
            )

        matches = [
            self.store.match_visible_item_delivery(
                thread_id="thread-1",
                turn_id="turn-repeated-final",
                item_id=f"notification-{index}",
                item_type="agentMessage",
                item_origin="notification",
                content_fingerprint="intentionally-identical",
            )
            for index in (1, 2)
        ]

        self.assertEqual(matches, [601, 602])
        paired = self.store.connection.execute(
            """
            SELECT counterpart_item_id
            FROM visible_item_deliveries
            WHERE thread_id = ? AND turn_id = ?
            ORDER BY id
            """,
            ("thread-1", "turn-repeated-final"),
        ).fetchall()
        self.assertEqual(
            [row["counterpart_item_id"] for row in paired],
            ["notification-1", "notification-2"],
        )

    def test_new_thread_request_is_replay_safe(self) -> None:
        first = self.store.reserve_new_thread_request(
            chat_id=-1001,
            message_id=100,
            prompt_hash="same-hash",
        )
        self.store.update_new_thread_request(
            chat_id=-1001,
            message_id=100,
            status="thread_created",
            thread_id="thread-new",
        )
        replay = self.store.reserve_new_thread_request(
            chat_id=-1001,
            message_id=100,
            prompt_hash="same-hash",
        )

        self.assertEqual(first.status, "pending")
        self.assertEqual(replay.thread_id, "thread-new")
        self.assertEqual(replay.status, "thread_created")
        with self.assertRaisesRegex(RuntimeError, "different content"):
            self.store.reserve_new_thread_request(
                chat_id=-1001,
                message_id=100,
                prompt_hash="different-hash",
            )

    def test_control_prompt_claim_is_exact_durable_and_single_use(self) -> None:
        self.store.save_control_prompt(
            public_id="prompt-token",
            thread_id="thread-1",
            chat_id=-1001,
            topic_id=50,
            telegram_message_id=80,
            mode="steer",
        )

        reopened = BridgeStore(self.store.path)
        try:
            self.assertIsNone(
                reopened.claim_control_prompt(
                    chat_id=-1001,
                    topic_id=51,
                    telegram_message_id=80,
                )
            )
            claimed = reopened.claim_control_prompt(
                chat_id=-1001,
                topic_id=50,
                telegram_message_id=80,
            )
            replay = reopened.claim_control_prompt(
                chat_id=-1001,
                topic_id=50,
                telegram_message_id=80,
            )
        finally:
            reopened.close()

        self.assertIsNotNone(claimed)
        self.assertEqual(claimed.mode, "steer")
        self.assertEqual(claimed.status, "consumed")
        self.assertIsNone(replay)

    def test_only_latest_status_card_authorizes_controls(self) -> None:
        self.store.save_control_status_card(
            thread_id="thread-1",
            chat_id=-1001,
            topic_id=50,
            telegram_message_id=80,
        )
        self.assertTrue(
            self.store.control_status_card_matches(
                thread_id="thread-1",
                chat_id=-1001,
                topic_id=50,
                telegram_message_id=80,
            )
        )

        self.store.save_control_status_card(
            thread_id="thread-1",
            chat_id=-1001,
            topic_id=50,
            telegram_message_id=81,
        )

        self.assertFalse(
            self.store.control_status_card_matches(
                thread_id="thread-1",
                chat_id=-1001,
                topic_id=50,
                telegram_message_id=80,
            )
        )
        self.assertTrue(
            self.store.control_status_card_matches(
                thread_id="thread-1",
                chat_id=-1001,
                topic_id=50,
                telegram_message_id=81,
            )
        )
        self.assertTrue(
            self.store.claim_control_status_action(
                thread_id="thread-1",
                chat_id=-1001,
                topic_id=50,
                telegram_message_id=81,
                action="stop",
            )
        )
        self.assertFalse(
            self.store.claim_control_status_action(
                thread_id="thread-1",
                chat_id=-1001,
                topic_id=50,
                telegram_message_id=81,
                action="stop",
            )
        )

    def test_status_card_delivery_is_durable_and_activates_exact_card(
        self,
    ) -> None:
        reserved = self.store.reserve_status_card_delivery(
            thread_id="thread-status",
            chat_id=-1001,
            topic_id=50,
            request_message_id=80,
        )
        self.assertEqual(reserved.state, "reserved")
        replay = self.store.reserve_status_card_delivery(
            thread_id="thread-status",
            chat_id=-1001,
            topic_id=50,
            request_message_id=80,
        )
        self.assertEqual(replay, reserved)
        self.assertTrue(reserved.newly_reserved)
        self.assertFalse(replay.newly_reserved)
        with self.assertRaisesRegex(RuntimeError, "different parameters"):
            self.store.reserve_status_card_delivery(
                thread_id="different-thread",
                chat_id=-1001,
                topic_id=51,
                request_message_id=80,
            )

        unknown = self.store.mark_status_card_outcome_unknown(
            chat_id=-1001,
            request_message_id=80,
        )
        self.assertEqual(unknown.state, "outcome_unknown")
        reopened = BridgeStore(self.store.path)
        try:
            delivered = reopened.complete_status_card_delivery(
                chat_id=-1001,
                request_message_id=80,
                telegram_message_id=100,
            )
            authorized = reopened.control_status_card_matches(
                thread_id="thread-status",
                chat_id=-1001,
                topic_id=50,
                telegram_message_id=100,
            )
        finally:
            reopened.close()

        self.assertEqual(delivered.state, "delivered")
        self.assertTrue(authorized)
        self.assertFalse(
            self.store.control_status_card_matches(
                thread_id="thread-status",
                chat_id=-1001,
                topic_id=50,
                telegram_message_id=80,
            )
        )
        self.store.reserve_status_card_delivery(
            thread_id="thread-status",
            chat_id=-1001,
            topic_id=50,
            request_message_id=81,
        )
        self.store.complete_status_card_delivery(
            chat_id=-1001,
            request_message_id=81,
            telegram_message_id=101,
        )
        # Replaying completion of an older delivery must not reactivate the
        # stale card after a newer exact card has become authoritative.
        self.store.complete_status_card_delivery(
            chat_id=-1001,
            request_message_id=80,
            telegram_message_id=100,
        )
        self.assertTrue(
            self.store.control_status_card_matches(
                thread_id="thread-status",
                chat_id=-1001,
                topic_id=50,
                telegram_message_id=101,
            )
        )
        self.assertFalse(
            self.store.control_status_card_matches(
                thread_id="thread-status",
                chat_id=-1001,
                topic_id=50,
                telegram_message_id=100,
            )
        )

    def test_status_card_definite_failure_and_general_status_are_safe(
        self,
    ) -> None:
        self.store.reserve_status_card_delivery(
            thread_id="thread-retry",
            chat_id=-1001,
            topic_id=50,
            request_message_id=81,
        )
        self.assertTrue(
            self.store.clear_status_card_delivery_after_definite_failure(
                chat_id=-1001,
                request_message_id=81,
            )
        )
        retried = self.store.reserve_status_card_delivery(
            thread_id="thread-retry",
            chat_id=-1001,
            topic_id=50,
            request_message_id=81,
        )
        self.assertEqual(retried.state, "reserved")

        general = self.store.reserve_status_card_delivery(
            thread_id=None,
            chat_id=-1001,
            topic_id=1,
            request_message_id=82,
        )
        self.assertIsNone(general.thread_id)
        self.store.complete_status_card_delivery(
            chat_id=-1001,
            request_message_id=82,
            telegram_message_id=102,
        )
        self.assertFalse(
            self.store.control_status_card_matches(
                thread_id="thread-retry",
                chat_id=-1001,
                topic_id=1,
                telegram_message_id=102,
            )
        )

    def test_status_action_claim_can_release_only_before_completion(self) -> None:
        self.store.save_control_status_card(
            thread_id="thread-action",
            chat_id=-1001,
            topic_id=50,
            telegram_message_id=100,
        )
        exact = {
            "thread_id": "thread-action",
            "chat_id": -1001,
            "topic_id": 50,
            "telegram_message_id": 100,
            "action": "steer",
        }
        self.assertTrue(self.store.claim_control_status_action(**exact))
        self.assertEqual(
            self.store.delivery_uncertainty_health()[
                "controlActionsClaimed"
            ],
            1,
        )
        self.assertFalse(
            self.store.release_control_status_action(
                **{**exact, "topic_id": 51}
            )
        )
        self.assertTrue(self.store.release_control_status_action(**exact))
        self.assertTrue(self.store.claim_control_status_action(**exact))
        self.assertTrue(self.store.complete_control_status_action(**exact))
        self.assertFalse(self.store.release_control_status_action(**exact))
        self.assertFalse(self.store.claim_control_status_action(**exact))
        self.assertEqual(
            self.store.delivery_uncertainty_health()[
                "controlActionsClaimed"
            ],
            0,
        )

    def test_delivery_uncertainty_health_is_sanitized_and_complete(self) -> None:
        self.store.reserve_outbound_delivery(
            kind="queue_announcement",
            source_key="sensitive-queue-key",
            thread_id="thread-health",
            chat_id=-1001,
            topic_id=50,
            reply_to_message_id=80,
        )
        self.store.reserve_outbound_delivery(
            kind="status_card",
            source_key="sensitive-status-key",
            thread_id="thread-health",
            chat_id=-1001,
            topic_id=50,
            reply_to_message_id=81,
        )
        self.store.mark_outbound_delivery_outcome_unknown(
            "status_card",
            "sensitive-status-key",
        )
        self.store.reserve_visible_item_delivery_intent(
            thread_id="thread-health",
            turn_id="turn-health",
            item_id="sensitive-final-id",
            item_type="agentMessage",
            item_origin="history",
            content_fingerprint="sensitive-fingerprint",
        )
        self.store.mark_visible_item_delivery_outcome_unknown(
            "thread-health",
            "sensitive-final-id",
        )
        self.store.reserve_visible_item_delivery_intent(
            thread_id="thread-health",
            turn_id="turn-health",
            item_id="sensitive-user-id",
            item_type="userMessage",
            item_origin="history",
            content_fingerprint="sensitive-user-fingerprint",
        )
        self.store.mark_visible_item_delivery_outcome_unknown(
            "thread-health",
            "sensitive-user-id",
        )
        self.store.reserve_visible_item_delivery_intent(
            thread_id="thread-health",
            turn_id="turn-health",
            item_id="sensitive-attachment-id",
            item_type="finalAttachment",
            item_origin="history",
            content_fingerprint="sensitive-attachment-fingerprint",
        )
        self.store.mark_visible_item_delivery_outcome_unknown(
            "thread-health",
            "sensitive-attachment-id",
        )
        self.store.update_turn_progress_state(
            thread_id="thread-health",
            turn_id="turn-health",
            send_outcome_unknown=True,
        )
        self.store.save_control_status_card(
            thread_id="thread-health",
            chat_id=-1001,
            topic_id=50,
            telegram_message_id=120,
        )
        self.store.claim_control_status_action(
            thread_id="thread-health",
            chat_id=-1001,
            topic_id=50,
            telegram_message_id=120,
            action="queue",
        )

        health = self.store.delivery_uncertainty_health()

        self.assertEqual(
            health,
            {
                "queueAnnouncementsReserved": 1,
                "queueAnnouncementsOutcomeUnknown": 0,
                "statusCardsReserved": 0,
                "statusCardsOutcomeUnknown": 1,
                "visibleFinalsReserved": 0,
                "visibleFinalsOutcomeUnknown": 1,
                "visibleUserMessagesReserved": 0,
                "visibleUserMessagesOutcomeUnknown": 1,
                "finalAttachmentsReserved": 0,
                "finalAttachmentsOutcomeUnknown": 1,
                "userAttachmentsReserved": 0,
                "userAttachmentsOutcomeUnknown": 0,
                "threadAttachmentsReserved": 0,
                "threadAttachmentsOutcomeUnknown": 0,
                "progressCardsOutcomeUnknown": 1,
                "controlActionsClaimed": 1,
                "approvalPromptsReserved": 0,
                "approvalPromptsOutcomeUnknown": 0,
                "totalUncertain": 7,
            },
        )
        rendered = repr(health)
        self.assertNotIn("sensitive", rendered)

    def test_progress_ambiguous_initial_send_state_is_durable(self) -> None:
        self.store.update_turn_progress_state(
            thread_id="thread-1",
            turn_id="turn-unknown",
            render_mode="rich_details",
            send_outcome_unknown=True,
        )

        reopened = BridgeStore(self.store.path)
        try:
            context = reopened.turn_context("thread-1", "turn-unknown")
        finally:
            reopened.close()

        self.assertIsNotNone(context)
        self.assertTrue(context.progress_send_outcome_unknown)

    def test_processed_telegram_update_round_trip(self) -> None:
        self.assertFalse(self.store.telegram_update_processed(123))

        self.store.mark_telegram_update_processed(123)
        self.store.mark_telegram_update_processed(123)

        self.assertTrue(self.store.telegram_update_processed(123))
        count = self.store.connection.execute(
            """
            SELECT COUNT(*) FROM processed_telegram_updates
            WHERE update_id = 123
            """
        ).fetchone()[0]
        self.assertEqual(count, 1)

    def test_legacy_duplicate_client_ids_are_deduplicated_before_index(
        self,
    ) -> None:
        legacy_path = Path(self.temp_dir.name) / "legacy.sqlite3"
        connection = sqlite3.connect(legacy_path)
        connection.executescript(
            """
            CREATE TABLE queued_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                thread_id TEXT NOT NULL,
                chat_id INTEGER NOT NULL,
                topic_id INTEGER NOT NULL,
                telegram_message_id INTEGER NOT NULL,
                text TEXT NOT NULL,
                client_id TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            INSERT INTO queued_messages(
                thread_id, chat_id, topic_id, telegram_message_id,
                text, client_id, status, created_at, updated_at
            ) VALUES
                ('thread-1', -1, 10, 20, 'one', 'duplicate', 'pending', 'x', 'x'),
                ('thread-1', -1, 10, 20, 'one', 'duplicate', 'sent', 'x', 'x');
            """
        )
        connection.close()

        legacy = BridgeStore(legacy_path)
        try:
            rows = legacy.connection.execute(
                """
                SELECT status FROM queued_messages
                WHERE client_id = 'duplicate'
                """
            ).fetchall()
            self.assertEqual([row["status"] for row in rows], ["sent"])
        finally:
            legacy.close()

    def test_mirrored_items_are_idempotent(self) -> None:
        self.store.mark_mirrored_item("thread-1", "item-1", "agentMessage", 5)
        self.store.mark_mirrored_item("thread-1", "item-1", "agentMessage", 6)
        self.assertTrue(self.store.has_mirrored_item("thread-1", "item-1"))
        count = self.store.connection.execute(
            "SELECT COUNT(*) FROM mirrored_items"
        ).fetchone()[0]
        self.assertEqual(count, 1)

    def test_null_mirrored_item_can_be_reopened_for_safe_backfill(self) -> None:
        self.store.mark_mirrored_item(
            "thread-1",
            "skipped-item",
            "userMessage",
            None,
        )

        self.assertEqual(
            self.store.mirrored_item_state("thread-1", "skipped-item"),
            (True, None),
        )
        self.assertTrue(
            self.store.unmark_null_mirrored_item_for_backfill(
                "thread-1",
                "skipped-item",
            )
        )
        self.assertEqual(
            self.store.mirrored_item_state("thread-1", "skipped-item"),
            (False, None),
        )

    def test_ambiguous_delivery_intent_blocks_null_item_reopen(self) -> None:
        self.store.mark_mirrored_item(
            "thread-1",
            "ambiguous-item",
            "agentMessage",
            None,
        )
        self.store.reserve_visible_item_delivery_intent(
            thread_id="thread-1",
            turn_id="turn-1",
            item_id="ambiguous-item",
            item_type="agentMessage",
            item_origin="history",
            content_fingerprint="fingerprint",
        )

        self.assertFalse(
            self.store.unmark_null_mirrored_item_for_backfill(
                "thread-1",
                "ambiguous-item",
            )
        )
        self.assertEqual(
            self.store.mirrored_item_state("thread-1", "ambiguous-item"),
            (True, None),
        )

    def test_mirror_claim_is_exclusive_across_store_connections(self) -> None:
        database_path = self.store.path
        start_together = Barrier(2)

        def attempt_claim() -> bool:
            contender = BridgeStore(database_path)
            try:
                start_together.wait(timeout=5)
                return contender.claim_mirrored_item("thread-1", "race-item")
            finally:
                contender.close()

        with ThreadPoolExecutor(max_workers=2) as executor:
            claims = list(executor.map(lambda _: attempt_claim(), range(2)))

        self.assertEqual(sorted(claims), [False, True])
        claim_count = self.store.connection.execute(
            """
            SELECT COUNT(*) FROM mirror_claims
            WHERE thread_id = ? AND item_id = ?
            """,
            ("thread-1", "race-item"),
        ).fetchone()[0]
        self.assertEqual(claim_count, 1)

        self.store.release_mirror_claim("thread-1", "race-item")
        second_connection = BridgeStore(database_path)
        try:
            self.assertTrue(
                second_connection.claim_mirrored_item(
                    "thread-1",
                    "race-item",
                )
            )
        finally:
            second_connection.release_mirror_claim("thread-1", "race-item")
            second_connection.close()


if __name__ == "__main__":
    unittest.main()
