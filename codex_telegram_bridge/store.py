from __future__ import annotations

import fcntl
import json
import os
import re
import sqlite3
import stat
import tempfile
import threading
import uuid
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .input_types import LocalInput, normalize_local_inputs


CURRENT_SCHEMA_VERSION = 6
MIGRATION_BACKUP_RETENTION = 5
MIGRATION_BACKUP_PREFIX = "bridge-schema-"
OPERATIONAL_BACKUP_RETENTION = 7
OPERATIONAL_BACKUP_MAX_RETENTION = 100
OPERATIONAL_BACKUP_PREFIX = "bridge-operational-"
OPERATIONAL_BACKUP_LOCK_NAME = ".bridge-operational-backup.lock"
OPERATIONAL_BACKUP_NAME = re.compile(
    rf"^{re.escape(OPERATIONAL_BACKUP_PREFIX)}"
    r"v[0-9]+-[0-9]{8}T[0-9]{12}Z-[0-9a-f]{8}\.sqlite3$"
)
OPERATIONAL_BACKUP_TEMP_NAME = re.compile(
    rf"^\.{re.escape(OPERATIONAL_BACKUP_PREFIX)}"
    r"[a-z0-9_]{8}\.tmp(?:-(?:journal|wal|shm))?$"
)
_OPERATIONAL_BACKUP_PROCESS_LOCK = threading.Lock()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class SchemaVersionError(RuntimeError):
    """The database schema cannot be safely used by this bridge version."""


def _execute_sql_script(
    connection: sqlite3.Connection,
    script: str,
) -> None:
    """Execute a SQL script without sqlite3.executescript's implicit COMMIT."""

    statement = ""
    for line in script.splitlines(keepends=True):
        statement += line
        if sqlite3.complete_statement(statement):
            sql = statement.strip()
            if sql:
                connection.execute(sql)
            statement = ""
    if statement.strip():
        raise sqlite3.OperationalError("incomplete migration SQL statement")


@dataclass(frozen=True)
class Binding:
    chat_id: int
    allowed_user_id: int
    bot_id: int
    bot_username: str
    chat_title: str


@dataclass(frozen=True)
class TopicBinding:
    thread_id: str
    chat_id: int
    topic_id: int
    title: str
    archived: bool
    last_updated_at: int


@dataclass(frozen=True)
class TopicCreationIntent:
    thread_id: str
    chat_id: int
    title: str
    topic_id: int | None
    status: str
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class ArchivedThread:
    thread_id: str
    restore_token: str
    title: str
    thread_created_at: int
    archived_at: str
    source_topic_id: int | None
    status: str
    restored_at: str | None
    replacement_topic_id: int | None
    replacement_outcome_unknown: bool
    replacement_title: str | None
    replacement_started_at: str | None
    presentation: str
    summary: str
    archive_message_state: str
    archive_message_id: int | None


@dataclass(frozen=True)
class QueuedMessage:
    queue_id: int
    thread_id: str
    chat_id: int
    topic_id: int
    telegram_message_id: int
    text: str
    client_id: str
    status: str
    status_message_id: int | None
    dispatch_started_at: str | None
    dispatch_history_miss_count: int
    dispatch_last_miss_at: str | None
    local_inputs: tuple[LocalInput, ...]


@dataclass(frozen=True)
class OutboundDelivery:
    kind: str
    source_key: str
    thread_id: str | None
    chat_id: int
    topic_id: int
    reply_to_message_id: int
    state: str
    telegram_message_id: int | None
    created_at: str
    updated_at: str
    newly_reserved: bool = field(default=False, compare=False)


@dataclass(frozen=True)
class TurnContext:
    thread_id: str
    turn_id: str
    source_message_id: int | None
    status_message_id: int | None
    final_message_id: int | None
    progress_render_mode: str | None
    progress_closed: bool
    progress_outcome: str | None
    progress_send_outcome_unknown: bool
    created_at: str


@dataclass(frozen=True)
class ProgressEntry:
    entry_id: int
    thread_id: str
    turn_id: str
    item_id: str
    entry_kind: str
    text: str
    created_at: str


@dataclass(frozen=True)
class NewThreadRequest:
    chat_id: int
    message_id: int
    prompt_hash: str
    thread_id: str | None
    echo_message_id: int | None
    status: str


@dataclass(frozen=True)
class ManualTopicThreadIntent:
    chat_id: int
    topic_id: int
    title: str
    thread_id: str | None
    status: str


@dataclass(frozen=True)
class ControlPrompt:
    public_id: str
    thread_id: str
    chat_id: int
    topic_id: int
    telegram_message_id: int
    mode: str
    status: str


@dataclass(frozen=True)
class OperationalBackup:
    path: Path
    created_at: str
    schema_version: int
    size_bytes: int
    retained_backups: int
    pruned_backups: int


class BridgeStore:
    def __init__(self, path: str | Path, *, read_only: bool = False):
        requested_path = Path(path).expanduser()
        if requested_path.is_symlink():
            raise SchemaVersionError("database path must not be a symlink")
        self.path = requested_path.resolve(strict=False)
        self.read_only = read_only
        self._service_lock_descriptor: int | None = None
        existed_with_data = self.path.exists() and self.path.stat().st_size > 0

        if read_only:
            if not self.path.is_file():
                raise FileNotFoundError(self.path)
            connection_uri = f"{self.path.resolve().as_uri()}?mode=ro"
            self.connection = sqlite3.connect(connection_uri, uri=True)
            self.connection.row_factory = sqlite3.Row
            self.connection.execute("PRAGMA query_only = ON")
            self.connection.execute("PRAGMA busy_timeout = 5000")
            self.connection.execute("PRAGMA foreign_keys = ON")
            try:
                self._require_current_schema()
            except BaseException:
                self.connection.close()
                raise
            return

        parent_existed = self.path.parent.exists()
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        if not parent_existed:
            os.chmod(self.path.parent, 0o700)
        if not self.path.exists():
            descriptor = os.open(
                self.path,
                os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                0o600,
            )
            os.close(descriptor)
        else:
            os.chmod(self.path, 0o600)
        self.connection = sqlite3.connect(self.path)
        os.chmod(self.path, 0o600)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA busy_timeout = 5000")
        self.connection.execute("PRAGMA foreign_keys = ON")
        try:
            self._initialize_schema(existed_with_data=existed_with_data)
            self.connection.execute("PRAGMA journal_mode = WAL")
        except BaseException:
            self.connection.close()
            raise

    def close(self) -> None:
        self.release_service_lock()
        self.connection.close()

    @staticmethod
    def _schema_version(connection: sqlite3.Connection) -> int:
        row = connection.execute("PRAGMA user_version").fetchone()
        return 0 if row is None else int(row[0])

    def _require_current_schema(self) -> None:
        version = self._schema_version(self.connection)
        if version > CURRENT_SCHEMA_VERSION:
            raise SchemaVersionError(
                "database schema "
                f"{version} is newer than supported {CURRENT_SCHEMA_VERSION}"
            )
        if version < CURRENT_SCHEMA_VERSION:
            raise SchemaVersionError(
                "database schema "
                f"{version} requires migration to {CURRENT_SCHEMA_VERSION}; "
                "read-only open cannot migrate"
            )
        self._validate_schema(
            self.connection,
            expected_version=CURRENT_SCHEMA_VERSION,
        )

    def _initialize_schema(self, *, existed_with_data: bool) -> None:
        version = self._schema_version(self.connection)
        if version > CURRENT_SCHEMA_VERSION:
            raise SchemaVersionError(
                "database schema "
                f"{version} is newer than supported {CURRENT_SCHEMA_VERSION}"
            )
        if version == CURRENT_SCHEMA_VERSION:
            self._validate_schema(self.connection, expected_version=version)
            return

        migration_lock = self.path.with_name(f".{self.path.name}.migration.lock")
        open_flags = os.O_CREAT | os.O_RDWR
        if hasattr(os, "O_NOFOLLOW"):
            open_flags |= os.O_NOFOLLOW
        descriptor = os.open(
            migration_lock,
            open_flags,
            0o600,
        )
        try:
            lock_status = os.fstat(descriptor)
            if (
                not stat.S_ISREG(lock_status.st_mode)
                or lock_status.st_uid != os.getuid()
            ):
                raise SchemaVersionError(
                    "database migration lock has an unsafe owner or type"
                )
            os.fchmod(descriptor, 0o600)
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            # Another process may have completed a migration while this
            # process waited. Plan only from the version protected by the
            # lock, never from the optimistic read above.
            version = self._schema_version(self.connection)
            if version > CURRENT_SCHEMA_VERSION:
                raise SchemaVersionError(
                    "database schema "
                    f"{version} is newer than supported {CURRENT_SCHEMA_VERSION}"
                )
            if version == CURRENT_SCHEMA_VERSION:
                self._validate_schema(self.connection, expected_version=version)
                return
            if existed_with_data:
                self._validate_migrations_on_copy(version)
                self._create_migration_backup(version, CURRENT_SCHEMA_VERSION)
            self._run_migrations(self.connection, version, validating=False)
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)

    def _run_migrations(
        self,
        connection: sqlite3.Connection,
        from_version: int,
        *,
        validating: bool,
    ) -> None:
        try:
            connection.execute("BEGIN IMMEDIATE")
            for target_version in range(
                from_version + 1,
                CURRENT_SCHEMA_VERSION + 1,
            ):
                if target_version == 1:
                    self._migrate_schema_v1(connection)
                elif target_version == 2:
                    self._migrate_schema_v2(connection)
                elif target_version == 3:
                    self._migrate_schema_v3(connection)
                elif target_version == 4:
                    self._migrate_schema_v4(connection)
                elif target_version == 5:
                    self._migrate_schema_v5(connection)
                elif target_version == 6:
                    self._migrate_schema_v6(connection)
                else:
                    raise SchemaVersionError(
                        f"no migration is registered for schema {target_version}"
                    )
                self._after_migration_step(
                    connection,
                    target_version,
                    validating=validating,
                )
                connection.execute(f"PRAGMA user_version = {target_version}")
            self._validate_schema(
                connection,
                expected_version=CURRENT_SCHEMA_VERSION,
            )
            connection.commit()
        except BaseException:
            if connection.in_transaction:
                connection.rollback()
            raise

    def _after_migration_step(
        self,
        connection: sqlite3.Connection,
        target_version: int,
        *,
        validating: bool,
    ) -> None:
        """Fault-injection seam; production migrations intentionally do nothing."""

    def _migrate_schema_v1(self, connection: sqlite3.Connection) -> None:
        primary_connection = self.connection
        self.connection = connection
        try:
            self._migrate_schema_v1_current_connection()
        finally:
            self.connection = primary_connection

    def _migrate_schema_v2(self, connection: sqlite3.Connection) -> None:
        # The schema builder is intentionally idempotent. Reusing it here
        # upgrades existing v1 databases with the durable delivery/manual
        # Topic tables and archive-reconciliation columns added in v2.
        self._migrate_schema_v1(connection)

    def _migrate_schema_v3(self, connection: sqlite3.Connection) -> None:
        _execute_sql_script(
            connection,
            """
            CREATE TABLE IF NOT EXISTS outbound_deliveries (
                kind TEXT NOT NULL CHECK (
                    kind IN ('queue_announcement', 'status_card')
                ),
                source_key TEXT NOT NULL,
                thread_id TEXT,
                chat_id INTEGER NOT NULL,
                topic_id INTEGER NOT NULL,
                reply_to_message_id INTEGER NOT NULL,
                state TEXT NOT NULL CHECK (
                    state IN ('reserved', 'outcome_unknown', 'delivered')
                ),
                telegram_message_id INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(kind, source_key),
                CHECK (
                    (
                        state = 'delivered'
                        AND telegram_message_id IS NOT NULL
                    )
                    OR (
                        state IN ('reserved', 'outcome_unknown')
                        AND telegram_message_id IS NULL
                    )
                )
            );

            CREATE INDEX IF NOT EXISTS outbound_deliveries_state
                ON outbound_deliveries(kind, state, created_at);
            """,
        )
        queue_columns = {
            str(row["name"])
            for row in connection.execute(
                "PRAGMA table_info(queued_messages)"
            ).fetchall()
        }
        if "dispatch_started_at" not in queue_columns:
            connection.execute(
                """
                ALTER TABLE queued_messages
                ADD COLUMN dispatch_started_at TEXT
                """
            )
        if "dispatch_history_miss_count" not in queue_columns:
            connection.execute(
                """
                ALTER TABLE queued_messages
                ADD COLUMN dispatch_history_miss_count
                    INTEGER NOT NULL DEFAULT 0
                    CHECK (dispatch_history_miss_count >= 0)
                """
            )
        if "dispatch_last_miss_at" not in queue_columns:
            connection.execute(
                """
                ALTER TABLE queued_messages
                ADD COLUMN dispatch_last_miss_at TEXT
                """
            )
        connection.execute(
            """
            UPDATE queued_messages
            SET dispatch_started_at = updated_at
            WHERE
                status = 'dispatching'
                AND dispatch_started_at IS NULL
            """
        )

        delivery_intent_columns = {
            str(row["name"])
            for row in connection.execute(
                "PRAGMA table_info(visible_item_delivery_intents)"
            ).fetchall()
        }
        if "state" not in delivery_intent_columns:
            connection.execute(
                """
                ALTER TABLE visible_item_delivery_intents
                ADD COLUMN state TEXT NOT NULL DEFAULT 'reserved'
                    CHECK (state IN ('reserved', 'outcome_unknown'))
                """
            )
            # An intent that survived long enough to be migrated was left
            # behind by the v2 reserve-before-send path. Its Telegram outcome
            # is unknowable, so a new release must never retry it as fresh.
            connection.execute(
                """
                UPDATE visible_item_delivery_intents
                SET state = 'outcome_unknown'
                """
            )

        status_action_columns = {
            str(row["name"])
            for row in connection.execute(
                "PRAGMA table_info(control_status_actions)"
            ).fetchall()
        }
        if "state" not in status_action_columns:
            connection.execute(
                """
                ALTER TABLE control_status_actions
                ADD COLUMN state TEXT NOT NULL DEFAULT 'completed'
                    CHECK (state IN ('claimed', 'completed'))
                """
            )
        if "updated_at" not in status_action_columns:
            connection.execute(
                """
                ALTER TABLE control_status_actions
                ADD COLUMN updated_at TEXT NOT NULL DEFAULT ''
                """
            )
            connection.execute(
                """
                UPDATE control_status_actions
                SET updated_at = created_at
                WHERE updated_at = ''
                """
            )

    def _migrate_schema_v4(self, connection: sqlite3.Connection) -> None:
        queue_columns = {
            str(row["name"])
            for row in connection.execute(
                "PRAGMA table_info(queued_messages)"
            ).fetchall()
        }
        if "local_inputs_json" not in queue_columns:
            connection.execute(
                """
                ALTER TABLE queued_messages
                ADD COLUMN local_inputs_json TEXT NOT NULL DEFAULT '[]'
                """
            )

    def _migrate_schema_v5(self, connection: sqlite3.Connection) -> None:
        archive_columns = {
            str(row["name"])
            for row in connection.execute(
                "PRAGMA table_info(archived_threads)"
            ).fetchall()
        }
        # Existing archive rows remain in the legacy editable index. Only a
        # later, newly detected archive event opts a row into the card format.
        if "presentation" not in archive_columns:
            connection.execute(
                """
                ALTER TABLE archived_threads
                ADD COLUMN presentation TEXT NOT NULL DEFAULT 'legacy_index'
                    CHECK (presentation IN ('legacy_index', 'card'))
                """
            )

        if "summary" not in archive_columns:
            connection.execute(
                """
                ALTER TABLE archived_threads
                ADD COLUMN summary TEXT NOT NULL DEFAULT ''
                """
            )
        if "archive_message_state" not in archive_columns:
            connection.execute(
                """
                ALTER TABLE archived_threads
                ADD COLUMN archive_message_state TEXT NOT NULL DEFAULT 'sent'
                    CHECK (
                        archive_message_state IN (
                            'reserved',
                            'outcome_unknown',
                            'sent'
                        )
                    )
                """
            )
        if "archive_message_id" not in archive_columns:
            connection.execute(
                """
                ALTER TABLE archived_threads
                ADD COLUMN archive_message_id INTEGER
                """
            )

    def _migrate_schema_v6(self, connection: sqlite3.Connection) -> None:
        """Persist a bounded, content-free record of skipped bad updates."""

        _execute_sql_script(
            connection,
            """
            CREATE TABLE IF NOT EXISTS telegram_update_quarantine (
                update_id INTEGER PRIMARY KEY,
                update_kind TEXT NOT NULL,
                error_type TEXT NOT NULL,
                quarantined_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS telegram_update_quarantine_recent
                ON telegram_update_quarantine(quarantined_at);
            """,
        )

    def _migrate_schema_v1_current_connection(self) -> None:
        _execute_sql_script(
            self.connection,
            """
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS thread_topics (
                thread_id TEXT PRIMARY KEY,
                chat_id INTEGER NOT NULL,
                topic_id INTEGER NOT NULL,
                title TEXT NOT NULL,
                archived INTEGER NOT NULL DEFAULT 0,
                last_updated_at INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(chat_id, topic_id)
            );

            CREATE TABLE IF NOT EXISTS mirrored_items (
                thread_id TEXT NOT NULL,
                item_id TEXT NOT NULL,
                item_type TEXT NOT NULL,
                telegram_message_id INTEGER,
                created_at TEXT NOT NULL,
                PRIMARY KEY(thread_id, item_id)
            );

            CREATE TABLE IF NOT EXISTS mirror_claims (
                thread_id TEXT NOT NULL,
                item_id TEXT NOT NULL,
                claimed_at TEXT NOT NULL,
                PRIMARY KEY(thread_id, item_id)
            );

            CREATE TABLE IF NOT EXISTS visible_item_deliveries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                thread_id TEXT NOT NULL,
                turn_id TEXT NOT NULL,
                item_type TEXT NOT NULL,
                item_origin TEXT NOT NULL CHECK (
                    item_origin IN ('history', 'notification')
                ),
                content_fingerprint TEXT NOT NULL,
                primary_item_id TEXT NOT NULL,
                counterpart_item_id TEXT,
                telegram_message_id INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(thread_id, primary_item_id),
                UNIQUE(thread_id, counterpart_item_id)
            );

            CREATE INDEX IF NOT EXISTS visible_item_deliveries_match
                ON visible_item_deliveries(
                    thread_id, turn_id, item_type,
                    content_fingerprint, item_origin, id
                );

            CREATE TABLE IF NOT EXISTS visible_item_delivery_intents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                thread_id TEXT NOT NULL,
                turn_id TEXT NOT NULL,
                item_type TEXT NOT NULL,
                item_origin TEXT NOT NULL CHECK (
                    item_origin IN ('history', 'notification')
                ),
                content_fingerprint TEXT NOT NULL,
                primary_item_id TEXT NOT NULL,
                counterpart_item_id TEXT,
                created_at TEXT NOT NULL,
                UNIQUE(thread_id, primary_item_id),
                UNIQUE(thread_id, counterpart_item_id)
            );

            CREATE INDEX IF NOT EXISTS visible_item_delivery_intents_match
                ON visible_item_delivery_intents(
                    thread_id, turn_id, item_type,
                    content_fingerprint, item_origin, id
                );

            CREATE TABLE IF NOT EXISTS queued_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                thread_id TEXT NOT NULL,
                chat_id INTEGER NOT NULL,
                topic_id INTEGER NOT NULL,
                telegram_message_id INTEGER NOT NULL,
                text TEXT NOT NULL,
                client_id TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                status_message_id INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS queued_messages_pending
                ON queued_messages(thread_id, status, id);

            CREATE TABLE IF NOT EXISTS observed_topics (
                chat_id INTEGER NOT NULL,
                topic_id INTEGER NOT NULL,
                title TEXT NOT NULL,
                observed_at TEXT NOT NULL,
                PRIMARY KEY(chat_id, topic_id)
            );

            CREATE TABLE IF NOT EXISTS topic_creation_intents (
                thread_id TEXT PRIMARY KEY,
                chat_id INTEGER NOT NULL,
                title TEXT NOT NULL,
                topic_id INTEGER,
                status TEXT NOT NULL CHECK (
                    status IN (
                        'reserved',
                        'outcome_unknown',
                        'created',
                        'completed'
                    )
                ),
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                CHECK (
                    (
                        status IN ('reserved', 'outcome_unknown')
                        AND topic_id IS NULL
                    )
                    OR (
                        status IN ('created', 'completed')
                        AND topic_id IS NOT NULL
                    )
                )
            );

            CREATE INDEX IF NOT EXISTS topic_creation_intents_unresolved
                ON topic_creation_intents(status, created_at);

            CREATE TABLE IF NOT EXISTS archived_threads (
                thread_id TEXT PRIMARY KEY,
                restore_token TEXT NOT NULL UNIQUE,
                title TEXT NOT NULL,
                thread_created_at INTEGER NOT NULL DEFAULT 0,
                archived_at TEXT NOT NULL,
                source_topic_id INTEGER,
                status TEXT NOT NULL CHECK (
                    status IN (
                        'detected',
                        'archived',
                        'restoring',
                        'restored'
                    )
                ),
                restored_at TEXT,
                replacement_topic_id INTEGER,
                replacement_outcome_unknown INTEGER NOT NULL DEFAULT 0,
                replacement_title TEXT,
                replacement_started_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS archived_threads_current
                ON archived_threads(status, archived_at);

            CREATE TABLE IF NOT EXISTS pending_requests (
                public_id TEXT PRIMARY KEY,
                thread_id TEXT NOT NULL,
                request_kind TEXT NOT NULL,
                telegram_message_id INTEGER,
                status TEXT NOT NULL,
                metadata_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS control_confirmations (
                public_id TEXT PRIMARY KEY,
                thread_id TEXT NOT NULL,
                chat_id INTEGER NOT NULL,
                topic_id INTEGER NOT NULL,
                telegram_message_id INTEGER NOT NULL,
                action TEXT NOT NULL CHECK (action = 'stop'),
                status TEXT NOT NULL CHECK (
                    status IN ('pending', 'confirmed', 'cancelled')
                ),
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS control_prompts (
                public_id TEXT PRIMARY KEY,
                thread_id TEXT NOT NULL,
                chat_id INTEGER NOT NULL,
                topic_id INTEGER NOT NULL,
                telegram_message_id INTEGER NOT NULL,
                mode TEXT NOT NULL CHECK (mode IN ('steer', 'queue')),
                status TEXT NOT NULL CHECK (
                    status IN ('pending', 'consumed', 'expired')
                ),
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(chat_id, telegram_message_id)
            );

            CREATE TABLE IF NOT EXISTS control_status_cards (
                thread_id TEXT NOT NULL,
                chat_id INTEGER NOT NULL,
                topic_id INTEGER NOT NULL,
                telegram_message_id INTEGER NOT NULL,
                status TEXT NOT NULL CHECK (
                    status IN ('active', 'superseded')
                ),
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(chat_id, telegram_message_id)
            );

            CREATE TABLE IF NOT EXISTS control_status_actions (
                thread_id TEXT NOT NULL,
                chat_id INTEGER NOT NULL,
                topic_id INTEGER NOT NULL,
                telegram_message_id INTEGER NOT NULL,
                action TEXT NOT NULL CHECK (
                    action IN ('steer', 'queue', 'stop')
                ),
                created_at TEXT NOT NULL,
                PRIMARY KEY(chat_id, telegram_message_id, action)
            );

            CREATE TABLE IF NOT EXISTS turn_contexts (
                thread_id TEXT NOT NULL,
                turn_id TEXT NOT NULL,
                source_message_id INTEGER,
                status_message_id INTEGER,
                final_message_id INTEGER,
                progress_render_mode TEXT,
                progress_closed INTEGER NOT NULL DEFAULT 0,
                progress_outcome TEXT,
                progress_send_outcome_unknown
                    INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(thread_id, turn_id)
            );

            CREATE TABLE IF NOT EXISTS turn_progress_entries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                thread_id TEXT NOT NULL,
                turn_id TEXT NOT NULL,
                item_id TEXT NOT NULL,
                item_origin TEXT NOT NULL DEFAULT 'history' CHECK (
                    item_origin IN ('history', 'notification')
                ),
                counterpart_item_id TEXT,
                entry_kind TEXT NOT NULL CHECK (
                    entry_kind IN ('commentary', 'plan', 'tool_status')
                ),
                text TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(thread_id, turn_id, item_id)
            );

            CREATE INDEX IF NOT EXISTS turn_progress_entries_order
                ON turn_progress_entries(thread_id, turn_id, id);

            CREATE TABLE IF NOT EXISTS telegram_new_threads (
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                prompt_hash TEXT NOT NULL,
                thread_id TEXT,
                echo_message_id INTEGER,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(chat_id, message_id)
            );

            CREATE TABLE IF NOT EXISTS manual_topic_threads (
                chat_id INTEGER NOT NULL,
                topic_id INTEGER NOT NULL,
                title TEXT NOT NULL,
                thread_id TEXT,
                status TEXT NOT NULL CHECK (
                    status IN (
                        'reserved',
                        'outcome_unknown',
                        'created',
                        'completed'
                    )
                ),
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(chat_id, topic_id)
            );

            CREATE TABLE IF NOT EXISTS processed_telegram_updates (
                update_id INTEGER PRIMARY KEY,
                processed_at TEXT NOT NULL
            );
            """
        )
        queue_columns = {
            str(row["name"])
            for row in self.connection.execute(
                "PRAGMA table_info(queued_messages)"
            ).fetchall()
        }
        if "status_message_id" not in queue_columns:
            self.connection.execute(
                """
                ALTER TABLE queued_messages
                ADD COLUMN status_message_id INTEGER
                """
            )
        archive_columns = {
            str(row["name"])
            for row in self.connection.execute(
                "PRAGMA table_info(archived_threads)"
            ).fetchall()
        }
        if "replacement_topic_id" not in archive_columns:
            self.connection.execute(
                """
                ALTER TABLE archived_threads
                ADD COLUMN replacement_topic_id INTEGER
                """
            )
        if "replacement_outcome_unknown" not in archive_columns:
            self.connection.execute(
                """
                ALTER TABLE archived_threads
                ADD COLUMN replacement_outcome_unknown
                    INTEGER NOT NULL DEFAULT 0
                """
            )
        if "replacement_title" not in archive_columns:
            self.connection.execute(
                """
                ALTER TABLE archived_threads
                ADD COLUMN replacement_title TEXT
                """
            )
        if "replacement_started_at" not in archive_columns:
            self.connection.execute(
                """
                ALTER TABLE archived_threads
                ADD COLUMN replacement_started_at TEXT
                """
            )
        turn_context_columns = {
            str(row["name"])
            for row in self.connection.execute(
                "PRAGMA table_info(turn_contexts)"
            ).fetchall()
        }
        if "progress_render_mode" not in turn_context_columns:
            self.connection.execute(
                """
                ALTER TABLE turn_contexts
                ADD COLUMN progress_render_mode TEXT
                """
            )
        if "progress_closed" not in turn_context_columns:
            self.connection.execute(
                """
                ALTER TABLE turn_contexts
                ADD COLUMN progress_closed INTEGER NOT NULL DEFAULT 0
                """
            )
        if "progress_outcome" not in turn_context_columns:
            self.connection.execute(
                """
                ALTER TABLE turn_contexts
                ADD COLUMN progress_outcome TEXT
                """
            )
        if "progress_send_outcome_unknown" not in turn_context_columns:
            self.connection.execute(
                """
                ALTER TABLE turn_contexts
                ADD COLUMN progress_send_outcome_unknown
                    INTEGER NOT NULL DEFAULT 0
                """
            )
        progress_entry_columns = {
            str(row["name"])
            for row in self.connection.execute(
                "PRAGMA table_info(turn_progress_entries)"
            ).fetchall()
        }
        if "item_origin" not in progress_entry_columns:
            self.connection.execute(
                """
                ALTER TABLE turn_progress_entries
                ADD COLUMN item_origin TEXT NOT NULL DEFAULT 'history'
                """
            )
        if "counterpart_item_id" not in progress_entry_columns:
            self.connection.execute(
                """
                ALTER TABLE turn_progress_entries
                ADD COLUMN counterpart_item_id TEXT
                """
            )
        self.connection.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS
                turn_progress_entries_counterpart
                ON turn_progress_entries(
                    thread_id, turn_id, counterpart_item_id
                )
                WHERE counterpart_item_id IS NOT NULL
            """
        )
        self.connection.execute(
            """
            DELETE FROM queued_messages
            WHERE id IN (
                SELECT id
                FROM (
                    SELECT
                        id,
                        ROW_NUMBER() OVER (
                            PARTITION BY client_id
                            ORDER BY
                                CASE status
                                    WHEN 'sent' THEN 0
                                    WHEN 'dispatching' THEN 1
                                    WHEN 'pending' THEN 2
                                    ELSE 3
                                END,
                                id
                        ) AS duplicate_rank
                    FROM queued_messages
                )
                WHERE duplicate_rank > 1
            )
            """
        )
        self.connection.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS queued_messages_client_id
            ON queued_messages(client_id)
            """
        )

    def _validate_schema(
        self,
        connection: sqlite3.Connection,
        *,
        expected_version: int,
    ) -> None:
        actual_version = self._schema_version(connection)
        if actual_version != expected_version:
            raise SchemaVersionError(
                f"database schema is {actual_version}, expected {expected_version}"
            )

        integrity_rows = connection.execute("PRAGMA integrity_check").fetchall()
        integrity = [str(row[0]) for row in integrity_rows]
        if integrity != ["ok"]:
            raise sqlite3.DatabaseError("database integrity check failed")
        if connection.execute("PRAGMA foreign_key_check").fetchone() is not None:
            raise sqlite3.IntegrityError("database foreign key check failed")

        required_tables = {
            "settings",
            "thread_topics",
            "mirrored_items",
            "mirror_claims",
            "visible_item_deliveries",
            "visible_item_delivery_intents",
            "outbound_deliveries",
            "queued_messages",
            "observed_topics",
            "topic_creation_intents",
            "archived_threads",
            "pending_requests",
            "control_confirmations",
            "control_prompts",
            "control_status_cards",
            "control_status_actions",
            "turn_contexts",
            "turn_progress_entries",
            "telegram_new_threads",
            "manual_topic_threads",
            "processed_telegram_updates",
            "telegram_update_quarantine",
        }
        present_tables = {
            str(row[0])
            for row in connection.execute(
                """
                SELECT name
                FROM sqlite_master
                WHERE type = 'table'
                """
            ).fetchall()
        }
        missing_tables = required_tables - present_tables
        if missing_tables:
            raise SchemaVersionError(
                "database schema is missing required tables: "
                + ", ".join(sorted(missing_tables))
            )
        required_columns = {
            "queued_messages": {
                "status_message_id",
                "dispatch_started_at",
                "dispatch_history_miss_count",
                "dispatch_last_miss_at",
                "local_inputs_json",
            },
            "visible_item_delivery_intents": {"state"},
            "outbound_deliveries": {
                "kind",
                "source_key",
                "thread_id",
                "chat_id",
                "topic_id",
                "reply_to_message_id",
                "state",
                "telegram_message_id",
                "created_at",
                "updated_at",
            },
            "control_status_actions": {"state", "updated_at"},
            "archived_threads": {
                "replacement_topic_id",
                "replacement_outcome_unknown",
                "replacement_title",
                "replacement_started_at",
                "presentation",
                "summary",
                "archive_message_state",
                "archive_message_id",
            },
            "turn_contexts": {
                "progress_render_mode",
                "progress_closed",
                "progress_outcome",
                "progress_send_outcome_unknown",
            },
            "turn_progress_entries": {
                "item_origin",
                "counterpart_item_id",
            },
        }
        for table, expected_columns in required_columns.items():
            present_columns = {
                str(row["name"])
                for row in connection.execute(
                    f"PRAGMA table_info({table})"
                ).fetchall()
            }
            missing_columns = expected_columns - present_columns
            if missing_columns:
                raise SchemaVersionError(
                    f"database table {table} is missing required columns: "
                    + ", ".join(sorted(missing_columns))
                )
        outbound_primary_key = tuple(
            str(row["name"])
            for row in sorted(
                (
                    row
                    for row in connection.execute(
                        "PRAGMA table_info(outbound_deliveries)"
                    ).fetchall()
                    if int(row["pk"]) > 0
                ),
                key=lambda row: int(row["pk"]),
            )
        )
        if outbound_primary_key != ("kind", "source_key"):
            raise SchemaVersionError(
                "database table outbound_deliveries must uniquely key "
                "(kind, source_key)"
            )
        required_indexes = {
            "visible_item_deliveries": {
                "visible_item_deliveries_match": (
                    False,
                    (
                        "thread_id",
                        "turn_id",
                        "item_type",
                        "content_fingerprint",
                        "item_origin",
                        "id",
                    ),
                    None,
                ),
            },
            "visible_item_delivery_intents": {
                "visible_item_delivery_intents_match": (
                    False,
                    (
                        "thread_id",
                        "turn_id",
                        "item_type",
                        "content_fingerprint",
                        "item_origin",
                        "id",
                    ),
                    None,
                ),
            },
            "outbound_deliveries": {
                "outbound_deliveries_state": (
                    False,
                    ("kind", "state", "created_at"),
                    None,
                ),
            },
            "queued_messages": {
                "queued_messages_pending": (
                    False,
                    ("thread_id", "status", "id"),
                    None,
                ),
                "queued_messages_client_id": (
                    True,
                    ("client_id",),
                    None,
                ),
            },
            "topic_creation_intents": {
                "topic_creation_intents_unresolved": (
                    False,
                    ("status", "created_at"),
                    None,
                ),
            },
            "archived_threads": {
                "archived_threads_current": (
                    False,
                    ("status", "archived_at"),
                    None,
                ),
            },
            "turn_progress_entries": {
                "turn_progress_entries_order": (
                    False,
                    ("thread_id", "turn_id", "id"),
                    None,
                ),
                "turn_progress_entries_counterpart": (
                    True,
                    ("thread_id", "turn_id", "counterpart_item_id"),
                    "counterpart_item_id is not null",
                ),
            },
        }
        for table, expected_indexes in required_indexes.items():
            present_indexes = {
                str(row["name"]): row
                for row in connection.execute(
                    f"PRAGMA index_list({table})"
                ).fetchall()
            }
            missing_indexes = expected_indexes.keys() - present_indexes.keys()
            invalid_indexes = set(missing_indexes)
            for name, (
                expected_unique,
                expected_index_columns,
                expected_predicate,
            ) in expected_indexes.items():
                if name not in present_indexes:
                    continue
                index_row = present_indexes[name]
                index_column_details = tuple(
                    (
                        str(row["name"]),
                        str(row["coll"]).lower(),
                        bool(row["desc"]),
                    )
                    for row in connection.execute(
                        f"PRAGMA index_xinfo({name})"
                    ).fetchall()
                    if bool(row["key"])
                )
                index_columns = tuple(
                    column_name
                    for column_name, _, _ in index_column_details
                )
                schema_row = connection.execute(
                    """
                    SELECT sql
                    FROM sqlite_master
                    WHERE type = 'index' AND name = ?
                    """,
                    (name,),
                ).fetchone()
                normalized_sql = " ".join(
                    str(schema_row["sql"] if schema_row else "")
                    .lower()
                    .split()
                )
                actual_predicate = (
                    normalized_sql.rsplit(" where ", 1)[1]
                    if " where " in normalized_sql
                    else None
                )
                predicate_matches = (
                    bool(index_row["partial"]) == bool(expected_predicate)
                    and actual_predicate == expected_predicate
                )
                if (
                    bool(index_row["unique"]) is not expected_unique
                    or index_columns != expected_index_columns
                    or any(
                        collation != "binary" or descending
                        for _, collation, descending in index_column_details
                    )
                    or not predicate_matches
                ):
                    invalid_indexes.add(name)
            if invalid_indexes:
                raise SchemaVersionError(
                    f"database table {table} has invalid required indexes: "
                    + ", ".join(sorted(invalid_indexes))
                )

    def _validate_migrations_on_copy(self, from_version: int) -> None:
        with tempfile.TemporaryDirectory(
            prefix=".bridge-schema-validation-",
            dir=self.path.parent,
        ) as temporary_directory:
            os.chmod(temporary_directory, 0o700)
            validation_path = Path(temporary_directory) / "validation.sqlite3"
            validation_connection = sqlite3.connect(validation_path)
            try:
                self.connection.backup(validation_connection)
                validation_connection.row_factory = sqlite3.Row
                validation_connection.execute("PRAGMA foreign_keys = ON")
                os.chmod(validation_path, 0o600)
                self._run_migrations(
                    validation_connection,
                    from_version,
                    validating=True,
                )
            finally:
                validation_connection.close()

    def _create_migration_backup(
        self,
        from_version: int,
        to_version: int,
    ) -> Path:
        backup_directory = self.path.parent / "backups"
        backup_directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(backup_directory, 0o700)
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{MIGRATION_BACKUP_PREFIX}",
            suffix=".tmp",
            dir=backup_directory,
        )
        os.close(descriptor)
        temporary_path = Path(temporary_name)
        final_name = (
            f"{MIGRATION_BACKUP_PREFIX}"
            f"v{from_version}-to-v{to_version}-"
            f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')}-"
            f"{uuid.uuid4().hex[:8]}.sqlite3"
        )
        final_path = backup_directory / final_name
        backup_connection: sqlite3.Connection | None = None
        try:
            os.chmod(temporary_path, 0o600)
            backup_connection = sqlite3.connect(temporary_path)
            self.connection.backup(backup_connection)
            backup_connection.close()
            backup_connection = None
            os.chmod(temporary_path, 0o600)
            self._validate_backup_file(
                temporary_path,
                expected_version=from_version,
            )
            with temporary_path.open("rb") as backup_file:
                os.fsync(backup_file.fileno())
            os.replace(temporary_path, final_path)
            os.chmod(final_path, 0o600)
            self._prune_migration_backups(backup_directory)
            directory_descriptor = os.open(backup_directory, os.O_RDONLY)
            try:
                os.fsync(directory_descriptor)
            finally:
                os.close(directory_descriptor)
            return final_path
        finally:
            if backup_connection is not None:
                backup_connection.close()
            if temporary_path.exists():
                temporary_path.unlink()

    def create_operational_backup(
        self,
        *,
        retention: int = OPERATIONAL_BACKUP_RETENTION,
    ) -> OperationalBackup:
        """Create and verify one online snapshot of the live SQLite database."""

        if (
            isinstance(retention, bool)
            or not isinstance(retention, int)
            or not 1 <= retention <= OPERATIONAL_BACKUP_MAX_RETENTION
        ):
            raise ValueError(
                "operational backup retention must be an integer between "
                f"1 and {OPERATIONAL_BACKUP_MAX_RETENTION}"
            )

        backup_directory = self.path.parent / "backups"
        self._ensure_private_backup_directory(backup_directory)
        with _OPERATIONAL_BACKUP_PROCESS_LOCK:
            lock_descriptor = self._open_operational_backup_lock(
                backup_directory
            )
            try:
                fcntl.flock(lock_descriptor, fcntl.LOCK_EX)
                self._cleanup_stale_operational_backup_temps(
                    backup_directory
                )
                return self._create_operational_backup_locked(
                    backup_directory,
                    retention=retention,
                )
            finally:
                fcntl.flock(lock_descriptor, fcntl.LOCK_UN)
                os.close(lock_descriptor)

    def _create_operational_backup_locked(
        self,
        backup_directory: Path,
        *,
        retention: int,
    ) -> OperationalBackup:
        created = datetime.now(timezone.utc)
        schema_version = self.schema_version()
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{OPERATIONAL_BACKUP_PREFIX}",
            suffix=".tmp",
            dir=backup_directory,
        )
        os.close(descriptor)
        temporary_path = Path(temporary_name)
        final_path = backup_directory / (
            f"{OPERATIONAL_BACKUP_PREFIX}"
            f"v{schema_version}-"
            f"{created.strftime('%Y%m%dT%H%M%S%fZ')}-"
            f"{uuid.uuid4().hex[:8]}.sqlite3"
        )
        backup_connection: sqlite3.Connection | None = None
        try:
            os.chmod(temporary_path, 0o600)
            backup_connection = sqlite3.connect(temporary_path)
            self.connection.backup(backup_connection)
            backup_connection.close()
            backup_connection = None
            os.chmod(temporary_path, 0o600)
            self._validate_backup_file(
                temporary_path,
                expected_version=schema_version,
            )
            with temporary_path.open("rb") as backup_file:
                os.fsync(backup_file.fileno())
            os.replace(temporary_path, final_path)
            os.chmod(final_path, 0o600)
            with final_path.open("rb") as backup_file:
                os.fsync(backup_file.fileno())
            retained_backups, pruned_backups = (
                self._prune_operational_backups(
                    backup_directory,
                    retention=retention,
                    preserve=final_path,
                )
            )
            directory_descriptor = os.open(backup_directory, os.O_RDONLY)
            try:
                os.fsync(directory_descriptor)
            finally:
                os.close(directory_descriptor)
            final_status = final_path.stat()
            return OperationalBackup(
                path=final_path,
                created_at=created.isoformat(),
                schema_version=schema_version,
                size_bytes=int(final_status.st_size),
                retained_backups=retained_backups,
                pruned_backups=pruned_backups,
            )
        finally:
            if backup_connection is not None:
                backup_connection.close()
            for suffix in ("", "-journal", "-wal", "-shm"):
                Path(f"{temporary_path}{suffix}").unlink(missing_ok=True)

    @staticmethod
    def _open_operational_backup_lock(backup_directory: Path) -> int:
        lock_path = backup_directory / OPERATIONAL_BACKUP_LOCK_NAME
        descriptor = os.open(
            lock_path,
            (
                os.O_CREAT
                | os.O_RDWR
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0)
            ),
            0o600,
        )
        try:
            status = os.fstat(descriptor)
            if (
                not stat.S_ISREG(status.st_mode)
                or status.st_uid != os.getuid()
            ):
                raise SchemaVersionError(
                    "operational backup lock has an unexpected owner or type"
                )
            os.fchmod(descriptor, 0o600)
            return descriptor
        except Exception:
            os.close(descriptor)
            raise

    @staticmethod
    def _cleanup_stale_operational_backup_temps(
        backup_directory: Path,
    ) -> int:
        removed = 0
        for path in backup_directory.iterdir():
            if OPERATIONAL_BACKUP_TEMP_NAME.fullmatch(path.name) is None:
                continue
            try:
                status = path.lstat()
            except FileNotFoundError:
                continue
            if (
                not stat.S_ISREG(status.st_mode)
                or status.st_uid != os.getuid()
            ):
                continue
            try:
                path.unlink()
            except FileNotFoundError:
                continue
            removed += 1
        return removed

    @staticmethod
    def _ensure_private_backup_directory(backup_directory: Path) -> None:
        if backup_directory.is_symlink():
            raise SchemaVersionError("backup directory must not be a symlink")
        backup_directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        status = backup_directory.lstat()
        if (
            not stat.S_ISDIR(status.st_mode)
            or status.st_uid != os.getuid()
        ):
            raise SchemaVersionError(
                "backup directory has an unexpected owner or type"
            )
        os.chmod(backup_directory, 0o700)

    @staticmethod
    def _prune_operational_backups(
        backup_directory: Path,
        *,
        retention: int,
        preserve: Path | None = None,
    ) -> tuple[int, int]:
        candidates: list[tuple[int, str, Path]] = []
        for path in backup_directory.iterdir():
            if OPERATIONAL_BACKUP_NAME.fullmatch(path.name) is None:
                continue
            try:
                status = path.lstat()
            except FileNotFoundError:
                continue
            if (
                not stat.S_ISREG(status.st_mode)
                or status.st_uid != os.getuid()
                or stat.S_IMODE(status.st_mode) & 0o077
            ):
                continue
            candidates.append((status.st_mtime_ns, path.name, path))

        candidates.sort(reverse=True)
        if preserve is not None:
            for index, candidate in enumerate(candidates):
                if candidate[2] == preserve:
                    candidates.insert(0, candidates.pop(index))
                    break
        pruned = 0
        for _, _, obsolete in candidates[retention:]:
            try:
                obsolete.unlink()
            except FileNotFoundError:
                continue
            pruned += 1
        retained = len(candidates) - pruned
        return retained, pruned

    @staticmethod
    def _validate_backup_file(
        path: Path,
        *,
        expected_version: int,
    ) -> None:
        connection_uri = f"{path.resolve().as_uri()}?mode=ro"
        connection = sqlite3.connect(connection_uri, uri=True)
        try:
            row = connection.execute("PRAGMA user_version").fetchone()
            version = 0 if row is None else int(row[0])
            if version != expected_version:
                raise SchemaVersionError(
                    f"backup schema is {version}, expected {expected_version}"
                )
            integrity = [
                str(item[0])
                for item in connection.execute(
                    "PRAGMA integrity_check"
                ).fetchall()
            ]
            if integrity != ["ok"]:
                raise sqlite3.DatabaseError("backup integrity check failed")
        finally:
            connection.close()

    @staticmethod
    def _prune_migration_backups(backup_directory: Path) -> None:
        backups = sorted(
            backup_directory.glob(
                f"{MIGRATION_BACKUP_PREFIX}v*-to-v*-*.sqlite3"
            ),
            key=lambda path: (path.stat().st_mtime_ns, path.name),
            reverse=True,
        )
        for obsolete in backups[MIGRATION_BACKUP_RETENTION:]:
            obsolete.unlink()

    def acquire_service_lock(self) -> None:
        if self.read_only:
            raise RuntimeError("read-only store cannot acquire the service lock")
        if self._service_lock_descriptor is not None:
            return
        lock_path = self.path.parent / "bridge-service.lock"
        descriptor = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
        os.chmod(lock_path, 0o600)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            os.close(descriptor)
            raise RuntimeError(
                "another Telegram bridge service is already running"
            ) from error
        self._service_lock_descriptor = descriptor

    def release_service_lock(self) -> None:
        descriptor = self._service_lock_descriptor
        if descriptor is None:
            return
        self._service_lock_descriptor = None
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)

    def recover_dispatching_queue(self) -> int:
        if self._service_lock_descriptor is None:
            raise RuntimeError("service lock is required for queue recovery")
        # A process restart is not evidence that Codex rejected an ambiguous
        # request. Preserve every dispatch reservation until multiple fresh
        # authoritative history reads and the grace window both say retrying
        # is safe.
        return 0

    def integrity_check(self) -> str:
        row = self.connection.execute("PRAGMA integrity_check").fetchone()
        return "unknown" if row is None else str(row[0])

    def schema_version(self) -> int:
        return self._schema_version(self.connection)

    def observed_unmapped_count(
        self,
        *,
        chat_id: int,
        excluding_topic_ids: tuple[int, ...] = (),
    ) -> int:
        parameters: list[int] = [chat_id]
        exclusion = ""
        if excluding_topic_ids:
            placeholders = ", ".join("?" for _ in excluding_topic_ids)
            exclusion = f"AND observed.topic_id NOT IN ({placeholders})"
            parameters.extend(excluding_topic_ids)
        row = self.connection.execute(
            f"""
            SELECT COUNT(*) AS count
            FROM observed_topics AS observed
            LEFT JOIN thread_topics AS bound
                ON
                    bound.chat_id = observed.chat_id
                    AND bound.topic_id = observed.topic_id
            WHERE
                observed.chat_id = ?
                AND bound.topic_id IS NULL
                {exclusion}
            """,
            parameters,
        ).fetchone()
        return 0 if row is None else int(row["count"])

    def queue_health(self) -> dict[str, int]:
        counts = {
            str(row["status"]): int(row["count"])
            for row in self.connection.execute(
                """
                SELECT status, COUNT(*) AS count
                FROM queued_messages
                GROUP BY status
                """
            ).fetchall()
        }
        row = self.connection.execute(
            """
            SELECT COALESCE(
                CAST(strftime('%s', 'now') AS INTEGER)
                - MIN(CAST(strftime('%s', created_at) AS INTEGER)),
                0
            ) AS age
            FROM queued_messages
            WHERE status IN ('pending', 'dispatching')
            """
        ).fetchone()
        return {
            "pending": counts.get("pending", 0),
            "dispatching": counts.get("dispatching", 0),
            "sent": counts.get("sent", 0),
            "oldestActiveAgeSeconds": 0 if row is None else int(row["age"]),
        }

    def delivery_uncertainty_health(self) -> dict[str, int]:
        outbound_counts = {
            (str(row["kind"]), str(row["state"])): int(row["count"])
            for row in self.connection.execute(
                """
                SELECT kind, state, COUNT(*) AS count
                FROM outbound_deliveries
                WHERE state IN ('reserved', 'outcome_unknown')
                GROUP BY kind, state
                """
            ).fetchall()
        }
        visible_counts = {
            (str(row["item_type"]), str(row["state"])): int(row["count"])
            for row in self.connection.execute(
                """
                SELECT item_type, state, COUNT(*) AS count
                FROM visible_item_delivery_intents
                WHERE item_type IN (
                    'agentMessage',
                    'userMessage',
                    'finalAttachment',
                    'userAttachment',
                    'threadAttachment'
                )
                GROUP BY item_type, state
                """
            ).fetchall()
        }
        progress_row = self.connection.execute(
            """
            SELECT COUNT(*) AS count
            FROM turn_contexts
            WHERE progress_send_outcome_unknown = 1
            """
        ).fetchone()
        claimed_actions_row = self.connection.execute(
            """
            SELECT COUNT(*) AS count
            FROM control_status_actions
            WHERE state = 'claimed'
            """
        ).fetchone()
        approval_counts = {
            str(row["status"]): int(row["count"])
            for row in self.connection.execute(
                """
                SELECT status, COUNT(*) AS count
                FROM pending_requests
                WHERE status IN (
                    'delivery_reserved',
                    'delivery_outcome_unknown'
                )
                GROUP BY status
                """
            ).fetchall()
        }
        result = {
            "queueAnnouncementsReserved": outbound_counts.get(
                ("queue_announcement", "reserved"),
                0,
            ),
            "queueAnnouncementsOutcomeUnknown": outbound_counts.get(
                ("queue_announcement", "outcome_unknown"),
                0,
            ),
            "statusCardsReserved": outbound_counts.get(
                ("status_card", "reserved"),
                0,
            ),
            "statusCardsOutcomeUnknown": outbound_counts.get(
                ("status_card", "outcome_unknown"),
                0,
            ),
            "visibleFinalsReserved": visible_counts.get(
                ("agentMessage", "reserved"),
                0,
            ),
            "visibleFinalsOutcomeUnknown": visible_counts.get(
                ("agentMessage", "outcome_unknown"),
                0,
            ),
            "visibleUserMessagesReserved": visible_counts.get(
                ("userMessage", "reserved"),
                0,
            ),
            "visibleUserMessagesOutcomeUnknown": visible_counts.get(
                ("userMessage", "outcome_unknown"),
                0,
            ),
            "finalAttachmentsReserved": visible_counts.get(
                ("finalAttachment", "reserved"),
                0,
            ),
            "finalAttachmentsOutcomeUnknown": visible_counts.get(
                ("finalAttachment", "outcome_unknown"),
                0,
            ),
            "userAttachmentsReserved": visible_counts.get(
                ("userAttachment", "reserved"),
                0,
            ),
            "userAttachmentsOutcomeUnknown": visible_counts.get(
                ("userAttachment", "outcome_unknown"),
                0,
            ),
            "threadAttachmentsReserved": visible_counts.get(
                ("threadAttachment", "reserved"),
                0,
            ),
            "threadAttachmentsOutcomeUnknown": visible_counts.get(
                ("threadAttachment", "outcome_unknown"),
                0,
            ),
            "progressCardsOutcomeUnknown": (
                0 if progress_row is None else int(progress_row["count"])
            ),
            "controlActionsClaimed": (
                0
                if claimed_actions_row is None
                else int(claimed_actions_row["count"])
            ),
            "approvalPromptsReserved": approval_counts.get(
                "delivery_reserved",
                0,
            ),
            "approvalPromptsOutcomeUnknown": approval_counts.get(
                "delivery_outcome_unknown",
                0,
            ),
        }
        result["totalUncertain"] = sum(result.values())
        return result

    def thread_queue_health(self, thread_id: str) -> dict[str, int]:
        row = self.connection.execute(
            """
            SELECT
                SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) AS pending,
                SUM(
                    CASE WHEN status = 'dispatching' THEN 1 ELSE 0 END
                ) AS dispatching,
                COALESCE(
                    CAST(strftime('%s', 'now') AS INTEGER)
                    - MIN(
                        CASE
                            WHEN status IN ('pending', 'dispatching')
                            THEN CAST(strftime('%s', created_at) AS INTEGER)
                        END
                    ),
                    0
                ) AS age
            FROM queued_messages
            WHERE thread_id = ?
            """,
            (thread_id,),
        ).fetchone()
        if row is None:
            return {
                "pending": 0,
                "dispatching": 0,
                "oldestActiveAgeSeconds": 0,
            }
        return {
            "pending": int(row["pending"] or 0),
            "dispatching": int(row["dispatching"] or 0),
            "oldestActiveAgeSeconds": int(row["age"] or 0),
        }

    def get_setting(self, key: str) -> str | None:
        row = self.connection.execute(
            "SELECT value FROM settings WHERE key = ?", (key,)
        ).fetchone()
        return None if row is None else str(row["value"])

    def settings_with_prefix(self, prefix: str) -> dict[str, str]:
        rows = self.connection.execute(
            """
            SELECT key, value
            FROM settings
            WHERE substr(key, 1, ?) = ?
            ORDER BY key
            """,
            (len(prefix), prefix),
        ).fetchall()
        return {
            str(row["key"]): str(row["value"])
            for row in rows
        }

    def set_setting(self, key: str, value: str | int) -> None:
        now = utc_now()
        self.connection.execute(
            """
            INSERT INTO settings(key, value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                value = excluded.value,
                updated_at = excluded.updated_at
            """,
            (key, str(value), now),
        )
        self.connection.commit()

    def bind(
        self,
        *,
        chat_id: int,
        allowed_user_id: int,
        bot_id: int,
        bot_username: str,
        chat_title: str,
    ) -> None:
        values = {
            "telegram_chat_id": chat_id,
            "telegram_allowed_user_id": allowed_user_id,
            "telegram_bot_id": bot_id,
            "telegram_bot_username": bot_username,
            "telegram_chat_title": chat_title,
        }
        with self.connection:
            for key, value in values.items():
                self.connection.execute(
                    """
                    INSERT INTO settings(key, value, updated_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(key) DO UPDATE SET
                        value = excluded.value,
                        updated_at = excluded.updated_at
                    """,
                    (key, str(value), utc_now()),
                )

    def binding(self) -> Binding | None:
        keys = {
            "chat_id": self.get_setting("telegram_chat_id"),
            "allowed_user_id": self.get_setting("telegram_allowed_user_id"),
            "bot_id": self.get_setting("telegram_bot_id"),
            "bot_username": self.get_setting("telegram_bot_username"),
            "chat_title": self.get_setting("telegram_chat_title"),
        }
        if any(value is None for value in keys.values()):
            return None
        return Binding(
            chat_id=int(keys["chat_id"]),
            allowed_user_id=int(keys["allowed_user_id"]),
            bot_id=int(keys["bot_id"]),
            bot_username=str(keys["bot_username"]),
            chat_title=str(keys["chat_title"]),
        )

    def telegram_offset(self) -> int | None:
        value = self.get_setting("telegram_update_offset")
        return None if value is None else int(value)

    def set_telegram_offset(self, offset: int) -> None:
        self.set_setting("telegram_update_offset", offset)

    def telegram_update_processed(self, update_id: int) -> bool:
        row = self.connection.execute(
            """
            SELECT 1 FROM processed_telegram_updates
            WHERE update_id = ?
            """,
            (update_id,),
        ).fetchone()
        return row is not None

    def mark_telegram_update_processed(self, update_id: int) -> None:
        with self.connection:
            self.connection.execute(
                """
                INSERT OR IGNORE INTO processed_telegram_updates(
                    update_id, processed_at
                )
                VALUES (?, ?)
                """,
                (update_id, utc_now()),
            )
            self.connection.execute(
                """
                DELETE FROM processed_telegram_updates
                WHERE update_id < (
                    SELECT COALESCE(MAX(update_id), 0) - 10000
                    FROM processed_telegram_updates
                )
                """
            )

    def quarantine_telegram_update(
        self,
        update_id: int,
        *,
        update_kind: str,
        error_type: str,
    ) -> None:
        """Atomically skip one bad update without retaining message content."""

        safe_kind = re.sub(r"[^a-z_]+", "_", update_kind.lower())[:40]
        safe_type = re.sub(r"[^A-Za-z0-9_]+", "_", error_type)[:80]
        now = utc_now()
        with self.connection:
            self.connection.execute(
                """
                INSERT OR IGNORE INTO telegram_update_quarantine(
                    update_id, update_kind, error_type, quarantined_at
                ) VALUES (?, ?, ?, ?)
                """,
                (update_id, safe_kind or "unknown", safe_type or "Exception", now),
            )
            self.connection.execute(
                """
                INSERT OR IGNORE INTO processed_telegram_updates(
                    update_id, processed_at
                ) VALUES (?, ?)
                """,
                (update_id, now),
            )
            self.connection.execute(
                """
                DELETE FROM telegram_update_quarantine
                WHERE update_id < (
                    SELECT COALESCE(MAX(update_id), 0) - 100
                    FROM telegram_update_quarantine
                )
                """
            )

    def telegram_update_quarantine_health(self) -> dict[str, int]:
        row = self.connection.execute(
            "SELECT COUNT(*) AS count FROM telegram_update_quarantine"
        ).fetchone()
        return {"quarantined": 0 if row is None else int(row["count"])}

    def initial_history_complete(self, thread_id: str) -> bool:
        return self.get_setting(f"initial_history_complete:{thread_id}") == "1"

    def set_initial_history_complete(self, thread_id: str) -> None:
        self.set_setting(f"initial_history_complete:{thread_id}", "1")

    def upsert_topic(
        self,
        *,
        thread_id: str,
        chat_id: int,
        topic_id: int,
        title: str,
        archived: bool = False,
        last_updated_at: int = 0,
    ) -> None:
        now = utc_now()
        self.connection.execute(
            """
            INSERT INTO thread_topics(
                thread_id, chat_id, topic_id, title, archived,
                last_updated_at, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(thread_id) DO UPDATE SET
                chat_id = excluded.chat_id,
                topic_id = excluded.topic_id,
                title = excluded.title,
                archived = excluded.archived,
                last_updated_at = excluded.last_updated_at,
                updated_at = excluded.updated_at
            """,
            (
                thread_id,
                chat_id,
                topic_id,
                title,
                int(archived),
                last_updated_at,
                now,
                now,
            ),
        )
        self.connection.commit()

    @staticmethod
    def _topic_from_row(row: sqlite3.Row | None) -> TopicBinding | None:
        if row is None:
            return None
        return TopicBinding(
            thread_id=str(row["thread_id"]),
            chat_id=int(row["chat_id"]),
            topic_id=int(row["topic_id"]),
            title=str(row["title"]),
            archived=bool(row["archived"]),
            last_updated_at=int(row["last_updated_at"]),
        )

    def topic_for_thread(self, thread_id: str) -> TopicBinding | None:
        row = self.connection.execute(
            "SELECT * FROM thread_topics WHERE thread_id = ?", (thread_id,)
        ).fetchone()
        return self._topic_from_row(row)

    def topic_for_telegram(
        self, chat_id: int, topic_id: int
    ) -> TopicBinding | None:
        row = self.connection.execute(
            """
            SELECT * FROM thread_topics
            WHERE chat_id = ? AND topic_id = ?
            """,
            (chat_id, topic_id),
        ).fetchone()
        return self._topic_from_row(row)

    def list_topics(self) -> list[TopicBinding]:
        rows = self.connection.execute(
            "SELECT * FROM thread_topics ORDER BY created_at"
        ).fetchall()
        return [self._topic_from_row(row) for row in rows if row is not None]

    def archive_hub_topic_id(self) -> int | None:
        value = self.get_setting("telegram_archive_hub_topic_id")
        return None if value is None else int(value)

    def archive_index_message_id(self) -> int | None:
        value = self.get_setting("telegram_archive_index_message_id")
        return None if value is None else int(value)

    def set_archive_hub_topic_id(self, topic_id: int) -> None:
        self.set_setting("telegram_archive_hub_topic_id", topic_id)

    def set_archive_index_message_id(self, message_id: int) -> None:
        self.set_setting("telegram_archive_index_message_id", message_id)

    def clear_archive_index_message_id(self) -> None:
        self.connection.execute(
            "DELETE FROM settings WHERE key = ?",
            ("telegram_archive_index_message_id",),
        )
        self.connection.commit()

    def clear_archive_hub(self) -> None:
        topic_id = self.archive_hub_topic_id()
        binding = self.binding()
        with self.connection:
            self.connection.execute(
                """
                DELETE FROM settings
                WHERE key IN (
                    'telegram_archive_hub_topic_id',
                    'telegram_archive_index_message_id'
                )
                """
            )
            if topic_id is not None and binding is not None:
                self.connection.execute(
                    """
                    DELETE FROM observed_topics
                    WHERE chat_id = ? AND topic_id = ?
                    """,
                    (binding.chat_id, topic_id),
                )

    def unbound_observed_topic_ids(
        self,
        *,
        chat_id: int,
        title: str,
        excluding_topic_id: int | None = None,
        observed_after: str | None = None,
    ) -> list[int]:
        rows = self.connection.execute(
            """
            SELECT observed.topic_id
            FROM observed_topics AS observed
            LEFT JOIN thread_topics AS bound
                ON
                    bound.chat_id = observed.chat_id
                    AND bound.topic_id = observed.topic_id
            WHERE
                observed.chat_id = ?
                AND observed.title = ?
                AND bound.topic_id IS NULL
                AND (? IS NULL OR observed.topic_id != ?)
                AND (? IS NULL OR observed.observed_at >= ?)
            ORDER BY observed.observed_at, observed.topic_id
            """,
            (
                chat_id,
                title,
                excluding_topic_id,
                excluding_topic_id,
                observed_after,
                observed_after,
            ),
        ).fetchall()
        return [int(row["topic_id"]) for row in rows]

    @staticmethod
    def _archived_thread_from_row(
        row: sqlite3.Row | None,
    ) -> ArchivedThread | None:
        if row is None:
            return None
        return ArchivedThread(
            thread_id=str(row["thread_id"]),
            restore_token=str(row["restore_token"]),
            title=str(row["title"]),
            thread_created_at=int(row["thread_created_at"]),
            archived_at=str(row["archived_at"]),
            source_topic_id=(
                None
                if row["source_topic_id"] is None
                else int(row["source_topic_id"])
            ),
            status=str(row["status"]),
            restored_at=(
                None if row["restored_at"] is None else str(row["restored_at"])
            ),
            replacement_topic_id=(
                None
                if row["replacement_topic_id"] is None
                else int(row["replacement_topic_id"])
            ),
            replacement_outcome_unknown=bool(
                row["replacement_outcome_unknown"]
            ),
            replacement_title=(
                None
                if row["replacement_title"] is None
                else str(row["replacement_title"])
            ),
            replacement_started_at=(
                None
                if row["replacement_started_at"] is None
                else str(row["replacement_started_at"])
            ),
            presentation=str(row["presentation"]),
            summary=str(row["summary"]),
            archive_message_state=str(row["archive_message_state"]),
            archive_message_id=(
                None
                if row["archive_message_id"] is None
                else int(row["archive_message_id"])
            ),
        )

    def archived_thread(self, thread_id: str) -> ArchivedThread | None:
        row = self.connection.execute(
            "SELECT * FROM archived_threads WHERE thread_id = ?",
            (thread_id,),
        ).fetchone()
        return self._archived_thread_from_row(row)

    def archived_thread_for_token(
        self,
        restore_token: str,
    ) -> ArchivedThread | None:
        row = self.connection.execute(
            "SELECT * FROM archived_threads WHERE restore_token = ?",
            (restore_token,),
        ).fetchone()
        return self._archived_thread_from_row(row)

    def reserve_archived_thread(
        self,
        *,
        thread_id: str,
        restore_token: str,
        title: str,
        thread_created_at: int,
        source_topic_id: int | None,
        summary: str = "",
    ) -> ArchivedThread:
        existing = self.archived_thread(thread_id)
        now = utc_now()
        if existing is None:
            status = "detected"
            self.connection.execute(
                """
                INSERT INTO archived_threads(
                    thread_id, restore_token, title, thread_created_at,
                    archived_at, source_topic_id, status, restored_at,
                    replacement_topic_id, replacement_outcome_unknown,
                    replacement_title, replacement_started_at,
                    presentation, summary, archive_message_state,
                    archive_message_id,
                    created_at, updated_at
                )
                VALUES (
                    ?, ?, ?, ?, ?, ?, ?, NULL, NULL, 0, NULL, NULL,
                    'card', ?, 'reserved', NULL, ?, ?
                )
                """,
                (
                    thread_id,
                    restore_token,
                    title,
                    thread_created_at,
                    now,
                    source_topic_id,
                    status,
                    summary,
                    now,
                    now,
                ),
            )
        elif existing.status == "restored":
            status = "detected"
            self.connection.execute(
                """
                UPDATE archived_threads
                SET
                    restore_token = ?,
                    title = ?,
                    thread_created_at = ?,
                    archived_at = ?,
                    source_topic_id = ?,
                    status = ?,
                    restored_at = NULL,
                    replacement_topic_id = NULL,
                    replacement_outcome_unknown = 0,
                    replacement_title = NULL,
                    replacement_started_at = NULL,
                    presentation = 'card',
                    summary = ?,
                    archive_message_state = 'reserved',
                    archive_message_id = NULL,
                    updated_at = ?
                WHERE thread_id = ?
                """,
                (
                    restore_token,
                    title,
                    thread_created_at,
                    now,
                    source_topic_id,
                    status,
                    summary,
                    now,
                    thread_id,
                ),
            )
        else:
            effective_thread_created_at = (
                thread_created_at
                if thread_created_at > 0
                else existing.thread_created_at
            )
            effective_source_topic_id = (
                existing.source_topic_id
                if existing.source_topic_id is not None
                else source_topic_id
            )
            if (
                existing.title == title
                and existing.thread_created_at == effective_thread_created_at
                and existing.source_topic_id == effective_source_topic_id
                and (
                    existing.presentation == "legacy_index"
                    or not summary
                    or existing.summary == summary
                )
            ):
                return existing
            self.connection.execute(
                """
                UPDATE archived_threads
                SET
                    title = ?,
                    thread_created_at = CASE
                        WHEN ? > 0 THEN ?
                        ELSE thread_created_at
                    END,
                    source_topic_id = COALESCE(source_topic_id, ?),
                    summary = CASE
                        WHEN presentation = 'card' AND ? != '' THEN ?
                        ELSE summary
                    END,
                    updated_at = ?
                WHERE thread_id = ?
                """,
                (
                    title,
                    thread_created_at,
                    thread_created_at,
                    source_topic_id,
                    summary,
                    summary,
                    now,
                    thread_id,
                ),
            )
        self.connection.commit()
        record = self.archived_thread(thread_id)
        assert record is not None
        return record

    def mark_archive_card_outcome_unknown(
        self,
        thread_id: str,
    ) -> ArchivedThread:
        cursor = self.connection.execute(
            """
            UPDATE archived_threads
            SET archive_message_state = 'outcome_unknown', updated_at = ?
            WHERE
                thread_id = ?
                AND presentation = 'card'
                AND archive_message_state = 'reserved'
                AND archive_message_id IS NULL
            """,
            (utc_now(), thread_id),
        )
        self.connection.commit()
        if cursor.rowcount != 1:
            raise RuntimeError("archive card is not reserved")
        record = self.archived_thread(thread_id)
        assert record is not None
        return record

    def reset_archive_card_after_definite_failure(
        self,
        thread_id: str,
    ) -> ArchivedThread:
        cursor = self.connection.execute(
            """
            UPDATE archived_threads
            SET archive_message_state = 'reserved', updated_at = ?
            WHERE
                thread_id = ?
                AND presentation = 'card'
                AND archive_message_state = 'outcome_unknown'
                AND archive_message_id IS NULL
            """,
            (utc_now(), thread_id),
        )
        self.connection.commit()
        if cursor.rowcount != 1:
            raise RuntimeError("archive card outcome is not unknown")
        record = self.archived_thread(thread_id)
        assert record is not None
        return record

    def complete_archive_card(
        self,
        thread_id: str,
        message_id: int,
    ) -> ArchivedThread:
        if message_id <= 0:
            raise ValueError("archive card message id must be positive")
        current = self.archived_thread(thread_id)
        if current is None or current.presentation != "card":
            raise RuntimeError("archive card is unavailable")
        if current.archive_message_state == "sent":
            if current.archive_message_id != message_id:
                raise RuntimeError(
                    "archive card already has a different Telegram message"
                )
            return current
        if current.archive_message_state != "outcome_unknown":
            raise RuntimeError("archive card was not sent")
        self.connection.execute(
            """
            UPDATE archived_threads
            SET
                archive_message_state = 'sent',
                archive_message_id = ?,
                updated_at = ?
            WHERE thread_id = ?
            """,
            (message_id, utc_now(), thread_id),
        )
        self.connection.commit()
        record = self.archived_thread(thread_id)
        assert record is not None
        return record

    def begin_archive_restore(
        self,
        thread_id: str,
        *,
        replacement_title: str,
    ) -> ArchivedThread:
        now = utc_now()
        cursor = self.connection.execute(
            """
            UPDATE archived_threads
            SET
                status = 'restoring',
                restored_at = NULL,
                replacement_title = ?,
                replacement_started_at = NULL,
                updated_at = ?
            WHERE thread_id = ? AND status != 'restored'
            """,
            (replacement_title, now, thread_id),
        )
        self.connection.commit()
        if cursor.rowcount != 1:
            raise RuntimeError("archived thread was not reserved")
        record = self.archived_thread(thread_id)
        assert record is not None
        return record

    def mark_archive_replacement_outcome_unknown(
        self,
        thread_id: str,
    ) -> ArchivedThread:
        now = utc_now()
        cursor = self.connection.execute(
            """
            UPDATE archived_threads
            SET
                replacement_outcome_unknown = 1,
                replacement_title = COALESCE(replacement_title, title),
                replacement_started_at = COALESCE(
                    replacement_started_at,
                    ?
                ),
                updated_at = ?
            WHERE thread_id = ? AND status = 'restoring'
            """,
            (now, now, thread_id),
        )
        self.connection.commit()
        if cursor.rowcount != 1:
            raise RuntimeError("archive restoration is not active")
        record = self.archived_thread(thread_id)
        assert record is not None
        return record

    def clear_archive_replacement_outcome_unknown(
        self,
        thread_id: str,
    ) -> ArchivedThread:
        cursor = self.connection.execute(
            """
            UPDATE archived_threads
            SET
                replacement_outcome_unknown = 0,
                replacement_started_at = NULL,
                updated_at = ?
            WHERE thread_id = ? AND status = 'restoring'
            """,
            (utc_now(), thread_id),
        )
        self.connection.commit()
        if cursor.rowcount != 1:
            raise RuntimeError("archive restoration is not active")
        record = self.archived_thread(thread_id)
        assert record is not None
        return record

    def record_archive_replacement_topic(
        self,
        thread_id: str,
        topic_id: int,
    ) -> ArchivedThread:
        current = self.archived_thread(thread_id)
        if current is None or current.status != "restoring":
            raise RuntimeError("archive restoration is not active")
        if (
            current.replacement_topic_id is not None
            and current.replacement_topic_id != topic_id
        ):
            raise RuntimeError(
                "archive restoration already has a different Topic"
            )
        self.connection.execute(
            """
            UPDATE archived_threads
            SET
                replacement_topic_id = ?,
                replacement_outcome_unknown = 0,
                replacement_started_at = NULL,
                updated_at = ?
            WHERE thread_id = ?
            """,
            (topic_id, utc_now(), thread_id),
        )
        self.connection.commit()
        record = self.archived_thread(thread_id)
        assert record is not None
        return record

    def set_archived_thread_status(
        self,
        thread_id: str,
        status: str,
    ) -> ArchivedThread:
        if status not in {"detected", "archived", "restoring", "restored"}:
            raise ValueError(f"invalid archive status: {status}")
        now = utc_now()
        cursor = self.connection.execute(
            """
            UPDATE archived_threads
            SET
                status = ?,
                restored_at = CASE
                    WHEN ? = 'restored' THEN ?
                    ELSE NULL
                END,
                updated_at = ?
            WHERE thread_id = ?
            """,
            (status, status, now, now, thread_id),
        )
        self.connection.commit()
        if cursor.rowcount != 1:
            raise RuntimeError("archived thread was not reserved")
        record = self.archived_thread(thread_id)
        assert record is not None
        return record

    def list_current_archived_threads(self) -> list[ArchivedThread]:
        rows = self.connection.execute(
            """
            SELECT * FROM archived_threads
            WHERE status != 'restored'
            ORDER BY archived_at DESC, thread_id
            """
        ).fetchall()
        return [
            record
            for row in rows
            if (record := self._archived_thread_from_row(row)) is not None
        ]

    @staticmethod
    def _topic_creation_intent_from_row(
        row: sqlite3.Row | None,
    ) -> TopicCreationIntent | None:
        if row is None:
            return None
        return TopicCreationIntent(
            thread_id=str(row["thread_id"]),
            chat_id=int(row["chat_id"]),
            title=str(row["title"]),
            topic_id=(
                None if row["topic_id"] is None else int(row["topic_id"])
            ),
            status=str(row["status"]),
            created_at=str(row["created_at"]),
            updated_at=str(row["updated_at"]),
        )

    def topic_creation_intent(
        self,
        thread_id: str,
    ) -> TopicCreationIntent | None:
        row = self.connection.execute(
            """
            SELECT * FROM topic_creation_intents
            WHERE thread_id = ?
            """,
            (thread_id,),
        ).fetchone()
        return self._topic_creation_intent_from_row(row)

    def reserve_topic_creation(
        self,
        *,
        thread_id: str,
        chat_id: int,
        title: str,
    ) -> TopicCreationIntent:
        now = utc_now()
        self.connection.execute(
            """
            INSERT OR IGNORE INTO topic_creation_intents(
                thread_id, chat_id, title, status, created_at, updated_at
            )
            VALUES (?, ?, ?, 'reserved', ?, ?)
            """,
            (thread_id, chat_id, title, now, now),
        )
        self.connection.commit()
        intent = self.topic_creation_intent(thread_id)
        assert intent is not None
        if intent.chat_id != chat_id or intent.title != title:
            raise RuntimeError(
                "topic creation was replayed with different parameters"
            )
        return intent

    def mark_topic_creation_outcome_unknown(
        self,
        thread_id: str,
    ) -> TopicCreationIntent:
        cursor = self.connection.execute(
            """
            UPDATE topic_creation_intents
            SET status = 'outcome_unknown', updated_at = ?
            WHERE thread_id = ? AND status = 'reserved'
            """,
            (utc_now(), thread_id),
        )
        self.connection.commit()
        intent = self.topic_creation_intent(thread_id)
        if intent is None:
            raise RuntimeError("topic creation was not reserved")
        if cursor.rowcount == 0 and intent.status not in {
            "outcome_unknown",
            "created",
            "completed",
        }:
            raise RuntimeError(
                f"invalid topic creation state: {intent.status}"
            )
        return intent

    def record_created_topic(
        self,
        thread_id: str,
        topic_id: int,
    ) -> TopicCreationIntent:
        with self.connection:
            row = self.connection.execute(
                """
                SELECT * FROM topic_creation_intents
                WHERE thread_id = ?
                """,
                (thread_id,),
            ).fetchone()
            intent = self._topic_creation_intent_from_row(row)
            if intent is None:
                raise RuntimeError("topic creation was not reserved")
            if intent.status in {"created", "completed"}:
                if intent.topic_id != topic_id:
                    raise RuntimeError(
                        "topic creation was replayed with a different result"
                    )
                return intent
            if intent.status != "outcome_unknown":
                raise RuntimeError(
                    "topic creation outcome was not marked unknown"
                )
            self.connection.execute(
                """
                UPDATE topic_creation_intents
                SET topic_id = ?, status = 'created', updated_at = ?
                WHERE thread_id = ? AND status = 'outcome_unknown'
                """,
                (topic_id, utc_now(), thread_id),
            )
        created = self.topic_creation_intent(thread_id)
        assert created is not None
        return created

    def complete_topic_creation(
        self,
        thread_id: str,
        *,
        archived: bool = False,
        last_updated_at: int = 0,
    ) -> TopicBinding:
        with self.connection:
            row = self.connection.execute(
                """
                SELECT * FROM topic_creation_intents
                WHERE thread_id = ?
                """,
                (thread_id,),
            ).fetchone()
            intent = self._topic_creation_intent_from_row(row)
            if intent is None:
                raise RuntimeError("topic creation was not reserved")
            if intent.status not in {"created", "completed"}:
                raise RuntimeError("topic creation result was not recorded")
            if intent.topic_id is None:
                raise RuntimeError("topic creation result has no topic id")

            existing = self.connection.execute(
                """
                SELECT * FROM thread_topics
                WHERE thread_id = ?
                """,
                (thread_id,),
            ).fetchone()
            existing_topic = self._topic_from_row(existing)
            if existing_topic is not None and (
                existing_topic.chat_id != intent.chat_id
                or existing_topic.topic_id != intent.topic_id
            ):
                raise RuntimeError(
                    "thread already has a different Telegram topic"
                )

            now = utc_now()
            self.connection.execute(
                """
                INSERT INTO thread_topics(
                    thread_id, chat_id, topic_id, title, archived,
                    last_updated_at, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(thread_id) DO UPDATE SET
                    title = excluded.title,
                    archived = excluded.archived,
                    last_updated_at = excluded.last_updated_at,
                    updated_at = excluded.updated_at
                """,
                (
                    intent.thread_id,
                    intent.chat_id,
                    intent.topic_id,
                    intent.title,
                    int(archived),
                    last_updated_at,
                    now,
                    now,
                ),
            )
            self.connection.execute(
                """
                UPDATE topic_creation_intents
                SET status = 'completed', updated_at = ?
                WHERE thread_id = ?
                """,
                (now, thread_id),
            )

        topic = self.topic_for_thread(thread_id)
        assert topic is not None
        return topic

    def unresolved_topic_creations(self) -> list[TopicCreationIntent]:
        rows = self.connection.execute(
            """
            SELECT * FROM topic_creation_intents
            WHERE status != 'completed'
            ORDER BY created_at, thread_id
            """
        ).fetchall()
        return [
            intent
            for row in rows
            if (intent := self._topic_creation_intent_from_row(row)) is not None
        ]

    def clear_topic_creation_after_definite_failure(
        self,
        thread_id: str,
    ) -> bool:
        """Allow retry only after the caller proved no Topic was created."""
        with self.connection:
            cursor = self.connection.execute(
                """
                DELETE FROM topic_creation_intents
                WHERE
                    thread_id = ?
                    AND status = 'outcome_unknown'
                    AND topic_id IS NULL
                """,
                (thread_id,),
            )
            if cursor.rowcount == 1:
                return True
            intent = self.topic_creation_intent(thread_id)
            if intent is None:
                return False
            raise RuntimeError(
                "only an unknown topic creation without a result "
                "can be cleared after definite failure"
            )

    def update_topic_state(
        self,
        thread_id: str,
        *,
        title: str | None = None,
        archived: bool | None = None,
        last_updated_at: int | None = None,
    ) -> None:
        current = self.topic_for_thread(thread_id)
        if current is None:
            return
        self.upsert_topic(
            thread_id=thread_id,
            chat_id=current.chat_id,
            topic_id=current.topic_id,
            title=current.title if title is None else title,
            archived=current.archived if archived is None else archived,
            last_updated_at=(
                current.last_updated_at
                if last_updated_at is None
                else last_updated_at
            ),
        )

    @staticmethod
    def _turn_context_from_row(
        row: sqlite3.Row | None,
    ) -> TurnContext | None:
        if row is None:
            return None
        return TurnContext(
            thread_id=str(row["thread_id"]),
            turn_id=str(row["turn_id"]),
            source_message_id=(
                None
                if row["source_message_id"] is None
                else int(row["source_message_id"])
            ),
            status_message_id=(
                None
                if row["status_message_id"] is None
                else int(row["status_message_id"])
            ),
            final_message_id=(
                None
                if row["final_message_id"] is None
                else int(row["final_message_id"])
            ),
            progress_render_mode=(
                None
                if row["progress_render_mode"] is None
                else str(row["progress_render_mode"])
            ),
            progress_closed=bool(row["progress_closed"]),
            progress_outcome=(
                None
                if row["progress_outcome"] is None
                else str(row["progress_outcome"])
            ),
            progress_send_outcome_unknown=bool(
                row["progress_send_outcome_unknown"]
            ),
            created_at=str(row["created_at"]),
        )

    def upsert_turn_context(
        self,
        *,
        thread_id: str,
        turn_id: str,
        source_message_id: int | None = None,
        status_message_id: int | None = None,
        final_message_id: int | None = None,
    ) -> TurnContext:
        now = utc_now()
        self.connection.execute(
            """
            INSERT INTO turn_contexts(
                thread_id, turn_id, source_message_id, status_message_id,
                final_message_id, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(thread_id, turn_id) DO UPDATE SET
                source_message_id = COALESCE(
                    excluded.source_message_id,
                    turn_contexts.source_message_id
                ),
                status_message_id = COALESCE(
                    excluded.status_message_id,
                    turn_contexts.status_message_id
                ),
                final_message_id = COALESCE(
                    excluded.final_message_id,
                    turn_contexts.final_message_id
                ),
                updated_at = excluded.updated_at
            """,
            (
                thread_id,
                turn_id,
                source_message_id,
                status_message_id,
                final_message_id,
                now,
                now,
            ),
        )
        self.connection.commit()
        context = self.turn_context(thread_id, turn_id)
        assert context is not None
        return context

    def turn_context(self, thread_id: str, turn_id: str) -> TurnContext | None:
        row = self.connection.execute(
            """
            SELECT * FROM turn_contexts
            WHERE thread_id = ? AND turn_id = ?
            """,
            (thread_id, turn_id),
        ).fetchone()
        return self._turn_context_from_row(row)

    def turn_context_for_status_message(
        self,
        thread_id: str,
        telegram_message_id: int,
    ) -> TurnContext | None:
        row = self.connection.execute(
            """
            SELECT * FROM turn_contexts
            WHERE thread_id = ? AND status_message_id = ?
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            (thread_id, telegram_message_id),
        ).fetchone()
        return self._turn_context_from_row(row)

    def latest_turn_context(self, thread_id: str) -> TurnContext | None:
        row = self.connection.execute(
            """
            SELECT * FROM turn_contexts
            WHERE thread_id = ?
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            (thread_id,),
        ).fetchone()
        return self._turn_context_from_row(row)

    def update_turn_progress_state(
        self,
        *,
        thread_id: str,
        turn_id: str,
        status_message_id: int | None = None,
        render_mode: str | None = None,
        closed: bool | None = None,
        outcome: str | None = None,
        send_outcome_unknown: bool | None = None,
    ) -> TurnContext:
        self.upsert_turn_context(thread_id=thread_id, turn_id=turn_id)
        assignments = ["updated_at = ?"]
        values: list[Any] = [utc_now()]
        if status_message_id is not None:
            assignments.append("status_message_id = ?")
            values.append(status_message_id)
        if render_mode is not None:
            if render_mode not in {"rich_details", "expandable_quote"}:
                raise ValueError("unsupported progress render mode")
            assignments.append("progress_render_mode = ?")
            values.append(render_mode)
        if closed is not None:
            assignments.append("progress_closed = ?")
            values.append(int(closed))
            if closed:
                if outcome not in {"completed", "failed", "interrupted"}:
                    raise ValueError(
                        "closed progress state requires a terminal outcome"
                    )
                assignments.append("progress_outcome = ?")
                values.append(outcome)
            else:
                assignments.append("progress_outcome = NULL")
        if send_outcome_unknown is not None:
            assignments.append("progress_send_outcome_unknown = ?")
            values.append(int(send_outcome_unknown))
        values.extend((thread_id, turn_id))
        self.connection.execute(
            f"""
            UPDATE turn_contexts
            SET {", ".join(assignments)}
            WHERE thread_id = ? AND turn_id = ?
            """,
            values,
        )
        self.connection.commit()
        context = self.turn_context(thread_id, turn_id)
        assert context is not None
        return context

    @staticmethod
    def _progress_entry_from_row(row: sqlite3.Row) -> ProgressEntry:
        return ProgressEntry(
            entry_id=int(row["id"]),
            thread_id=str(row["thread_id"]),
            turn_id=str(row["turn_id"]),
            item_id=str(row["item_id"]),
            entry_kind=str(row["entry_kind"]),
            text=str(row["text"]),
            created_at=str(row["created_at"]),
        )

    def append_progress_entry(
        self,
        *,
        thread_id: str,
        turn_id: str,
        item_id: str,
        entry_kind: str,
        sanitized_text: str,
        item_origin: str = "history",
    ) -> bool:
        if entry_kind not in {"commentary", "plan", "tool_status"}:
            raise ValueError("unsupported progress entry kind")
        if not sanitized_text:
            raise ValueError("progress entry text must not be empty")
        if item_origin not in {"history", "notification"}:
            raise ValueError("unsupported progress item origin")
        with self.connection:
            if self.connection.execute(
                """
                SELECT 1
                FROM turn_progress_entries
                WHERE thread_id = ?
                  AND turn_id = ?
                  AND (
                      item_id = ?
                      OR counterpart_item_id = ?
                  )
                LIMIT 1
                """,
                (thread_id, turn_id, item_id, item_id),
            ).fetchone():
                return False
            counterpart_origin = (
                "notification"
                if item_origin == "history"
                else "history"
            )
            counterpart = self.connection.execute(
                """
                SELECT id
                FROM turn_progress_entries
                WHERE thread_id = ?
                  AND turn_id = ?
                  AND entry_kind = ?
                  AND text = ?
                  AND item_origin = ?
                  AND counterpart_item_id IS NULL
                ORDER BY id
                LIMIT 1
                """,
                (
                    thread_id,
                    turn_id,
                    entry_kind,
                    sanitized_text,
                    counterpart_origin,
                ),
            ).fetchone()
            if counterpart is not None:
                self.connection.execute(
                    """
                    UPDATE turn_progress_entries
                    SET counterpart_item_id = ?
                    WHERE id = ?
                    """,
                    (item_id, int(counterpart["id"])),
                )
                return False
            cursor = self.connection.execute(
                """
                INSERT OR IGNORE INTO turn_progress_entries(
                    thread_id, turn_id, item_id, item_origin,
                    counterpart_item_id,
                    entry_kind, text, created_at
                )
                VALUES (?, ?, ?, ?, NULL, ?, ?, ?)
                """,
                (
                    thread_id,
                    turn_id,
                    item_id,
                    item_origin,
                    entry_kind,
                    sanitized_text,
                    utc_now(),
                ),
            )
        return cursor.rowcount == 1

    def progress_entries(
        self,
        thread_id: str,
        turn_id: str,
    ) -> list[ProgressEntry]:
        rows = self.connection.execute(
            """
            SELECT * FROM turn_progress_entries
            WHERE thread_id = ? AND turn_id = ?
            ORDER BY id
            """,
            (thread_id, turn_id),
        ).fetchall()
        return [self._progress_entry_from_row(row) for row in rows]

    def reconcile_progress_entries_with_history(
        self,
        *,
        thread_id: str,
        turn_id: str,
        canonical_entries: list[tuple[str, str, str]],
    ) -> bool:
        """Collapse already-stored live/history aliases one-to-one.

        Older bridge versions stored a live notification item and its durable
        history representation as two progress rows because their item IDs
        differ. Canonical history IDs let us identify the durable row without
        collapsing legitimately repeated equal text: each canonical row may
        consume at most one unmatched provisional row.
        """

        grouped: dict[tuple[str, str], set[str]] = {}
        for item_id, entry_kind, sanitized_text in canonical_entries:
            if not item_id or not sanitized_text:
                continue
            grouped.setdefault(
                (entry_kind, sanitized_text),
                set(),
            ).add(item_id)

        changed = False
        with self.connection:
            for (entry_kind, sanitized_text), canonical_ids in grouped.items():
                rows = self.connection.execute(
                    """
                    SELECT id, item_id, counterpart_item_id
                    FROM turn_progress_entries
                    WHERE thread_id = ?
                      AND turn_id = ?
                      AND entry_kind = ?
                      AND text = ?
                    ORDER BY id
                    """,
                    (
                        thread_id,
                        turn_id,
                        entry_kind,
                        sanitized_text,
                    ),
                ).fetchall()
                canonical_rows = [
                    row
                    for row in rows
                    if str(row["item_id"]) in canonical_ids
                    or (
                        row["counterpart_item_id"] is not None
                        and str(row["counterpart_item_id"]) in canonical_ids
                    )
                ]
                provisional_rows = [
                    row
                    for row in rows
                    if str(row["item_id"]) not in canonical_ids
                    and (
                        row["counterpart_item_id"] is None
                        or str(row["counterpart_item_id"])
                        not in canonical_ids
                    )
                    and row["counterpart_item_id"] is None
                ]
                available_canonical_rows = [
                    row
                    for row in canonical_rows
                    if row["counterpart_item_id"] is None
                ]
                for canonical_row, provisional_row in zip(
                    available_canonical_rows,
                    provisional_rows,
                ):
                    self.connection.execute(
                        """
                        UPDATE turn_progress_entries
                        SET counterpart_item_id = ?
                        WHERE id = ?
                        """,
                        (
                            str(provisional_row["item_id"]),
                            int(canonical_row["id"]),
                        ),
                    )
                    self.connection.execute(
                        """
                        DELETE FROM turn_progress_entries
                        WHERE id = ?
                        """,
                        (int(provisional_row["id"]),),
                    )
                    changed = True
        return changed

    @staticmethod
    def _new_thread_request_from_row(
        row: sqlite3.Row | None,
    ) -> NewThreadRequest | None:
        if row is None:
            return None
        return NewThreadRequest(
            chat_id=int(row["chat_id"]),
            message_id=int(row["message_id"]),
            prompt_hash=str(row["prompt_hash"]),
            thread_id=(
                None if row["thread_id"] is None else str(row["thread_id"])
            ),
            echo_message_id=(
                None
                if row["echo_message_id"] is None
                else int(row["echo_message_id"])
            ),
            status=str(row["status"]),
        )

    def reserve_new_thread_request(
        self,
        *,
        chat_id: int,
        message_id: int,
        prompt_hash: str,
    ) -> NewThreadRequest:
        now = utc_now()
        self.connection.execute(
            """
            INSERT OR IGNORE INTO telegram_new_threads(
                chat_id, message_id, prompt_hash, status, created_at, updated_at
            )
            VALUES (?, ?, ?, 'pending', ?, ?)
            """,
            (chat_id, message_id, prompt_hash, now, now),
        )
        self.connection.commit()
        request = self.new_thread_request(chat_id, message_id)
        assert request is not None
        if request.prompt_hash != prompt_hash:
            raise RuntimeError(
                "Telegram message id was replayed with different content"
            )
        return request

    def new_thread_request(
        self,
        chat_id: int,
        message_id: int,
    ) -> NewThreadRequest | None:
        row = self.connection.execute(
            """
            SELECT * FROM telegram_new_threads
            WHERE chat_id = ? AND message_id = ?
            """,
            (chat_id, message_id),
        ).fetchone()
        return self._new_thread_request_from_row(row)

    def update_new_thread_request(
        self,
        *,
        chat_id: int,
        message_id: int,
        status: str,
        thread_id: str | None = None,
        echo_message_id: int | None = None,
    ) -> NewThreadRequest:
        self.connection.execute(
            """
            UPDATE telegram_new_threads
            SET
                thread_id = COALESCE(?, thread_id),
                echo_message_id = COALESCE(?, echo_message_id),
                status = ?,
                updated_at = ?
            WHERE chat_id = ? AND message_id = ?
            """,
            (
                thread_id,
                echo_message_id,
                status,
                utc_now(),
                chat_id,
                message_id,
            ),
        )
        self.connection.commit()
        request = self.new_thread_request(chat_id, message_id)
        if request is None:
            raise RuntimeError("new-thread request was not reserved")
        return request

    @staticmethod
    def _manual_topic_thread_intent_from_row(
        row: sqlite3.Row | None,
    ) -> ManualTopicThreadIntent | None:
        if row is None:
            return None
        return ManualTopicThreadIntent(
            chat_id=int(row["chat_id"]),
            topic_id=int(row["topic_id"]),
            title=str(row["title"]),
            thread_id=(
                None if row["thread_id"] is None else str(row["thread_id"])
            ),
            status=str(row["status"]),
        )

    def manual_topic_thread_intent(
        self,
        *,
        chat_id: int,
        topic_id: int,
    ) -> ManualTopicThreadIntent | None:
        row = self.connection.execute(
            """
            SELECT * FROM manual_topic_threads
            WHERE chat_id = ? AND topic_id = ?
            """,
            (chat_id, topic_id),
        ).fetchone()
        return self._manual_topic_thread_intent_from_row(row)

    def reserve_manual_topic_thread(
        self,
        *,
        chat_id: int,
        topic_id: int,
        title: str,
    ) -> ManualTopicThreadIntent:
        now = utc_now()
        self.connection.execute(
            """
            INSERT OR IGNORE INTO manual_topic_threads(
                chat_id, topic_id, title, status, created_at, updated_at
            )
            VALUES (?, ?, ?, 'reserved', ?, ?)
            """,
            (chat_id, topic_id, title, now, now),
        )
        self.connection.commit()
        intent = self.manual_topic_thread_intent(
            chat_id=chat_id,
            topic_id=topic_id,
        )
        assert intent is not None
        return intent

    def mark_manual_topic_thread_outcome_unknown(
        self,
        *,
        chat_id: int,
        topic_id: int,
    ) -> ManualTopicThreadIntent:
        self.connection.execute(
            """
            UPDATE manual_topic_threads
            SET status = 'outcome_unknown', updated_at = ?
            WHERE chat_id = ? AND topic_id = ? AND status = 'reserved'
            """,
            (utc_now(), chat_id, topic_id),
        )
        self.connection.commit()
        intent = self.manual_topic_thread_intent(
            chat_id=chat_id,
            topic_id=topic_id,
        )
        if intent is None:
            raise RuntimeError("manual Topic thread was not reserved")
        return intent

    def record_manual_topic_thread(
        self,
        *,
        chat_id: int,
        topic_id: int,
        thread_id: str,
    ) -> ManualTopicThreadIntent:
        current = self.manual_topic_thread_intent(
            chat_id=chat_id,
            topic_id=topic_id,
        )
        if current is None:
            raise RuntimeError("manual Topic thread was not reserved")
        if current.thread_id is not None and current.thread_id != thread_id:
            raise RuntimeError(
                "manual Topic is already bound to another Codex thread"
            )
        self.connection.execute(
            """
            UPDATE manual_topic_threads
            SET thread_id = ?, status = 'created', updated_at = ?
            WHERE chat_id = ? AND topic_id = ?
            """,
            (thread_id, utc_now(), chat_id, topic_id),
        )
        self.connection.commit()
        intent = self.manual_topic_thread_intent(
            chat_id=chat_id,
            topic_id=topic_id,
        )
        assert intent is not None
        return intent

    def complete_manual_topic_thread(
        self,
        *,
        chat_id: int,
        topic_id: int,
    ) -> ManualTopicThreadIntent:
        self.connection.execute(
            """
            UPDATE manual_topic_threads
            SET status = 'completed', updated_at = ?
            WHERE
                chat_id = ?
                AND topic_id = ?
                AND thread_id IS NOT NULL
            """,
            (utc_now(), chat_id, topic_id),
        )
        self.connection.commit()
        intent = self.manual_topic_thread_intent(
            chat_id=chat_id,
            topic_id=topic_id,
        )
        if intent is None or intent.status != "completed":
            raise RuntimeError("manual Topic thread is incomplete")
        return intent

    def has_mirrored_item(self, thread_id: str, item_id: str) -> bool:
        row = self.connection.execute(
            """
            SELECT 1 FROM mirrored_items
            WHERE thread_id = ? AND item_id = ?
            """,
            (thread_id, item_id),
        ).fetchone()
        return row is not None

    def mirrored_item_state(
        self,
        thread_id: str,
        item_id: str,
    ) -> tuple[bool, int | None]:
        row = self.connection.execute(
            """
            SELECT telegram_message_id
            FROM mirrored_items
            WHERE thread_id = ? AND item_id = ?
            """,
            (thread_id, item_id),
        ).fetchone()
        if row is None:
            return False, None
        return (
            True,
            (
                None
                if row["telegram_message_id"] is None
                else int(row["telegram_message_id"])
            ),
        )

    def unmark_null_mirrored_item_for_backfill(
        self,
        thread_id: str,
        item_id: str,
    ) -> bool:
        with self.connection:
            cursor = self.connection.execute(
                """
                DELETE FROM mirrored_items
                WHERE
                    thread_id = ?
                    AND item_id = ?
                    AND telegram_message_id IS NULL
                    AND NOT EXISTS (
                        SELECT 1
                        FROM visible_item_delivery_intents
                        WHERE
                            thread_id = ?
                            AND (
                                primary_item_id = ?
                                OR counterpart_item_id = ?
                            )
                    )
                """,
                (thread_id, item_id, thread_id, item_id, item_id),
            )
        return cursor.rowcount == 1

    def update_mirrored_item_message_id(
        self,
        thread_id: str,
        item_id: str,
        telegram_message_id: int,
    ) -> None:
        self.connection.execute(
            """
            UPDATE mirrored_items
            SET telegram_message_id = ?
            WHERE
                thread_id = ?
                AND item_id = ?
                AND telegram_message_id IS NULL
            """,
            (telegram_message_id, thread_id, item_id),
        )
        self.connection.commit()

    def claim_mirrored_item(self, thread_id: str, item_id: str) -> bool:
        expires_before = (
            datetime.now(timezone.utc) - timedelta(minutes=5)
        ).isoformat()
        with self.connection:
            self.connection.execute(
                "DELETE FROM mirror_claims WHERE claimed_at < ?",
                (expires_before,),
            )
            if self.connection.execute(
                """
                SELECT 1 FROM mirrored_items
                WHERE thread_id = ? AND item_id = ?
                """,
                (thread_id, item_id),
            ).fetchone():
                return False
            cursor = self.connection.execute(
                """
                INSERT OR IGNORE INTO mirror_claims(
                    thread_id, item_id, claimed_at
                )
                VALUES (?, ?, ?)
                """,
                (thread_id, item_id, utc_now()),
            )
        return cursor.rowcount == 1

    def release_mirror_claim(self, thread_id: str, item_id: str) -> None:
        self.connection.execute(
            """
            DELETE FROM mirror_claims
            WHERE thread_id = ? AND item_id = ?
            """,
            (thread_id, item_id),
        )
        self.connection.commit()

    def thread_has_mirrored_items(self, thread_id: str) -> bool:
        row = self.connection.execute(
            "SELECT 1 FROM mirrored_items WHERE thread_id = ? LIMIT 1",
            (thread_id,),
        ).fetchone()
        return row is not None

    def match_visible_item_delivery_intent(
        self,
        *,
        thread_id: str,
        turn_id: str,
        item_id: str,
        item_type: str,
        item_origin: str,
        content_fingerprint: str,
    ) -> bool:
        if item_origin not in {"history", "notification"}:
            raise ValueError("unsupported visible item origin")
        counterpart_origin = (
            "notification" if item_origin == "history" else "history"
        )
        with self.connection:
            existing = self.connection.execute(
                """
                SELECT id FROM visible_item_delivery_intents
                WHERE thread_id = ?
                  AND (
                      primary_item_id = ?
                      OR counterpart_item_id = ?
                  )
                LIMIT 1
                """,
                (thread_id, item_id, item_id),
            ).fetchone()
            if existing is not None:
                return True
            counterpart = self.connection.execute(
                """
                SELECT id FROM visible_item_delivery_intents
                WHERE thread_id = ?
                  AND turn_id = ?
                  AND item_type = ?
                  AND content_fingerprint = ?
                  AND item_origin = ?
                  AND counterpart_item_id IS NULL
                ORDER BY id
                LIMIT 1
                """,
                (
                    thread_id,
                    turn_id,
                    item_type,
                    content_fingerprint,
                    counterpart_origin,
                ),
            ).fetchone()
            if counterpart is None:
                return False
            self.connection.execute(
                """
                UPDATE visible_item_delivery_intents
                SET counterpart_item_id = ?
                WHERE id = ?
                """,
                (item_id, int(counterpart["id"])),
            )
            return True

    def reserve_visible_item_delivery_intent(
        self,
        *,
        thread_id: str,
        turn_id: str,
        item_id: str,
        item_type: str,
        item_origin: str,
        content_fingerprint: str,
    ) -> None:
        if item_origin not in {"history", "notification"}:
            raise ValueError("unsupported visible item origin")
        self.connection.execute(
            """
            INSERT OR IGNORE INTO visible_item_delivery_intents(
                thread_id, turn_id, item_type, item_origin,
                content_fingerprint, primary_item_id,
                counterpart_item_id, state, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, NULL, 'reserved', ?)
            """,
            (
                thread_id,
                turn_id,
                item_type,
                item_origin,
                content_fingerprint,
                item_id,
                utc_now(),
            ),
        )
        self.connection.commit()

    def mark_visible_item_delivery_outcome_unknown(
        self,
        thread_id: str,
        item_id: str,
    ) -> bool:
        cursor = self.connection.execute(
            """
            UPDATE visible_item_delivery_intents
            SET state = 'outcome_unknown'
            WHERE
                thread_id = ?
                AND (
                    primary_item_id = ?
                    OR counterpart_item_id = ?
                )
                AND state = 'reserved'
            """,
            (thread_id, item_id, item_id),
        )
        self.connection.commit()
        if cursor.rowcount == 1:
            return True
        row = self.connection.execute(
            """
            SELECT 1 FROM visible_item_delivery_intents
            WHERE
                thread_id = ?
                AND (
                    primary_item_id = ?
                    OR counterpart_item_id = ?
                )
                AND state = 'outcome_unknown'
            """,
            (thread_id, item_id, item_id),
        ).fetchone()
        return row is not None

    def clear_visible_item_delivery_intent(
        self,
        *,
        thread_id: str,
        item_id: str,
    ) -> None:
        self.connection.execute(
            """
            DELETE FROM visible_item_delivery_intents
            WHERE thread_id = ?
              AND (
                  primary_item_id = ?
                  OR counterpart_item_id = ?
              )
            """,
            (thread_id, item_id, item_id),
        )
        self.connection.commit()

    def match_visible_item_delivery(
        self,
        *,
        thread_id: str,
        turn_id: str,
        item_id: str,
        item_type: str,
        item_origin: str,
        content_fingerprint: str,
    ) -> int | None:
        if item_origin not in {"history", "notification"}:
            raise ValueError("unsupported visible item origin")
        counterpart_origin = (
            "notification" if item_origin == "history" else "history"
        )
        with self.connection:
            existing = self.connection.execute(
                """
                SELECT telegram_message_id
                FROM visible_item_deliveries
                WHERE thread_id = ?
                  AND (
                      primary_item_id = ?
                      OR counterpart_item_id = ?
                  )
                LIMIT 1
                """,
                (thread_id, item_id, item_id),
            ).fetchone()
            if existing is not None:
                return int(existing["telegram_message_id"])
            counterpart = self.connection.execute(
                """
                SELECT id, telegram_message_id
                FROM visible_item_deliveries
                WHERE thread_id = ?
                  AND turn_id = ?
                  AND item_type = ?
                  AND content_fingerprint = ?
                  AND item_origin = ?
                  AND counterpart_item_id IS NULL
                ORDER BY id
                LIMIT 1
                """,
                (
                    thread_id,
                    turn_id,
                    item_type,
                    content_fingerprint,
                    counterpart_origin,
                ),
            ).fetchone()
            if counterpart is None:
                return None
            self.connection.execute(
                """
                UPDATE visible_item_deliveries
                SET counterpart_item_id = ?
                WHERE id = ?
                """,
                (item_id, int(counterpart["id"])),
            )
            return int(counterpart["telegram_message_id"])

    def record_visible_item_delivery(
        self,
        *,
        thread_id: str,
        turn_id: str,
        item_id: str,
        item_type: str,
        item_origin: str,
        content_fingerprint: str,
        telegram_message_id: int,
    ) -> None:
        if item_origin not in {"history", "notification"}:
            raise ValueError("unsupported visible item origin")
        with self.connection:
            self.connection.execute(
                """
                INSERT OR IGNORE INTO visible_item_deliveries(
                    thread_id, turn_id, item_type, item_origin,
                    content_fingerprint, primary_item_id,
                    counterpart_item_id, telegram_message_id, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, NULL, ?, ?)
                """,
                (
                    thread_id,
                    turn_id,
                    item_type,
                    item_origin,
                    content_fingerprint,
                    item_id,
                    telegram_message_id,
                    utc_now(),
                ),
            )
            self.connection.execute(
                """
                DELETE FROM visible_item_delivery_intents
                WHERE thread_id = ?
                  AND (
                      primary_item_id = ?
                      OR counterpart_item_id = ?
                  )
                """,
                (thread_id, item_id, item_id),
            )

    def mark_mirrored_item(
        self,
        thread_id: str,
        item_id: str,
        item_type: str,
        telegram_message_id: int | None,
    ) -> None:
        self.connection.execute(
            """
            INSERT OR IGNORE INTO mirrored_items(
                thread_id, item_id, item_type, telegram_message_id, created_at
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                thread_id,
                item_id,
                item_type,
                telegram_message_id,
                utc_now(),
            ),
        )
        self.connection.commit()

    def mark_mirrored_items(
        self,
        items: list[tuple[str, str, str, int | None]],
    ) -> None:
        if not items:
            return
        now = utc_now()
        with self.connection:
            self.connection.executemany(
                """
                INSERT OR IGNORE INTO mirrored_items(
                    thread_id, item_id, item_type,
                    telegram_message_id, created_at
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                [
                    (
                        thread_id,
                        item_id,
                        item_type,
                        telegram_message_id,
                        now,
                    )
                    for thread_id, item_id, item_type, telegram_message_id in items
                ],
            )

    @staticmethod
    def _outbound_delivery_from_row(
        row: sqlite3.Row | None,
    ) -> OutboundDelivery | None:
        if row is None:
            return None
        return OutboundDelivery(
            kind=str(row["kind"]),
            source_key=str(row["source_key"]),
            thread_id=(
                None if row["thread_id"] is None else str(row["thread_id"])
            ),
            chat_id=int(row["chat_id"]),
            topic_id=int(row["topic_id"]),
            reply_to_message_id=int(row["reply_to_message_id"]),
            state=str(row["state"]),
            telegram_message_id=(
                None
                if row["telegram_message_id"] is None
                else int(row["telegram_message_id"])
            ),
            created_at=str(row["created_at"]),
            updated_at=str(row["updated_at"]),
        )

    @staticmethod
    def _validate_outbound_kind(kind: str) -> None:
        if kind not in {"queue_announcement", "status_card"}:
            raise ValueError("unsupported outbound delivery kind")

    @staticmethod
    def _status_card_source_key(chat_id: int, request_message_id: int) -> str:
        return f"{chat_id}:{request_message_id}"

    def outbound_delivery(
        self,
        kind: str,
        source_key: str,
    ) -> OutboundDelivery | None:
        self._validate_outbound_kind(kind)
        row = self.connection.execute(
            """
            SELECT * FROM outbound_deliveries
            WHERE kind = ? AND source_key = ?
            """,
            (kind, source_key),
        ).fetchone()
        return self._outbound_delivery_from_row(row)

    def reserve_outbound_delivery(
        self,
        *,
        kind: str,
        source_key: str,
        thread_id: str | None,
        chat_id: int,
        topic_id: int,
        reply_to_message_id: int,
    ) -> OutboundDelivery:
        self._validate_outbound_kind(kind)
        if not source_key:
            raise ValueError("outbound delivery source key cannot be empty")
        now = utc_now()
        with self.connection:
            cursor = self.connection.execute(
                """
                INSERT OR IGNORE INTO outbound_deliveries(
                    kind, source_key, thread_id, chat_id, topic_id,
                    reply_to_message_id, state, telegram_message_id,
                    created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, 'reserved', NULL, ?, ?)
                """,
                (
                    kind,
                    source_key,
                    thread_id,
                    chat_id,
                    topic_id,
                    reply_to_message_id,
                    now,
                    now,
                ),
            )
            row = self.connection.execute(
                """
                SELECT * FROM outbound_deliveries
                WHERE kind = ? AND source_key = ?
                """,
                (kind, source_key),
            ).fetchone()
            delivery = self._outbound_delivery_from_row(row)
            assert delivery is not None
            expected_context = (
                thread_id,
                chat_id,
                topic_id,
                reply_to_message_id,
            )
            actual_context = (
                delivery.thread_id,
                delivery.chat_id,
                delivery.topic_id,
                delivery.reply_to_message_id,
            )
            if actual_context != expected_context:
                raise RuntimeError(
                    "outbound delivery key is already reserved "
                    "for different parameters"
                )
        return replace(delivery, newly_reserved=cursor.rowcount == 1)

    def mark_outbound_delivery_outcome_unknown(
        self,
        kind: str,
        source_key: str,
    ) -> OutboundDelivery:
        self._validate_outbound_kind(kind)
        with self.connection:
            row = self.connection.execute(
                """
                SELECT state FROM outbound_deliveries
                WHERE kind = ? AND source_key = ?
                """,
                (kind, source_key),
            ).fetchone()
            if row is None:
                raise RuntimeError("outbound delivery was not reserved")
            if str(row["state"]) == "reserved":
                self.connection.execute(
                    """
                    UPDATE outbound_deliveries
                    SET state = 'outcome_unknown', updated_at = ?
                    WHERE
                        kind = ?
                        AND source_key = ?
                        AND state = 'reserved'
                    """,
                    (utc_now(), kind, source_key),
                )
        delivery = self.outbound_delivery(kind, source_key)
        assert delivery is not None
        return delivery

    def _complete_outbound_delivery_locked(
        self,
        *,
        kind: str,
        source_key: str,
        telegram_message_id: int,
    ) -> OutboundDelivery:
        row = self.connection.execute(
            """
            SELECT * FROM outbound_deliveries
            WHERE kind = ? AND source_key = ?
            """,
            (kind, source_key),
        ).fetchone()
        delivery = self._outbound_delivery_from_row(row)
        if delivery is None:
            raise RuntimeError("outbound delivery was not reserved")
        if delivery.state == "delivered":
            if delivery.telegram_message_id != telegram_message_id:
                raise RuntimeError(
                    "outbound delivery already recorded a different result"
                )
            return delivery
        self.connection.execute(
            """
            UPDATE outbound_deliveries
            SET
                state = 'delivered',
                telegram_message_id = ?,
                updated_at = ?
            WHERE
                kind = ?
                AND source_key = ?
                AND state IN ('reserved', 'outcome_unknown')
            """,
            (telegram_message_id, utc_now(), kind, source_key),
        )
        row = self.connection.execute(
            """
            SELECT * FROM outbound_deliveries
            WHERE kind = ? AND source_key = ?
            """,
            (kind, source_key),
        ).fetchone()
        completed = self._outbound_delivery_from_row(row)
        assert completed is not None
        return completed

    def complete_outbound_delivery(
        self,
        kind: str,
        source_key: str,
        telegram_message_id: int,
    ) -> OutboundDelivery:
        self._validate_outbound_kind(kind)
        with self.connection:
            return self._complete_outbound_delivery_locked(
                kind=kind,
                source_key=source_key,
                telegram_message_id=telegram_message_id,
            )

    def clear_outbound_delivery_after_definite_failure(
        self,
        kind: str,
        source_key: str,
    ) -> bool:
        self._validate_outbound_kind(kind)
        with self.connection:
            row = self.connection.execute(
                """
                SELECT state FROM outbound_deliveries
                WHERE kind = ? AND source_key = ?
                """,
                (kind, source_key),
            ).fetchone()
            if row is None:
                return False
            if str(row["state"]) == "delivered":
                raise RuntimeError("delivered outbound state cannot be cleared")
            cursor = self.connection.execute(
                """
                DELETE FROM outbound_deliveries
                WHERE
                    kind = ?
                    AND source_key = ?
                    AND state IN ('reserved', 'outcome_unknown')
                """,
                (kind, source_key),
            )
        return cursor.rowcount == 1

    def reserve_queue_announcement_delivery(
        self,
        queue_id: int,
    ) -> OutboundDelivery:
        queued = self.queued_message(queue_id)
        if queued is None:
            raise RuntimeError("queued message does not exist")
        return self.reserve_outbound_delivery(
            kind="queue_announcement",
            source_key=str(queue_id),
            thread_id=queued.thread_id,
            chat_id=queued.chat_id,
            topic_id=queued.topic_id,
            reply_to_message_id=queued.telegram_message_id,
        )

    def mark_queue_announcement_outcome_unknown(
        self,
        queue_id: int,
    ) -> OutboundDelivery:
        return self.mark_outbound_delivery_outcome_unknown(
            "queue_announcement",
            str(queue_id),
        )

    def complete_queue_announcement_delivery(
        self,
        queue_id: int,
        telegram_message_id: int,
    ) -> OutboundDelivery:
        with self.connection:
            delivery = self._complete_outbound_delivery_locked(
                kind="queue_announcement",
                source_key=str(queue_id),
                telegram_message_id=telegram_message_id,
            )
            cursor = self.connection.execute(
                """
                UPDATE queued_messages
                SET status_message_id = ?, updated_at = ?
                WHERE id = ?
                """,
                (telegram_message_id, utc_now(), queue_id),
            )
            if cursor.rowcount != 1:
                raise RuntimeError("queued message does not exist")
        return delivery

    def clear_queue_announcement_delivery_after_definite_failure(
        self,
        queue_id: int,
    ) -> bool:
        return self.clear_outbound_delivery_after_definite_failure(
            "queue_announcement",
            str(queue_id),
        )

    def reserve_status_card_delivery(
        self,
        *,
        thread_id: str | None,
        chat_id: int,
        topic_id: int,
        request_message_id: int,
    ) -> OutboundDelivery:
        if thread_id is None and topic_id not in {0, 1}:
            raise ValueError("general status delivery requires the general topic")
        if thread_id is not None and topic_id in {0, 1}:
            raise ValueError("thread status delivery requires a forum topic")
        return self.reserve_outbound_delivery(
            kind="status_card",
            source_key=self._status_card_source_key(
                chat_id,
                request_message_id,
            ),
            thread_id=thread_id,
            chat_id=chat_id,
            topic_id=topic_id,
            reply_to_message_id=request_message_id,
        )

    def mark_status_card_outcome_unknown(
        self,
        *,
        chat_id: int,
        request_message_id: int,
    ) -> OutboundDelivery:
        return self.mark_outbound_delivery_outcome_unknown(
            "status_card",
            self._status_card_source_key(chat_id, request_message_id),
        )

    def _activate_control_status_card_locked(
        self,
        *,
        thread_id: str,
        chat_id: int,
        topic_id: int,
        telegram_message_id: int,
        now: str,
    ) -> None:
        self.connection.execute(
            """
            UPDATE control_status_cards
            SET status = 'superseded', updated_at = ?
            WHERE thread_id = ? AND status = 'active'
            """,
            (now, thread_id),
        )
        self.connection.execute(
            """
            INSERT INTO control_status_cards(
                thread_id, chat_id, topic_id, telegram_message_id,
                status, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, 'active', ?, ?)
            ON CONFLICT(chat_id, telegram_message_id) DO UPDATE SET
                thread_id = excluded.thread_id,
                topic_id = excluded.topic_id,
                status = 'active',
                updated_at = excluded.updated_at
            """,
            (
                thread_id,
                chat_id,
                topic_id,
                telegram_message_id,
                now,
                now,
            ),
        )

    def complete_status_card_delivery(
        self,
        *,
        chat_id: int,
        request_message_id: int,
        telegram_message_id: int,
    ) -> OutboundDelivery:
        source_key = self._status_card_source_key(
            chat_id,
            request_message_id,
        )
        with self.connection:
            existing_row = self.connection.execute(
                """
                SELECT state FROM outbound_deliveries
                WHERE kind = 'status_card' AND source_key = ?
                """,
                (source_key,),
            ).fetchone()
            was_delivered = (
                existing_row is not None
                and str(existing_row["state"]) == "delivered"
            )
            delivery = self._complete_outbound_delivery_locked(
                kind="status_card",
                source_key=source_key,
                telegram_message_id=telegram_message_id,
            )
            if delivery.chat_id != chat_id:
                raise RuntimeError(
                    "status delivery belongs to a different chat"
                )
            if delivery.thread_id is not None and not was_delivered:
                self._activate_control_status_card_locked(
                    thread_id=delivery.thread_id,
                    chat_id=delivery.chat_id,
                    topic_id=delivery.topic_id,
                    telegram_message_id=telegram_message_id,
                    now=utc_now(),
                )
        return delivery

    def clear_status_card_delivery_after_definite_failure(
        self,
        *,
        chat_id: int,
        request_message_id: int,
    ) -> bool:
        return self.clear_outbound_delivery_after_definite_failure(
            "status_card",
            self._status_card_source_key(chat_id, request_message_id),
        )

    def enqueue(
        self,
        *,
        thread_id: str,
        chat_id: int,
        topic_id: int,
        telegram_message_id: int,
        text: str,
        client_id: str,
        local_inputs: tuple[LocalInput, ...] = (),
    ) -> QueuedMessage:
        normalized_inputs = normalize_local_inputs(local_inputs)
        self._validate_new_local_inputs(normalized_inputs)
        serialized_inputs = json.dumps(
            [item.to_payload() for item in normalized_inputs],
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )
        existing = self.connection.execute(
            "SELECT * FROM queued_messages WHERE client_id = ? ORDER BY id LIMIT 1",
            (client_id,),
        ).fetchone()
        if existing is not None:
            queued = self._queue_from_row(existing)
            assert queued is not None
            if (
                queued.thread_id != thread_id
                or queued.chat_id != chat_id
                or queued.topic_id != topic_id
                or queued.telegram_message_id != telegram_message_id
                or queued.text != text
                or queued.local_inputs != normalized_inputs
            ):
                raise RuntimeError(
                    "queue client id already belongs to different content"
                )
            return queued
        now = utc_now()
        cursor = self.connection.execute(
            """
            INSERT OR IGNORE INTO queued_messages(
                thread_id, chat_id, topic_id, telegram_message_id,
                text, client_id, status, local_inputs_json,
                created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?)
            """,
            (
                thread_id,
                chat_id,
                topic_id,
                telegram_message_id,
                text,
                client_id,
                serialized_inputs,
                now,
                now,
            ),
        )
        self.connection.commit()
        if cursor.rowcount == 1:
            queued = self.queued_message(int(cursor.lastrowid))
        else:
            row = self.connection.execute(
                """
                SELECT * FROM queued_messages
                WHERE client_id = ?
                ORDER BY id
                LIMIT 1
                """,
                (client_id,),
            ).fetchone()
            queued = self._queue_from_row(row)
        assert queued is not None
        return queued

    @staticmethod
    def _queue_from_row(row: sqlite3.Row | None) -> QueuedMessage | None:
        if row is None:
            return None
        try:
            raw_inputs = json.loads(str(row["local_inputs_json"]))
            if not isinstance(raw_inputs, list):
                raise ValueError("local inputs must be a list")
            local_inputs = normalize_local_inputs(raw_inputs)
        except (json.JSONDecodeError, TypeError, ValueError):
            raise SchemaVersionError(
                "queued message contains invalid local inputs"
            ) from None
        return QueuedMessage(
            queue_id=int(row["id"]),
            thread_id=str(row["thread_id"]),
            chat_id=int(row["chat_id"]),
            topic_id=int(row["topic_id"]),
            telegram_message_id=int(row["telegram_message_id"]),
            text=str(row["text"]),
            client_id=str(row["client_id"]),
            status=str(row["status"]),
            status_message_id=(
                None
                if row["status_message_id"] is None
                else int(row["status_message_id"])
            ),
            dispatch_started_at=(
                None
                if row["dispatch_started_at"] is None
                else str(row["dispatch_started_at"])
            ),
            dispatch_history_miss_count=int(
                row["dispatch_history_miss_count"]
            ),
            dispatch_last_miss_at=(
                None
                if row["dispatch_last_miss_at"] is None
                else str(row["dispatch_last_miss_at"])
            ),
            local_inputs=local_inputs,
        )

    def _validate_new_local_inputs(
        self,
        local_inputs: tuple[LocalInput, ...],
    ) -> None:
        if not local_inputs:
            return
        try:
            media_root = (self.path.parent / "media").resolve(strict=True)
        except (FileNotFoundError, OSError, RuntimeError):
            raise ValueError(
                "local input must be an existing bridge media file"
            ) from None
        for item in local_inputs:
            candidate = Path(item.path)
            try:
                resolved = candidate.resolve(strict=True)
                resolved.relative_to(media_root)
                status = resolved.stat()
            except (FileNotFoundError, OSError, RuntimeError, ValueError):
                raise ValueError(
                    "local input must be an existing bridge media file"
                ) from None
            if (
                not candidate.is_absolute()
                or candidate.is_symlink()
                or not stat.S_ISREG(status.st_mode)
                or status.st_uid != os.getuid()
            ):
                raise ValueError(
                    "local input must be an owner-controlled regular file"
                )

    def active_local_input_paths(self) -> set[Path]:
        rows = self.connection.execute(
            """
            SELECT local_inputs_json
            FROM queued_messages
            WHERE status IN ('pending', 'dispatching')
            """
        ).fetchall()
        paths: set[Path] = set()
        for row in rows:
            try:
                raw_inputs = json.loads(str(row["local_inputs_json"]))
                if not isinstance(raw_inputs, list):
                    raise ValueError
                inputs = normalize_local_inputs(raw_inputs)
            except (json.JSONDecodeError, TypeError, ValueError):
                raise SchemaVersionError(
                    "active queued message contains invalid local inputs"
                ) from None
            paths.update(Path(item.path) for item in inputs)
        return paths

    def queued_message(self, queue_id: int) -> QueuedMessage | None:
        row = self.connection.execute(
            "SELECT * FROM queued_messages WHERE id = ?", (queue_id,)
        ).fetchone()
        return self._queue_from_row(row)

    def queued_message_for_client_id(
        self,
        client_id: str,
    ) -> QueuedMessage | None:
        row = self.connection.execute(
            """
            SELECT * FROM queued_messages
            WHERE client_id = ?
            ORDER BY id
            LIMIT 1
            """,
            (client_id,),
        ).fetchone()
        return self._queue_from_row(row)

    def active_queued_messages(self, thread_id: str) -> list[QueuedMessage]:
        rows = self.connection.execute(
            """
            SELECT * FROM queued_messages
            WHERE thread_id = ?
              AND status IN ('pending', 'dispatching')
            ORDER BY id
            """,
            (thread_id,),
        ).fetchall()
        return [
            queued
            for row in rows
            if (queued := self._queue_from_row(row)) is not None
        ]

    def next_queued(self, thread_id: str) -> QueuedMessage | None:
        row = self.connection.execute(
            """
            SELECT * FROM queued_messages
            WHERE thread_id = ? AND status = 'pending'
            ORDER BY id
            LIMIT 1
            """,
            (thread_id,),
        ).fetchone()
        return self._queue_from_row(row)

    def pending_queue_thread_ids(self) -> list[str]:
        """Return dispatchable threads in durable global FIFO order.

        A thread with an outcome-unknown dispatch reservation is deliberately
        excluded so a later message cannot overtake a request that may already
        have reached Codex.
        """

        rows = self.connection.execute(
            """
            SELECT pending.thread_id, MIN(pending.id) AS first_pending_id
            FROM queued_messages AS pending
            WHERE
                pending.status = 'pending'
                AND NOT EXISTS (
                    SELECT 1
                    FROM queued_messages AS uncertain
                    WHERE
                        uncertain.thread_id = pending.thread_id
                        AND uncertain.status = 'dispatching'
                )
            GROUP BY pending.thread_id
            ORDER BY first_pending_id, pending.thread_id
            """
        ).fetchall()
        return [str(row["thread_id"]) for row in rows]

    def dispatching_queue_thread_ids(self) -> set[str]:
        """Return threads whose Codex mutation outcome is still unknown."""

        rows = self.connection.execute(
            """
            SELECT DISTINCT thread_id
            FROM queued_messages
            WHERE status = 'dispatching'
            """
        ).fetchall()
        return {str(row["thread_id"]) for row in rows}

    def claim_queue(self, queue_id: int) -> bool:
        now = utc_now()
        cursor = self.connection.execute(
            """
            UPDATE queued_messages
            SET
                status = 'dispatching',
                dispatch_started_at = ?,
                dispatch_history_miss_count = 0,
                dispatch_last_miss_at = NULL,
                updated_at = ?
            WHERE id = ? AND status = 'pending'
            """,
            (now, now, queue_id),
        )
        self.connection.commit()
        return cursor.rowcount == 1

    def claim_next_queued(self, thread_id: str) -> QueuedMessage | None:
        with self.connection:
            row = self.connection.execute(
                """
                SELECT * FROM queued_messages
                WHERE thread_id = ? AND status = 'pending'
                ORDER BY id
                LIMIT 1
                """,
                (thread_id,),
            ).fetchone()
            queued = self._queue_from_row(row)
            if queued is None:
                return None
            cursor = self.connection.execute(
                """
                UPDATE queued_messages
                SET
                    status = 'dispatching',
                    dispatch_started_at = ?,
                    dispatch_history_miss_count = 0,
                    dispatch_last_miss_at = NULL,
                    updated_at = ?
                WHERE id = ? AND status = 'pending'
                """,
                (
                    (now := utc_now()),
                    now,
                    queued.queue_id,
                ),
            )
            if cursor.rowcount != 1:
                return None
        return self.queued_message(queued.queue_id)

    def mark_queue(self, queue_id: int, status: str) -> None:
        now = utc_now()
        if status == "dispatching":
            self.connection.execute(
                """
                UPDATE queued_messages
                SET
                    status = 'dispatching',
                    dispatch_started_at = CASE
                        WHEN status = 'dispatching'
                        THEN COALESCE(dispatch_started_at, ?)
                        ELSE ?
                    END,
                    dispatch_history_miss_count = CASE
                        WHEN status = 'dispatching'
                        THEN dispatch_history_miss_count
                        ELSE 0
                    END,
                    dispatch_last_miss_at = CASE
                        WHEN status = 'dispatching'
                        THEN dispatch_last_miss_at
                        ELSE NULL
                    END,
                    updated_at = ?
                WHERE id = ?
                """,
                (now, now, now, queue_id),
            )
        else:
            self.connection.execute(
                """
                UPDATE queued_messages
                SET
                    status = ?,
                    dispatch_started_at = NULL,
                    dispatch_history_miss_count = 0,
                    dispatch_last_miss_at = NULL,
                    updated_at = ?
                WHERE id = ?
                """,
                (status, now, queue_id),
            )
        self.connection.commit()

    def record_queue_dispatch_history_miss(
        self,
        queue_id: int,
        *,
        required_fresh_misses: int = 2,
        grace_seconds: int = 5,
        observed_at: str | None = None,
    ) -> bool:
        if required_fresh_misses < 2:
            raise ValueError("at least two fresh history misses are required")
        if grace_seconds < 0:
            raise ValueError("dispatch grace cannot be negative")
        observed = datetime.fromisoformat(observed_at or utc_now())
        if observed.tzinfo is None:
            raise ValueError("history observation time must include a timezone")
        observed_text = observed.astimezone(timezone.utc).isoformat()
        with self.connection:
            row = self.connection.execute(
                """
                SELECT
                    status,
                    dispatch_started_at,
                    dispatch_history_miss_count,
                    dispatch_last_miss_at
                FROM queued_messages
                WHERE id = ?
                """,
                (queue_id,),
            ).fetchone()
            if row is None:
                raise RuntimeError("queued message does not exist")
            if str(row["status"]) != "dispatching":
                return False
            started_text = row["dispatch_started_at"]
            if started_text is None:
                raise RuntimeError("dispatching message has no start time")
            started = datetime.fromisoformat(str(started_text))
            if started.tzinfo is None:
                raise RuntimeError("dispatch start time has no timezone")
            last_miss_text = row["dispatch_last_miss_at"]
            miss_count = int(row["dispatch_history_miss_count"])
            is_fresh = True
            if last_miss_text is not None:
                last_miss = datetime.fromisoformat(str(last_miss_text))
                if last_miss.tzinfo is None:
                    raise RuntimeError("history miss time has no timezone")
                is_fresh = observed > last_miss
            if is_fresh:
                miss_count = min(miss_count + 1, required_fresh_misses)
                self.connection.execute(
                    """
                    UPDATE queued_messages
                    SET
                        dispatch_history_miss_count = ?,
                        dispatch_last_miss_at = ?,
                        updated_at = ?
                    WHERE id = ? AND status = 'dispatching'
                    """,
                    (miss_count, observed_text, observed_text, queue_id),
                )
        grace_elapsed = (
            observed.astimezone(timezone.utc)
            - started.astimezone(timezone.utc)
        ) >= timedelta(seconds=grace_seconds)
        return miss_count >= required_fresh_misses and grace_elapsed

    def set_queue_status_message(
        self,
        queue_id: int,
        telegram_message_id: int,
    ) -> None:
        self.connection.execute(
            """
            UPDATE queued_messages
            SET status_message_id = ?, updated_at = ?
            WHERE id = ?
            """,
            (telegram_message_id, utc_now(), queue_id),
        )
        self.connection.commit()

    def observe_topic(self, chat_id: int, topic_id: int, title: str) -> None:
        self.connection.execute(
            """
            INSERT INTO observed_topics(chat_id, topic_id, title, observed_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(chat_id, topic_id) DO UPDATE SET
                title = excluded.title,
                observed_at = excluded.observed_at
            """,
            (chat_id, topic_id, title, utc_now()),
        )
        self.connection.commit()

    def forget_observed_topic(self, chat_id: int, topic_id: int) -> None:
        self.connection.execute(
            """
            DELETE FROM observed_topics
            WHERE chat_id = ? AND topic_id = ?
            """,
            (chat_id, topic_id),
        )
        self.connection.commit()

    def observed_topic_title(self, chat_id: int, topic_id: int) -> str | None:
        row = self.connection.execute(
            """
            SELECT title FROM observed_topics
            WHERE chat_id = ? AND topic_id = ?
            """,
            (chat_id, topic_id),
        ).fetchone()
        return None if row is None else str(row["title"])

    def save_pending_request(
        self,
        *,
        public_id: str,
        thread_id: str,
        request_kind: str,
        metadata: dict[str, Any],
        telegram_message_id: int | None = None,
        status: str = "pending",
    ) -> None:
        now = utc_now()
        self.connection.execute(
            """
            INSERT INTO pending_requests(
                public_id, thread_id, request_kind, telegram_message_id,
                status, metadata_json, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(public_id) DO UPDATE SET
                telegram_message_id = excluded.telegram_message_id,
                status = excluded.status,
                metadata_json = excluded.metadata_json,
                updated_at = excluded.updated_at
            """,
            (
                public_id,
                thread_id,
                request_kind,
                telegram_message_id,
                status,
                json.dumps(metadata, ensure_ascii=False),
                now,
                now,
            ),
        )
        self.connection.commit()

    def bind_outcome_unknown_pending_request(
        self,
        *,
        public_id: str,
        telegram_message_id: int,
    ) -> bool:
        if telegram_message_id <= 0:
            return False
        cursor = self.connection.execute(
            """
            UPDATE pending_requests
            SET
                telegram_message_id = ?,
                status = 'pending',
                updated_at = ?
            WHERE
                public_id = ?
                AND telegram_message_id IS NULL
                AND status = 'delivery_outcome_unknown'
            """,
            (telegram_message_id, utc_now(), public_id),
        )
        self.connection.commit()
        if cursor.rowcount == 1:
            return True
        row = self.connection.execute(
            """
            SELECT telegram_message_id, status
            FROM pending_requests
            WHERE public_id = ?
            """,
            (public_id,),
        ).fetchone()
        return bool(
            row is not None
            and str(row["status"]) == "pending"
            and int(row["telegram_message_id"] or 0) == telegram_message_id
        )

    def save_control_confirmation(
        self,
        *,
        public_id: str,
        thread_id: str,
        chat_id: int,
        topic_id: int,
        telegram_message_id: int,
        action: str,
    ) -> None:
        if action != "stop":
            raise ValueError("unsupported control confirmation action")
        now = utc_now()
        self.connection.execute(
            """
            INSERT INTO control_confirmations(
                public_id, thread_id, chat_id, topic_id,
                telegram_message_id, action, status, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?)
            """,
            (
                public_id,
                thread_id,
                chat_id,
                topic_id,
                telegram_message_id,
                action,
                now,
                now,
            ),
        )
        self.connection.commit()

    @staticmethod
    def _control_prompt_from_row(
        row: sqlite3.Row | None,
    ) -> ControlPrompt | None:
        if row is None:
            return None
        return ControlPrompt(
            public_id=str(row["public_id"]),
            thread_id=str(row["thread_id"]),
            chat_id=int(row["chat_id"]),
            topic_id=int(row["topic_id"]),
            telegram_message_id=int(row["telegram_message_id"]),
            mode=str(row["mode"]),
            status=str(row["status"]),
        )

    def save_control_prompt(
        self,
        *,
        public_id: str,
        thread_id: str,
        chat_id: int,
        topic_id: int,
        telegram_message_id: int,
        mode: str,
    ) -> ControlPrompt:
        if mode not in {"steer", "queue"}:
            raise ValueError("unsupported control prompt mode")
        now = utc_now()
        with self.connection:
            self.connection.execute(
                """
                UPDATE control_prompts
                SET status = 'expired', updated_at = ?
                WHERE
                    thread_id = ?
                    AND mode = ?
                    AND status = 'pending'
                """,
                (now, thread_id, mode),
            )
            self.connection.execute(
                """
                INSERT INTO control_prompts(
                    public_id, thread_id, chat_id, topic_id,
                    telegram_message_id, mode, status, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?)
                """,
                (
                    public_id,
                    thread_id,
                    chat_id,
                    topic_id,
                    telegram_message_id,
                    mode,
                    now,
                    now,
                ),
            )
        prompt = self.control_prompt(
            chat_id=chat_id,
            topic_id=topic_id,
            telegram_message_id=telegram_message_id,
        )
        assert prompt is not None
        return prompt

    def save_control_status_card(
        self,
        *,
        thread_id: str,
        chat_id: int,
        topic_id: int,
        telegram_message_id: int,
    ) -> None:
        now = utc_now()
        with self.connection:
            self._activate_control_status_card_locked(
                thread_id=thread_id,
                chat_id=chat_id,
                topic_id=topic_id,
                telegram_message_id=telegram_message_id,
                now=now,
            )

    def control_status_card_matches(
        self,
        *,
        thread_id: str,
        chat_id: int,
        topic_id: int,
        telegram_message_id: int,
    ) -> bool:
        row = self.connection.execute(
            """
            SELECT 1 FROM control_status_cards
            WHERE
                thread_id = ?
                AND chat_id = ?
                AND topic_id = ?
                AND telegram_message_id = ?
                AND status = 'active'
            """,
            (
                thread_id,
                chat_id,
                topic_id,
                telegram_message_id,
            ),
        ).fetchone()
        return row is not None

    def claim_control_status_action(
        self,
        *,
        thread_id: str,
        chat_id: int,
        topic_id: int,
        telegram_message_id: int,
        action: str,
    ) -> bool:
        if action not in {"steer", "queue", "stop"}:
            raise ValueError("unsupported control status action")
        with self.connection:
            active = self.connection.execute(
                """
                SELECT 1 FROM control_status_cards
                WHERE
                    thread_id = ?
                    AND chat_id = ?
                    AND topic_id = ?
                    AND telegram_message_id = ?
                    AND status = 'active'
                """,
                (
                    thread_id,
                    chat_id,
                    topic_id,
                    telegram_message_id,
                ),
            ).fetchone()
            if active is None:
                return False
            cursor = self.connection.execute(
                """
                INSERT OR IGNORE INTO control_status_actions(
                    thread_id, chat_id, topic_id, telegram_message_id,
                    action, state, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, 'claimed', ?, ?)
                """,
                (
                    thread_id,
                    chat_id,
                    topic_id,
                    telegram_message_id,
                    action,
                    (now := utc_now()),
                    now,
                ),
            )
        return cursor.rowcount == 1

    def complete_control_status_action(
        self,
        *,
        thread_id: str,
        chat_id: int,
        topic_id: int,
        telegram_message_id: int,
        action: str,
    ) -> bool:
        if action not in {"steer", "queue", "stop"}:
            raise ValueError("unsupported control status action")
        cursor = self.connection.execute(
            """
            UPDATE control_status_actions
            SET state = 'completed', updated_at = ?
            WHERE
                thread_id = ?
                AND chat_id = ?
                AND topic_id = ?
                AND telegram_message_id = ?
                AND action = ?
                AND state = 'claimed'
            """,
            (
                utc_now(),
                thread_id,
                chat_id,
                topic_id,
                telegram_message_id,
                action,
            ),
        )
        self.connection.commit()
        return cursor.rowcount == 1

    def release_control_status_action(
        self,
        *,
        thread_id: str,
        chat_id: int,
        topic_id: int,
        telegram_message_id: int,
        action: str,
    ) -> bool:
        if action not in {"steer", "queue", "stop"}:
            raise ValueError("unsupported control status action")
        cursor = self.connection.execute(
            """
            DELETE FROM control_status_actions
            WHERE
                thread_id = ?
                AND chat_id = ?
                AND topic_id = ?
                AND telegram_message_id = ?
                AND action = ?
                AND state = 'claimed'
            """,
            (
                thread_id,
                chat_id,
                topic_id,
                telegram_message_id,
                action,
            ),
        )
        self.connection.commit()
        return cursor.rowcount == 1

    def control_prompt(
        self,
        *,
        chat_id: int,
        topic_id: int,
        telegram_message_id: int,
    ) -> ControlPrompt | None:
        row = self.connection.execute(
            """
            SELECT * FROM control_prompts
            WHERE
                chat_id = ?
                AND topic_id = ?
                AND telegram_message_id = ?
            """,
            (chat_id, topic_id, telegram_message_id),
        ).fetchone()
        return self._control_prompt_from_row(row)

    def claim_control_prompt(
        self,
        *,
        chat_id: int,
        topic_id: int,
        telegram_message_id: int,
    ) -> ControlPrompt | None:
        with self.connection:
            cursor = self.connection.execute(
                """
                UPDATE control_prompts
                SET status = 'consumed', updated_at = ?
                WHERE
                    chat_id = ?
                    AND topic_id = ?
                    AND telegram_message_id = ?
                    AND status = 'pending'
                """,
                (
                    utc_now(),
                    chat_id,
                    topic_id,
                    telegram_message_id,
                ),
            )
            if cursor.rowcount != 1:
                return None
            row = self.connection.execute(
                """
                SELECT * FROM control_prompts
                WHERE
                    chat_id = ?
                    AND topic_id = ?
                    AND telegram_message_id = ?
                """,
                (chat_id, topic_id, telegram_message_id),
            ).fetchone()
        return self._control_prompt_from_row(row)

    def resolve_control_confirmation(
        self,
        *,
        public_id: str,
        thread_id: str,
        chat_id: int,
        topic_id: int,
        telegram_message_id: int,
        status: str,
    ) -> bool:
        if status not in {"confirmed", "cancelled"}:
            raise ValueError("invalid control confirmation status")
        cursor = self.connection.execute(
            """
            UPDATE control_confirmations
            SET status = ?, updated_at = ?
            WHERE
                public_id = ?
                AND thread_id = ?
                AND chat_id = ?
                AND topic_id = ?
                AND telegram_message_id = ?
                AND action = 'stop'
                AND status = 'pending'
            """,
            (
                status,
                utc_now(),
                public_id,
                thread_id,
                chat_id,
                topic_id,
                telegram_message_id,
            ),
        )
        self.connection.commit()
        return cursor.rowcount == 1

    def resolve_pending_request(self, public_id: str, status: str) -> None:
        self.connection.execute(
            """
            UPDATE pending_requests
            SET status = ?, updated_at = ?
            WHERE public_id = ?
            """,
            (status, utc_now(), public_id),
        )
        self.connection.commit()

    def expire_pending_prompt_cards(self) -> list[tuple[str, int]]:
        rows = self.connection.execute(
            """
            SELECT thread_id, telegram_message_id
            FROM pending_requests
            WHERE
                status IN (
                    'pending',
                    'delivery_reserved',
                    'delivery_outcome_unknown'
                )
                AND telegram_message_id IS NOT NULL
            """
        ).fetchall()
        cursor = self.connection.execute(
            """
            UPDATE pending_requests
            SET status = 'expired_restart', updated_at = ?
            WHERE status IN (
                'pending',
                'delivery_reserved',
                'delivery_outcome_unknown'
            )
            """,
            (utc_now(),),
        )
        if cursor.rowcount:
            self.connection.commit()
        return [
            (str(row["thread_id"]), int(row["telegram_message_id"]))
            for row in rows
        ]
