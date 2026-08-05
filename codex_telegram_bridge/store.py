Warning: truncated output (original token count: 45681)
Total output lines: 5379

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
        # authoritative history reads and the grace window both…15681 tokens truncated…         (thread_id, utc_now(), chat_id, topic_id),
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
