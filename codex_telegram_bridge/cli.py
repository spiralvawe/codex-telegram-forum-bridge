from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import logging
import os
import shutil
import signal
import stat
import subprocess
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

from .codex import (
    CodexAppServer,
    CodexProtocolCompatibilityError,
    codex_daemon_environment,
)
from .config import BridgeConfig, read_bot_token
from .deployment import deployment_health
from .service import BridgeService, bootstrap_group
from .service import (
    FINAL_ANSWER_CUSTOM_EMOJI_ALT,
    FINAL_ANSWER_CUSTOM_EMOJI_SETTING,
)
from .runtime_health import read_telegram_update_health
from .store import (
    OPERATIONAL_BACKUP_RETENTION,
    BridgeStore,
    CURRENT_SCHEMA_VERSION,
)
from .systemd_notify import SystemdNotifier
from .telegram import TelegramAPI, TelegramError


LOGGER = logging.getLogger(__name__)
MAX_HEALTHY_QUEUE_AGE_SECONDS = 6 * 60 * 60
MAX_HEALTHY_DISPATCH_AGE_SECONDS = 5 * 60


def requirements_allow_full_access(
    requirements: dict[str, Any] | None,
) -> bool:
    if requirements is None:
        return True
    if not isinstance(requirements, dict):
        return False
    approval_policies = requirements.get("allowedApprovalPolicies")
    sandbox_modes = requirements.get("allowedSandboxModes")
    permission_profiles = requirements.get("allowedPermissionProfiles")
    default_permissions = requirements.get("defaultPermissions")
    legacy_sandbox_allowed = (
        permission_profiles is None
        and default_permissions is None
    )
    approval_allowed = (
        approval_policies is None
        or (
            isinstance(approval_policies, list)
            and "never" in approval_policies
        )
    )
    sandbox_allowed = (
        sandbox_modes is None
        or (
            isinstance(sandbox_modes, list)
            and "danger-full-access" in sandbox_modes
        )
    )
    return legacy_sandbox_allowed and approval_allowed and sandbox_allowed


def detect_desktop_shared_socket(
    *,
    process_lines: list[str],
    lsof_lines: list[str],
    socket_path: Path,
) -> tuple[bool, bool | None]:
    desktop_running = any(
        line.strip().startswith(
            "/Applications/ChatGPT.app/Contents/MacOS/ChatGPT"
        )
        for line in process_lines
    )
    if not desktop_running:
        return False, None

    private_desktop_server = any(
        "features.code_mode_host=true app-server --analytics-default-enabled"
        in line
        and "--listen unix://" not in line
        for line in process_lines
    )
    listener_endpoints = {
        parts[5]
        for line in lsof_lines
        if len(parts := line.split()) >= 8
        and parts[-1] == str(socket_path)
    }
    desktop_attached = any(
        len(parts := line.split()) >= 8
        and parts[0].startswith("ChatGPT")
        and parts[-1].startswith("->")
        and parts[-1][2:] in listener_endpoints
        for line in lsof_lines
    )
    return True, bool(desktop_attached and not private_desktop_server)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Mirror one Codex workspace into Telegram forum topics."
    )
    parser.add_argument(
        "--config",
        default=None,
        help="Installed JSON instance configuration.",
    )
    parser.add_argument(
        "--workspace",
        default=None,
        help="Exact Codex project working directory.",
    )
    parser.add_argument(
        "--state-dir",
        default=None,
        help="Local state directory outside the repository.",
    )
    parser.add_argument(
        "--instance",
        default=None,
        help="Stable instance name used to isolate project state.",
    )
    parser.add_argument(
        "--secret-backend",
        choices=("macos-keychain", "proton-pass", "file"),
        default=None,
        help="Where the Telegram bot token is stored.",
    )
    parser.add_argument(
        "--secret-reference",
        default=None,
        help="Keychain service, Proton Pass item title/URI, or secret-file path.",
    )
    parser.add_argument(
        "--secret-vault",
        default=None,
        help="Optional Proton Pass vault name.",
    )
    parser.add_argument(
        "--keychain-service",
        default=None,
        help="Deprecated alias for --secret-reference on macOS.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable diagnostic logging without secret payloads.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    bootstrap = subparsers.add_parser(
        "bootstrap",
        help="Bind the Telegram forum after /connect is posted.",
    )
    bootstrap.add_argument(
        "--wait-seconds",
        type=int,
        default=0,
        help="Wait for /connect for up to this many seconds.",
    )
    subparsers.add_parser("doctor", help="Run read-only configuration checks.")
    backup = subparsers.add_parser(
        "backup",
        help="Create and verify an online backup of the bridge database.",
    )
    backup.add_argument(
        "--retention",
        type=int,
        default=OPERATIONAL_BACKUP_RETENTION,
        help="Number of operational database backups to retain.",
    )
    subparsers.add_parser(
        "probe-local",
        help="Check only local database and Codex app-server health.",
    )
    final_icon = subparsers.add_parser(
        "setup-final-icon",
        help="Create/reuse the Codex custom emoji for final answers.",
    )
    final_icon.add_argument(
        "--image",
        default=None,
        help="100x100 PNG/WEBP custom emoji source.",
    )
    subparsers.add_parser("sync-once", help="Create/update Topics once and exit.")
    subparsers.add_parser("serve", help="Run the long-lived bridge.")
    return parser


def create_config(args: argparse.Namespace) -> BridgeConfig:
    config_file = getattr(args, "config", None)
    if config_file:
        return BridgeConfig.from_file(config_file)
    return BridgeConfig.from_paths(
        workspace=getattr(args, "workspace", None) or Path.cwd(),
        state_dir=getattr(args, "state_dir", None),
        keychain_service=getattr(args, "keychain_service", None),
        instance_id=getattr(args, "instance", None),
        secret_backend=getattr(args, "secret_backend", None),
        secret_reference=getattr(args, "secret_reference", None),
        secret_vault=getattr(args, "secret_vault", None),
    )


def create_runtime(
    args: argparse.Namespace,
    *,
    read_only: bool = False,
) -> tuple[BridgeConfig, BridgeStore, TelegramAPI]:
    config = create_config(args)
    if not read_only:
        config.state_dir.mkdir(parents=True, exist_ok=True)
    store = BridgeStore(config.database_path, read_only=read_only)
    token = read_bot_token(config)
    telegram = TelegramAPI(token)
    return config, store, telegram


def queue_health_is_acceptable(queue: dict[str, int]) -> bool:
    age = int(queue.get("oldestActiveAgeSeconds", 0))
    dispatching = int(queue.get("dispatching", 0))
    if age > MAX_HEALTHY_QUEUE_AGE_SECONDS:
        return False
    return not (dispatching > 0 and age > MAX_HEALTHY_DISPATCH_AGE_SECONDS)


def mapping_health(
    *,
    active_threads: list[dict[str, Any]],
    topics: list[Any],
) -> dict[str, int | bool]:
    active_ids = {
        str(thread.get("id") or "")
        for thread in active_threads
        if str(thread.get("id") or "")
    }
    working_ids = {
        str(topic.thread_id)
        for topic in topics
        if not bool(topic.archived)
    }
    missing = active_ids - working_ids
    stale = working_ids - active_ids
    return {
        "activeThreads": len(active_ids),
        "openMappings": len(working_ids),
        "missingMappings": len(missing),
        "staleMappings": len(stale),
        "exact": not missing and not stale,
    }


def thread_contract_is_valid(thread: dict[str, Any]) -> bool:
    return bool(
        isinstance(thread, dict)
        and str(thread.get("id") or "")
        and isinstance(thread.get("turns", []), list)
    )


def setup_final_icon(
    *,
    config: BridgeConfig,
    store: BridgeStore,
    telegram: TelegramAPI,
    image_path: str | Path | None = None,
) -> dict[str, Any]:
    binding = store.binding()
    if binding is None:
        raise RuntimeError("Telegram group is not bound; run bootstrap first")
    image = (
        Path(image_path).expanduser().resolve()
        if image_path
        else Path(__file__).resolve().parent
        / "assets"
        / "codex-final-emoji.png"
    )
    if not image.is_file():
        raise FileNotFoundError(image)

    set_name = f"codexfinal_by_{binding.bot_username}"
    created = False
    try:
        sticker_set = telegram.get_sticker_set(set_name)
    except TelegramError as error:
        if "STICKERSET_INVALID" not in str(error):
            raise
        uploaded = telegram.upload_static_sticker(
            user_id=binding.allowed_user_id,
            file_path=image,
        )
        file_id = str(uploaded.get("file_id") or "")
        if not file_id:
            raise RuntimeError("Telegram did not return an uploaded sticker file")
        try:
            telegram.create_custom_emoji_set(
                user_id=binding.allowed_user_id,
                name=set_name,
                title="Codex final answer",
                sticker_file_id=file_id,
                emoji=FINAL_ANSWER_CUSTOM_EMOJI_ALT,
            )
            created = True
        except TelegramError as create_error:
            if not create_error.outcome_ambiguous:
                raise
        sticker_set = telegram.get_sticker_set(set_name)

    stickers = list(sticker_set.get("stickers") or [])
    custom_emoji_id = (
        str(stickers[0].get("custom_emoji_id") or "") if stickers else ""
    )
    if not custom_emoji_id:
        raise RuntimeError("Telegram sticker set has no custom emoji identifier")
    store.set_setting(
        FINAL_ANSWER_CUSTOM_EMOJI_SETTING,
        custom_emoji_id,
    )
    return {
        "ok": True,
        "configured": True,
        "stickerSetCreated": created,
    }


def doctor_database_health(store: BridgeStore) -> dict[str, Any]:
    queue = store.queue_health()
    pending_archive_deletions = sum(
        record.status == "detected"
        for record in store.list_current_archived_threads()
    )
    return {
        "databaseIntegrity": store.integrity_check(),
        "databaseSchemaVersion": store.schema_version(),
        "expectedDatabaseSchemaVersion": CURRENT_SCHEMA_VERSION,
        "queue": queue,
        "queueHealthy": queue_health_is_acceptable(queue),
        "deliveryUncertainty": store.delivery_uncertainty_health(),
        "telegramUpdateQuarantine": store.telegram_update_quarantine_health(),
        "unresolvedTopicCreations": len(
            store.unresolved_topic_creations()
        ),
        "pendingArchiveDeletions": pending_archive_deletions,
    }


def run_operational_backup(
    store: BridgeStore,
    *,
    retention: int,
) -> dict[str, Any]:
    backup = store.create_operational_backup(retention=retention)
    return {
        "ok": True,
        "path": str(backup.path),
        "createdAt": backup.created_at,
        "schemaVersion": backup.schema_version,
        "sizeBytes": backup.size_bytes,
        "retainedBackups": backup.retained_backups,
        "prunedBackups": backup.pruned_backups,
    }


async def run_local_probe(
    config: BridgeConfig,
    store: BridgeStore,
) -> dict[str, Any]:
    database = doctor_database_health(store)

    async def ignore(_: dict[str, Any]) -> None:
        return None

    app_server = CodexAppServer(
        codex_binary=config.codex_binary,
        cwd=str(config.workspace),
        on_notification=ignore,
        on_server_request=ignore,
        socket_path=config.codex_app_server_socket,
        compatible_versions=config.compatible_codex_versions,
        full_access=config.codex_full_access,
    )
    app_server_healthy = False
    error_type: str | None = None
    try:
        await app_server.start()
        response = await app_server.request("config/read", {}, timeout=10)
        app_server_healthy = isinstance(response.get("config"), dict)
    except Exception as error:
        error_type = type(error).__name__
    finally:
        with contextlib.suppress(Exception):
            await app_server.stop()

    result: dict[str, Any] = {
        **database,
        **read_telegram_update_health(config.state_dir),
        "codexAppServer": app_server_healthy,
        "codexProtocolSmoke": app_server_healthy,
    }
    if error_type is not None:
        result["errorType"] = error_type
    result["ok"] = bool(
        database["databaseIntegrity"] == "ok"
        and database["databaseSchemaVersion"] == CURRENT_SCHEMA_VERSION
        and app_server_healthy
    )
    return result


def doctor_media_health(config: BridgeConfig) -> dict[str, bool]:
    ffmpeg_path = Path(config.ffmpeg_binary).expanduser()
    try:
        ffmpeg_status = ffmpeg_path.stat()
        ffmpeg_available = bool(
            stat.S_ISREG(ffmpeg_status.st_mode)
            and os.access(ffmpeg_path, os.X_OK)
        )
    except OSError:
        ffmpeg_available = False

    media_path = config.media_directory
    try:
        storage_path = media_path if media_path.exists() else config.state_dir
        storage_status = storage_path.lstat()
        storage_owner_only = bool(
            not storage_path.is_symlink()
            and stat.S_ISDIR(storage_status.st_mode)
            and storage_status.st_uid == os.getuid()
            and stat.S_IMODE(storage_status.st_mode) & 0o077 == 0
            and os.access(storage_path, os.W_OK)
        )
    except OSError:
        storage_owner_only = False
    return {
        "ffmpegAvailable": ffmpeg_available,
        "mediaStorageOwnerOnly": storage_owner_only,
        "mediaInputReady": ffmpeg_available and storage_owner_only,
        "mediaWorkerConfigured": config.media_worker is not None,
        "mediaWorkerRequired": False,
    }


def doctor_topic_health(
    store: BridgeStore,
    *,
    chat_id: int,
) -> dict[str, int | bool]:
    open_mapped_topics = [
        topic
        for topic in store.list_topics()
        if not topic.archived
    ]
    archive_hub_topic_id = store.archive_hub_topic_id()
    observed_unmapped_topics = store.observed_unmapped_count(
        chat_id=chat_id,
        excluding_topic_ids=(
            ()
            if archive_hub_topic_id is None
            else (archive_hub_topic_id,)
        ),
    )
    open_service_topics = 1 + (archive_hub_topic_id is not None)
    return {
        "openMappedTopics": len(open_mapped_topics),
        "openServiceTopics": open_service_topics,
        "openObservedUnmappedTopics": observed_unmapped_topics,
        "openKnownTelegramTopics": (
            len(open_mapped_topics)
            + open_service_topics
            + observed_unmapped_topics
        ),
        "telegramTopicEnumerationAvailable": False,
    }


async def run_doctor(
    config: BridgeConfig,
    store: BridgeStore,
    telegram: TelegramAPI,
) -> dict[str, Any]:
    identity = await asyncio.to_thread(telegram.identity)
    binding = store.binding()
    socket_mode: int | None = None
    if config.codex_app_server_socket.exists():
        socket_mode = stat.S_IMODE(config.codex_app_server_socket.stat().st_mode)

    try:
        version_result = await asyncio.to_thread(
            subprocess.run,
            [config.codex_binary, "app-server", "daemon", "version"],
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
            env=codex_daemon_environment(config.codex_app_server_socket),
        )
    except (OSError, subprocess.TimeoutExpired):
        version_result = subprocess.CompletedProcess([], 1, "", "")
    try:
        version_payload = (
            json.loads(version_result.stdout)
            if version_result.returncode == 0
            else {}
        )
    except json.JSONDecodeError:
        version_payload = {}
    cli_version = str(version_payload.get("cliVersion") or "")
    app_server_version = str(version_payload.get("appServerVersion") or "")

    ps_binary = shutil.which("ps") or "/bin/ps"
    try:
        process_result = await asyncio.to_thread(
            subprocess.run,
            [ps_binary, "-axo", "command="],
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.TimeoutExpired):
        process_result = subprocess.CompletedProcess([], 1, "", "")
    process_lines = process_result.stdout.splitlines()
    bridge_process_running = any(
        "serve" in line
        and (
            "codex-telegram-bridge" in line
            or "codex_telegram_bridge" in line
        )
        for line in process_lines
    )
    lsof_binary = shutil.which("lsof")
    try:
        if not lsof_binary:
            raise FileNotFoundError("lsof")
        lsof_result = await asyncio.to_thread(
            subprocess.run,
            [lsof_binary, "-n", "-U"],
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.TimeoutExpired):
        lsof_result = subprocess.CompletedProcess([], 1, "", "")
    desktop_running, desktop_uses_shared = detect_desktop_shared_socket(
        process_lines=process_lines,
        lsof_lines=(
            lsof_result.stdout.splitlines()
            if lsof_result.returncode == 0
            else []
        ),
        socket_path=config.codex_app_server_socket,
    )

    result: dict[str, Any] = {
        "ok": True,
        "instance": config.instance_id,
        "workspaceExists": config.workspace.is_dir(),
        "stateDirectory": str(config.state_dir),
        "secretBackend": config.secret_backend,
        "codexFullAccess": config.codex_full_access,
        "codexFullAccessAllowed": None,
        "codexPermissionDefaultsMatch": None,
        "botUsername": identity.username,
        "telegramBound": binding is not None,
        "codexAppServer": False,
        "codexCliVersion": cli_version or None,
        "codexAppServerVersion": app_server_version or None,
        "codexVersionMatch": bool(
            cli_version
            and app_server_version
            and cli_version == app_server_version
        ),
        "sharedSocketOwnerOnly": socket_mode == 0o600,
        "desktopRunning": desktop_running,
        "desktopUsesSharedAppServer": desktop_uses_shared,
        "desktopSharedSocketVerified": desktop_uses_shared is True,
        "bridgeProcessRunning": bridge_process_running,
        **doctor_database_health(store),
        **read_telegram_update_health(config.state_dir),
        **doctor_media_health(config),
        **deployment_health(config.state_dir, Path(__file__).resolve().parent),
    }
    if binding:
        member = await asyncio.to_thread(
            telegram.get_chat_member, binding.chat_id, identity.bot_id
        )
        archive_hub_topic_id = store.archive_hub_topic_id()
        result.update(
            {
                "chatTitle": binding.chat_title,
                "botIsAdmin": member.get("status") in {"administrator", "creator"},
                "botCanManageTopics": bool(member.get("can_manage_topics")),
                "mappedTopics": len(store.list_topics()),
                **doctor_topic_health(
                    store,
                    chat_id=binding.chat_id,
                ),
                "archiveHubConfigured": (
                    archive_hub_topic_id is not None
                    and store.archive_index_message_id() is not None
                ),
                "archivedThreads": len(
                    store.list_current_archived_threads()
                ),
            }
        )

    async def ignore(_: dict[str, Any]) -> None:
        return None

    app_server = CodexAppServer(
        codex_binary=config.codex_binary,
        cwd=str(config.workspace),
        on_notification=ignore,
        on_server_request=ignore,
        socket_path=config.codex_app_server_socket,
        compatible_versions=config.compatible_codex_versions,
        full_access=config.codex_full_access,
    )
    try:
        try:
            await app_server.start()
            native_config_result = await app_server.request("config/read", {})
            requirements_result = await app_server.request(
                "configRequirements/read",
                {},
            )
            native_config = native_config_result.get("config")
            requirements_present = "requirements" in requirements_result
            requirements = requirements_result.get("requirements")
            config_contract_valid = (
                isinstance(native_config, dict)
                and requirements_present
                and (
                    requirements is None
                    or isinstance(requirements, dict)
                )
            )
            full_access_allowed = (
                requirements_allow_full_access(requirements)
                if config_contract_valid
                else False
            )
            result["codexFullAccessAllowed"] = full_access_allowed
            result["codexPermissionDefaultsMatch"] = bool(
                isinstance(native_config, dict)
                and native_config.get("approval_policy") == "never"
                and native_config.get("sandbox_mode")
                == "danger-full-access"
            )
            threads = await app_server.list_threads(archived=False)
            archived_threads = await app_server.list_threads(archived=True)
            contract_valid = all(
                str(thread.get("id") or "")
                for thread in (*threads, *archived_threads)
            ) and config_contract_valid
            if threads:
                sample = await app_server.read_thread(str(threads[0]["id"]))
                contract_valid = (
                    contract_valid and thread_contract_is_valid(sample)
                )
            result["codexAppServer"] = True
            result["activeWorkspaceThreads"] = len(threads)
            result["codexProtocolCompatible"] = True
            result["codexProtocolSmoke"] = bool(contract_valid)
            result["mapping"] = mapping_health(
                active_threads=threads,
                topics=store.list_topics(),
            )
        except CodexProtocolCompatibilityError:
            result["codexProtocolCompatible"] = False
            result["codexProtocolSmoke"] = False
    finally:
        await app_server.stop()
    result["ok"] = bool(
        result["workspaceExists"]
        and result["codexAppServer"]
        and result["codexVersionMatch"]
        and result["sharedSocketOwnerOnly"]
        and result["mediaInputReady"]
        and result["deploymentIntegrity"]
        and result["databaseIntegrity"] == "ok"
        and result["queueHealthy"]
        and (
            not bridge_process_running
            or (
                result.get("telegramUpdateLoopObserved") is True
                and result.get("telegramUpdateLoopHealthy") is True
            )
        )
        and result["unresolvedTopicCreations"] == 0
        and result["pendingArchiveDeletions"] == 0
        and result.get("codexProtocolSmoke") is True
        and (
            not config.codex_full_access
            or result.get("codexFullAccessAllowed") is True
        )
        and bool((result.get("mapping") or {}).get("exact"))
        and desktop_uses_shared is not False
        and binding is not None
        and result.get("botIsAdmin") is True
        and result.get("botCanManageTopics") is True
        and (
            not result.get("archivedThreads")
            or result.get("archiveHubConfigured")
        )
    )
    return result


async def run_sync_once(
    config: BridgeConfig,
    store: BridgeStore,
    telegram: TelegramAPI,
) -> dict[str, Any]:
    store.acquire_service_lock()
    service = BridgeService(config=config, store=store, telegram=telegram)
    await service.codex.start()
    try:
        before = len(store.list_topics())
        await service.sync_threads(dispatch_queue=False)
        after = len(store.list_topics())
        return {
            "ok": True,
            "topicsBefore": before,
            "topicsAfter": after,
            "topicsCreated": max(after - before, 0),
        }
    finally:
        await service.codex.stop()


async def run_serve(
    config: BridgeConfig,
    store: BridgeStore,
    telegram: TelegramAPI,
) -> None:
    store.acquire_service_lock()
    service = BridgeService(config=config, store=store, telegram=telegram)
    notifier = SystemdNotifier.from_environment()
    loop = asyncio.get_running_loop()
    for signal_name in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(signal_name, service.stop)
    watchdog_task = asyncio.create_task(
        notifier.watchdog_loop(service.local_watchdog_healthy),
        name="systemd-watchdog-loop",
    )
    try:
        await service.serve(
            on_ready=lambda: notifier.notify(
                "READY=1\nSTATUS=Codex Telegram bridge is running"
            )
        )
    finally:
        watchdog_task.cancel()
        await asyncio.gather(watchdog_task, return_exceptions=True)
        notifier.notify("STOPPING=1\nSTATUS=Codex Telegram bridge is stopping")


def configure_logging(
    verbose: bool,
    *,
    log_file: Path | None = None,
) -> None:
    handler: logging.Handler
    if log_file is None:
        handler = logging.StreamHandler()
    else:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        handler = RotatingFileHandler(
            log_file,
            maxBytes=2 * 1024 * 1024,
            backupCount=3,
            encoding="utf-8",
        )
        os.chmod(log_file, 0o600)
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=[handler],
        force=True,
    )


def main(argv: list[str] | None = None) -> int:
    os.umask(0o077)
    parser = build_parser()
    args = parser.parse_args(argv)
    configured_config = create_config(args)
    configured_state_dir = configured_config.state_dir
    transaction_journal = (
        configured_state_dir
        / "install-transactions"
        / "active.json"
    )
    if args.command != "doctor" and transaction_journal.is_file():
        print(
            "Managed bridge installation recovery is required; "
            "runtime start refused.",
            file=sys.stderr,
        )
        return 75
    configure_logging(
        args.verbose,
        log_file=(
            configured_config.log_path
            if args.command == "serve"
            else None
        ),
    )
    if args.command in {"backup", "probe-local"}:
        local_store: BridgeStore | None = None
        try:
            local_store = BridgeStore(
                configured_config.database_path,
                read_only=True,
            )
            if args.command == "backup":
                result = run_operational_backup(
                    local_store,
                    retention=int(args.retention),
                )
            else:
                result = asyncio.run(
                    run_local_probe(configured_config, local_store)
                )
            print(json.dumps(result, ensure_ascii=False, indent=2))
            return 0 if result.get("ok") else 2
        except KeyboardInterrupt:
            return 130
        except Exception as error:
            LOGGER.error("%s", error)
            return 1
        finally:
            if local_store is not None:
                local_store.close()

    config, store, telegram = create_runtime(
        args,
        read_only=args.command == "doctor",
    )
    try:
        if args.command == "bootstrap":
            store.acquire_service_lock()
            result = bootstrap_group(
                store=store,
                telegram=telegram,
                wait_seconds=max(0, int(args.wait_seconds)),
            )
            print(json.dumps(result, ensure_ascii=False, indent=2))
            return 0 if result.get("ok") else 2
        if args.command == "doctor":
            result = asyncio.run(run_doctor(config, store, telegram))
            print(json.dumps(result, ensure_ascii=False, indent=2))
            return 0 if result.get("ok") else 2
        if args.command == "setup-final-icon":
            result = setup_final_icon(
                config=config,
                store=store,
                telegram=telegram,
                image_path=args.image,
            )
            print(json.dumps(result, ensure_ascii=False, indent=2))
            return 0
        if args.command == "sync-once":
            result = asyncio.run(run_sync_once(config, store, telegram))
            print(json.dumps(result, ensure_ascii=False, indent=2))
            return 0 if result.get("ok") else 2
        if args.command == "serve":
            asyncio.run(run_serve(config, store, telegram))
            return 0
        parser.error(f"Unknown command: {args.command}")
    except KeyboardInterrupt:
        return 130
    except Exception as error:
        LOGGER.error("%s", error)
        return 1
    finally:
        store.close()
    return 1
