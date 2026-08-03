Warning: truncated output (original token count: 83460)
Total output lines: 8810

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import html
import json
import logging
import os
import random
import re
import secrets
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Callable
from urllib.parse import unquote, urlparse
from zoneinfo import ZoneInfo

from .codex import (
    CodexAppServer,
    CodexProtocolCompatibilityError,
    CodexProtocolError,
)
from .config import BridgeConfig
from .input_types import LocalInput
from .media import (
    MediaProcessingError,
    MediaProcessor,
    PreparedDocument,
    PreparedMedia,
    document_request_text,
    media_request_text,
)
from .media_pipeline import HybridMediaProcessor, build_media_worker_client
from .outbound_media import (
    OutboundAttachment,
    OutboundMediaError,
    OutboundMediaResolver,
)
from .runtime_health import (
    TELEGRAM_UPDATE_LOCAL_FAILURES,
    TELEGRAM_UPDATE_STALE_SECONDS,
    write_telegram_update_health,
)
from .store import (
    ArchivedThread,
    Binding,
    BridgeStore,
    ControlPrompt,
    ProgressEntry,
    QueuedMessage,
    TopicBinding,
)
from .telegram import TelegramAPI, TelegramError, exponential_backoff_delay


LOGGER = logging.getLogger(__name__)

ACCEPT_WORDS = {
    "+",
    "да",
    "ок",
    "ok",
    "yes",
    "approve",
    "approved",
    "разрешить",
    "подтверждаю",
}
DENY_WORDS = {
    "-",
    "нет",
    "no",
    "deny",
    "denied",
    "отклонить",
    "запретить",
}
NEW_THREAD_PROMPT = "➕ Опишите задачу для нового треда."
CONTROL_PROMPT_TEXT = {
    "steer": "⚡ Напишите текст для текущего хода.",
    "queue": "↪️ Напишите текст для следующего хода.",
}
TELEGRAM_UPDATE_BACKOFF_INITIAL_SECONDS = 1.0
TELEGRAM_UPDATE_BACKOFF_MAXIMUM_SECONDS = 30.0
THREAD_SYNC_BACKOFF_INITIAL_SECONDS = 2.0
THREAD_SYNC_BACKOFF_MAXIMUM_SECONDS = 60.0
CODEX_RECONNECT_BACKOFF_INITIAL_SECONDS = 1.0
CODEX_RECONNECT_BACKOFF_MAXIMUM_SECONDS = 60.0
LOOP_BACKOFF_JITTER_RATIO = 0.2
QUEUE_DISPATCH_RECONCILIATION_REQUIRED_MISSES = 2
QUEUE_DISPATCH_RECONCILIATION_GRACE_SECONDS = 5
TERMINAL_TURN_TOMBSTONE_LIMIT = 1_024
PROGRESS_ENTRY_MAX_BYTES = 4_000
RICH_PROGRESS_TEXT_LIMIT_BYTES = 30_000
RICH_PROGRESS_BLOCK_LIMIT = 100
FALLBACK_PROGRESS_INPUT_LIMIT = 3_700
PROGRESS_DETAILS_TITLE = "Ход работы Codex"
PROGRESS_HEARTBEAT_CHECK_SECONDS = 5.0
PROGRESS_COLLAPSE_RESET_SECONDS = 0.35
LOCAL_STT_DEFAULT_MIN_AVAILABLE_MEMORY_MIB = 450
FINAL_ANSWER_CUSTOM_EMOJI_SETTING = (
    "telegram_final_answer_custom_emoji_id"
)
FINAL_ANSWER_CUSTOM_EMOJI_ALT = "💻"
EFFORT_LABELS = {
    "low": ("Light", "Light"),
    "medium": ("Medium", "Medium"),
    "high": ("High", "High"),
    "xhigh": ("Extra High", "XHigh"),
    "max": ("Max", "Max"),
    "ultra": ("Ultra", "Ultra"),
}
STANDARD_SERVICE_TIER = "default"
FAST_SERVICE_TIER = "priority"
TOPIC_MODE_SUFFIX_RE = re.compile(
    r"\s+· 🧠[^·]{1,24} · ⚡[^·]{1,24}$"
)

PROGRESS_KIND_LABELS = {
    "commentary": "Ход работы",
    "plan": "План",
    "tool_status": "Статус инструмента",
}
ARCHIVE_TOPIC_TITLE = "Архивные треды"
VOICE_THREAD_PENDING_TITLE = "Распознаётся голосовая задача"
VOICE_THREAD_TITLE_GUIDANCE = (
    "Название: 2–5 слов, телеграфно; объект + действие/состояние; "
    "привычные сокращения."
)
ARCHIVE_PAGE_SIZE = 20
ARCHIVE_DELETE_CONCURRENCY = 6
ARCHIVE_DELETE_CONFIRM_ATTEMPTS = 4
ARCHIVE_DELETE_CONFIRM_DELAY_SECONDS = 0.35
ARCHIVE_TIMEZONE = ZoneInfo("Europe/Warsaw")
ARCHIVE_SUMMARY_REQUEST_LIMIT_BYTES = 700
ARCHIVE_SUMMARY_RESULT_LIMIT_BYTES = 1_400
WEEKLY_LIMIT_MIN_MINUTES = 6 * 24 * 60
WEEKLY_LIMIT_MAX_MINUTES = 8 * 24 * 60
ATTACHMENT_ERROR_LABELS = {
    "attachment_too_large": "вложение превышает лимит Telegram",
    "credential_url": "URL вложения содержит учётные данные",
    "invalid_audio": "аудиовложение повреждено или имеет неверный тип",
    "invalid_data_url": "вложение повреждено",
    "invalid_image": "изображение повреждено или имеет неверный тип",
    "missing_attachment": "локальное вложение больше недоступно",
    "unsafe_attachment": "вложение заблокировано проверкой безопасности",
    "unsafe_file_url": "небезопасная локальная ссылка на вложение",
    "unsafe_remote_url": "небезопасный URL вложения",
    "unsafe_storage": "приватное хранилище вложений недоступно",
    "unsupported_data_url": "формат встроенного вложения не поддерживается",
    "unsupported_url": "схема URL вложения не поддерживается",
}

CODEX_DEGRADED_LABELS = {
    "connecting": "подключение к общему App Server",
    "socket_unavailable": "общий App Server недоступен",
    "connection_lost": "соединение с общим App Server потеряно",
    "protocol_incompatible": "версия протокола Codex ещё не проверена",
    "split_brain": "Codex Desktop не подключён к общему серверу",
    "sync_unavailable": "синхронизация Codex временно недоступна",
    "compatibility_guard": "проверка совместимости не пройдена",
}

SECRET_PATTERNS = (
    (
        re.compile(
            r"-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----"
            r".*?"
            r"-----END [A-Z0-9 ]*PRIVATE KEY-----",
            re.IGNORECASE | re.DOTALL,
        ),
        "[REDACTED_PRIVATE_KEY]",
    ),
    (
        re.compile(r"\b\d{6,12}:[A-Za-z0-9_-]{20,}\b"),
        "[REDACTED_TELEGRAM_TOKEN]",
    ),
    (
        re.compile(
            r"(?i)\b("
            r"(?:[a-z0-9]+_)*"
            r"(?:token|password|passwd|secret|api[_-]?key"
            r"|client[_-]?secret|secret[_-]?access[_-]?key)"
            r")"
            r"([\"']?)(\s*[:=]\s*)"
            r"(?:"
            r"\"(?:\\.|[^\"\\\r\n])*\""
            r"|'(?:\\.|[^'\\\r\n])*'"
            r"|[^\s,;}\]]+"
            r")"
        ),
        r"\1\2\3[REDACTED]",
    ),
    (
        re.compile(r"(?i)(authorization\s*:\s*bearer\s+)[^\s\"']+"),
        r"\1[REDACTED]",
    ),
    (
        re.compile(r"(?i)(authorization\s*:\s*basic\s+)[^\s\"']+"),
        r"\1[REDACTED]",
    ),
    (
        re.compile(
            r"(?i)([?&](?:access_token|api[_-]?key|token|secret|password)=)"
            r"[^&\s]+"
        ),
        r"\1[REDACTED]",
    ),
    (
        re.compile(
            r"(?i)(--?(?:access-token|api-key|token|secret|password|passwd)"
            r"(?:=|\s+))"
            r"(?:"
            r"\"(?:\\.|[^\"\\\r\n])*\""
            r"|'(?:\\.|[^'\\\r\n])*'"
            r"|[^\s,;}\]]+"
            r")"
        ),
        r"\1[REDACTED]",
    ),
    (
        re.compile(r"\bgh[pousr]_[A-Za-z0-9]{20,}\b"),
        "[REDACTED_GITHUB_TOKEN]",
    ),
    (
        re.compile(r"\bAKIA[A-Z0-9]{16}\b"),
        "[REDACTED_AWS_ACCESS_KEY]",
    ),
    (
        re.compile(
            r"\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}"
            r"\.[A-Za-z0-9_-]{8,}\b"
        ),
        "[REDACTED_JWT]",
    ),
    (
        re.compile(r"(?i)(cookie\s*:\s*)[^\r\n]+"),
        r"\1[REDACTED]",
    ),
    (
        re.compile(
            r"(?i)([a-z][a-z0-9+.-]*://)"
            r"([^/\s:@]*):([^@\s/]+)@"
        ),
        r"\1[REDACTED]@",
    ),
)


def redact_sensitive(text: str) -> str:
    value = text
    for pattern, replacement in SECRET_PATTERNS:
        value = pattern.sub(replacement, value)
    return value


MAX_TELEGRAM_REPLY_CONTEXT_CHARS = 600


def telegram_reply_context(message: dict[str, Any]) -> str | None:
    replied = message.get("reply_to_message") or {}
    raw = str(replied.get("text") or replied.get("caption") or "").strip()
    if not raw:
        if replied.get("photo"):
            raw = "Фотография без подписи"
        elif replied.get("video"):
            raw = "Видео без подписи"
        elif replied.get("document"):
            raw = "Файл без подписи"
        else:
            return None
    compact = re.sub(r"\s+", " ", redact_sensitive(raw)).strip()
    if len(compact) > MAX_TELEGRAM_REPLY_CONTEXT_CHARS:
        compact = compact[: MAX_TELEGRAM_REPLY_CONTEXT_CHARS - 1].rstrip() + "…"
    return compact or None


def with_telegram_reply_context(text: str, context: str | None) -> str:
    if context is None:
        return text
    return (
        "Контекст Telegram reply: пользователь ответил на сообщение Codex "
        f"«{context}».\n\n{text}"
    )


LOCAL_MARKDOWN_LINK = re.compile(
    r"(?<![!\\])\[([^\]\n]+)\]\("
    r"(?:<((?:/|~/|file://)[^>\n]+)>"
    r"|((?:/|~/|file://|\./|\.\./)[^)\s]+))"
    r"\)"
)
LOCAL_MARKDOWN_IMAGE = re.compile(
    r"(?<!\\)!\[([^\]\n]*)\]\("
    r"(?:<((?:/|~/|file://)[^>\n]+)>"
    r"|((?:/|~/|file://|\./|\.\./)[^)\s]+))"
    r"\)"
)
CODEX_FILE_CITATION = re.compile(
    r":codex-file-citation\{([^}\n]*)\}"
)
CODEX_FILE_CITATION_ATTRIBUTE = re.compile(
    r'([A-Za-z_][A-Za-z0-9_-]*)="((?:\\.|[^"\\])*)"'
)
MAX_FINAL_ANSWER_ATTACHMENTS = 10
MAX_TELEGRAM_DOCUMENT_BYTES = 49 * 1024 * 1024
BLOCKED_ATTACHMENT_PARTS = frozenset({".git", ".venv", "node_modules"})
BLOCKED_ATTACHMENT_NAMES = frozenset(
    {
        ".env",
        "secrets.yaml",
        "secrets.yml",
    }
)
BLOCKED_ATTACHMENT_SUFFIXES = frozenset(
    {
        ".db",
        ".key",
        ".p12",
        ".pfx",
        ".pem",
        ".sqlite",
        ".sqlite3",
    }
)
PERSON_REVIEW_DIRECTORY_PARTS = ("outputs", "person-review")
PERSON_REVIEW_IMAGE_SUFFIXES = frozenset({".jpg", ".jpeg", ".png", ".webp"})
PERSON_REVIEW_SETTING_PREFIX = "telegram_person_review:"


def person_review_token(relative_path: Path) -> str | None:
    if (
        len(relative_path.parts) < 3
        or relative_path.parts[:2] != PERSON_REVIEW_DIRECTORY_PARTS
        or relative_path.suffix.lower() not in PERSON_REVIEW_IMAGE_SUFFIXES
    ):
        return None
    return hashlib.sha256(str(relative_path).encode("utf-8")).hexdigest()[:20]


def person_review_keyboard(token: str) -> dict[str, Any]:
    return {
        "inline_keyboard": [
            [
                {"text": "✅ Подходит", "callback_data": f"prv:{token}:yes"},
                {
                    "text": "❌ Не подходит, поискать лучше",
                    "callback_data": f"prv:{token}:no",
                },
            ]
        ]
    }


def parse_person_review_text(text: str) -> str | None:
    normalized = re.sub(r"\s+", " ", text.casefold()).strip(" \t\r\n.,!?;:")
    if normalized in {
        "да",
        "подходит",
        "это подходит",
        "да подходит",
        "да, подходит",
    }:
        return "yes"
    if normalized in {
        "нет",
        "не подходит",
        "это не подходит",
        "нет не подходит",
        "нет, не подходит",
        "не подходит поискать лучше",
        "не подходит, поискать лучше",
    }:
        return "no"
    return None


def _codex_file_citation_attributes(value: str) -> dict[str, str]:
    return {
        key: raw.replace(r"\"", '"').replace(r"\\", "\\")
        for key, raw in CODEX_FILE_CITATION_ATTRIBUTE.findall(value)
    }


def _transform_outside_code(
    text: str,
    transform: Any,
) -> str:
    rendered: list[str] = []
    cursor = 0
    while cursor < len(text):
        marker = text.find("`", cursor)
        if marker < 0:
            rendered.append(transform(text[cursor:]))
            break
        rendered.append(transform(text[cursor:marker]))
        run_end = marker
        while run_end < len(text) and text[run_end] == "`":
            run_end += 1
        delimiter = text[marker:run_end]
        closing = text.find(delimiter, run_end)
        if closing < 0:
            rendered.append(text[marker:])
            break
        closing_end = closing + len(delimiter)
        rendered.append(text[marker:closing_end])
        cursor = closing_end
    return "".join(rendered)


def _attachment_candidate(
    destination: str,
    workspace: Path,
    *,
    require_outputs_directory: bool,
) -> Path | None:
    path_text = destination.strip()
    line_match = re.search(r"(:\d+(?::\d+)?)$", path_text)
    if line_match:
        path_text = path_text[: -len(line_match.group(1))]
    if path_text.startswith("file://"):
        path_text = unquote(urlparse(path_text).path)
    candidate = Path(path_text).expanduser()
    if not candidate.is_absolute():
        candidate = workspace / candidate
    try:
        resolved = candidate.resolve(strict=True)
        workspace_path = workspace.resolve(strict=True)
        relative = resolved.relative_to(workspace_path)
        status = resolved.stat()
    except (FileNotFoundError, OSError, RuntimeError, ValueError):
        return None
    lowered_parts = {part.lower() for part in relative.parts}
    lowered_name = resolved.name.lower()
    if (
        not resolved.is_file()
        or resolved.is_symlink()
        or lowered_parts.intersection(BLOCKED_ATTACHMENT_PARTS)
        or lowered_name in BLOCKED_ATTACHMENT_NAMES
        or resolved.suffix.lower() in BLOCKED_ATTACHMENT_SUFFIXES
        or any(
            marker in lowered_name
            for marker in ("credential", "password", "secret", "token")
        )
        or status.st_size > MAX_TELEGRAM_DOCUMENT_BYTES
        or (
            require_outputs_directory
            and (not relative.parts or relative.parts[0] != "outputs")
        )
    ):
        return None
    return resolved


def final_answer_attachments(text: str, workspace: Path) -> list[Path]:
    """Return explicit result files, never ordinary technical references."""

    candidates: list[Path] = []
    seen: set[Path] = set()

    def inspect(segment: str) -> str:
        for citation in CODEX_FILE_CITATION.finditer(segment):
            attributes = _codex_file_citation_attributes(citation.group(1))
            if attributes.get("purpose") != "output":
                continue
            candidate = _attachment_candidate(
                attributes.get("path", ""),
                workspace,
                require_outputs_directory=True,
            )
            if candidate is not None and candidate not in seen:
                seen.add(candidate)
                candidates.append(candidate)
        for image in LOCAL_MARKDOWN_IMAGE.finditer(segment):
            candidate = _attachment_candidate(
                image.group(2) or image.group(3) or "",
                workspace,
                require_outputs_directory=True,
            )
            if candidate is not None and candidate not in seen:
                seen.add(candidate)
                candidates.append(candidate)
        for link in LOCAL_MARKDOWN_LINK.finditer(segment):
            candidate = _attachment_candidate(
                link.group(2) or link.group(3) or "",
                workspace,
                require_outputs_directory=True,
            )
            if candidate is not None and candidate not in seen:
                seen.add(candidate)
                candidates.append(candidate)
        return segment

    _transform_outside_code(text, inspect)
    return candidates[:MAX_FINAL_ANSWER_ATTACHMENTS]


def _local_link_display(destination: str, workspace: Path) -> str:
    suffix = ""
    path_text = destination
    line_match = re.search(r"(:\d+(?::\d+)?)$", path_text)
    if line_match:
        suffix = line_match.group(1)
        path_text = path_text[: -len(suffix)]
    if path_text.startswith("file://"):
        path_text = unquote(urlparse(path_text).path)
    path = Path(path_text).expanduser()
    if not path.is_absolute():
        path = workspace / path
    normalized = path.resolve(strict=False)
    workspace_path = workspace.resolve(strict=False)
    home_path = Path.home().resolve(strict=False)
    try:
        display = str(normalized.relative_to(workspace_path))
    except ValueError:
        try:
            display = "~/" + str(normalized.relative_to(home_path))
        except ValueError:
            display = f"локальный файл: {normalized.name}"
    return display + suffix


def telegram_visible_text(text: str, workspace: Path) -> str:
    """Make Codex Markdown readable on a phone without touching code/web URLs."""

    def replace_links(segment: str) -> str:
        def replace_citation(match: re.Match[str]) -> str:
            attributes = _codex_file_citation_attributes(match.group(1))
            destination = attributes.get("path", "")
            if not destination:
                return "локальный файл"
            filename = Path(destination).name or "Файл"
            return (
                f"{filename} — "
                f"{_local_link_display(destination, workspace)}"
            )

        def replace(match: re.Match[str]) -> str:
            label = match.group(1)
            destination = match.group(2) or match.group(3) or ""
            return f"{label} — {_local_link_display(destination, workspace)}"

        def replace_image(match: re.Match[str]) -> str:
            label = match.group(1).strip() or "Изображение"
            destination = match.group(2) or match.group(3) or ""
            return f"{label} — {_local_link_display(destination, workspace)}"

        without_citations = CODEX_FILE_CITATION.sub(
            replace_citation,
            segment,
        )
        without_images = LOCAL_MARKDOWN_IMAGE.sub(
            replace_image,
            without_citations,
        )
        return LOCAL_MARKDOWN_LINK.sub(replace, without_images)

    return _transform_outside_code(text, replace_links)


def truncate_utf8(text: str, limit: int) -> str:
    encoded = text.encode("utf-8")
    if len(encoded) <= limit:
        return text
    suffix = "…"
    available = max(0, limit - len(suffix.encode("utf-8")))
    shortened = encoded[:available]
    while shortened:
        try:
            return shortened.decode("utf-8").rstrip() + suffix
        except UnicodeDecodeError:
            shortened = shortened[:-1]
    return suffix if limit >= len(suffix.encode("utf-8")) else ""


def visible_progress_entry(
    item: dict[str, Any],
) -> tuple[str, str] | None:
    item_type = str(item.get("type") or "")
    if item_type == "agentMessage":
        if item.get("phase") == "final_answer":
            return None
        body = str(item.get("text") or "").strip()
        return ("commentary", body) if body else None
    if item_type == "plan":
        body = str(item.get("text") or "").strip()
        return ("plan", body) if body else None
    if item_type in {"toolStatus", "tool_status"}:
        # Deliberately ignore output/command/result fields. Only an explicit
        # visible status summary may enter the progress journal.
        body = str(
            item.get("summary")
            or item.get("statusText")
            or item.get("text")
            or ""
        ).strip()
        return ("tool_status", body) if body else None
    return None


def russian_elapsed_minutes(minutes: int) -> str:
    value = max(0, int(minutes))
    last_two = value % 100
    last = value % 10
    if last == 1 and last_two != 11:
        noun = "минуту"
    elif 2 <= last <= 4 and not 12 <= last_two <= 14:
        noun = "минуты"
    else:
        noun = "минут"
    return f"{value} {noun}"


def progress_elapsed_minutes(started_at: str) -> int:
    try:
        started_epoch = datetime.fromisoformat(started_at).timestamp()
    except (TypeError, ValueError):
        return 0
    return max(0, int((time.time() - started_epoch) // 60))


def progress_summary(
    *,
    closed: bool,
    outcome: str,
    elapsed_minutes: int = 0,
) -> str:
    if not closed:
        elapsed = (
            f" {russian_elapsed_minutes(elapsed_minutes)}"
            if elapsed_minutes > 0
            else ""
        )
        return f"🟡 Codex работает{elapsed}."
    if outcome == "failed":
        return f"⚠️ {PROGRESS_DETAILS_TITLE}: ошибка"
    if outcome == "interrupted":
        return f"⏹ {PROGRESS_DETAILS_TITLE} остановлен"
    if elapsed_minutes > 0:
        return f"Codex работал {russian_elapsed_minutes(elapsed_minutes)}."
    return "Codex закончил работу."


def progress_entry_body(entry: ProgressEntry, index: int) -> str:
    return f"Шаг {index}\n{entry.text}"


def progress_collapse_keyboard(revision: int = 0) -> dict[str, Any]:
    return {
        "inline_keyboard": [
            [
                {
                    "text": "▲ Свернуть",
                    "callback_data": f"pgc:{revision % 2}",
                }
            ]
        ]
    }


def progress_collapsed_reset_text(
    entries: list[ProgressEntry],
    *,
    outcome: str,
) -> str:
    return progress_summary(
        closed=True,
        outcome=outcome,
    )


def build_rich_progress_message(
    entries: list[ProgressEntry],
    *,
    closed: bool,
    outcome: str = "completed",
    elapsed_minutes: int = 0,
) -> dict[str, Any]:
    summary = progress_summary(
        closed=closed,
        outcome=outcome,
        elapsed_minutes=elapsed_minutes,
    )
    used_bytes = len(summary.encode("utf-8"))
    rendered_bodies: list[str] = []
    detail_blocks: list[dict[str, Any]] = []
    rendered = 0
    for index, entry in enumerate(entries[:RICH_PROGRESS_BLOCK_LIMIT], start=1):
        body = progress_entry_body(entry, index)
        separator = "\n\n" if rendered_bodies else ""
        # Keep enough headroom for the final durable-omission footer even when
        # the exact omitted count changes after this entry is accepted.
        reserve = 256 if index < len(entries) else 0
        remaining = (
            RICH_PROGRESS_TEXT_LIMIT_BYTES
            - used_bytes
            - len(separator.encode("utf-8"))
            - reserve
        )
        if remaining < 32:
            break
        body = truncate_utf8(body, remaining)
        if not body:
            break
        rendered_bodies.append(body)
        used_bytes += len((separator + body).encode("utf-8"))
        rendered += 1
    if rendered_bodies:
        detail_blocks.append(
            {
                "type": "paragraph",
                "text": "\n\n".join(rendered_bodies),
            }
        )
    omitted = len(entries) - rendered
    if omitted:
        note = f"… Ещё {omitted} записей сохранено локально."
        remaining = RICH_PROGRESS_TEXT_LIMIT_BYTES - used_bytes
        note = truncate_utf8(note, remaining)
        if note:
            detail_blocks.append({"type": "footer", "text": note})
    details: dict[str, Any] = {
        "type": "details",
        "summary": PROGRESS_DETAILS_TITLE,
        "blocks": detail_blocks,
    }
    if not closed:
        details["is_open"] = True
    return {
        "blocks": [
            {"type": "paragraph", "text": summary},
            details,
        ],
        "skip_entity_detection": True,
    }


def _truncate_for_escaped_html(text: str, limit: int) -> str:
    if len(html.escape(text, quote=False)) <= limit:
        return text
    low, high = 0, len(text)
    while low < high:
        middle = (low + high + 1) // 2
        candidate = text[:middle].rstrip() + "…"
        if len(html.escape(candidate, quote=False)) <= limit:
            low = middle
        else:
            high = middle - 1
    return text[:low].rstrip() + "…"


def build_fallback_progress_text(
    entries: list[ProgressEntry],
    *,
    closed: bool,
    outcome: str = "completed",
    elapsed_minutes: int = 0,
) -> str:
    summary = progress_summary(
        closed=closed,
        outcome=outcome,
        elapsed_minutes=elapsed_minutes,
    )
    open_tag = "<blockquote expandable>" if closed else "<blockquote>"
    close_tag = "</blockquote>"
    body = "\n\n".join(
        progress_entry_body(entry, index)
        for index, entry in enumerate(entries, start=1)
    )
    fixed_size = (
        len(summary)
        + 1
        + len(open_tag)
        + len(close_tag)
    )
    available = max(0, FALLBACK_PROGRESS_INPUT_LIMIT - fixed_size)
    bounded = _truncate_for_escaped_html(body, available)
    escaped = html.escape(bounded, quote=False)
    return f"{summary}\n{open_tag}{escaped}{close_tag}"


def clean_topic_title(value: str | None, fallback: str = "Codex thread") -> str:
    title = re.sub(r"\s+", " ", redact_sensitive((value or "").strip()))
    if not title:
        title = redact_sensitive(fallback)
    return title[:128]


def strip_topic_mode_suffix(value: str | None) -> str:
    title = clean_topic_title(value)
    stripped = TOPIC_MODE_SUFFIX_RE.sub("", title).rstrip()
    return clean_topic_title(stripped)


def effort_label(effort: str | None, *, compact: bool = False) -> str:
    normalized = str(effort or "").strip().lower()
    labels = EFFORT_LABELS.get(normalized)
    if labels is not None:
        return labels[1 if compact else 0]
    return normalized.replace("_", " ").title() or "не определён"


def speed_label(service_tier: str | None) -> str:
    normalized = str(service_tier or STANDARD_SERVICE_TIER).strip().lower()
    if normalized in {"", STANDARD_SERVICE_TIER, "standard"}:
        return "Standard"
    if normalized in {FAST_SERVICE_TIER, "fast"}:
        return "Fast"
    return normalized.replace("_", " ").title()


@dataclass(frozen=True)
class ThreadMode:
    model: str | None = None
    effort: str | None = None
    service_tier: str = STANDARD_SERVICE_TIER


def topic_title_with_mode(
    base_title: str | None,
    mode: ThreadMode | None,
) -> str:
    base = strip_topic_mode_suffix(base_title)
    if mode is None or not mode.effort:
        return base
    suffix = (
        f" · 🧠{effort_label(mode.effort, compact=True)}"
        f" · ⚡{speed_label(mode.service_tier)}"
    )
    available = max(1, 128 - len(suffix))
    return f"{base[:available].rstrip()}{suffix}"


def _format_archive_epoch(value: int) -> str:
    if value <= 0:
        return "неизвестно"
    return datetime.fromtimestamp(value, ARCHIVE_TIMEZONE).strftime(
        "%d.%m.%Y %H:%M"
    )


def _format_archive_iso(value: str) -> str:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return "неизвестно"
    return parsed.astimezone(ARCHIVE_TIMEZONE).strftime("%d.%m.%Y %H:%M")


def _telegram_error_contains(error: TelegramError, *phrases: str) -> bool:
    message = str(error).lower()
    return any(phrase.lower() in message for phrase in phrases)


def _codex_error_indicates_archived_thread(error: CodexProtocolError) -> bool:
    message = str(error).lower()
    return "archived" in message and "unarchive" in message


def user_message_text(item: dict[str, Any]) -> str:
    parts: list[str] = []
    for content in item.get("content") or []:
        content_type = content.get("type")
        if content_type == "text":
            parts.append(str(content.get("text") or ""))
        elif content_type in {"image", "localImage"}:
            parts.append("[изображение]")
        elif content_type in {"audio", "localAudio"}:
            parts.append("[аудио]")
        elif content_type == "skill":
            parts.append(f"[skill: {content.get('name') or 'unknown'}]")
        elif content_type == "mention":
            parts.append(f"@{content.get('name') or 'mention'}")
    return "\n".join(part for part in parts if part).strip()


def _archive_summary_part(
    text: str,
    *,
    workspace: Path,
    limit: int,
) -> str:
    visible = telegram_visible_text(
        redact_sensitive(text),
        workspace,
    )
    compact = re.sub(r"\s+", " ", visible).strip()
    return truncate_utf8(compact, limit)


def build_archive_summary(
    thread: dict[str, Any],
    *,
    workspace: Path,
) -> str:
    user_messages: list[str] = []
    final_answers: list[str] = []
    visible_agent_updates: list[str] = []
    for turn in thread.get("turns") or []:
        for item in turn.get("items") or []:
            item_type = str(item.get("type") or "")
            if item_type == "userMessage":
                body = user_message_text(item)
                if body:
                    user_messages.append(body)
            elif item_type == "agentMessage":
                body = str(item.get("text") or "").strip()
                if not body:
                    continue
                visible_agent_updates.append(body)
                if item.get("phase") == "final_answer":
                    final_answers.append(body)

    request = (
        user_messages[0]
        if user_messages
        else str(thread.get("preview") or thread.get("name") or "").strip()
    )
    result = (
        final_answers[-1]
        if final_answers
        else (
            visible_agent_updates[-1]
            if visible_agent_updates
            else "Итоговое сообщение в истории треда отсутствует."
        )
    )
    request = _archive_summary_part(
        request or "Описание запроса отсутствует.",
        workspace=workspace,
        limit=ARCHIVE_SUMMARY_REQUEST_LIMIT_BYTES,
    )
    result = _archive_summary_part(
        result,
        workspace=workspace,
        limit=ARCHIVE_SUMMARY_RESULT_LIMIT_BYTES,
    )
    return f"О чём: {request}\n\nЧто сделали: {result}"


def archive_card_payload(
    record: ArchivedThread,
    *,
    restored: bool = False,
) -> tuple[str, dict[str, Any]]:
    title = html.escape(record.title[:128], quote=False)
    thread_id = html.escape(record.thread_id, quote=False)
    summary = html.escape(
        record.summary or "Краткое описание отсутствует.",
        quote=False,
    )
    lines = [
        f"🗄 <b>{title}</b>",
        f"Создан: {_format_archive_epoch(record.thread_created_at)}",
        f"Архивирован: {_format_archive_iso(record.archived_at)}",
        f"Codex thread ID: <code>{thread_id}</code>",
    ]
    if restored:
        lines.append("Статус: ✅ восстановлен")
    lines.extend(
        [
            "",
            (
                "<blockquote expandable>"
                "<b>Краткое описание</b>\n"
                f"{summary}"
                "</blockquote>"
            ),
        ]
    )
    text = "\n".join(lines)
    keyboard = (
        {"inline_keyboard": []}
        if restored
        else {
            "inline_keyboard": [
                [
                    {
                        "text": "♻️ Восстановить",
                        "callback_data": f"arcun:{record.restore_token}",
                    }
                ]
            ]
        }
    )
    return text, keyboard


def telegram_source_message_id(
    client_id: str,
    *,
    expected_chat_id: int,
) -> int | None:
    match = re.fullmatch(r"tg:(-?\d+):(\d+)", client_id)
    if match is None or int(match.group(1)) != expected_chat_id:
        return None
    message_id = int(match.group(2))
    return message_id if message_id > 0 else None


def parse_message_mode(
    text: str,
    bot_username: str,
) -> tuple[str, str]:
    stripped = text.strip()
    username = re.escape(bot_username)
    command = re.match(
        rf"^/(steer|queue|new|status|mode|audit|limits|cancel|archive)"
        rf"(?:@{username})?"
        rf"(?:\s+([\s\S]*))?$",
        stripped,
        re.IGNORECASE,
    )
    if command:
        return command.group(1).lower(), (command.group(2) or "").strip()
    mention = re.compile(rf"(?<!\w)@{username}\b", re.IGNORECASE)
    if mention.search(stripped):
        return "steer", mention.sub("", stripped).strip()
    return "plain", stripped


@dataclass
class PendingServerRequest:
    public_id: str
    server_request_id: int | str
    method: str
    thread_id: str
    params: dict[str, Any]
    telegram_message_id: int | None = None
    collected_answers: dict[str, list[str]] = field(default_factory=dict)


@dataclass
class LoopHealth:
    last_success_at: float | None = None
    last_error_at: float | None = None
    last_error_kind: str | None = None
    last_error_type: str | None = None
    consecutive_failures: int = 0

    def record_success(self) -> None:
        self.last_success_at = time.time()
        self.consecutive_failures = 0

    def record_failure(self, kind: str, error_type: str | None = None) -> None:
        self.last_error_at = time.time()
        self.last_error_kind = kind
        self.last_error_type = error_type
        self.consecutive_failures += 1


class TopicCreationUnresolvedError(RuntimeError):
    """A remote Topic may exist, so automatic creation must stop."""


class TurnStartOutcome(Enum):
    """Result of one guarded turn-start attempt.

    Only ``STARTED`` is truthy so existing boolean callers keep their
    behavior while queue dispatch can distinguish capacity blocking from an
    outcome-unknown Codex RPC.
    """

    STARTED = "started"
    BLOCKED = "blocked"
    OUTCOME_UNKNOWN = "outcome_unknown"

    def __bool__(self) -> bool:
        return self is TurnStartOutcome.STARTED


def loop_error_kind(error: BaseException) -> str:
    if isinstance(error, TelegramError):
        return error.kind
    if isinstance(error, CodexProtocolError):
        return "codex_protocol"
    if isinstance(error, TimeoutError):
        return "timeout"
    return "unexpected"


def approval_keyboard(
    public_id: str,
    *,
    allow_once: bool = True,
    allow_session: bool = True,
    allow_deny: bool = True,
) -> dict[str, Any]:
    rows: list[list[dict[str, str]]] = []
    accepts: list[dict[str, str]] = []
    if allow_once:
        accepts.append(
            {
                "text": "✅ Разрешить один раз",
                "callback_data": f"apr:{public_id}:once",
            }
        )
    if allow_session:
        accepts.append(
            {
                "text": "♻️ Для сессии",
                "callback_data": f"apr:{public_id}:session",
            }
        )
    if accepts:
        rows.append(accepts)
    if allow_deny:
        rows.append(
            [
                {
                    "text": "❌ Отклонить",
                    "callback_data": f"apr:{public_id}:deny",
                }
            ]
        )
    return {"inline_keyboard": rows}


def queue_keyboard(queue_id: int) -> dict[str, Any]:
    return {
        "inline_keyboard": [
            [
                {
                    "text": "⚡ Передать сейчас (steer)",
                    "callback_data": f"stq:{queue_id}",
                }
            ]
        ]
    }


def new_thread_keyboard() -> dict[str, Any]:
    return {
        "inline_keyboard": [
            [
                {
                    "text": "➕ Новый тред",
                    "callback_data": "newthread",
                }
            ]
        ]
    }


def new_thread_force_reply() -> dict[str, Any]:
    return {
        "force_reply": True,
        "input_field_placeholder": "Опишите задачу для нового треда",
    }


def control_force_reply(mode: str) -> dict[str, Any]:
    placeholder = (
        "Текст в текущий ход"
        if mode == "steer"
        else "Текст следующего хода"
    )
    return {
        "force_reply": True,
        "input_field_placeholder": placeholder,
    }


def topic_status_keyboard(
    *,
    busy: bool,
    degraded: bool = False,
    mode_available: bool = False,
) -> dict[str, Any]:
    rows: list[list[dict[str, str]]] = []
    if busy and not degraded:
        rows.extend(
            [
                [
                    {
                        "text": "⚡ В текущий ход",
                        "callback_data": "ctl:steer",
                    },
                    {
                        "text": "↪️ Следующим ходом",
                        "callback_data": "ctl:queue",
                    },
                ],
                [
                    {
                        "text": "⏹ Стоп",
                        "callback_data": "ctl:stop",
                    }
                ],
            ]
        )
    if mode_available and not degraded:
        rows.append(
            [
                {
                    "text": "⚙️ Интеллект и скорость",
                    "callback_data": "ctl:mode",
                }
            ]
        )
    refresh_row = [{"text": "↻ Обновить", "callback_data": "ctl:refresh"}]
    if not degraded:
        refresh_row.append(
            {"text": "➕ Новый тред", "callback_data": "newthread"}
        )
    rows.append(refresh_row)
    return {"inline_keyboard": rows}


def topic_mode_keyboard(
    mode: ThreadMode,
    model: dict[str, Any] | None,
) -> dict[str, Any]:
    effort_values = [
        str(option.get("reasoningEffort") or "")
        for option in (
            (model or {}).get("supportedReasoningEfforts") or []
        )
        if str(option.get("reasoningEffort") or "")
    ]
    if mode.effort and mode.effort not in effort_values:
        effort_values.append(mode.effort)
    rows: list[list[dict[str, str]]] = []
    for start in range(0, len(effort_values), 3):
        row: list[dict[str, str]] = []
        for value in effort_values[start : start + 3]:
            selected = value == mode.effort
            row.append(
                {
                    "text": (
                        ("✓ " if selected else "")
                        + effort_label(value)
                    ),
                    "callback_data": f"mode:e:{value}",
                }
            )
        rows.append(row)

    tier_options = [
        {
            "id": STANDARD_SERVICE_TIER,
            "name": "Standard",
        },
        *((model or {}).get("serviceTiers") or []),
    ]
    speed_row: list[dict[str, str]] = []
    for option in tier_options:
        tier_id = str(option.get("id") or "")
        if not re.fullmatch(r"[a-z0-9_-]{1,32}", tier_id):
            continue
        selected = (
            tier_id == mode.service_tier
            or (
                tier_id == STANDARD_SERVICE_TIER
                and mode.service_tier in {"", "standard"}
            )
        )
        name = str(option.get("name") or speed_label(tier_id))
        speed_row.append(
            {
                "text": ("✓ " if selected else "") + name[:24],
                "callback_data": f"mode:s:{tier_id}",
            }
        )
    if speed_row:
        rows.append(speed_row)
    rows.append(
        [
            {"text": "↻ Обновить", "callback_data": "ctl:mode"},
            {"text": "📊 Статус", "callback_data": "ctl:refresh"},
        ]
    )
    return {"inline_keyboard": rows}


def stop_confirmation_keyboard(public_id: str) -> dict[str, Any]:
    return {
        "inline_keyboard": [
            [
                {
                    "text": "⏹ Да, остановить",
                    "callback_data": f"stop:{public_id}:yes",
                },
                {
                    "text": "Не останавливать",
                    "callback_data": f"stop:{public_id}:no",
                },
            ]
        ]
    }


def format_health_age(timestamp: float | None) -> str:
    if timestamp is None:
        return "ещё не было"
    age = max(0, int(time.time() - timestamp))
    if age < 60:
        return f"{age} сек. назад"
    if age < 3600:
        return f"{age // 60} мин. назад"
    return f"{age // 3600} ч. назад"


def weekly_codex_remaining_percent(
    payload: dict[str, Any],
) -> float | None:
    candidates: list[dict[str, Any]] = []
    default = payload.get("rateLimits")
    if isinstance(default, dict):
        candidates.append(default)
    by_limit_id = payload.get("rateLimitsByLimitId")
    if isinstance(by_limit_id, dict):
        codex = by_limit_id.get("codex")
        if isinstance(codex, dict) and codex is not default:
            candidates.append(codex)

    for snapshot in candidates:
        for slot in ("primary", "secondary"):
            window = snapshot.get(slot)
            if not isinstance(window, dict):
                continue
            duration = window.get("windowDurationMins")
            used = window.get("usedPercent")
            if isinstance(duration, bool) or not isinstance(
                duration,
                (int, float),
            ):
                continue
            if not (
                WEEKLY_LIMIT_MIN_MINUTES
                <= float(duration)
                <= WEEKLY_LIMIT_MAX_MINUTES
            ):
                continue
            if isinstance(used, bool) or not isinstance(used, (int, float)):
                continue
            used_percent = float(used)
            if not 0 <= used_percent <= 100:
                continue
            return 100 - used_percent
    return None


def format_limit_percent(value: float) -> str:
    # Python 3.10 preserves ``int`` for ``round(int, ndigits)`` while newer
    # versions return a float. Normalize first so the formatting path behaves
    # identically across every supported interpreter.
    rounded = round(float(value), 1)
    if rounded.is_integer():
        return str(int(rounded))
    return f"{rounded:.1f}".replace(".", ",")


def format_token_count(value: int) -> str:
    value = max(0, int(value))
    if value < 1_000:
        return str(value)
    if value < 1_000_000:
        scaled = round(value / 1_000, 1)
        suffix = "k"
    else:
        scaled = round(value / 1_000_000, 1)
        suffix = "m"
    rendered = f"{scaled:.1f}".rstrip("0").rstrip(".")
    return rendered.replace(".", ",") + suffix


class BridgeService:
    def __init__(
        self,
        *,
        config: BridgeConfig,
        store: BridgeStore,
        telegram: TelegramAPI,
    ):
        self.config = config
        self.store = store
        self.telegram = telegram
        self.binding = self._require_binding()
        self.codex = CodexAppServer(
            codex_binary=config.codex_binary,
            cwd=str(config.workspace),
            on_notification=self.on_codex_notification,
            on_server_request=self.on_codex_server_request,
            socket_path=config.codex_app_server_socket,
            compatible_versions=config.compatible_codex_versions,
            full_access=config.codex_full_access,
        )
        self.media = MediaProcessor(
            root=config.media_directory,
            ffmpeg_binary=config.ffmpeg_binary,
            timeout_seconds=config.media_processing_timeout_seconds,
            retention_seconds=config.media_retention_days * 24 * 60 * 60,
            storage_limit_bytes=config.media_storage_limit_bytes,
        )
        try:
            media_worker = build_media_worker_client(config.media_worker)
        except Exception as error:
            LOGGER.warning(
                "Optional media worker configuration is unavailable; "
                "reason=%s; using local ffmpeg",
                type(error).__name__,
            )
            media_worker = None
        # Keep only the remote client's circuit state here.  The hybrid
        # adapter itself is intentionally rebuilt for each inbound item so
        # tests and embedders that replace ``self.media`` retain the existing
        # dependency-injection behavior.
        self.media_worker = media_worker
        self.outbound_media = OutboundMediaResolver(
            root=config.media_directory,
            max_bytes=MAX_TELEGRAM_DOCUMENT_BYTES,
        )
        self.active_turns: dict[str, str] = {}
        self.observed_turns: dict[str, str] = {}
        self.busy_threads: set[str] = set()
        self.thread_modes: dict[str, ThreadMode] = {}
        self.model_catalog: dict[str, dict[str, Any]] = {}
        self.pending_requests: dict[str, PendingServerRequest] = {}
        self._pending_locks: dict[str, asyncio.Lock] = {}
        self._dispatch_locks: dict[str, asyncio.Lock] = {}
        self._turn_start_lock = asyncio.Lock()
        self._queue_dispatch_lock = asyncio.Lock()
        self._capacity_reserved_threads: set[str] = set()
        self._mirror_locks: dict[str, asyncio.Lock] = {}
        self._progress_locks: dict[str, asyncio.Lock] = {}
        self._progress_elapsed_minutes_rendered: dict[
            tuple[str, str],
            int,
        ] = {}
        self._archive_locks: dict[str, asyncio.Lock] = {}
        self._attachment_backfill_checked: set[tuple[str, str]] = set()
        self._started_items: dict[
            tuple[str, str, str],
            dict[str, Any],
        ] = {}
        self._pending_final_items: dict[
            tuple[str, str],
            dict[str, Any],
        ] = {}
        self._turn_token_usage: dict[tuple[str, str], tuple[int, int]] = {}
        self._thread_token_totals: dict[str, int] = {}
        self._terminal_turns: dict[tuple[str, str], None] = {}
        self._telegram_slots = asyncio.Semaphore(16)
        self._topic_create_lock = asyncio.Lock()
        self._archive_hub_lock = asyncio.Lock()
        self._stop_event = asyncio.Event()
        self._sync_requested = asyncio.Event()
        self._codex_reconnect_requested = asyncio.Event()
        self._codex_ready = asyncio.Event()
        # Direct one-shot callers construct the service only after connecting
        # Codex themselves. ``serve`` explicitly clears this optimistic
        # initial state before it starts the reconnect supervisor.
        self._codex_ready.set()
        self._codex_guard_reason: str | None = None
        self.codex_degraded_reason: str | None = None
        self.codex_last_healthy_at: float | None = None
        self.codex_last_healthy_version: str | None = None
        self._recovery_notice_pending = False
        self._initial_sync = True
        self.telegram_update_health = LoopHealth()
        self._telegram_update_started_at = time.time()
        self._telegram_update_health_persisted_at = 0.0
        self.thread_sync_health = LoopHealth()
        self._retry_jitter = random.random
        self._reported_unresolved_topic_creation_count: int | None = None

    def _require_binding(self) -> Binding:
        binding = self.store.binding()
        if binding is None:
            raise RuntimeError("Telegram group is not bound; run bootstrap first")
        return binding

    @property
    def codex_available(self) -> bool:
        return self._codex_ready.is_set() and self._codex_guard_reason is None

    @staticmethod
    def _normalized_degraded_reason(reason: str | None) -> str:
        if reason in CODEX_DEGRADED_LABELS:
            return str(reason)
        return "compatibility_guard"

    def degraded_reason_label(self) -> str:
        reason = self._normalized_degraded_reason(
            self.codex_degraded_reason
        )
        return CODEX_DEGRADED_LABELS[reason]

    def request_codex_reconnect(self, reason: str) -> None:
        normalized = self._normalized_degraded_reason(reason)
        if self.codex_available or self.codex_last_healthy_at is not None:
            self._recovery_notice_pending = True
        self.codex_degraded_reason = normalized
        self._codex_ready.clear()
        self.active_turns.clear()
        self.observed_turns.clear()
        self.busy_threads.clear()
        self._terminal_turns.clear()
        self._codex_reconnect_requested.set()

    def set_codex_guard(self, reason: str | None) -> None:
        """Apply a runtime compatibility/split-brain guard.

        A separate local monitor may call this hook after positively checking
        the Desktop shared-socket peer. Only fixed reason kinds are retained;
        raw diagnostic text never reaches Telegram.
        """

        if reason is None:
            self._codex_guard_reason = None
            self._codex_reconnect_requested.set()
            return
        normalized = self._normalized_degraded_reason(reason)
        self._codex_guard_reason = normalized
        self.request_codex_reconnect(normalized)

    async def _enter_codex_degraded(
        self,
        reason: str,
        *,
        expire_prompts: bool = True,
        recovery_expected: bool = True,
    ) -> None:
        normalized = self._normalized_degraded_reason(reason)
        already_degraded = (
            not self.codex_available
            and self.codex_degraded_reason == normalized
        )
        if recovery_expected and normalized != "connecting":
            self._recovery_notice_pending = True
        self.codex_degraded_reason = normalized
        self._codex_ready.clear()
        self.active_turns.clear()
        self.observed_turns.clear()
        self.busy_threads.clear()
        self._terminal_turns.clear()
        if expire_prompts and not already_degraded:
            self.pending_requests.clear()
            await self.expire_stale_prompt_cards(
                reason_text="после разрыва связи с Codex"
            )

    async def _mark_codex_healthy(self) -> None:
        self.codex_degraded_reason = None
        self.codex_last_healthy_at = time.time()
        server_version = getattr(self.codex, "server_version", None)
        if server_version:
            self.codex_last_healthy_version = truncate_utf8(
                redact_sensitive(str(server_version)),
                128,
            )
        self._codex_ready.set()
        if not self._recovery_notice_pending:
            return
        self._recovery_notice_pending = False
        queue = self.store.queue_health()
        active_queue = queue["pending"] + queue["dispatching"]
        with contextlib.suppress(TelegramError):
            await self._tg(
                "send_message",
                chat_id=self.binding.chat_id,
                text=(
                    "✅ Связь с Codex восстановлена. "
                    f"Сообщений в очереди: {active_queue}. "
                    "Они будут переданы по порядку."
                ),
            )

    async def _send_degraded_message(
        self,
        *,
        chat_id: int,
        topic_id: int | None,
        reply_to: int | None,
        queued: bool = False,
    ) -> None:
        suffix = (
            "Сообщение сохранено в очереди и будет передано после восстановления."
            if queued
            else (
                "Действие не выполнено. Обычное сообщение можно сохранить "
                "в очереди."
            )
        )
        await self._tg(
            "send_message",
            chat_id=chat_id,
            message_thread_id=topic_id,
            reply_to_message_id=reply_to,
            text=(
                "⚠️ Codex временно работает в режиме только очереди: "
                f"{self.degraded_reason_label()}. {suffix}"
            ),
        )

    def _report_unresolved_topic_creations(self) -> int:
        count = len(self.store.unresolved_topic_creations())
        previous = self._reported_unresolved_topic_creation_count
        if count == previous:
            return count
        self._reported_unresolved_topic_creation_count = count
        if count:
            LOGGER.error(
                "Telegram Topic creation requires reconciliation; "
                "unresolved_count=%d; automatic create is disabled",
                count,
            )
        elif previous:
            LOGGER.info(
                "Telegram Topic creation reconciliation is clear; "
                "unresolved_count=0"
            )
        return count

    async def _tg(self, method: str, *args: Any, **kwargs: Any) -> Any:
        async with self._telegram_slots:
            function = getattr(self.telegram, method)
            loop = asyncio.get_running_loop()
            future: asyncio.Future[Any] = loop.create_future()

            def finish_result(result: Any) -> None:
                if not future.done():
                    future.set_result(result)

            def finish_error(error: BaseException) -> None:
                if not future.done():
                    future.set_exception(error)

            def run() -> None:
                try:
                    result = function(*args, **kwargs)
                except BaseException as error:
                    with contextlib.suppress(RuntimeError):
                        loop.call_soon_threadsafe(finish_error, error)
                else:
                    with contextlib.suppress(RuntimeError):
                        loop.call_soon_threadsafe(finish_result, result)

            threading.Thread(
                target=run,
                name=f"telegram-{method}",
                daemon=True,
            ).start()
            try:
                return await future
            except TelegramError:
                # Callback acknowledgement is ephemeral UI feedback. An old
                # or expired callback must not poison the durable update
                # cursor after the real action has already been handled.
                if method == "answer_callback_query":
                    return False
                raise

    async def serve(
        self,
        *,
        on_ready: Callable[[], object] | None = None,
    ) -> None:
        await self._enter_codex_degraded(
            "connecting",
            expire_prompts=False,
            recovery_expected=False,
        )
        await self.expire_stale_prompt_cards()
        critical_tasks = [
            asyncio.create_task(self.telegram_loop(), name="telegram-loop"),
            asyncio.create_task(
                self.codex_connection_loop(),
                name="codex-connection-loop",
            ),
            asyncio.create_task(
                self.progress_heartbeat_loop(),
                name="progress-heartbeat-loop",
            ),
        ]
        setup_task = asyncio.create_task(
            self.telegram_setup_loop(),
            name="telegram-setup-loop",
        )
        stop_task = asyncio.create_task(
            self._stop_event.wait(),
            name="stop-waiter",
        )
        all_tasks = (*critical_tasks, setup_task, stop_task)
        if on_ready is not None:
            on_ready()
        try:
            completed, _ = await asyncio.wait(
                (*critical_tasks, stop_task),
                return_when=asyncio.FIRST_COMPLETED,
            )
            if stop_task in completed:
                return
            for task in critical_tasks:
                if task not in completed:
                    continue
                if task.cancelled():
                    raise RuntimeError(
                        f"Critical bridge worker {task.get_name()} "
                        "was cancelled unexpectedly"
                    )
                error = task.exception()
                if error is not None:
                    raise error
                raise RuntimeError(
                    f"Critical bridge worker {task.get_name()} "
                    "exited unexpectedly"
                )
            raise RuntimeError("Bridge supervisor lost all critical workers")
        finally:
            for task in all_tasks:
                task.cancel()
            await asyncio.gather(*all_tasks, return_exceptions=True)
            await self.codex.stop()

    async def expire_stale_prompt_cards(
        self,
        *,
        reason_text: str = "после перезапуска bridge",
    ) -> None:
        for thread_id, telegram_message_id in (
            self.store.expire_pending_prompt_cards()
        ):
            topic = self.store.topic_for_thread(thread_id)
            if topic is None:
                continue
            with contextlib.suppress(TelegramError):
                await self._tg(
                    "edit_message_text",
                    chat_id=topic.chat_id,
                    message_id=telegram_message_id,
                    text=(
                        f"⌛ Запрос истёк {reason_text}. "
                        "Ответьте на новый запрос в Codex App или Telegram."
                    ),
                    reply_markup={"inline_keyboard": []},
                )

    def stop(self) -> None:
        self._stop_event.set()

    async def telegram_setup_loop(self) -> None:
        retry_attempt = 0
        while True:
            try:
                await self._tg("set_commands")
                return
            except asyncio.CancelledError:
                raise
            except Exception as error:
                kind = loop_error_kind(error)
                delay = exponential_backoff_delay(
                    retry_attempt,
                    initial_seconds=TELEGRAM_UPDATE_BACKOFF_INITIAL_SECONDS,
                    maximum_seconds=TELEGRAM_UPDATE_BACKOFF_MAXIMUM_SECONDS,
                    jitter_ratio=LOOP_BACKOFF_JITTER_RATIO,
                    jitter_sample=self._retry_jitter(),
                )
                retry_attempt += 1
                LOGGER.warning(
                    "Telegram command setup failed; kind=%s; retrying in %.2fs",
                    kind,
                    delay,
                )
                await asyncio.sleep(delay)

    async def _wait_for_codex_retry(self, delay: float) -> None:
        self._codex_reconnect_requested.clear()
        try:
            await asyncio.wait_for(
                self._codex_reconnect_requested.wait(),
                timeout=delay,
            )
        except asyncio.TimeoutError:
            pass

    async def codex_connection_loop(self) -> None:
        retry_attempt = 0
        while True:
            if self._codex_guard_reason is not None:
                await self._enter_codex_degraded(
                    self._codex_guard_reason,
                    expire_prompts=False,
                )
                delay = exponential_backoff_delay(
                    retry_attempt,
                    initial_seconds=CODEX_RECONNECT_BACKOFF_INITIAL_SECONDS,
                    maximum_seconds=CODEX_RECONNECT_BACKOFF_MAXIMUM_SECONDS,
                    jitter_ratio=LOOP_BACKOFF_JITTER_RATIO,
                    jitter_sample=self._retry_jitter(),
                )
                retry_attempt += 1
                await self._wait_for_codex_retry(delay)
                continue

            self._codex_reconnect_requested.clear()
            try:
                await self.codex.start()
                # This full first pass performs the per-connection thread
                # subscriptions and reconciles history before queued work is
                # allowed to resume.
                self._initial_sync = True
                await self.sync_threads(dispatch_queue=False)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                reason = (
                    "protocol_incompatible"
                    if isinstance(error, CodexProtocolCompatibilityError)
                    else "socket_unavailable"
                )
                await self._enter_codex_degraded(reason)
                await self.codex.stop()
                delay = exponential_backoff_delay(
                    retry_attempt,
                    initial_seconds=CODEX_RECONNECT_BACKOFF_INITIAL_SECONDS,
                    maximum_seconds=CODEX_RECONNECT_BACKOFF_MAXIMUM_SECONDS,
                    jitter_ratio=LOOP_BACKOFF_JITTER_RATIO,
                    jitter_sample=self._retry_jitter(),
                )
                retry_attempt += 1
                LOGGER.warning(
                    "Codex connection unavailable; kind=%s; retrying in %.2fs",
                    reason,
                    delay,
                )
                await self._wait_for_codex_retry(delay)
                continue

            retry_attempt = 0
            await self._mark_codex_healthy()
            await self.dispatch_queued_capacity()

            monitor_task = asyncio.create_task(
                self.codex.wait_closed(),
                name="codex-app-server-monitor",
            )
            sync_task = asyncio.create_task(
                self.thread_sync_loop(),
                name="thread-sync-loop",
            )
            reconnect_task = asyncio.create_task(
                self._codex_reconnect_requested.wait(),
                name="codex-reconnect-waiter",
            )
            try:
                await asyncio.wait(
                    {monitor_task, sync_task, reconnect_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )
            finally:
                for task in (monitor_task, sync_task, reconnect_task):
                    task.cancel()
                for task in (monitor_task, sync_task, reconnect_task):
                    with contextlib.suppress(
                        asyncio.CancelledError,
                        CodexProtocolError,
                    ):
                        await task

            reason = self._codex_guard_reason or "connection_lost"
            await self._enter_codex_degraded(reason)
            await self.codex.stop()
            delay = exponential_backoff_delay(
                retry_attempt,
                initial_seconds=CODEX_RECONNECT_BACKOFF_INITIAL_SECONDS,
                maximum_seconds=CODEX_RECONNECT_BACKOFF_MAXIMUM_SECONDS,
                jitter_ratio=LOOP_BACKOFF_JITTER_RATIO,
                jitter_sample=self._retry_jitter(),
            )
            retry_attempt += 1
            await self._wait_for_codex_retry(delay)

    async def telegram_loop(self) -> None:
        offset = self.store.telegram_offset()
        retry_attempt = 0
        self._persist_telegram_update_health(force=True)
        while True:
            try:
                poll_timeout = (
                    0
                    if self.telegram_update_health.consecutive_failures
                    >= TELEGRAM_UPDATE_LOCAL_FAILURES
                    else self.config.telegram_long_poll_seconds
                )
                updates = await self._tg(
                    "get_updates",
                    offset=offset,
                    timeout=poll_timeout,
                )
                for update in updates:
                    update_id = int(update["update_id"])
                    if not self.store.telegram_update_processed(update_id):
                        await self.handle_telegram_update(update)
                        self.store.mark_telegram_update_processed(update_id)
                    offset = update_id + 1
                    self.store.set_telegram_offset(offset)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                kind = loop_error_kind(error)
                error_type = type(error).__name__
                self.telegram_update_health.record_failure(kind, error_type)
                self._persist_telegram_update_health(force=True)
                delay = exponential_backoff_delay(
                    retry_attempt,
                    initial_seconds=TELEGRAM_UPDATE_BACKOFF_INITIAL_SECONDS,
                    maximum_seconds=TELEGRAM_UPDATE_BACKOFF_MAXIMUM_SECONDS,
                    jitter_ratio=LOOP_BACKOFF_JITTER_RATIO,
                    jitter_sample=self._retry_jitter(),
                )
                retry_attempt += 1
                LOGGER.warning(
                    "Telegram update loop failed; kind=%s; error_type=%s; "
                    "retrying in %.2fs",
                    kind,
                    error_type,
                    delay,
                )
                await asyncio.sleep(delay)
            else:
                retry_attempt = 0
                recovered = self.telegram_update_health.consecutive_failures > 0
                self.telegram_update_health.record_success()
                self._persist_telegram_update_health(force=recovered)

    def _persist_telegram_update_health(self, *, force: bool = False) -> None:
        health = self.telegram_update_health
        now = time.time()
        if not force and now - self._telegram_update_health_persisted_at < 60.0:
            return
        try:
            write_telegram_update_health(
                self.config.state_dir,
                {
                    "pid": os.getpid(),
                    "startedAt": self._telegram_update_started_at,
                    "lastSuccessAt": health.last_success_at,
                    "lastErrorAt": health.last_error_at,
                    "lastErrorKind": health.last_error_kind,
                    "lastErrorType": health.last_error_type,
                    "consecutiveFailures": health.consecutive_failures,
                    "updatedAt": now,
                },
            )
            self._telegram_update_health_persisted_at = now
        except OSError as error:
            LOGGER.warning(
                "Telegram update health persistence failed; error_type=%s",
                type(error).__name__,
            )

    def local_watchdog_healthy(self) -> bool:
        health = self.telegram_update_health
        if (
            health.last_error_kind != "unexpected"
            or health.consecutive_failures < TELEGRAM_UPDATE_LOCAL_FAILURES
        ):
            return True
        reference = (
            health.last_success_at
            if health.last_success_at is not None
            else self._telegram_update_started_at
        )
        return time.time() - reference <= TELEGRAM_UPDATE_STALE_SECONDS

    async def thread_sync_loop(self) -> None:
        retry_attempt = 0
        while True:
            # Preserve notifications that arrive during sync: clearing before
            # the read means a later set() wakes the post-sync wait.
            self._sync_requested.clear()
            try:
                await self.sync_threads()
            except asyncio.CancelledError:
                raise
            except CodexProtocolError:
                self.thread_sync_health.record_failure("codex_protocol")
                LOGGER.warning(
                    "Codex thread sync lost the shared connection; reconnecting"
                )
                self.request_codex_reconnect("connection_lost")
                return
            except Exception as error:
                kind = loop_error_kind(error)
                self.thread_sync_health.record_failure(kind)
                delay = exponential_backoff_delay(
                    retry_attempt,
                    initial_seconds=THREAD_SYNC_BACKOFF_INITIAL_SECONDS,
                    maximum_seconds=THREAD_SYNC_BACKOFF_MAXIMUM_SECONDS,
                    jitter_ratio=LOOP_BACKOFF_JITTER_RATIO,
                    jitter_sample=self._retry_jitter(),
                )
                retry_attempt += 1
                LOGGER.warning(
                    "Codex thread sync failed; kind=%s; retrying in %.2fs",
                    kind,
                    delay,
                )
                await asyncio.sleep(delay)
            else:
                retry_attempt = 0
                self.thread_sync_health.record_success()
                try:
                    await asyncio.wait_for(
                        self._sync_requested.wait(),
                        timeout=self.config.thread_poll_seconds,
                    )
                except asyncio.TimeoutError:
                    pass

    async def _refresh_progress_heartbeats(self) -> None:
        for thread_id in tuple(self.busy_threads):
            turn_id = (
                self.active_turns.get(thread_id)
                or self.observed_turns.get(thread_id)
            )
            topic = self.store.topic_for_thread(thread_id)
            if (
                not turn_id
                or topic is None
                or topic.archived
            ):
                continue
            try:
                await self._render_progress_card(
                    topic,
                    turn_id=turn_id,
                    closed=False,
                    heartbeat_only=True,
                )
            except asyncio.CancelledError:
                raise
            except Exception as error:
                LOGGER.warning(
                    "Progress heartbeat update failed; kind=%s",
                    loop_error_kind(error),
                )

    async def progress_heartbeat_loop(self) -> None:
        while True:
            await self._refresh_progress_heartbeats()
            await asyncio.sleep(PROGRESS_HEARTBEAT_CHECK_SECONDS)

    @staticmethod
    def _telegram_media_descriptor(
        message: dict[str, Any],
    ) -> tuple[str, dict[str, Any]] | None:
        document = message.get("document")
        if isinstance(document, dict):
            return "document", document
        voice = message.get("voice")
        if isinstance(voice, dict):
            return "voice", voice
        video_note = message.get("video_note")
        if isinstance(video_note, dict):
            return "video_note", video_note
        video = message.get("video")
        if isinstance(video, dict):
            return "video", video
        return None

    async def _local_stt_transcript(
        self,
        prepared: PreparedMedia,
    ) -> str | None:
        command = os.environ.get("CODEX_TELEGRAM_LOCAL_STT_COMMAND", "").strip()
        if not command or prepared.kind != "voice":
            return None
        admission_reason = self._local_stt_admission_reason()
        if admission_reason is not None:
            # Native audio remains a fully supported input. Skipping an
            # optional transcript is preferable to competing with a live
            # Codex turn for the last memory on a small bridge host.
            LOGGER.info("Local STT skipped; reason=%s", admission_reason)
            return None
        audio = next(
            (item.path for item in prepared.inputs if item.kind == "localAudio"),
            None,
        )
        if not audio:
            return None
        command_path = Path(command).expanduser().resolve(strict=False)
        try:
            status = command_path.stat()
            if (
                command_path.is_symlink()
                or not command_path.is_file()
                or status.st_uid != os.getuid()
                or not os.access(command_path, os.X_OK)
            ):
                return None
        except OSError:
            return None
        timeout = max(
            1,
            int(os.environ.get("CODEX_TELEGRAM_LOCAL_STT_TIMEOUT_SECONDS", "210")),
        )
        max_duration = max(
            1,
            int(os.environ.get("CODEX_TELEGRAM_LOCAL_STT_MAX_DURATION_SECONDS", "60")),
        )
        try:
            process = await asyncio.create_subprocess_exec(
                str(command_path),
                audio,
                "--duration-seconds",
                str(prepared.duration_seconds),
                "--max-duration-seconds",
                str(max_duration),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
            )
            stdout, _ = await asyncio.wait_for(process.communicate(), timeout=timeout)
        except (OSError, ValueError, asyncio.TimeoutError):
            return None
        if process.returncode != 0:
            return None
        transcript = stdout.decode("utf-8", errors="replace").strip()
        return transcript[:8_000] or None

    async def _remote_stt_transcript(
        self,
        *,
        media_key: str,
        source_path: Path,
        duration_seconds: int,
    ) -> str | None:
        """Use the optional isolated worker without ever loading STT on Pi."""

        worker = self.media_worker
        transcribe = getattr(worker, "transcribe", None)
        if not callable(transcribe):
            return None
        try:
            transcript = await asyncio.to_thread(
                transcribe,
                media_key=media_key,
                source_path=source_path,
                duration_seconds=duration_seconds,
                destination_directory=self.media.message_directory(media_key),
            )
        except Exception as error:
            # The worker client already uses deterministic job IDs, bounded
            # retries, mTLS, and a circuit breaker.  Its failure must remain
            # isolated from the Telegram process and must not cause a local
            # Whisper process to contend for Pi memory as a fallback.
            LOGGER.warning(
                "Remote STT unavailable; reason=%s",
                type(error).__name__,
            )
            return None
        if not isinstance(transcript, str):
            return None
        sanitized = transcript.replace("\x00", "").strip()
        return sanitized[:8_000] or None

    def _local_stt_admission_reason(self) -> str | None:
        """Return a local-only reason to preserve native-audio fallback.

        Local speech recognition is optional and can transiently consume a
        large fraction of a small host. It must never start alongside any
        active Codex work, and Linux hosts must retain a conservative amount
        of immediately available memory before it is admitted.
        """

        if self.busy_threads or self.active_turns:
            return "active_turn"
        available_memory = self._linux_available_memory_bytes()
        if available_memory is None:
            return None
        minimum_memory = self._local_stt_min_available_memory_bytes()
        if available_memory < minimum_memory:
            return "low_memory"
        return None

    @staticmethod
    def _local_stt_min_available_memory_bytes() -> int:
        raw = os.environ.get(
            "CODEX_TELEGRAM_LOCAL_STT_MIN_AVAILABLE_MEMORY_MIB",
            str(LOCAL_STT_DEFAULT_MIN_AVAILABLE_MEMORY_MIB),
        )
        try:
            memory_mib = int(raw)
        except ValueError:
            memory_mib = LOCAL_STT_DEFAULT_MIN_AVAILABLE_MEMORY_MIB
        return max(0, memory_mib) * 1024 * 1024

    @staticmethod
    def _linux_available_memory_bytes() -> int | None:
        """Read Linux MemAvailable without importing an optional dependency."""

        try:
            for line in Path("/proc/meminfo").read_text(
                encoding="ascii",
                errors="strict",
            ).splitlines():
                fields = line.split()
                if (
                    len(fields) == 3
                    and fields[0] == "MemAvailable:"
                    and fields[2] == "kB"
                ):
                    return max(0, int(fields[1])) * 1024
        except (OSError, UnicodeError, ValueError):
            return None
        return None

    async def _prepare_telegram_media(
        self,
        message: dict[str, Any],
        *,
        client_id: str,
        user_text: str,
    ) -> tuple[str, tuple[LocalInput, ...]]:
        descriptor = self._telegram_media_descriptor(message)
        if descriptor is None:
            return user_text, ()
        kind, media = descriptor
        file_id = str(media.get("file_id") or "")
        duration = max(0, int(media.get("duration") or 0))
        known_size = max(0, int(media.get("file_size") or 0))
        if not file_id:
            raise MediaProcessingError("invalid_media")
        if known_size > self.config.telegram_media_max_bytes:
            raise MediaProcessingError("file_too_large")

        with contextlib.suppress(TelegramError):
            await self._tg(
                "send_chat_action",
                chat_id=int((message.get("chat") or {}).get("id") or 0),
                message_thread_id=(
                    int(message.get("message_thread_id") or 0) or None
                ),
                action="typing",
            )

        media_key = hashlib.sha256(client_id.encode("utf-8")).hexdigest()[:32]
        protected = self.store.active_local_input_paths()
        await asyncio.to_thread(
            self.media.prune,
            protected_paths=protected,
        )
        media_pipeline = HybridMediaProcessor(
            local=self.media,
            worker=self.media_worker,
        )
        if kind == "document":
            source = await asyncio.to_thread(
                self.media.document_path,
                media_key,
                str(media.get("file_name") or "document.bin"),
            )
        else:
            source = await asyncio.to_thread(self.media.source_path, media_key)
        file_info = await self._tg("get_file", file_id)
        remote_size = max(0, int((file_info or {}).get("file_size") or 0))
        if remote_size > self.config.telegram_media_max_bytes:
            raise MediaProcessingError("file_too_large")
        file_path = str((file_info or {}).get("file_path") or "")
        if not file_path:
            raise MediaProcessingError("invalid_media")
        try:
            await self._tg(
                "download_file",
                file_path=file_path,
                destination=source,
                max_bytes=self.config.telegram_media_max_bytes,
            )
        except TelegramError as error:
            if error.kind == "file_too_large":
                raise MediaProcessingError("file_too_large") from None
            if error.kind == "unsafe_destination":
                raise MediaProcessingError("unsafe_storage") from None
            if error.kind == "unsafe_file_path":
                raise MediaProcessingError("invalid_media") from None
            raise
        if kind == "document":
            prepared_document: PreparedDocument = await asyncio.to_thread(
                self.media.prepare_document,
                media_key=media_key,
                source_path=source,
                display_name=str(media.get("file_name") or source.name),
                mime_type=str(
                    media.get("mime_type") or "application/octet-stream"
                ),
            )
            prepared_text = document_request_text(
                prepared_document,
                user_text=user_text,
            )
            prepared_inputs = (prepared_document.input,)
        elif kind == "voice":
            prepared: PreparedMedia = await asyncio.to_thread(
                media_pipeline.prepare_voice,
                media_key=media_key,
                source_path=source,
                duration_seconds=duration,
            )
        elif kind == "video_note":
            prepared = await asyncio.to_thread(
                media_pipeline.prepare_video_note,
                media_key=media_key,
                source_path=source,
                duration_seconds=duration,
            )
        else:
            prepared = await asyncio.to_thread(
                media_pipeline.prepare_video,
                media_key=media_key,
                source_path=source,
                duration_seconds=duration,
            )
        if kind != "document":
            transcript = None
            if kind == "voice":
                transcript = await self._remote_stt_transcript(
                    media_key=media_key,
                    source_path=source,
                    duration_seconds=duration,
                )
                if transcript is None and callable(
                    getattr(self.media_worker, "transcribe", None)
                ):
                    # A configured remote worker is authoritative for voice
                    # recognition.  Do not fall back to native audio (which
                    # Codex cannot transcribe) or to RAM-heavy local STT.
                    raise MediaProcessingError("transcription_unavailable")
            if transcript is None:
                transcript = await self._local_stt_transcript(prepared)
            if transcript is not None:
                prepared_text = (
                    f"{user_text.strip()}\n\n" if user_text.strip() else ""
                ) + f"🎙 Расшифровка голосового сообщения:\n{transcript}"
                prepared_inputs = ()
            else:
                prepared_text = media_request_text(prepared, user_text=user_text)
                prepared_inputs = prepared.inputs
        await asyncio.to_thread(
            self.media.prune,
            protected_paths=(
                *protected,
                *(item.path for item in prepared_inputs),
            ),
        )
        return prepared_text, prepared_inputs

    async def _send_media_processing_error(
        self,
        message: dict[str, Any],
        error: MediaProcessingError,
    ) -> None:
        if error.kind == "file_too_large":
            text = (
                "Файл больше 20 МБ — облачный Telegram Bot API не позволяет "
                "боту скачать его. Отправьте файл меньшего размера."
            )
        elif error.kind == "transcription_unavailable":
            text = (
                "Расшифровка голосового сообщения сейчас недоступна. "
                "Сообщение не передано в Codex как бесполезный аудиофайл; "
                "отправьте его повторно через минуту или напишите текстом."
            )
        elif error.kind == "ffmpeg_unavailable":
            text = (
                "Обработка голосовых и видео временно недоступна. "
                "Попробуйте ещё раз позже или отправьте текст."
            )
        elif error.kind == "unsafe_storage":
            text = (
                "Локальное хранилище медиа сейчас недоступно. "
                "Сообщение не было передано в Codex."
            )
        else:
            text = (
                "Не удалось прочитать Telegram-файл. "
                "Отправьте его ещё раз либо напишите текстом."
            )
        await self._tg(
            "send_message",
            chat_id=int((message.get("chat") or {}).get("id") or 0),
            message_thread_id=(
                int(message.get("message_thread_id") or 0) or None
            ),
            reply_to_message_id=int(message.get("message_id") or 0) or None,
            text=text,
        )

    async def handle_telegram_update(self, update: dict[str, Any]) -> None:
        if update.get("callback_query"):
            await self.handle_callback(update["callback_query"])
            return
        # Edits are not new user intent. Treating edited_message as a fresh
        # command can duplicate turns or approvals.
        message = update.get("message")
        if not message:
            return
        chat = message.get("chat") or {}
        chat_id = int(chat.get("id") or 0)
        topic_id = int(message.get("message_thread_id") or 0)

        created = message.get("forum_topic_created")
        if created and chat_id == self.binding.chat_id and topic_id:
            created_title = clean_topic_title(created.get("name"))
            self.store.observe_topic(
                chat_id,
                topic_id,
                created_title,
            )
            if (
                created_title == ARCHIVE_TOPIC_TITLE
                and self.store.archive_hub_topic_id() is None
            ):
                self.store.set_archive_hub_topic_id(topic_id)
            return

        sender = message.get("from") or {}
        if (
            chat_id != self.binding.chat_id
            or int(sender.get("id") or 0) != self.binding.allowed_user_id
            or sender.get("is_bot")
        ):
            return

        text = str(message.get("text") or message.get("caption") or "").strip()
        has_media = self._telegram_media_descriptor(message) is not None
        if not text and not has_media:
            return
        source_message_id = int(message["message_id"])
        client_id = f"tg:{chat_id}:{source_message_id}"
        prepared_text: str | None = None
        local_inputs: tuple[LocalInput, ...] = ()

        replied = message.get("reply_to_message") or {}
        replied_sender = replied.get("from") or {}
        replied_message_id = int(replied.get("message_id") or 0)
        is_reply_to_bridge = bool(
            replied_message_id
            and int(replied_sender.get("id") or 0) == self.binding.bot_id
            and replied_sender.get("is_bot")
        )
        reply_context = (
            telegram_reply_context(message)
            if is_reply_to_bridge
            else None
        )
        if is_reply_to_bridge:
            if has_media:
                try:
                    prepared_text, local_inputs = (
                        await self._prepare_telegram_media(
                            message,
                            client_id=client_id,
                            user_text=text,
                        )
                    )
                except MediaProcessingError as error:
                    await self._send_media_processing_error(message, error)
                    return
            if (
                not has_media
                and await self.handle_person_review_text_reply(
                    message=message,
                    text=text,
                )
            ):
                return
            prompt = self.store.claim_control_prompt(
                chat_id=chat_id,
                topic_id=topic_id,
                telegram_message_id=replied_message_id,
            )
            if prompt is not None:
                await self.handle_control_prompt_reply(
                    prompt,
                    message=message,
                    text=prepared_text or text,
                    local_inputs=local_inputs,
                )
                return
            if str(replied.get("text") or "") in CONTROL_PROMPT_TEXT.values():
                await self._tg(
                    "send_message",
                    chat_id=chat_id,
                    message_thread_id=topic_id,
                    reply_to_message_id=int(message["message_id"]),
                    text=(
                        "Эта форма уже использована или недоступна. "
                        "Откройте свежий /status."
                    ),
                )
                return

        if self.is_new_thread_prompt_reply(message):
            mode, payload = "new", text
        else:
            mode, payload = parse_message_mode(text, self.binding.bot_username)
        topic = self.store.topic_for_telegram(chat_id, topic_id)
        if mode == "limits":
            await self.send_limits(
                topic_id=topic_id,
                reply_to=int(message["message_id"]),
            )
            return
        if topic_id == self.store.archive_hub_topic_id():
            if mode == "status":
                await self._render_archive_index()
                await self._tg(
                    "send_message",
                    chat_id=chat_id,
                    message_thread_id=topic_id,
                    reply_to_message_id=int(message["message_id"]),
                    text="Архивный реестр обновлён.",
                )
            else:
                await self._tg(
                    "send_message",
                    chat_id=chat_id,
                    message_thread_id=topic_id,
                    reply_to_message_id=int(message["message_id"]),
                    text=(
                        "Это служебный Topic архива. "
                        "Используйте кнопки под реестром."
                    ),
                )
            return
        if topic is not None and topic.archived:
            await self._delete_resurrected_archived_topic(topic)
            return
        if mode == "audit":
            await self.send_audit(
                topic_id=topic_id,
                reply_to=int(message["message_id"]),
            )
            return
        if mode == "mode":
            if topic is None:
                await self._tg(
                    "send_message",
                    chat_id=chat_id,
                    message_thread_id=(
                        None if topic_id in {0, 1} else topic_id
                    ),
                    reply_to_message_id=int(message["message_id"]),
                    text="/mode доступен только в связанном рабочем Topic.",
                )
                return
            await self.send_status(
                topic,
                reply_to=int(message["message_id"]),
                mode_picker=True,
            )
            return
        if mode == "status":
            await self.send_status(topic, reply_to=int(message["message_id"]))
            return
        if mode == "archive" and payload:
            await self._tg(
                "send_message",
                chat_id=chat_id,
                message_thread_id=(
                    None if topic_id in {0, 1} else topic_id
                ),
                reply_to_message_id=int(message["message_id"]),
                text="Формат: /archive",
            )
            return
        if has_media and prepared_text is None:
            try:
                prepared_text, local_inputs = (
                    await self._prepare_telegram_media(
                        message,
                        client_id=client_id,
                        user_text=payload,
                    )
                )
            except MediaProcessingError as error:
                await self._send_media_processing_error(message, error)
                return
        request_text = prepared_text if prepared_text is not None else payload
        if not self.codex_available:
            reply_to = int(message["message_id"])
            if topic is not None:
                has_pending_prompt = bool(
                    self._pending_for_thread(
                        topic.thread_id,
                        kind="approval",
                    )
                    or self._pending_for_thread(
                        topic.thread_id,
                        kind="user_input",
                    )
                )
                if (
                    mode in {"plain", "queue"}
                    and request_text
                    and not has_pending_prompt
                ):
                    queued = self.store.enqueue(
                        thread_id=topic.thread_id,
                        chat_id=chat_id,
                        topic_id=topic.topic_id,
                        telegram_message_id=reply_to,
                        text=with_telegram_reply_context(
                            request_text,
                            reply_context,
                        ),
                        client_id=f"tg:{chat_id}:{reply_to}",
                        local_inputs=local_inputs,
                    )
                    await self._announce_queued(
                        queued,
                        text=(
                            "⚠️ Codex временно недоступен; сообщение "
                            "сохранено в очереди и будет передано после "
                            "восстановления."
                        ),
                        allow_steer=False,
                    )
                    return
            await self._send_degraded_message(
                chat_id=chat_id,
                topic_id=None if topic_id in {0, 1} else topic_id,
                reply_to=reply_to,
            )
            return
        if mode == "cancel":
            await self.cancel_turn(topic, reply_to=int(message["message_id"]))
            return
        if mode == "archive":
            if topic is None:
                await self._tg(
                    "send_message",
                    chat_id=chat_id,
                    message_thread_id=(
                        None if topic_id in {0, 1} else topic_id
                    ),
                    reply_to_message_id=int(message["message_id"]),
                    text="/archive доступен только в связанном рабочем Topic.",
                )
                return
            await self.archive_thread_from_telegram(
                topic,
                reply_to=int(message["message_id"]),
            )
            return
        if mode == "new":
            if not request_text:
                await self.prompt_new_thread(
                    topic_id=topic_id,
                    reply_to=int(message["message_id"]),
                )
                return
            create_arguments: dict[str, Any] = {
                "source_message_id": int(message["message_id"]),
            }
            if local_inputs:
                create_arguments["local_inputs"] = local_inputs
            descriptor = self._telegram_media_descriptor(message)
            if descriptor is not None and descriptor[0] == "voice":
                create_arguments["defer_title_to_codex"] = True
            await self.create_thread_from_general(
                request_text,
                **create_arguments,
            )
            return
        if (
            topic
            and mode == "plain"
            and not has_media
            and await self._try_resolve_pending_text(
                topic.thread_id,
                text,
                telegram_reply_to_message_id=int(
                    (message.get("reply_to_message") or {}).get("message_id")
                    or 0
                )
                or None,
                source_message_id=int(message["message_id"]),
            )
        ):
            return

        if is_reply_to_bridge and request_text:
            request_text = with_telegram_reply_context(
                request_text,
                reply_context,
            )

        if topic is None:
            if topic_id in {0, 1}:
                await self._tg(
                    "send_message",
                    chat_id=chat_id,
                    text=(
                        "Используйте /new <задача> для нового треда "
                        "или откройте один из Topic."
                    ),
                    reply_to_message_id=int(message["message_id"]),
                )
                return
            topic = await self.bind_manual_topic(
                topic_id,
                request_text or text,
                source_message_id=int(message["message_id"]),
            )
            if topic is None:
                return

        if not request_text:
            await self._tg(
                "send_message",
                chat_id=chat_id,
                message_thread_id=topic.topic_id,
                reply_to_message_id=int(message["message_id"]),
                text="После команды нужен текст сообщения.",
            )
            return

        if mode == "steer":
            await self.send_steer_or_start(
                topic=topic,
                text=request_text,
                client_id=client_id,
                reply_to=int(message["message_id"]),
                local_inputs=local_inputs,
            )
            return

        queued = self.store.enqueue(
            thread_id=topic.thread_id,
            chat_id=chat_id,
            topic_id=topic.topic_id,
            telegram_message_id=int(message["message_id"]),
            text=request_text,
            client_id=client_id,
            local_inputs=local_inputs,
        )
        if queued.status == "sent":
            return
        if mode == "queue" or self.is_thread_busy(topic.thread_id):
            await self._announce_queued(
                queued,
                text="↪️ Поставлено в очередь следующего хода.",
            )
            if mode == "queue" and not self.is_thread_busy(topic.thread_id):
                await self.dispatch_next_queued(topic.thread_id)
            return

        await self.dispatch_next_queued(topic.thread_id)
        current = self.store.queued_message(queued.queue_id) or queued
        if current.status != "sent":
            await self._announce_queued(
                current,
                text=(
                    "↪️ Ход не стартовал; сообщение сохранено в очереди."
                    if self.codex_available
                    else (
                        "⚠️ Связь с Codex потеряна; сообщение сохранено "
                        "в очереди до восстановления."
                    )
                ),
                allow_steer=self.codex_available,
            )

    async def _announce_queued(
        self,
        queued: QueuedMessage,
        *,
        text: str,
        allow_steer: bool = True,
    ) -> None:
        current = self.store.queued_message(queued.queue_id) or queued
        if current.status_message_id is not None:
            return
        delivery = self.store.reserve_queue_announcement_delivery(
            current.queue_id
        )
        if not delivery.newly_reserved:
            # A concurrent or prior attempt owns this logical send and may
            # already be visible even when its Telegram response was lost.
            return
        try:
            sent = await self._tg(
                "send_message",
                chat_id=current.chat_id,
                message_thread_id=current.topic_id,
                reply_to_message_id=current.telegram_message_id,
                text=text,
                reply_markup=(
                    queue_keyboard(current.queue_id)
                    if allow_steer
                    else {"inline_keyboard": []}
                ),
            )
        except TelegramError as error:
            if error.outcome_ambiguous:
                self.store.mark_queue_announcement_outcome_unknown(
                    current.queue_id
                )
            else:
                self.store.clear_queue_announcement_delivery_after_definite_fai…33460 tokens truncated…                if item.get("type") == "agentMessage"
            ),
            None,
        )
        status = last_turn.get("status")
        recently_updated = (
            int(thread.get("updatedAt") or 0) > 0
            and time.time() - int(thread.get("updatedAt") or 0) < 20
        )
        has_unfinished_shape = (
            last_turn.get("completedAt") is None
            and last_agent is not None
            and last_agent.get("phase") != "final_answer"
            and recently_updated
        )
        if (
            (
                isinstance(thread_status, dict)
                and thread_status.get("type") == "active"
            )
            or status == "inProgress"
            or has_unfinished_shape
        ):
            self.busy_threads.add(thread_id)
        elif status in {"completed", "failed", "interrupted"} and turn_id:
            self._clear_terminal_turn_tracking(thread_id, turn_id)
        elif thread_id not in self.active_turns:
            self.busy_threads.discard(thread_id)

    async def _observed_topic_candidate_is_live(
        self,
        *,
        chat_id: int,
        topic_id: int,
    ) -> bool:
        try:
            await self._tg(
                "send_chat_action",
                chat_id=chat_id,
                message_thread_id=topic_id,
                action="typing",
            )
        except TelegramError as error:
            if _telegram_error_contains(
                error,
                "message thread not found",
                "forum topic not found",
                "topic not found",
                "topic_id_invalid",
            ):
                self.store.forget_observed_topic(chat_id, topic_id)
                return False
            raise
        return True

    async def _create_topic_for_thread(
        self, *, thread_id: str, title: str, updated_at: int
    ) -> TopicBinding:
        async with self._topic_create_lock:
            existing = self.store.topic_for_thread(thread_id)
            if existing is not None:
                return existing

            intent = self.store.topic_creation_intent(thread_id)
            if intent is not None:
                if intent.status in {"created", "completed"}:
                    topic = self.store.complete_topic_creation(
                        thread_id,
                        archived=False,
                        last_updated_at=updated_at,
                    )
                    if intent.status == "created":
                        await self._send_topic_header(topic)
                        await asyncio.sleep(0.2)
                    return topic
                if intent.chat_id != self.binding.chat_id:
                    raise RuntimeError(
                        "topic creation was replayed with different parameters"
                    )
                creation_title = intent.title
                if intent.status == "outcome_unknown":
                    candidates = [
                        candidate
                        for candidate in self.store.unbound_observed_topic_ids(
                            chat_id=self.binding.chat_id,
                            title=creation_title,
                            observed_after=intent.updated_at,
                        )
                        if candidate != self.store.archive_hub_topic_id()
                    ]
                    if len(candidates) == 1:
                        if not await self._observed_topic_candidate_is_live(
                            chat_id=self.binding.chat_id,
                            topic_id=candidates[0],
                        ):
                            self._report_unresolved_topic_creations()
                            raise TopicCreationUnresolvedError(
                                "Telegram Topic candidate is no longer live"
                            )
                        self.store.record_created_topic(
                            thread_id,
                            candidates[0],
                        )
                        topic = self.store.complete_topic_creation(
                            thread_id,
                            archived=False,
                            last_updated_at=updated_at,
                        )
                        await self._send_topic_header(topic)
                        await asyncio.sleep(0.2)
                        self._report_unresolved_topic_creations()
                        return topic
                    self._report_unresolved_topic_creations()
                    raise TopicCreationUnresolvedError(
                        "Telegram Topic creation requires reconciliation"
                    )
                # A durable reserved intent proves that the remote call had
                # not started yet. Resume it instead of wedging the mapping.
                if intent.status != "reserved":
                    raise RuntimeError(
                        f"invalid topic creation state: {intent.status}"
                    )
            else:
                creation_title = title
                self.store.reserve_topic_creation(
                    thread_id=thread_id,
                    chat_id=self.binding.chat_id,
                    title=creation_title,
                )
            self.store.mark_topic_creation_outcome_unknown(thread_id)
            try:
                result = await self._tg(
                    "create_forum_topic",
                    self.binding.chat_id,
                    creation_title,
                )
                topic_id = int(result["message_thread_id"])
            except TelegramError as error:
                if error.outcome_ambiguous:
                    self.store.mark_topic_creation_outcome_unknown(thread_id)
                    self._report_unresolved_topic_creations()
                    raise TopicCreationUnresolvedError(
                        "Telegram Topic creation outcome is ambiguous"
                    ) from error
                self.store.clear_topic_creation_after_definite_failure(
                    thread_id
                )
                raise
            except Exception:
                self._report_unresolved_topic_creations()
                raise

            self.store.record_created_topic(thread_id, topic_id)
            topic = self.store.complete_topic_creation(
                thread_id,
                archived=False,
                last_updated_at=updated_at,
            )
            await self._send_topic_header(topic)
            await asyncio.sleep(0.2)
            return topic

    async def _send_topic_header(self, topic: TopicBinding) -> None:
        mode = self.thread_modes.get(topic.thread_id)
        mode_lines = (
            []
            if mode is None
            else [
                (
                    f"Режим: 🧠 {effort_label(mode.effort)} · "
                    f"⚡ {speed_label(mode.service_tier)}."
                ),
                "/mode — изменить интеллект и скорость.",
            ]
        )
        await self._tg(
            "send_message",
            chat_id=topic.chat_id,
            message_thread_id=topic.topic_id,
            text="\n".join(
                [
                    "🔗 Topic связан с тредом Codex этого проекта.",
                    "",
                    *mode_lines,
                    (
                        "Текст, голосовое или видеокружок — следующий ход "
                        "или очередь, если Codex занят."
                    ),
                    (
                        f"/steer текст или @{self.binding.bot_username} "
                        "текст — передать в текущий ход."
                    ),
                    (
                        "Для голосового steer: /status → «В текущий ход» → "
                        "ответьте на форму голосовым или кружком."
                    ),
                    "/status — состояние, /cancel — остановка.",
                ]
            ),
        )

    @staticmethod
    def _rich_fallback_allowed(error: TelegramError) -> bool:
        return (
            not error.outcome_ambiguous
            and error.kind in {"api", "http"}
        )

    async def _render_progress_card(
        self,
        topic: TopicBinding,
        *,
        turn_id: str,
        closed: bool,
        outcome: str = "completed",
        force: bool = False,
        heartbeat_only: bool = False,
    ) -> int | None:
        lock = self._progress_locks.setdefault(
            topic.thread_id,
            asyncio.Lock(),
        )
        async with lock:
            context = self.store.turn_context(topic.thread_id, turn_id)
            elapsed_minutes = (
                0
                if context is None
                else progress_elapsed_minutes(context.created_at)
            )
            rendered_key = (topic.thread_id, turn_id)
            if heartbeat_only:
                last_rendered = self._progress_elapsed_minutes_rendered.get(
                    rendered_key,
                    0,
                )
                if (
                    closed
                    or context is None
                    or context.progress_closed
                    or elapsed_minutes < 1
                    or elapsed_minutes <= last_rendered
                ):
                    return (
                        None
                        if context is None
                        else context.status_message_id
                    )
            if (
                not force
                and closed
                and context is not None
                and context.progress_closed
                and context.progress_outcome == outcome
            ):
                return context.status_message_id
            return await self._render_progress_card_unlocked(
                topic,
                turn_id=turn_id,
                closed=closed,
                outcome=outcome,
            )

    async def _render_progress_card_unlocked(
        self,
        topic: TopicBinding,
        *,
        turn_id: str,
        closed: bool,
        outcome: str = "completed",
    ) -> int | None:
        entries = self.store.progress_entries(topic.thread_id, turn_id)
        if not entries:
            return None
        context = self.store.upsert_turn_context(
            thread_id=topic.thread_id,
            turn_id=turn_id,
        )
        elapsed_minutes = (
            0
            if closed
            else progress_elapsed_minutes(context.created_at)
        )
        rich_message = build_rich_progress_message(
            entries,
            closed=closed,
            outcome=outcome,
            elapsed_minutes=elapsed_minutes,
        )
        fallback_text = build_fallback_progress_text(
            entries,
            closed=closed,
            outcome=outcome,
            elapsed_minutes=elapsed_minutes,
        )
        reply_markup = progress_collapse_keyboard() if closed else None
        mode = context.progress_render_mode or "rich_details"
        message_id = context.status_message_id

        if message_id is None and context.progress_send_outcome_unknown:
            return None

        if message_id is not None:
            if closed and not context.progress_closed:
                try:
                    await self._tg(
                        "edit_message_text",
                        chat_id=topic.chat_id,
                        message_id=message_id,
                        text=progress_collapsed_reset_text(
                            entries,
                            outcome=outcome,
                        ),
                        reply_markup=reply_markup,
                    )
                except TelegramError as error:
                    if "message is not modified" not in str(error).lower():
                        raise
                await asyncio.sleep(PROGRESS_COLLAPSE_RESET_SECONDS)
            if mode == "expandable_quote":
                try:
                    await self._tg(
                        "edit_message_text",
                        chat_id=topic.chat_id,
                        message_id=message_id,
                        text=fallback_text,
                        parse_mode="HTML",
                        reply_markup=reply_markup,
                    )
                except TelegramError as error:
                    if "message is not modified" not in str(error).lower():
                        raise
            else:
                try:
                    await self._tg(
                        "edit_message_text",
                        chat_id=topic.chat_id,
                        message_id=message_id,
                        rich_message=rich_message,
                        reply_markup=reply_markup,
                    )
                except TelegramError as error:
                    if "message is not modified" in str(error).lower():
                        pass
                    elif self._rich_fallback_allowed(error):
                        try:
                            await self._tg(
                                "edit_message_text",
                                chat_id=topic.chat_id,
                                message_id=message_id,
                                text=fallback_text,
                                parse_mode="HTML",
                                reply_markup=reply_markup,
                            )
                        except TelegramError as fallback_error:
                            if (
                                "message is not modified"
                                not in str(fallback_error).lower()
                            ):
                                raise
                        mode = "expandable_quote"
                    else:
                        raise
        elif mode == "expandable_quote":
            self.store.update_turn_progress_state(
                thread_id=topic.thread_id,
                turn_id=turn_id,
                render_mode=mode,
                send_outcome_unknown=True,
            )
            try:
                sent = await self._tg(
                    "send_message",
                    chat_id=topic.chat_id,
                    message_thread_id=topic.topic_id,
                    reply_to_message_id=context.source_message_id,
                    text=fallback_text,
                    parse_mode="HTML",
                    reply_markup=reply_markup,
                )
            except TelegramError as error:
                if not error.outcome_ambiguous:
                    self.store.update_turn_progress_state(
                        thread_id=topic.thread_id,
                        turn_id=turn_id,
                        send_outcome_unknown=False,
                    )
                raise
            if sent:
                message_id = int(sent[0]["message_id"])
        else:
            self.store.update_turn_progress_state(
                thread_id=topic.thread_id,
                turn_id=turn_id,
                render_mode=mode,
                send_outcome_unknown=True,
            )
            try:
                sent_rich = await self._tg(
                    "send_rich_message",
                    chat_id=topic.chat_id,
                    message_thread_id=topic.topic_id,
                    reply_to_message_id=context.source_message_id,
                    rich_message=rich_message,
                    reply_markup=reply_markup,
                )
                message_id = int(sent_rich["message_id"])
            except TelegramError as error:
                if not self._rich_fallback_allowed(error):
                    if not error.outcome_ambiguous:
                        self.store.update_turn_progress_state(
                            thread_id=topic.thread_id,
                            turn_id=turn_id,
                            send_outcome_unknown=False,
                        )
                    raise
                mode = "expandable_quote"
                self.store.update_turn_progress_state(
                    thread_id=topic.thread_id,
                    turn_id=turn_id,
                    render_mode=mode,
                    send_outcome_unknown=True,
                )
                try:
                    sent = await self._tg(
                        "send_message",
                        chat_id=topic.chat_id,
                        message_thread_id=topic.topic_id,
                        reply_to_message_id=context.source_message_id,
                        text=fallback_text,
                        parse_mode="HTML",
                        reply_markup=reply_markup,
                    )
                except TelegramError as fallback_error:
                    if not fallback_error.outcome_ambiguous:
                        self.store.update_turn_progress_state(
                            thread_id=topic.thread_id,
                            turn_id=turn_id,
                            send_outcome_unknown=False,
                        )
                    raise
                if sent:
                    message_id = int(sent[0]["message_id"])

        if message_id is None:
            return None
        self.store.update_turn_progress_state(
            thread_id=topic.thread_id,
            turn_id=turn_id,
            status_message_id=message_id,
            render_mode=mode,
            closed=closed,
            outcome=outcome if closed else None,
            send_outcome_unknown=False,
        )
        rendered_key = (topic.thread_id, turn_id)
        if closed:
            self._progress_elapsed_minutes_rendered.pop(rendered_key, None)
        else:
            self._progress_elapsed_minutes_rendered[rendered_key] = (
                elapsed_minutes
            )
        return message_id

    async def _finalize_progress_card(
        self,
        topic: TopicBinding,
        *,
        turn_id: str | None,
        outcome: str,
    ) -> None:
        if not turn_id:
            return
        await self._render_progress_card(
            topic,
            turn_id=turn_id,
            closed=True,
            outcome=outcome,
        )

    def _should_retry_null_mirrored_item(
        self,
        topic: TopicBinding,
        item: dict[str, Any],
    ) -> bool:
        item_type = str(item.get("type") or "")
        if item_type == "userMessage":
            client_id = str(item.get("clientId") or "")
            if (
                telegram_source_message_id(
                    client_id,
                    expected_chat_id=topic.chat_id,
                )
                is not None
            ):
                return False
            return bool(user_message_text(item))
        if (
            item_type == "agentMessage"
            and item.get("phase") == "final_answer"
        ):
            return bool(str(item.get("text") or "").strip())
        progress = visible_progress_entry(item)
        return bool(progress is not None and progress[1].strip())

    async def _backfill_existing_item_attachments(
        self,
        topic: TopicBinding,
        item: dict[str, Any],
        *,
        turn_id: str | None,
        item_origin: str,
        mirrored_message_id: int | None,
    ) -> None:
        if turn_id is None:
            return
        item_id = str(item.get("id") or "")
        backfill_key = (topic.thread_id, item_id)
        if not item_id or backfill_key in self._attachment_backfill_checked:
            return
        if item.get("type") == "userMessage":
            client_id = str(item.get("clientId") or "")
            if (
                telegram_source_message_id(
                    client_id,
                    expected_chat_id=topic.chat_id,
                )
                is not None
            ):
                self._attachment_backfill_checked.add(backfill_key)
                return
        (
            attachments,
            delivery_type,
            source_label,
            _,
        ) = self._resolve_item_attachments(item)
        if not attachments:
            self._attachment_backfill_checked.add(backfill_key)
            return
        context = self.store.turn_context(topic.thread_id, turn_id)
        reply_to_message_id = mirrored_message_id
        if reply_to_message_id is None and context is not None:
            if (
                item.get("type") == "agentMessage"
                and item.get("phase") == "final_answer"
            ):
                reply_to_message_id = (
                    context.final_message_id or context.source_message_id
                )
            else:
                reply_to_message_id = context.source_message_id
        first_message_id = await self._deliver_visible_attachments(
            topic,
            turn_id=turn_id,
            item_id=item_id,
            item_origin=item_origin,
            attachments=attachments,
            delivery_type=delivery_type,
            source_label=source_label,
            reply_to_message_id=reply_to_message_id,
        )
        if mirrored_message_id is None and first_message_id is not None:
            self.store.update_mirrored_item_message_id(
                topic.thread_id,
                item_id,
                first_message_id,
            )
        self._attachment_backfill_checked.add(backfill_key)

    async def sync_thread_history(
        self,
        thread: dict[str, Any],
        topic: TopicBinding,
        *,
        initial: bool,
    ) -> None:
        visible: list[tuple[str, dict[str, Any]]] = []
        reconciled_turns: set[str] = set()
        telegram_client_ids: set[str] = set()
        for turn in thread.get("turns") or []:
            turn_id = str(turn.get("id") or "")
            canonical_progress: list[tuple[str, str, str]] = []
            for item in turn.get("items") or []:
                if item.get("type") == "userMessage" and turn_id:
                    client_id = str(item.get("clientId") or "")
                    source_message_id = self._telegram_turn_source_message_id(
                        topic,
                        client_id,
                    )
                    if source_message_id is not None:
                        telegram_client_ids.add(client_id)
                        self.store.upsert_turn_context(
                            thread_id=topic.thread_id,
                            turn_id=turn_id,
                            source_message_id=source_message_id,
                        )
                progress = visible_progress_entry(item)
                item_id = str(item.get("id") or "")
                if progress is None or not item_id:
                    continue
                entry_kind, body = progress
                safe_body = truncate_utf8(
                    telegram_visible_text(
                        redact_sensitive(body),
                        self.config.workspace,
                    ),
                    PROGRESS_ENTRY_MAX_BYTES,
                )
                if safe_body:
                    canonical_progress.append(
                        (item_id, entry_kind, safe_body)
                    )
            if (
                turn_id
                and canonical_progress
                and self.store.reconcile_progress_entries_with_history(
                    thread_id=topic.thread_id,
                    turn_id=turn_id,
                    canonical_entries=canonical_progress,
                )
            ):
                reconciled_turns.add(turn_id)
            visible.extend(
                (turn_id, item)
                for item in turn.get("items") or []
                if item.get("type")
                in {
                    "userMessage",
                    "agentMessage",
                    "plan",
                    "toolStatus",
                    "tool_status",
                    "reasoning",
                    "imageGeneration",
                    "imageView",
                }
            )
        deliver_ids: set[str] | None = None
        if initial and self.config.initial_history_messages > 0:
            deliverable = [
                item
                for _, item in visible
                if item.get("type")
                in {
                    "userMessage",
                    "agentMessage",
                    "plan",
                    "toolStatus",
                    "tool_status",
                    "imageGeneration",
                    "imageView",
                }
            ]
            deliver_ids = {
                str(item.get("id"))
                for item in deliverable[-self.config.initial_history_messages :]
            }
        skipped_items: list[tuple[str, str, str, int | None]] = []
        for turn_id, item in visible:
            item_id = str(item.get("id") or "")
            if not item_id:
                continue
            mirrored, mirrored_message_id = self.store.mirrored_item_state(
                topic.thread_id,
                item_id,
            )
            if mirrored:
                if deliver_ids is not None and item_id not in deliver_ids:
                    continue
                if (
                    mirrored_message_id is None
                    and (
                        self.config.initial_history_messages == 0
                        or (
                            initial
                            and (
                                deliver_ids is None
                                or item_id in deliver_ids
                            )
                        )
                    )
                    and self._should_retry_null_mirrored_item(topic, item)
                    and self.store.unmark_null_mirrored_item_for_backfill(
                        topic.thread_id,
                        item_id,
                    )
                ):
                    mirrored = False
                else:
                    await self._backfill_existing_item_attachments(
                        topic,
                        item,
                        turn_id=turn_id or None,
                        item_origin="history",
                        mirrored_message_id=mirrored_message_id,
                    )
                    continue
            if deliver_ids is not None and item_id not in deliver_ids:
                skipped_items.append(
                    (
                        topic.thread_id,
                        item_id,
                        str(item.get("type")),
                        None,
                    )
                )
                continue
            await self.mirror_item(
                topic,
                item,
                turn_id=turn_id or None,
                item_origin="history",
            )
        self.store.mark_mirrored_items(skipped_items)
        for turn in thread.get("turns") or []:
            turn_id = str(turn.get("id") or "")
            status = str(turn.get("status") or "").lower()
            terminal = status in {"completed", "failed", "interrupted"}
            if turn_id in reconciled_turns:
                await self._render_progress_card(
                    topic,
                    turn_id=turn_id,
                    closed=terminal,
                    outcome=status if terminal else "completed",
                    force=True,
                )
            if turn_id and terminal:
                await self._finalize_progress_card(
                    topic,
                    turn_id=turn_id,
                    outcome=status,
                )
        for queued in self.store.active_queued_messages(topic.thread_id):
            if queued.client_id in telegram_client_ids:
                self.store.mark_queue(queued.queue_id, "sent")
                await self._finalize_queue_card(
                    queued,
                    text="✅ Уже принято Codex; повтор не отправлялся.",
                )
            elif queued.status == "dispatching":
                # A single empty history read can race Codex persistence. A
                # durable miss counter plus grace period prevents an
                # ambiguous start RPC from creating a duplicate turn.
                if self.store.record_queue_dispatch_history_miss(
                    queued.queue_id,
                    required_fresh_misses=(
                        QUEUE_DISPATCH_RECONCILIATION_REQUIRED_MISSES
                    ),
                    grace_seconds=(
                        QUEUE_DISPATCH_RECONCILIATION_GRACE_SECONDS
                    ),
                ):
                    self.store.mark_queue(queued.queue_id, "pending")

    def _telegram_turn_source_message_id(
        self,
        topic: TopicBinding,
        client_id: str,
    ) -> int | None:
        parsed = telegram_source_message_id(
            client_id,
            expected_chat_id=topic.chat_id,
        )
        if parsed is None:
            return None
        queued = self.store.queued_message_for_client_id(client_id)
        if (
            queued is not None
            and queued.thread_id == topic.thread_id
            and queued.chat_id == topic.chat_id
            and queued.topic_id == topic.topic_id
        ):
            # /new originates in General but is echoed inside the newly
            # created Topic. The durable queue stores that Topic-local echo
            # ID, which must remain the reply anchor for progress and final
            # output. Parsed client IDs are only a fallback.
            return queued.telegram_message_id
        return parsed

    async def mirror_item(
        self,
        topic: TopicBinding,
        item: dict[str, Any],
        *,
        turn_id: str | None = None,
        item_origin: str = "history",
    ) -> None:
        item_id = str(item.get("id") or "")
        if not item_id:
            return
        lock = self._mirror_locks.setdefault(topic.thread_id, asyncio.Lock())
        async with lock:
            if self.store.has_mirrored_item(topic.thread_id, item_id):
                return
            if not self.store.claim_mirrored_item(topic.thread_id, item_id):
                return
            try:
                await self._mirror_claimed_item(
                    topic,
                    item,
                    turn_id=turn_id,
                    item_origin=item_origin,
                )
            finally:
                self.store.release_mirror_claim(topic.thread_id, item_id)

    def _resolve_item_attachments(
        self,
        item: dict[str, Any],
    ) -> tuple[list[OutboundAttachment], str, str, list[str]]:
        item_type = str(item.get("type") or "")
        attachments: list[OutboundAttachment] = []
        warnings: list[str] = []
        delivery_type = "threadAttachment"
        source_label = "Codex"

        if item_type == "userMessage":
            delivery_type = "userAttachment"
            source_label = "Codex App"
            for index, content in enumerate(item.get("content") or [], start=1):
                if not isinstance(content, dict):
                    continue
                if str(content.get("type") or "") not in {
                    "audio",
                    "image",
                    "localAudio",
                    "localImage",
                    "mention",
                }:
                    continue
                try:
                    attachment = self.outbound_media.resolve_user_input(
                        content,
                        index=index,
                    )
                except OutboundMediaError as error:
                    warnings.append(
                        ATTACHMENT_ERROR_LABELS.get(
                            error.kind,
                            "вложение не удалось подготовить",
                        )
                    )
                    continue
                if attachment is not None:
                    attachments.append(attachment)
        elif (
            item_type == "agentMessage"
            and item.get("phase") == "final_answer"
        ):
            delivery_type = "finalAttachment"
            source_label = ""
            for path in final_answer_attachments(
                str(item.get("text") or ""),
                self.config.workspace,
            ):
                try:
                    attachments.append(
                        self.outbound_media.resolve_path(path)
                    )
                except OutboundMediaError as error:
                    warnings.append(
                        ATTACHMENT_ERROR_LABELS.get(
                            error.kind,
                            "выходной файл не удалось подготовить",
                        )
                    )
        elif item_type in {"imageGeneration", "imageView"}:
            try:
                attachments.extend(
                    self.outbound_media.resolve_thread_item(item)
                )
            except OutboundMediaError as error:
                warnings.append(
                    ATTACHMENT_ERROR_LABELS.get(
                        error.kind,
                        "изображение не удалось подготовить",
                    )
                )
        return attachments, delivery_type, source_label, warnings

    @staticmethod
    def _attachment_caption(
        attachment: OutboundAttachment,
        *,
        source_label: str,
    ) -> str:
        icon = {
            "animation": "🎞",
            "audio": "🎵",
            "document": "📎",
            "photo": "🖼",
            "video": "🎬",
        }[attachment.media_kind]
        name = redact_sensitive(attachment.display_name)
        label = f"{source_label}: " if source_label else ""
        return f"{icon} {label}{name}"[:1024]

    async def _deliver_visible_attachments(
        self,
        topic: TopicBinding,
        *,
        turn_id: str,
        item_id: str,
        item_origin: str,
        attachments: list[OutboundAttachment],
        delivery_type: str,
        source_label: str,
        reply_to_message_id: int | None,
    ) -> int | None:
        first_message_id: int | None = None
        for index, attachment in enumerate(attachments, start=1):
            relative_path: Path | None = None
            if attachment.path is not None:
                try:
                    relative_path = attachment.path.relative_to(
                        self.config.workspace.resolve(strict=True)
                    )
                except ValueError:
                    pass
            review_token = (
                None
                if delivery_type != "finalAttachment"
                or relative_path is None
                else person_review_token(relative_path)
            )
            review_caption = (
                attachment.path.stem
                if attachment.path is not None
                else attachment.display_name
            )
            content_fingerprint = hashlib.sha256(
                (
                    str(index)
                    + "\0"
                    + attachment.media_kind
                    + "\0"
                    + attachment.fingerprint
                ).encode("utf-8")
            ).hexdigest()
            delivery_item_id = (
                f"{item_id}:attachment:{content_fingerprint}"
            )
            delivered_message_id = self.store.match_visible_item_delivery(
                thread_id=topic.thread_id,
                turn_id=turn_id,
                item_id=delivery_item_id,
                item_type=delivery_type,
                item_origin=item_origin,
                content_fingerprint=content_fingerprint,
            )
            if delivered_message_id is not None:
                if first_message_id is None:
                    first_message_id = delivered_message_id
                if review_token is not None:
                    assert relative_path is not None
                    self._prepare_person_review_record(
                        token=review_token,
                        relative_path=relative_path,
                        caption=review_caption,
                        topic=topic,
                        message_id=delivered_message_id,
                    )
                continue
            if self.store.match_visible_item_delivery_intent(
                thread_id=topic.thread_id,
                turn_id=turn_id,
                item_id=delivery_item_id,
                item_type=delivery_type,
                item_origin=item_origin,
                content_fingerprint=content_fingerprint,
            ):
                # The upload may already have reached Telegram while the
                # response was lost. Preserve at-most-once delivery.
                continue
            self.store.reserve_visible_item_delivery_intent(
                thread_id=topic.thread_id,
                turn_id=turn_id,
                item_id=delivery_item_id,
                item_type=delivery_type,
                item_origin=item_origin,
                content_fingerprint=content_fingerprint,
            )
            if review_token is not None:
                assert relative_path is not None
                self._prepare_person_review_record(
                    token=review_token,
                    relative_path=relative_path,
                    caption=review_caption,
                    topic=topic,
                    message_id=None,
                )
            try:
                if review_token is not None:
                    sent = await self._tg(
                        "send_photo",
                        chat_id=topic.chat_id,
                        message_thread_id=topic.topic_id,
                        reply_to_message_id=reply_to_message_id,
                        file_path=attachment.path,
                        caption=review_caption,
                        reply_markup=person_review_keyboard(review_token),
                    )
                else:
                    sent = await self._tg(
                        "send_attachment",
                        chat_id=topic.chat_id,
                        message_thread_id=topic.topic_id,
                        reply_to_message_id=reply_to_message_id,
                        media_kind=attachment.media_kind,
                        file_path=attachment.path,
                        url=attachment.url,
                        caption=self._attachment_caption(
                            attachment,
                            source_label=source_label,
                        ),
                    )
            except TelegramError as error:
                if error.outcome_ambiguous:
                    self.store.mark_visible_item_delivery_outcome_unknown(
                        topic.thread_id,
                        delivery_item_id,
                    )
                else:
                    self.store.clear_visible_item_delivery_intent(
                        thread_id=topic.thread_id,
                        item_id=delivery_item_id,
                    )
                raise
            except (FileNotFoundError, OSError, ValueError):
                self.store.clear_visible_item_delivery_intent(
                    thread_id=topic.thread_id,
                    item_id=delivery_item_id,
                )
                raise
            self.store.record_visible_item_delivery(
                thread_id=topic.thread_id,
                turn_id=turn_id,
                item_id=delivery_item_id,
                item_type=delivery_type,
                item_origin=item_origin,
                content_fingerprint=content_fingerprint,
                telegram_message_id=int(sent["message_id"]),
            )
            if first_message_id is None:
                first_message_id = int(sent["message_id"])
            if review_token is not None:
                assert relative_path is not None
                self._prepare_person_review_record(
                    token=review_token,
                    relative_path=relative_path,
                    caption=review_caption,
                    topic=topic,
                    message_id=int(sent["message_id"]),
                )
        return first_message_id

    async def _mirror_claimed_item(
        self,
        topic: TopicBinding,
        item: dict[str, Any],
        *,
        turn_id: str | None,
        item_origin: str,
    ) -> None:
        item_id = str(item.get("id") or "")
        item_type = str(item.get("type") or "unknown")
        text: str | None = None
        delivery_fingerprint_text: str | None = None
        message_entities: list[dict[str, Any]] | None = None
        attachments: list[OutboundAttachment] = []
        attachment_delivery_type = "threadAttachment"
        attachment_source_label = "Codex"
        attachment_warnings: list[str] = []
        context = (
            self.store.turn_context(topic.thread_id, turn_id)
            if turn_id
            else None
        )
        reply_to_message_id = (
            None if context is None else context.source_message_id
        )
        progress = visible_progress_entry(item)
        if progress is not None and turn_id:
            entry_kind, body = progress
            safe_body = truncate_utf8(
                telegram_visible_text(
                    redact_sensitive(body),
                    self.config.workspace,
                ),
                PROGRESS_ENTRY_MAX_BYTES,
            )
            if safe_body:
                content_changed = self.store.append_progress_entry(
                    thread_id=topic.thread_id,
                    turn_id=turn_id,
                    item_id=item_id,
                    entry_kind=entry_kind,
                    sanitized_text=safe_body,
                    item_origin=item_origin,
                )
                existing_context = self.store.turn_context(
                    topic.thread_id,
                    turn_id,
                )
                if (
                    content_changed
                    or existing_context is None
                    or existing_context.status_message_id is None
                ):
                    telegram_message_id = await self._render_progress_card(
                        topic,
                        turn_id=turn_id,
                        closed=False,
                    )
                else:
                    telegram_message_id = existing_context.status_message_id
                self.store.mark_mirrored_item(
                    topic.thread_id,
                    item_id,
                    item_type,
                    telegram_message_id,
                )
                return

        if item_type == "userMessage":
            client_id = str(item.get("clientId") or "")
            source_message_id = self._telegram_turn_source_message_id(
                topic,
                client_id,
            )
            if source_message_id is not None:
                if turn_id:
                    self.store.upsert_turn_context(
                        thread_id=topic.thread_id,
                        turn_id=turn_id,
                        source_message_id=source_message_id,
                    )
                queued = self.store.queued_message_for_client_id(client_id)
                if queued is not None and queued.status != "sent":
                    self.store.mark_queue(queued.queue_id, "sent")
                    await self._finalize_queue_card(
                        queued,
                        text="✅ Уже принято Codex; повтор не отправлялся.",
                    )
                self.store.mark_mirrored_item(
                    topic.thread_id, item_id, item_type, None
                )
                return
            (
                attachments,
                attachment_delivery_type,
                attachment_source_label,
                attachment_warnings,
            ) = self._resolve_item_attachments(item)
            body = user_message_text(item)
            if attachment_warnings:
                warning_text = "\n".join(
                    f"⚠️ {warning}" for warning in attachment_warnings
                )
                body = f"{body}\n\n{warning_text}".strip()
            if body:
                text = f"👤 Codex App\n\n{body}"
        elif (
            item_type == "agentMessage"
            and item.get("phase") == "final_answer"
        ):
            body = str(item.get("text") or "").strip()
            if body:
                (
                    attachments,
                    attachment_delivery_type,
                    attachment_source_label,
                    attachment_warnings,
                ) = self._resolve_item_attachments(item)
                if attachment_warnings:
                    body += "\n\n" + "\n".join(
                        f"⚠️ {warning}" for warning in attachment_warnings
                    )
                delivery_fingerprint_text = f"🟢 Codex\n\n{body}"
                usage = (
                    self._turn_token_usage.get((topic.thread_id, turn_id))
                    if turn_id
                    else None
                )
                if usage is not None:
                    turn_tokens, thread_tokens = usage
                    body += (
                        "\n\n"
                        f"({format_token_count(turn_tokens)} / "
                        f"{format_token_count(thread_tokens)} tkn)"
                    )
                await self._finalize_progress_card(
                    topic,
                    turn_id=turn_id,
                    outcome="completed",
                )
                text = f"🟢 Codex\n\n{body}"
        elif item_type in {"imageGeneration", "imageView"}:
            (
                attachments,
                attachment_delivery_type,
                attachment_source_label,
                attachment_warnings,
            ) = self._resolve_item_attachments(item)
            if attachment_warnings:
                text = "⚠️ Codex\n\n" + "\n".join(attachment_warnings)
        elif progress is not None:
            # A legacy notification without a turn id cannot be accumulated
            # durably. Preserve its visible status without inventing a turn.
            entry_kind, body = progress
            label = PROGRESS_KIND_LABELS[entry_kind]
            text = f"💬 {label}\n\n{body}"

        telegram_message_id: int | None = None
        if text:
            safe_text = telegram_visible_text(
                redact_sensitive(text),
                self.config.workspace,
            )
            track_delivery = bool(
                turn_id
                and (
                    item_type == "userMessage"
                    or (
                        item_type == "agentMessage"
                        and item.get("phase") == "final_answer"
                    )
                    or item_type in {"imageGeneration", "imageView"}
                )
            )
            content_fingerprint = ""
            if track_delivery:
                content_fingerprint = hashlib.sha256(
                    (
                        item_type
                        + "\0"
                        + str(item.get("phase") or "")
                        + "\0"
                        + (delivery_fingerprint_text or safe_text)
                        + "\0"
                        + "\0".join(
                            attachment.fingerprint
                            for attachment in attachments
                        )
                    ).encode("utf-8")
                ).hexdigest()
                assert turn_id is not None
                telegram_message_id = (
                    self.store.match_visible_item_delivery(
                        thread_id=topic.thread_id,
                        turn_id=turn_id,
                        item_id=item_id,
                        item_type=item_type,
                        item_origin=item_origin,
                        content_fingerprint=content_fingerprint,
                    )
                )
                if (
                    telegram_message_id is None
                    and self.store.match_visible_item_delivery_intent(
                        thread_id=topic.thread_id,
                        turn_id=turn_id,
                        item_id=item_id,
                        item_type=item_type,
                        item_origin=item_origin,
                        content_fingerprint=content_fingerprint,
                    )
                ):
                    # Telegram may already have applied the request whose
                    # response was lost. Fail closed instead of risking a
                    # duplicate final/user message.
                    if attachments and turn_id is not None:
                        await self._deliver_visible_attachments(
                            topic,
                            turn_id=turn_id,
                            item_id=item_id,
                            item_origin=item_origin,
                            attachments=attachments,
                            delivery_type=attachment_delivery_type,
                            source_label=attachment_source_label,
                            reply_to_message_id=reply_to_message_id,
                        )
                    self.store.mark_mirrored_item(
                        topic.thread_id,
                        item_id,
                        item_type,
                        None,
                    )
                    return
            if telegram_message_id is not None:
                if item_type == "userMessage":
                    self.store.upsert_turn_context(
                        thread_id=topic.thread_id,
                        turn_id=turn_id,
                        source_message_id=telegram_message_id,
                    )
                else:
                    self.store.upsert_turn_context(
                        thread_id=topic.thread_id,
                        turn_id=turn_id,
                        final_message_id=telegram_message_id,
                    )
                    if attachments:
                        await self._deliver_visible_attachments(
                            topic,
                            turn_id=turn_id,
                            item_id=item_id,
                            item_origin=item_origin,
                            attachments=attachments,
                            delivery_type=attachment_delivery_type,
                            source_label=attachment_source_label,
                            reply_to_message_id=telegram_message_id,
                        )
                self.store.mark_mirrored_item(
                    topic.thread_id,
                    item_id,
                    item_type,
                    telegram_message_id,
                )
                return
            if track_delivery:
                assert turn_id is not None
                self.store.reserve_visible_item_delivery_intent(
                    thread_id=topic.thread_id,
                    turn_id=turn_id,
                    item_id=item_id,
                    item_type=item_type,
                    item_origin=item_origin,
                    content_fingerprint=content_fingerprint,
                )
            try:
                sent = await self._tg(
                    "send_message",
                    chat_id=topic.chat_id,
                    message_thread_id=topic.topic_id,
                    reply_to_message_id=reply_to_message_id,
                    text=safe_text,
                    entities=message_entities,
                )
            except TelegramError as error:
                if track_delivery:
                    if error.outcome_ambiguous:
                        self.store.mark_visible_item_delivery_outcome_unknown(
                            thread_id=topic.thread_id,
                            item_id=item_id,
                        )
                    else:
                        self.store.clear_visible_item_delivery_intent(
                            thread_id=topic.thread_id,
                            item_id=item_id,
                        )
                raise
            if sent:
                telegram_message_id = int(sent[0]["message_id"])
                if track_delivery:
                    assert turn_id is not None
                    self.store.record_visible_item_delivery(
                        thread_id=topic.thread_id,
                        turn_id=turn_id,
                        item_id=item_id,
                        item_type=item_type,
                        item_origin=item_origin,
                        content_fingerprint=content_fingerprint,
                        telegram_message_id=telegram_message_id,
                    )

            if turn_id and telegram_message_id is not None:
                if item_type == "userMessage":
                    self.store.upsert_turn_context(
                        thread_id=topic.thread_id,
                        turn_id=turn_id,
                        source_message_id=telegram_message_id,
                    )
                elif (
                    item_type == "agentMessage"
                    and item.get("phase") == "final_answer"
                ):
                    self.store.upsert_turn_context(
                        thread_id=topic.thread_id,
                        turn_id=turn_id,
                        final_message_id=telegram_message_id,
                    )
            if attachments and turn_id is not None:
                await self._deliver_visible_attachments(
                    topic,
                    turn_id=turn_id,
                    item_id=item_id,
                    item_origin=item_origin,
                    attachments=attachments,
                    delivery_type=attachment_delivery_type,
                    source_label=attachment_source_label,
                    reply_to_message_id=(
                        telegram_message_id or reply_to_message_id
                    ),
                )
        elif attachments and turn_id is not None:
            telegram_message_id = await self._deliver_visible_attachments(
                topic,
                turn_id=turn_id,
                item_id=item_id,
                item_origin=item_origin,
                attachments=attachments,
                delivery_type=attachment_delivery_type,
                source_label=attachment_source_label,
                reply_to_message_id=reply_to_message_id,
            )
        self.store.mark_mirrored_item(
            topic.thread_id,
            item_id,
            item_type,
            telegram_message_id,
        )

    async def on_codex_notification(self, message: dict[str, Any]) -> None:
        method = str(message.get("method") or "")
        params = message.get("params") or {}
        thread_id = str(params.get("threadId") or "")
        if method == "thread/archived":
            forget_thread = getattr(self.codex, "forget_thread", None)
            if callable(forget_thread):
                forget_thread(thread_id)
            self.active_turns.pop(thread_id, None)
            self.observed_turns.pop(thread_id, None)
            self.busy_threads.discard(thread_id)
            self.thread_modes.pop(thread_id, None)
            self._sync_requested.set()
            return
        if method == "thread/unarchived":
            self._sync_requested.set()
            return
        if method == "thread/deleted":
            forget_thread = getattr(self.codex, "forget_thread", None)
            if callable(forget_thread):
                forget_thread(thread_id)
            self.active_turns.pop(thread_id, None)
            self.observed_turns.pop(thread_id, None)
            self.busy_threads.discard(thread_id)
            self.thread_modes.pop(thread_id, None)
            topic = self.store.topic_for_thread(thread_id)
            if topic is not None and not topic.archived:
                try:
                    await self._tg(
                        "delete_forum_topic",
                        topic.chat_id,
                        topic.topic_id,
                    )
                except TelegramError as error:
                    if not _telegram_error_contains(
                        error,
                        "message thread not found",
                        "forum topic not found",
                        "topic not found",
                        "topic_id_invalid",
                    ):
                        self._sync_requested.set()
                        return
                self.store.forget_observed_topic(
                    topic.chat_id,
                    topic.topic_id,
                )
                self.store.update_topic_state(
                    thread_id,
                    archived=True,
                )
            return
        if method == "thread/settings/updated":
            settings = dict(params.get("threadSettings") or {})
            remember = getattr(self.codex, "remember_thread_settings", None)
            if callable(remember):
                remember(thread_id, settings)
            mode = self._remember_thread_mode(thread_id, settings)
            topic = self.store.topic_for_thread(thread_id)
            if topic is not None and not topic.archived:
                await self._sync_topic_mode_title(topic, mode=mode)
            return
        if method == "thread/tokenUsage/updated":
            turn_id = str(params.get("turnId") or "")
            usage = dict(params.get("tokenUsage") or {})
            last = dict(usage.get("last") or {})
            total = dict(usage.get("total") or {})
            last_tokens = max(0, int(last.get("totalTokens") or 0))
            thread_tokens = max(0, int(total.get("totalTokens") or 0))
            if thread_id and turn_id and thread_tokens:
                previous_total = self._thread_token_totals.get(thread_id)
                delta = (
                    max(0, thread_tokens - previous_total)
                    if previous_total is not None
                    else last_tokens
                )
                current_turn, _ = self._turn_token_usage.get(
                    (thread_id, turn_id),
                    (0, thread_tokens),
                )
                self._turn_token_usage[(thread_id, turn_id)] = (
                    current_turn + delta,
                    thread_tokens,
                )
                self._thread_token_totals[thread_id] = thread_tokens
            return
        if method == "turn/started":
            turn = params.get("turn") or {}
            turn_id = str(turn.get("id") or "")
            if thread_id and turn_id:
                self.observed_turns[thread_id] = turn_id
                if (thread_id, turn_id) not in self._terminal_turns:
                    self.active_turns[thread_id] = turn_id
                    self.busy_threads.add(thread_id)
                self.store.upsert_turn_context(
                    thread_id=thread_id,
                    turn_id=turn_id,
                )
            return
        if method == "item/started":
            item = dict(params.get("item") or {})
            turn_id = str(params.get("turnId") or "")
            item_id = str(item.get("id") or params.get("itemId") or "")
            if thread_id and turn_id and item_id:
                self._started_items[(thread_id, turn_id, item_id)] = item
            return
        if method == "item/completed":
            topic = self.store.topic_for_thread(thread_id)
            item = params.get("item") or {}
            notification_turn_id = (
                str(params.get("turnId") or "")
                or self.active_turns.get(thread_id)
                or self.observed_turns.get(thread_id)
            )
            completed_item_id = str(
                item.get("id") or params.get("itemId") or ""
            )
            if notification_turn_id and completed_item_id:
                self._started_items.pop(
                    (
                        thread_id,
                        notification_turn_id,
                        completed_item_id,
                    ),
                    None,
                )
            if (
                thread_id
                and notification_turn_id
                and item.get("type") == "agentMessage"
                and item.get("phase") == "final_answer"
            ):
                self._pending_final_items[
                    (thread_id, notification_turn_id)
                ] = dict(item)
                context = self.store.turn_context(
                    thread_id,
                    notification_turn_id,
                )
                if context is None or context.source_message_id is None:
                    self._sync_requested.set()
                return
            context = (
                self.store.turn_context(thread_id, notification_turn_id)
                if notification_turn_id
                else None
            )
            if (
                item.get("type") != "userMessage"
                and (
                    context is None
                    or context.source_message_id is None
                )
            ):
                # History hydration must establish the Desktop/Telegram user
                # message first so commentary and the final answer keep their
                # chronological order and reply linkage.
                self._sync_requested.set()
                return
            if topic is not None and not topic.archived and item:
                await self.mirror_item(
                    topic,
                    item,
                    turn_id=notification_turn_id,
                    item_origin="notification",
                )
            return
        if method == "turn/completed":
            turn = params.get("turn") or {}
            status = str(turn.get("status") or "")
            turn_id = (
                str(turn.get("id") or "")
                or self.active_turns.get(thread_id)
                or self.observed_turns.get(thread_id)
            )
            capacity_released = self._clear_terminal_turn_tracking(
                thread_id,
                turn_id,
            )
            for key in tuple(self._started_items):
                if key[0] == thread_id and key[1] == turn_id:
                    self._started_items.pop(key, None)
            topic = self.store.topic_for_thread(thread_id)
            pending_final = self._pending_final_items.pop(
                (thread_id, turn_id),
                None,
            )
            context = self.store.turn_context(thread_id, turn_id)
            if (
                topic
                and not topic.archived
                and pending_final
                and context is not None
                and context.source_message_id is not None
            ):
                await self.mirror_item(
                    topic,
                    pending_final,
                    turn_id=turn_id,
                    item_origin="notification",
                )
            elif pending_final:
                self._sync_requested.set()
            if topic and not topic.archived:
                await self._finalize_progress_card(
                    topic,
                    turn_id=turn_id,
                    outcome=(
                        status
                        if status in {"failed", "interrupted"}
                        else "completed"
                    ),
                )
            if topic and not topic.archived and status in {"failed", "interrupted"}:
                error = turn.get("error") or {}
                detail = str(error.get("message") or status)
                await self._tg(
                    "send_message",
                    chat_id=topic.chat_id,
                    message_thread_id=topic.topic_id,
                    text=f"⚠️ Ход Codex: {redact_sensitive(detail)}",
                )
            if capacity_released:
                await self.dispatch_queued_capacity()
            if not pending_final or (
                context is not None and context.source_message_id is not None
            ):
                self._turn_token_usage.pop((thread_id, turn_id), None)
            return
        if method == "serverRequest/resolved":
            request_id = str(params.get("requestId") or "")
            resolved = next(
                (
                    pending
                    for pending in self.pending_requests.values()
                    if str(pending.server_request_id) == request_id
                ),
                None,
            )
            if resolved is not None:
                lock = self._pending_locks.setdefault(
                    resolved.public_id,
                    asyncio.Lock(),
                )
                async with lock:
                    current = self.pending_requests.get(resolved.public_id)
                    if current is resolved:
                        self.pending_requests.pop(resolved.public_id, None)
                        self.store.resolve_pending_request(
                            resolved.public_id,
                            "resolved_elsewhere",
                        )
                        await self._finalize_prompt_card(
                            resolved,
                            "✅ Решено в Codex App.",
                        )
                        item_id = str(
                            resolved.params.get("itemId") or ""
                        )
                        turn_id = str(
                            resolved.params.get("turnId") or ""
                        )
                        if item_id and turn_id:
                            self._started_items.pop(
                                (
                                    resolved.thread_id,
                                    turn_id,
                                    item_id,
                                ),
                                None,
                            )
            return
        if method == "thread/name/updated":
            topic = self.store.topic_for_thread(thread_id)
            name = topic_title_with_mode(
                params.get("threadName"),
                self.thread_modes.get(thread_id),
            )
            if topic and not topic.archived and name != topic.title:
                try:
                    await self._tg(
                        "edit_forum_topic",
                        topic.chat_id,
                        topic.topic_id,
                        name,
                    )
                except TelegramError:
                    # Preserve the confirmed title so periodic sync retries.
                    pass
                else:
                    self.store.update_topic_state(thread_id, title=name)
            self._sync_requested.set()

    async def on_codex_server_request(self, message: dict[str, Any]) -> None:
        method = str(message.get("method") or "")
        params = dict(message.get("params") or {})
        request_id = message.get("id")
        if not self.codex_available:
            # The shared server may still have another capable Desktop client.
            # Stay silent instead of racing it with a bridge-side response.
            return
        thread_id = str(
            params.get("threadId") or params.get("conversationId") or ""
        )
        turn_id = str(params.get("turnId") or "")
        item_id = str(params.get("itemId") or "")
        started_item = self._started_items.get(
            (thread_id, turn_id, item_id)
        )
        if started_item is not None:
            for key in (
                "command",
                "cwd",
                "changes",
                "fileChanges",
                "patch",
                "diff",
                "files",
                "paths",
            ):
                if params.get(key) is None and started_item.get(key) is not None:
                    params[key] = started_item[key]
        topic = self.store.topic_for_thread(thread_id)
        if topic is None or topic.archived:
            # Another subscribed client may own this request. Silence is
            # required on the shared App Server; an error response would win
            # the first-response race and break the capable client.
            return
        if method == "item/tool/requestUserInput" and any(
            bool(question.get("isSecret"))
            for question in (params.get("questions") or [])
        ):
            # Never render the secret prompt in Telegram. Leave it unanswered
            # here so Codex Desktop can collect it through a trusted surface.
            return
        public_id = secrets.token_hex(5)
        pending = PendingServerRequest(
            public_id=public_id,
            server_request_id=request_id,
            method=method,
            thread_id=thread_id,
            params=params,
        )
        self.pending_requests[public_id] = pending

        try:
            if method == "item/tool/requestUserInput":
                text, keyboard = self._render_user_input_request(pending)
                kind = "user_input"
            elif method in {
                "item/commandExecution/requestApproval",
                "item/fileChange/requestApproval",
                "item/permissions/requestApproval",
                "execCommandApproval",
                "applyPatchApproval",
            }:
                text = self._render_approval_request(pending)
                allow_once, allow_session, allow_deny = (
                    self._approval_decision_flags(pending)
                )
                keyboard = approval_keyboard(
                    public_id,
                    allow_once=allow_once,
                    allow_session=allow_session,
                    allow_deny=allow_deny,
                )
                kind = "approval"
            else:
                raise NotImplementedError(method)
        except (NotImplementedError, ValueError) as error:
            self.pending_requests.pop(public_id, None)
            # Unsupported or unsafe-to-render requests belong to another
            # subscribed client. Never answer with an error on its behalf.
            return

        turn_id = (
            str(params.get("turnId") or "")
            or self.active_turns.get(thread_id)
            or self.observed_turns.get(thread_id)
        )
        context = (
            self.store.turn_context(thread_id, turn_id)
            if turn_id
            else self.store.latest_turn_context(thread_id)
        )
        sent_message_id: int | None = None
        self.store.save_pending_request(
            public_id=public_id,
            thread_id=thread_id,
            request_kind=kind,
            metadata={"method": method},
            status="delivery_reserved",
        )
        try:
            sent = await self._tg(
                "send_message",
                chat_id=topic.chat_id,
                message_thread_id=topic.topic_id,
                reply_to_message_id=(
                    None if context is None else context.source_message_id
                ),
                text=text,
                reply_markup=keyboard,
            )
            if sent:
                sent_message_id = int(sent[0]["message_id"])
            self.store.save_pending_request(
                public_id=public_id,
                thread_id=thread_id,
                request_kind=kind,
                metadata={"method": method},
                telegram_message_id=sent_message_id,
            )
            pending.telegram_message_id = sent_message_id
        except TelegramError as error:
            if error.outcome_ambiguous:
                # Telegram may already show the interactive card. Keep the
                # live request usable by its buttons (and by sole-request text
                # approval), suppress replay, and surface the uncertainty.
                self.store.save_pending_request(
                    public_id=public_id,
                    thread_id=thread_id,
                    request_kind=kind,
                    metadata={"method": method},
                    status="delivery_outcome_unknown",
                )
                raise
            self.pending_requests.pop(public_id, None)
            self.store.save_pending_request(
                public_id=public_id,
                thread_id=thread_id,
                request_kind=kind,
                metadata={"method": method},
                status="telegram_delivery_failed",
            )
            raise
        except Exception:
            self.pending_requests.pop(public_id, None)
            if sent_message_id is not None:
                pending.telegram_message_id = sent_message_id
                await self._finalize_prompt_card(
                    pending,
                    "⚠️ Запрос недоступен: откройте его в Codex App.",
                )
            with contextlib.suppress(Exception):
                self.store.save_pending_request(
                    public_id=public_id,
                    thread_id=thread_id,
                    request_kind=kind,
                    metadata={"method": method},
                    telegram_message_id=sent_message_id,
                    status="telegram_delivery_failed",
                )
            raise

    @staticmethod
    def _approval_decision_flags(
        pending: PendingServerRequest,
    ) -> tuple[bool, bool, bool]:
        raw = pending.params.get("availableDecisions")
        if raw is None:
            return True, True, True
        if not isinstance(raw, list) or not raw:
            raise ValueError(
                "Approval request has no explicit available decisions"
            )
        decisions = {
            str(
                value.get("decision")
                if isinstance(value, dict)
                else value
            ).lower()
            for value in raw
        }
        once = bool(
            decisions
            & {
                "accept",
                "approved",
                "approve",
                "once",
            }
        )
        session = bool(
            decisions
            & {
                "acceptforsession",
                "approved_for_session",
                "session",
            }
        )
        deny = bool(
            decisions
            & {
                "decline",
                "denied",
                "deny",
            }
        )
        if not any((once, session, deny)):
            raise ValueError("Approval request has no supported decisions")
        return once, session, deny

    def _render_approval_request(self, pending: PendingServerRequest) -> str:
        params = pending.params
        method = pending.method
        if "command" in method.lower():
            scope = {
                key: value
                for key, value in params.items()
                if key
                not in {
                    "threadId",
                    "conversationId",
                    "turnId",
                    "itemId",
                    "reason",
                    "availableDecisions",
                    "createdAt",
                    "timestamp",
                }
                and value is not None
            }
            if "command" not in scope:
                raise ValueError(
                    "Command approval scope is unavailable; use Codex App"
                )
            detail = json.dumps(
                scope or {"command": "команда не указана"},
                ensure_ascii=False,
                sort_keys=True,
            )
            label = "команды"
        elif "filechange" in method.lower() or "patch" in method.lower():
            scope = {
                key: params[key]
                for key in (
                    "changes",
                    "fileChanges",
                    "patch",
                    "diff",
                    "files",
                    "paths",
                    "grantRoot",
                )
                if params.get(key) is not None
            }
            has_exact_change = any(
                params.get(key) is not None
                for key in (
                    "changes",
                    "fileChanges",
                    "patch",
                    "diff",
                    "files",
                    "paths",
                )
            )
            if not has_exact_change:
                raise ValueError(
                    "File-change scope is unavailable; review it in Codex App"
                )
            detail = json.dumps(
                scope or {"change": params.get("reason") or "изменение файлов"},
                ensure_ascii=False,
                sort_keys=True,
            )
            label = "изменения файлов"
        elif "permissions" in method.lower():
            permissions = {
                key: params[key]
                for key in (
                    "permissions",
                    "additionalPermissions",
                    "grantRoot",
                )
                if params.get(key) is not None
            }
            if not permissions:
                raise ValueError(
                    "Permission scope is unavailable; use Codex App"
                )
            detail = json.dumps(
                permissions,
                ensure_ascii=False,
                sort_keys=True,
            )
            label = "дополнительных разрешений"
        else:
            detail = str(params.get("reason") or "действие")
            label = "действия"
        detail = redact_sensitive(detail)
        if len(detail) > 1600:
            raise ValueError(
                "Запрос слишком велик для безопасного подтверждения в Telegram; "
                "подтвердите его в Codex App."
            )
        reason = str(params.get("reason") or "").strip()
        reason_line = f"\nПричина: {reason}" if reason and reason != detail else ""
        allow_once, _, allow_deny = self._approval_decision_flags(pending)
        shortcuts: list[str] = []
        if allow_once:
            shortcuts.append("`+`, `да` или `ок` = разрешить один раз.")
        if allow_deny:
            shortcuts.append("`-` или `нет` = отклонить.")
        return redact_sensitive(
            f"🔐 Codex запрашивает подтверждение {label}.\n\n"
            f"{detail}{reason_line}\n\n"
            + "\n".join(shortcuts)
        )

    def _render_user_input_request(
        self, pending: PendingServerRequest
    ) -> tuple[str, dict[str, Any] | None]:
        questions = pending.params.get("questions") or []
        lines = ["❓ Codex ожидает решение."]
        keyboard: dict[str, Any] | None = None
        for index, question in enumerate(questions, start=1):
            lines.append(
                f"\n{index}. {question.get('header') or ''}\n"
                f"{question.get('question') or ''}"
            )
            options = question.get("options") or []
            for option_index, option in enumerate(options, start=1):
                lines.append(
                    f"   {option_index}) {option.get('label') or ''}"
                    f" — {option.get('description') or ''}"
                )
        if len(questions) == 1 and questions[0].get("options"):
            keyboard = {
                "inline_keyboard": [
                    [
                        {
                            "text": str(option.get("label") or f"Вариант {index + 1}")[
                                :50
                            ],
                            "callback_data": f"ui:{pending.public_id}:{index}",
                        }
                    ]
                    for index, option in enumerate(questions[0]["options"])
                ]
            }
        if len(questions) > 1:
            lines.append(
                "\nОтветьте по одной строке на вопрос: `1: ответ`, `2: ответ`."
            )
        else:
            lines.append("\nМожно ответить обычным текстом в этом Topic.")
        return redact_sensitive("\n".join(lines)), keyboard

    def _pending_for_thread(
        self,
        thread_id: str,
        *,
        kind: str,
    ) -> list[PendingServerRequest]:
        return [
            value
            for value in self.pending_requests.values()
            if value.thread_id == thread_id
            and (
                (
                    kind == "user_input"
                    and value.method == "item/tool/requestUserInput"
                )
                or (
                    kind == "approval"
                    and "requestUserInput" not in value.method
                )
            )
        ]

    @staticmethod
    def _pending_for_reply(
        pending: list[PendingServerRequest],
        telegram_reply_to_message_id: int | None,
    ) -> PendingServerRequest | None:
        if telegram_reply_to_message_id is not None:
            return next(
                (
                    value
                    for value in pending
                    if value.telegram_message_id
                    == telegram_reply_to_message_id
                ),
                None,
            )
        return pending[0] if len(pending) == 1 else None

    async def _try_resolve_pending_text(
        self,
        thread_id: str,
        text: str,
        *,
        telegram_reply_to_message_id: int | None = None,
        source_message_id: int | None = None,
    ) -> bool:
        normalized = text.strip().lower()
        approvals = self._pending_for_thread(thread_id, kind="approval")
        user_inputs = self._pending_for_thread(thread_id, kind="user_input")
        if not self.codex_available and (approvals or user_inputs):
            topic = self.store.topic_for_thread(thread_id)
            if topic is not None:
                await self._send_degraded_message(
                    chat_id=topic.chat_id,
                    topic_id=topic.topic_id,
                    reply_to=source_message_id,
                )
            return True
        approval = self._pending_for_reply(
            approvals,
            telegram_reply_to_message_id,
        )
        if approval and normalized in ACCEPT_WORDS:
            await self.resolve_approval(approval.public_id, "once")
            return True
        if approval and normalized in DENY_WORDS:
            await self.resolve_approval(approval.public_id, "deny")
            return True
        if approvals and normalized in ACCEPT_WORDS | DENY_WORDS:
            topic = self.store.topic_for_thread(thread_id)
            if topic is not None:
                await self._tg(
                    "send_message",
                    chat_id=topic.chat_id,
                    message_thread_id=topic.topic_id,
                    reply_to_message_id=source_message_id,
                    text=(
                        "Есть несколько ожидающих подтверждений. "
                        "Ответьте `+`/`-` именно reply на нужную карточку."
                    ),
                )
            return True
        user_input = self._pending_for_reply(
            user_inputs,
            telegram_reply_to_message_id,
        )
        if user_input:
            await self.resolve_user_input_text(user_input.public_id, text)
            return True
        if user_inputs:
            topic = self.store.topic_for_thread(thread_id)
            if topic is not None:
                await self._tg(
                    "send_message",
                    chat_id=topic.chat_id,
                    message_thread_id=topic.topic_id,
                    reply_to_message_id=source_message_id,
                    text=(
                        "Есть несколько вопросов Codex. "
                        "Ответьте reply на конкретную карточку."
                    ),
                )
            return True
        return False

    async def resolve_approval(
        self,
        public_id: str,
        decision: str,
        *,
        callback_query_id: str | None = None,
    ) -> None:
        if not self.codex_available:
            if callback_query_id:
                await self._tg(
                    "answer_callback_query",
                    callback_query_id,
                    "Codex недоступен; решение не отправлено",
                )
            return
        lock = self._pending_locks.setdefault(public_id, asyncio.Lock())
        async with lock:
            pending = self.pending_requests.get(public_id)
            if pending is None:
                if callback_query_id:
                    await self._tg(
                        "answer_callback_query",
                        callback_query_id,
                        "Уже обработано",
                    )
                return
            if decision not in {"once", "session", "deny"}:
                if callback_query_id:
                    await self._tg(
                        "answer_callback_query",
                        callback_query_id,
                        "Недоступное решение",
                    )
                return
            allow_once, allow_session, allow_deny = (
                self._approval_decision_flags(pending)
            )
            allowed = {
                "once": allow_once,
                "session": allow_session,
                "deny": allow_deny,
            }[decision]
            if not allowed:
                if callback_query_id:
                    await self._tg(
                        "answer_callback_query",
                        callback_query_id,
                        "Это решение недоступно для запроса",
                    )
                else:
                    topic = self.store.topic_for_thread(pending.thread_id)
                    if topic is not None:
                        await self._tg(
                            "send_message",
                            chat_id=topic.chat_id,
                            message_thread_id=topic.topic_id,
                            reply_to_message_id=pending.telegram_message_id,
                            text=(
                                "Это решение недоступно для данного запроса. "
                                "Используйте одну из кнопок карточки."
                            ),
                        )
                return

            method = pending.method
            if method in {
                "item/commandExecution/requestApproval",
                "item/fileChange/requestApproval",
            }:
                codex_decision = {
                    "once": "accept",
                    "session": "acceptForSession",
                    "deny": "decline",
                }[decision]
                result = {"decision": codex_decision}
            elif method == "item/permissions/requestApproval":
                if decision == "deny":
                    result = {"permissions": {}, "scope": "turn"}
                else:
                    requested = pending.params.get("permissions") or {}
                    granted = {
                        key: value
                        for key, value in requested.items()
                        if value is not None
                    }
                    result = {
                        "permissions": granted,
                        "scope": (
                            "session" if decision == "session" else "turn"
                        ),
                    }
            elif method in {"execCommandApproval", "applyPatchApproval"}:
                codex_decision = {
                    "once": "approved",
                    "session": "approved_for_session",
                    "deny": "denied",
                }[decision]
                result = {"decision": codex_decision}
            else:
                return
            await self.codex.respond(pending.server_request_id, result=result)
            self.pending_requests.pop(public_id, None)
            self.store.resolve_pending_request(public_id, decision)
            if callback_query_id:
                with contextlib.suppress(TelegramError):
                    await self._tg(
                        "answer_callback_query",
                        callback_query_id,
                        "Решение передано Codex",
                    )
            topic = self.store.topic_for_thread(pending.thread_id)
            if topic:
                label = (
                    "✅ Разрешено" if decision != "deny" else "❌ Отклонено"
                )
                await self._finalize_prompt_card(
                    pending,
                    f"{label}: запрос обработан.",
                )

    async def resolve_user_input_option(
        self,
        public_id: str,
        option_index: int,
        *,
        callback_query_id: str | None = None,
    ) -> None:
        if not self.codex_available:
            if callback_query_id:
                await self._tg(
                    "answer_callback_query",
                    callback_query_id,
                    "Codex недоступен; ответ не отправлен",
                )
            return
        pending = self.pending_requests.get(public_id)
        if pending is None:
            if callback_query_id:
                await self._tg(
                    "answer_callback_query", callback_query_id, "Уже обработано"
                )
            return
        questions = pending.params.get("questions") or []
        if len(questions) != 1:
            if callback_query_id:
                await self._tg(
                    "answer_callback_query",
                    callback_query_id,
                    "Кнопка недоступна для этого запроса",
                )
            return
        options = questions[0].get("options") or []
        if option_index < 0 or option_index >= len(options):
            if callback_query_id:
                await self._tg(
                    "answer_callback_query",
                    callback_query_id,
                    "Вариант уже недоступен",
                )
            return
        answer = str(options[option_index].get("label") or "")
        await self._finish_user_input(pending, {str(questions[0]["id"]): [answer]})
        if callback_query_id:
            with contextlib.suppress(TelegramError):
                await self._tg(
                    "answer_callback_query",
                    callback_query_id,
                    "Ответ передан Codex",
                )

    async def resolve_user_input_text(self, public_id: str, text: str) -> None:
        if not self.codex_available:
            return
        pending = self.pending_requests.get(public_id)
        if pending is None:
            return
        questions = pending.params.get("questions") or []
        if len(questions) == 1:
            answers = {str(questions[0]["id"]): [text.strip()]}
        else:
            parsed: dict[str, list[str]] = {}
            for line in text.splitlines():
                if ":" not in line:
                    continue
                key, value = line.split(":", 1)
                parsed[key.strip()] = [value.strip()]
            answers = {}
            for index, question in enumerate(questions, start=1):
                question_id = str(question["id"])
                value = parsed.get(question_id) or parsed.get(str(index))
                if value:
                    answers[question_id] = value
            if len(answers) != len(questions):
                topic = self.store.topic_for_thread(pending.thread_id)
                if topic:
                    await self._tg(
                        "send_message",
                        chat_id=topic.chat_id,
                        message_thread_id=topic.topic_id,
                        text=(
                            "Для нескольких вопросов ответьте по строке "
                            "`1: ответ`, `2: ответ`."
                        ),
                    )
                return
        await self._finish_user_input(pending, answers)

    async def _finish_user_input(
        self,
        pending: PendingServerRequest,
        answers: dict[str, list[str]],
    ) -> None:
        if not self.codex_available:
            return
        lock = self._pending_locks.setdefault(
            pending.public_id,
            asyncio.Lock(),
        )
        async with lock:
            if self.pending_requests.get(pending.public_id) is not pending:
                return
            await self.codex.respond(
                pending.server_request_id,
                result={
                    "answers": {
                        key: {"answers": values}
                        for key, values in answers.items()
                    }
                },
            )
            self.pending_requests.pop(pending.public_id, None)
            self.store.resolve_pending_request(pending.public_id, "answered")
            topic = self.store.topic_for_thread(pending.thread_id)
            if topic:
                await self._finalize_prompt_card(
                    pending,
                    "✅ Ответ передан Codex.",
                )

    async def _finalize_prompt_card(
        self,
        pending: PendingServerRequest,
        status_text: str,
    ) -> None:
        topic = self.store.topic_for_thread(pending.thread_id)
        if topic is None:
            return
        if pending.telegram_message_id is not None:
            try:
                if pending.method == "item/tool/requestUserInput":
                    original, _ = self._render_user_input_request(pending)
                else:
                    original = self._render_approval_request(pending)
                await self._tg(
                    "edit_message_text",
                    chat_id=topic.chat_id,
                    message_id=pending.telegram_message_id,
                    text=f"{original}\n\n{status_text}",
                    reply_markup={"inline_keyboard": []},
                )
                return
            except TelegramError as error:
                if "message is not modified" in str(error).lower():
                    return
            except ValueError:
                pass
        with contextlib.suppress(TelegramError):
            await self._tg(
                "send_message",
                chat_id=topic.chat_id,
                message_thread_id=topic.topic_id,
                text=status_text,
            )


def bootstrap_group(
    *,
    store: BridgeStore,
    telegram: TelegramAPI,
    wait_seconds: int = 0,
) -> dict[str, Any]:
    if store.binding() is not None:
        raise RuntimeError(
            "Telegram group is already bound; explicit rebind is not supported"
        )
    identity = telegram.identity()
    deadline = time.monotonic() + wait_seconds
    offset = store.telegram_offset()
    candidate: tuple[dict[str, Any], dict[str, Any]] | None = None
    while True:
        updates = telegram.get_updates(
            offset=offset,
            timeout=min(25, max(0, wait_seconds)),
        )
        for update in updates:
            update_id = int(update["update_id"])
            offset = update_id + 1
            message = update.get("message") or {}
            text = str(message.get("text") or "").strip()
            chat = message.get("chat") or {}
            if not re.match(
                rf"^/connect(?:@{re.escape(identity.username)})?(?:\s|$)",
                text,
                re.IGNORECASE,
            ):
                continue
            if chat.get("type") == "supergroup" and chat.get("is_forum"):
                candidate = (update, message)
        if candidate or wait_seconds <= 0:
            break
        if time.monotonic() >= deadline:
            break

    if candidate is None:
        return {
            "ok": False,
            "needsConnectMessage": True,
            "botUsername": identity.username,
        }

    update, message = candidate
    chat = message["chat"]
    sender = message.get("from") or {}
    chat_id = int(chat["id"])
    sender_id = int(sender["id"])
    bot_member = telegram.get_chat_member(chat_id, identity.bot_id)
    admins = telegram.get_chat_administrators(chat_id)
    human_admins = [
        entry for entry in admins if not (entry.get("user") or {}).get("is_bot")
    ]
    sender_admin = next(
        (
            entry
            for entry in human_admins
            if int((entry.get("user") or {}).get("id") or 0) == sender_id
        ),
        None,
    )
    if sender_admin is None:
        raise RuntimeError("/connect must be sent by a human group administrator")
    if bot_member.get("status") not in {"administrator", "creator"}:
        raise RuntimeError("Bot is not an administrator in the forum supergroup")
    if not bot_member.get("can_manage_topics"):
        raise RuntimeError("Bot does not have Manage Topics permission")

    store.bind(
        chat_id=chat_id,
        allowed_user_id=sender_id,
        bot_id=identity.bot_id,
        bot_username=identity.username,
        chat_title=str(chat.get("title") or "Codex project"),
    )
    if offset is not None:
        store.set_telegram_offset(offset)
    telegram.set_commands()
    telegram.send_message(
        chat_id=chat_id,
        message_thread_id=int(message.get("message_thread_id") or 0) or None,
        reply_to_message_id=int(message["message_id"]),
        text=(
            "✅ Группа привязана к локальному Codex bridge. "
            "Теперь можно создавать Topics для тредов проекта."
        ),
    )
    return {
        "ok": True,
        "botUsername": identity.username,
        "chatTitle": str(chat.get("title") or ""),
        "isForum": bool(chat.get("is_forum")),
        "botCanManageTopics": bool(bot_member.get("can_manage_topics")),
        "allowedUserIsAdmin": True,
        "humanAdminCount": len(human_admins),
    }
