from __future__ import annotations

import json
import mimetypes
import os
import re
import secrets
import stat
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from pathlib import PurePosixPath
from typing import Any


TELEGRAM_TEXT_LIMIT = 4096
SAFE_TEXT_CHUNK = 3900


class TelegramError(RuntimeError):
    """Telegram failure with retry metadata for higher-level recovery policy.

    The message-only constructor remains supported for callers that do not need
    structured recovery information.
    """

    def __init__(
        self,
        message: str,
        *,
        method: str | None = None,
        kind: str = "api",
        retryable: bool = False,
        outcome_ambiguous: bool = False,
        retry_after_seconds: int | None = None,
        http_status: int | None = None,
    ) -> None:
        super().__init__(message)
        self.method = method
        self.kind = kind
        self.retryable = retryable
        self.outcome_ambiguous = outcome_ambiguous
        self.retry_after_seconds = retry_after_seconds
        self.http_status = http_status

    @property
    def ambiguous_outcome(self) -> bool:
        """Readable alias for integrations that phrase the condition first."""

        return self.outcome_ambiguous


_READ_ONLY_METHODS = frozenset(
    {
        "getMe",
        "getUpdates",
        "getFile",
        "getChatAdministrators",
        "getChatMember",
        "getStickerSet",
    }
)


def _is_side_effecting_method(method: str) -> bool:
    # Unknown methods are treated conservatively: retrying an unclassified Bot
    # API POST must not silently assume that the first request had no effect.
    return method not in _READ_ONLY_METHODS


def exponential_backoff_delay(
    attempt: int,
    *,
    initial_seconds: float = 1.0,
    multiplier: float = 2.0,
    maximum_seconds: float = 60.0,
    jitter_ratio: float = 0.2,
    jitter_sample: float = 0.5,
) -> float:
    """Return a bounded retry delay without sleeping.

    ``attempt`` is zero-based. ``jitter_sample`` accepts a caller-provided
    sample in the inclusive range 0..1, allowing production code to inject
    randomness while tests use exact deterministic values. A sample of 0.5
    applies no jitter.
    """

    if attempt < 0:
        raise ValueError("attempt must be non-negative")
    if initial_seconds <= 0:
        raise ValueError("initial_seconds must be positive")
    if multiplier < 1:
        raise ValueError("multiplier must be at least 1")
    if maximum_seconds <= 0:
        raise ValueError("maximum_seconds must be positive")
    if not 0 <= jitter_ratio <= 1:
        raise ValueError("jitter_ratio must be between 0 and 1")
    if not 0 <= jitter_sample <= 1:
        raise ValueError("jitter_sample must be between 0 and 1")

    try:
        uncapped = initial_seconds * (multiplier**attempt)
    except OverflowError:
        uncapped = maximum_seconds
    base_delay = min(uncapped, maximum_seconds)
    centered_sample = (2 * jitter_sample) - 1
    jittered_delay = base_delay * (1 + jitter_ratio * centered_sample)
    return min(maximum_seconds, max(0.0, jittered_delay))


@dataclass(frozen=True)
class TelegramIdentity:
    bot_id: int
    username: str


def split_telegram_text(text: str, limit: int = SAFE_TEXT_CHUNK) -> list[str]:
    if not text:
        return [""]
    chunks: list[str] = []
    remaining = text
    while len(remaining) > limit:
        split_at = remaining.rfind("\n", 0, limit + 1)
        if split_at >= limit // 2:
            split_at += 1
        else:
            split_at = remaining.rfind(" ", 0, limit + 1)
            if split_at >= limit // 2:
                split_at += 1
            else:
                split_at = limit
        chunks.append(remaining[:split_at])
        remaining = remaining[split_at:]
    if remaining:
        chunks.append(remaining)
    return chunks


class TelegramAPI:
    def __init__(self, token: str, timeout_seconds: int = 40):
        if not re.fullmatch(r"\d+:[A-Za-z0-9_-]{20,}", token):
            raise TelegramError("Keychain value is not a valid Telegram bot token")
        self._base_url = f"https://api.telegram.org/bot{token}/"
        self._file_base_url = f"https://api.telegram.org/file/bot{token}/"
        self._timeout_seconds = timeout_seconds

    def call(
        self,
        method: str,
        params: dict[str, Any] | None = None,
        *,
        retry_flood: bool = True,
    ) -> Any:
        http_status: int | None = None
        request = urllib.request.Request(
            self._base_url + method,
            data=json.dumps(params or {}).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(
                request, timeout=self._timeout_seconds
            ) as response:
                try:
                    payload = json.loads(response.read().decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError) as error:
                    raise TelegramError(
                        f"{method}: malformed Telegram response",
                        method=method,
                        kind="protocol",
                        retryable=True,
                        outcome_ambiguous=_is_side_effecting_method(method),
                    ) from error
        except urllib.error.HTTPError as error:
            http_status = int(error.code)
            try:
                payload = json.loads(error.read().decode("utf-8"))
            except Exception:
                raise TelegramError(
                    f"{method}: Telegram HTTP {http_status}",
                    method=method,
                    kind="http_5xx" if 500 <= http_status <= 599 else "http",
                    retryable=500 <= http_status <= 599,
                    outcome_ambiguous=(
                        500 <= http_status <= 599
                        and _is_side_effecting_method(method)
                    ),
                    http_status=http_status,
                ) from None
        except urllib.error.URLError as error:
            is_timeout = isinstance(error.reason, TimeoutError)
            raise TelegramError(
                f"{method}: Telegram network error",
                method=method,
                kind="network_timeout" if is_timeout else "network_error",
                retryable=True,
                outcome_ambiguous=_is_side_effecting_method(method),
            ) from error
        except TimeoutError as error:
            raise TelegramError(
                f"{method}: Telegram network error",
                method=method,
                kind="network_timeout",
                retryable=True,
                outcome_ambiguous=_is_side_effecting_method(method),
            ) from error

        if http_status is not None:
            if 500 <= http_status <= 599:
                raise TelegramError(
                    f"{method}: Telegram HTTP {http_status}",
                    method=method,
                    kind="http_5xx",
                    retryable=True,
                    outcome_ambiguous=_is_side_effecting_method(method),
                    http_status=http_status,
                )

        if payload.get("ok"):
            return payload.get("result")

        parameters = payload.get("parameters") or {}
        retry_after = parameters.get("retry_after")
        if retry_flood and retry_after:
            time.sleep(min(int(retry_after) + 1, 65))
            return self.call(method, params, retry_flood=False)

        description = str(payload.get("description") or "Telegram API error")
        retry_after_seconds = int(retry_after) if retry_after else None
        raise TelegramError(
            f"{method}: {description}",
            method=method,
            kind="flood_wait" if retry_after_seconds is not None else "api",
            retryable=retry_after_seconds is not None,
            retry_after_seconds=retry_after_seconds,
            http_status=http_status,
        )

    def call_multipart(
        self,
        method: str,
        params: dict[str, Any],
        *,
        file_field: str,
        file_path: str | Path,
    ) -> Any:
        path = Path(file_path)
        if not path.is_file():
            raise FileNotFoundError(path)
        boundary = f"CodexTelegramBridge{secrets.token_hex(16)}"
        body = bytearray()

        def append_field(name: str, value: Any) -> None:
            encoded = (
                json.dumps(value, ensure_ascii=False)
                if isinstance(value, (dict, list, bool))
                else str(value)
            )
            body.extend(f"--{boundary}\r\n".encode("ascii"))
            body.extend(
                (
                    f'Content-Disposition: form-data; name="{name}"'
                    "\r\n\r\n"
                ).encode("utf-8")
            )
            body.extend(encoded.encode("utf-8"))
            body.extend(b"\r\n")

        for name, value in params.items():
            append_field(name, value)
        mime_type = mimetypes.guess_type(path.name)[0] or (
            "application/octet-stream"
        )
        body.extend(f"--{boundary}\r\n".encode("ascii"))
        body.extend(
            (
                f'Content-Disposition: form-data; name="{file_field}"; '
                f'filename="{path.name}"\r\n'
                f"Content-Type: {mime_type}\r\n\r\n"
            ).encode("utf-8")
        )
        body.extend(path.read_bytes())
        body.extend(b"\r\n")
        body.extend(f"--{boundary}--\r\n".encode("ascii"))

        request = urllib.request.Request(
            self._base_url + method,
            data=bytes(body),
            headers={
                "Content-Type": f"multipart/form-data; boundary={boundary}",
            },
            method="POST",
        )
        http_status: int | None = None
        try:
            with urllib.request.urlopen(
                request, timeout=self._timeout_seconds
            ) as response:
                try:
                    payload = json.loads(response.read().decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError) as error:
                    raise TelegramError(
                        f"{method}: malformed Telegram response",
                        method=method,
                        kind="protocol",
                        retryable=True,
                        outcome_ambiguous=_is_side_effecting_method(method),
                    ) from error
        except urllib.error.HTTPError as error:
            http_status = int(error.code)
            try:
                payload = json.loads(error.read().decode("utf-8"))
            except Exception:
                raise TelegramError(
                    f"{method}: Telegram HTTP {http_status}",
                    method=method,
                    kind="http_5xx" if 500 <= http_status <= 599 else "http",
                    retryable=500 <= http_status <= 599,
                    outcome_ambiguous=(
                        500 <= http_status <= 599
                        and _is_side_effecting_method(method)
                    ),
                    http_status=http_status,
                ) from None
        except urllib.error.URLError as error:
            is_timeout = isinstance(error.reason, TimeoutError)
            raise TelegramError(
                f"{method}: Telegram network error",
                method=method,
                kind="network_timeout" if is_timeout else "network_error",
                retryable=True,
                outcome_ambiguous=_is_side_effecting_method(method),
            ) from error
        except TimeoutError as error:
            raise TelegramError(
                f"{method}: Telegram network error",
                method=method,
                kind="network_timeout",
                retryable=True,
                outcome_ambiguous=_is_side_effecting_method(method),
            ) from error

        if http_status is not None and 500 <= http_status <= 599:
            raise TelegramError(
                f"{method}: Telegram HTTP {http_status}",
                method=method,
                kind="http_5xx",
                retryable=True,
                outcome_ambiguous=_is_side_effecting_method(method),
                http_status=http_status,
            )
        if payload.get("ok"):
            return payload.get("result")
        parameters = payload.get("parameters") or {}
        retry_after = parameters.get("retry_after")
        raise TelegramError(
            f"{method}: "
            f"{payload.get('description') or 'Telegram API error'}",
            method=method,
            kind="flood_wait" if retry_after else "api",
            retryable=retry_after is not None,
            retry_after_seconds=int(retry_after) if retry_after else None,
            http_status=http_status,
        )

    def identity(self) -> TelegramIdentity:
        result = self.call("getMe")
        return TelegramIdentity(
            bot_id=int(result["id"]),
            username=str(result.get("username") or ""),
        )

    def get_updates(
        self,
        *,
        offset: int | None,
        timeout: int,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {
            "timeout": timeout,
            "limit": limit,
            "allowed_updates": [
                "message",
                "edited_message",
                "callback_query",
                "my_chat_member",
            ],
        }
        if offset is not None:
            params["offset"] = offset
        return list(self.call("getUpdates", params))

    def get_file(self, file_id: str) -> dict[str, Any]:
        if not file_id:
            raise ValueError("Telegram file id is required")
        return dict(self.call("getFile", {"file_id": file_id}))

    def download_file(
        self,
        *,
        file_path: str,
        destination: str | Path,
        max_bytes: int,
    ) -> Path:
        if max_bytes <= 0:
            raise ValueError("max_bytes must be positive")
        remote_path = PurePosixPath(file_path)
        if (
            not file_path
            or len(file_path) > 512
            or remote_path.is_absolute()
            or any(part in {"", ".", ".."} for part in remote_path.parts)
        ):
            raise TelegramError(
                "downloadFile: unsafe Telegram file path",
                method="downloadFile",
                kind="unsafe_file_path",
            )

        output = Path(destination)
        parent = output.parent
        try:
            parent_status = parent.stat()
        except OSError:
            raise TelegramError(
                "downloadFile: destination directory is unavailable",
                method="downloadFile",
                kind="unsafe_destination",
            ) from None
        if (
            parent.is_symlink()
            or not stat.S_ISDIR(parent_status.st_mode)
            or parent_status.st_uid != os.getuid()
            or output.is_symlink()
        ):
            raise TelegramError(
                "downloadFile: unsafe destination",
                method="downloadFile",
                kind="unsafe_destination",
            )

        encoded_path = urllib.parse.quote(file_path, safe="/")
        request = urllib.request.Request(
            self._file_base_url + encoded_path,
            method="GET",
        )
        temporary = output.with_name(
            f".{output.name}.part-{secrets.token_hex(8)}"
        )
        descriptor: int | None = None
        try:
            try:
                response = urllib.request.urlopen(
                    request,
                    timeout=self._timeout_seconds,
                )
            except urllib.error.HTTPError as error:
                raise TelegramError(
                    f"downloadFile: Telegram HTTP {int(error.code)}",
                    method="downloadFile",
                    kind=(
                        "http_5xx"
                        if 500 <= int(error.code) <= 599
                        else "http"
                    ),
                    retryable=500 <= int(error.code) <= 599,
                    http_status=int(error.code),
                ) from None
            except urllib.error.URLError as error:
                is_timeout = isinstance(error.reason, TimeoutError)
                raise TelegramError(
                    "downloadFile: Telegram network error",
                    method="downloadFile",
                    kind=(
                        "network_timeout" if is_timeout else "network_error"
                    ),
                    retryable=True,
                ) from error
            except TimeoutError as error:
                raise TelegramError(
                    "downloadFile: Telegram network error",
                    method="downloadFile",
                    kind="network_timeout",
                    retryable=True,
                ) from error

            with response:
                content_length = response.headers.get("Content-Length")
                if content_length is not None:
                    try:
                        declared_size = int(content_length)
                    except ValueError:
                        declared_size = -1
                    if declared_size > max_bytes:
                        raise TelegramError(
                            "downloadFile: Telegram file is too large",
                            method="downloadFile",
                            kind="file_too_large",
                        )
                descriptor = os.open(
                    temporary,
                    os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                    0o600,
                )
                with os.fdopen(descriptor, "wb") as stream:
                    descriptor = None
                    total = 0
                    while True:
                        chunk = response.read(64 * 1024)
                        if not chunk:
                            break
                        total += len(chunk)
                        if total > max_bytes:
                            raise TelegramError(
                                "downloadFile: Telegram file is too large",
                                method="downloadFile",
                                kind="file_too_large",
                            )
                        stream.write(chunk)
                    stream.flush()
                    os.fsync(stream.fileno())
            if temporary.stat().st_size <= 0:
                raise TelegramError(
                    "downloadFile: Telegram returned an empty file",
                    method="downloadFile",
                    kind="empty_file",
                )
            os.replace(temporary, output)
            os.chmod(output, 0o600)
            return output
        finally:
            if descriptor is not None:
                os.close(descriptor)
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass

    def get_chat_administrators(self, chat_id: int) -> list[dict[str, Any]]:
        return list(self.call("getChatAdministrators", {"chat_id": chat_id}))

    def get_chat_member(self, chat_id: int, user_id: int) -> dict[str, Any]:
        return dict(
            self.call(
                "getChatMember",
                {"chat_id": chat_id, "user_id": user_id},
            )
        )

    def get_sticker_set(self, name: str) -> dict[str, Any]:
        return dict(self.call("getStickerSet", {"name": name}))

    def upload_static_sticker(
        self,
        *,
        user_id: int,
        file_path: str | Path,
    ) -> dict[str, Any]:
        return dict(
            self.call_multipart(
                "uploadStickerFile",
                {
                    "user_id": user_id,
                    "sticker_format": "static",
                },
                file_field="sticker",
                file_path=file_path,
            )
        )

    def create_custom_emoji_set(
        self,
        *,
        user_id: int,
        name: str,
        title: str,
        sticker_file_id: str,
        emoji: str,
    ) -> bool:
        return bool(
            self.call(
                "createNewStickerSet",
                {
                    "user_id": user_id,
                    "name": name,
                    "title": title,
                    "stickers": [
                        {
                            "sticker": sticker_file_id,
                            "format": "static",
                            "emoji_list": [emoji],
                            "keywords": ["Codex"],
                        }
                    ],
                    "sticker_type": "custom_emoji",
                },
            )
        )

    def set_commands(self) -> None:
        self.call(
            "setMyCommands",
            {
                "commands": [
                    {
                        "command": "steer",
                        "description": "передать текст в текущий ход немедленно",
                    },
                    {
                        "command": "queue",
                        "description": "поставить текст следующим ходом",
                    },
                    {
                        "command": "new",
                        "description": "создать новый тред",
                    },
                    {
                        "command": "status",
                        "description": "показать состояние Topic/треда",
                    },
                    {
                        "command": "mode",
                        "description": "изменить интеллект и скорость треда",
                    },
                    {
                        "command": "limits",
                        "description": "показать остаток недельного лимита Codex",
                    },
                    {
                        "command": "archive",
                        "description": "архивировать связанный тред",
                    },
                    {
                        "command": "audit",
                        "description": "read-only аудит bridge в General",
                    },
                    {
                        "command": "cancel",
                        "description": "остановить текущий ход",
                    },
                ]
            },
        )

    def send_message(
        self,
        *,
        chat_id: int,
        text: str,
        message_thread_id: int | None = None,
        reply_to_message_id: int | None = None,
        reply_markup: dict[str, Any] | None = None,
        parse_mode: str | None = None,
        entities: list[dict[str, Any]] | None = None,
    ) -> list[dict[str, Any]]:
        if parse_mode and entities:
            raise ValueError("parse_mode and entities are mutually exclusive")
        results: list[dict[str, Any]] = []
        chunks = split_telegram_text(text)
        for index, chunk in enumerate(chunks):
            params: dict[str, Any] = {
                "chat_id": chat_id,
                "text": chunk or " ",
                "link_preview_options": {"is_disabled": True},
            }
            if message_thread_id:
                params["message_thread_id"] = message_thread_id
            if parse_mode:
                params["parse_mode"] = parse_mode
            if index == 0 and entities:
                params["entities"] = entities
            if index == 0 and reply_to_message_id:
                params["reply_parameters"] = {
                    "message_id": reply_to_message_id,
                    "allow_sending_without_reply": True,
                }
            if index == 0 and reply_markup:
                params["reply_markup"] = reply_markup
            try:
                results.append(dict(self.call("sendMessage", params)))
            except TelegramError as error:
                if results:
                    raise TelegramError(
                        "sendMessage: partial multi-chunk delivery",
                        method="sendMessage",
                        kind="partial_delivery",
                        retryable=False,
                        outcome_ambiguous=True,
                    ) from error
                raise
        return results

    def send_document(
        self,
        *,
        chat_id: int,
        file_path: str | Path,
        message_thread_id: int | None = None,
        reply_to_message_id: int | None = None,
        caption: str | None = None,
    ) -> dict[str, Any]:
        if caption is not None and len(caption) > 1024:
            raise ValueError("Telegram document caption is too long")
        params: dict[str, Any] = {"chat_id": chat_id}
        if message_thread_id:
            params["message_thread_id"] = message_thread_id
        if reply_to_message_id:
            params["reply_parameters"] = {
                "message_id": reply_to_message_id,
                "allow_sending_without_reply": True,
            }
        if caption:
            params["caption"] = caption
        return dict(
            self.call_multipart(
                "sendDocument",
                params,
                file_field="document",
                file_path=file_path,
            )
        )

    def send_photo(
        self,
        *,
        chat_id: int,
        file_path: str | Path,
        message_thread_id: int | None = None,
        reply_to_message_id: int | None = None,
        caption: str | None = None,
        reply_markup: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if caption is not None and len(caption) > 1024:
            raise ValueError("Telegram photo caption is too long")
        params: dict[str, Any] = {"chat_id": chat_id}
        if message_thread_id:
            params["message_thread_id"] = message_thread_id
        if reply_to_message_id:
            params["reply_parameters"] = {
                "message_id": reply_to_message_id,
                "allow_sending_without_reply": True,
            }
        if caption:
            params["caption"] = caption
        if reply_markup:
            params["reply_markup"] = reply_markup
        return dict(
            self.call_multipart(
                "sendPhoto",
                params,
                file_field="photo",
                file_path=file_path,
            )
        )

    def send_attachment(
        self,
        *,
        chat_id: int,
        media_kind: str,
        file_path: str | Path | None = None,
        url: str | None = None,
        message_thread_id: int | None = None,
        reply_to_message_id: int | None = None,
        caption: str | None = None,
    ) -> dict[str, Any]:
        methods = {
            "animation": ("sendAnimation", "animation"),
            "audio": ("sendAudio", "audio"),
            "document": ("sendDocument", "document"),
            "photo": ("sendPhoto", "photo"),
            "video": ("sendVideo", "video"),
        }
        if media_kind not in methods:
            raise ValueError("unsupported Telegram attachment kind")
        if (file_path is None) == (url is None):
            raise ValueError("exactly one Telegram attachment source is required")
        if caption is not None and len(caption) > 1024:
            raise ValueError("Telegram attachment caption is too long")
        method, file_field = methods[media_kind]
        params: dict[str, Any] = {"chat_id": chat_id}
        if message_thread_id:
            params["message_thread_id"] = message_thread_id
        if reply_to_message_id:
            params["reply_parameters"] = {
                "message_id": reply_to_message_id,
                "allow_sending_without_reply": True,
            }
        if caption:
            params["caption"] = caption
        if media_kind == "video":
            params["supports_streaming"] = True
        try:
            if file_path is not None:
                return dict(
                    self.call_multipart(
                        method,
                        params,
                        file_field=file_field,
                        file_path=file_path,
                    )
                )
            assert url is not None
            params[file_field] = url
            return dict(self.call(method, params))
        except TelegramError as error:
            if (
                media_kind == "document"
                or error.outcome_ambiguous
                or error.retryable
                or error.kind not in {"api", "http"}
            ):
                raise
            # Telegram accepts fewer codecs and dimensions for native media
            # than for documents. A definite native-format rejection cannot
            # have delivered the file, so preserving the original as a
            # document is safe. Ambiguous outcomes never take this fallback.
            return self.send_attachment(
                chat_id=chat_id,
                media_kind="document",
                file_path=file_path,
                url=url,
                message_thread_id=message_thread_id,
                reply_to_message_id=reply_to_message_id,
                caption=caption,
            )

    def send_rich_message(
        self,
        *,
        chat_id: int,
        rich_message: dict[str, Any],
        message_thread_id: int | None = None,
        reply_to_message_id: int | None = None,
        reply_markup: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "chat_id": chat_id,
            "rich_message": rich_message,
        }
        if message_thread_id:
            params["message_thread_id"] = message_thread_id
        if reply_to_message_id:
            params["reply_parameters"] = {
                "message_id": reply_to_message_id,
                "allow_sending_without_reply": True,
            }
        if reply_markup:
            params["reply_markup"] = reply_markup
        return dict(self.call("sendRichMessage", params))

    def edit_reply_markup(
        self,
        *,
        chat_id: int,
        message_id: int,
        reply_markup: dict[str, Any] | None = None,
    ) -> bool:
        return bool(
            self.call(
                "editMessageReplyMarkup",
                {
                    "chat_id": chat_id,
                    "message_id": message_id,
                    "reply_markup": reply_markup or {"inline_keyboard": []},
                },
            )
        )

    def edit_message_text(
        self,
        *,
        chat_id: int,
        message_id: int,
        text: str | None = None,
        rich_message: dict[str, Any] | None = None,
        reply_markup: dict[str, Any] | None = None,
        parse_mode: str | None = None,
    ) -> bool:
        if (text is None) == (rich_message is None):
            raise ValueError("exactly one of text or rich_message is required")
        params: dict[str, Any] = {
            "chat_id": chat_id,
            "message_id": message_id,
        }
        if rich_message is not None:
            params["rich_message"] = rich_message
        else:
            assert text is not None
            params["text"] = text[:TELEGRAM_TEXT_LIMIT]
            params["link_preview_options"] = {"is_disabled": True}
            if parse_mode:
                params["parse_mode"] = parse_mode
        if reply_markup is not None:
            params["reply_markup"] = reply_markup
        return bool(self.call("editMessageText", params))

    def edit_message_caption(
        self,
        *,
        chat_id: int,
        message_id: int,
        caption: str,
        reply_markup: dict[str, Any] | None = None,
    ) -> bool:
        params: dict[str, Any] = {
            "chat_id": chat_id,
            "message_id": message_id,
            "caption": caption[:1024],
        }
        if reply_markup is not None:
            params["reply_markup"] = reply_markup
        return bool(self.call("editMessageCaption", params))

    def delete_message(self, chat_id: int, message_id: int) -> bool:
        return bool(
            self.call(
                "deleteMessage",
                {
                    "chat_id": chat_id,
                    "message_id": message_id,
                },
            )
        )

    def send_chat_action(
        self,
        *,
        chat_id: int,
        action: str = "typing",
        message_thread_id: int | None = None,
    ) -> bool:
        params: dict[str, Any] = {
            "chat_id": chat_id,
            "action": action,
        }
        if message_thread_id:
            params["message_thread_id"] = message_thread_id
        return bool(self.call("sendChatAction", params))

    def create_forum_topic(self, chat_id: int, name: str) -> dict[str, Any]:
        return dict(
            self.call(
                "createForumTopic",
                {"chat_id": chat_id, "name": name[:128]},
            )
        )

    def edit_forum_topic(
        self, chat_id: int, message_thread_id: int, name: str
    ) -> bool:
        return bool(
            self.call(
                "editForumTopic",
                {
                    "chat_id": chat_id,
                    "message_thread_id": message_thread_id,
                    "name": name[:128],
                },
            )
        )

    def close_forum_topic(self, chat_id: int, message_thread_id: int) -> bool:
        return bool(
            self.call(
                "closeForumTopic",
                {
                    "chat_id": chat_id,
                    "message_thread_id": message_thread_id,
                },
            )
        )

    def delete_forum_topic(self, chat_id: int, message_thread_id: int) -> bool:
        return bool(
            self.call(
                "deleteForumTopic",
                {
                    "chat_id": chat_id,
                    "message_thread_id": message_thread_id,
                },
            )
        )

    def reopen_forum_topic(self, chat_id: int, message_thread_id: int) -> bool:
        return bool(
            self.call(
                "reopenForumTopic",
                {
                    "chat_id": chat_id,
                    "message_thread_id": message_thread_id,
                },
            )
        )

    def answer_callback_query(
        self, callback_query_id: str, text: str | None = None
    ) -> bool:
        params: dict[str, Any] = {"callback_query_id": callback_query_id}
        if text:
            params["text"] = text[:200]
        return bool(self.call("answerCallbackQuery", params))
