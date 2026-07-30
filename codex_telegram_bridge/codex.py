from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import socket
import subprocess
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import websocket
from websocket import (
    WebSocketConnectionClosedException,
    WebSocketTimeoutException,
)

from .input_types import LocalInput, normalize_local_inputs


LOGGER = logging.getLogger(__name__)
APP_SERVER_STREAM_LIMIT = 64 * 1024 * 1024
APP_SERVER_STOP_TIMEOUT = 3
APP_SERVER_IO_TIMEOUT = 2

NotificationHandler = Callable[[dict[str, Any]], Awaitable[None]]
ServerRequestHandler = Callable[[dict[str, Any]], Awaitable[None]]


class CodexProtocolError(RuntimeError):
    pass


class CodexProtocolCompatibilityError(CodexProtocolError):
    pass


def codex_daemon_environment(
    socket_path: str | Path | None,
) -> dict[str, str]:
    environment = dict(os.environ)
    if socket_path is None:
        return environment
    resolved = Path(socket_path).expanduser()
    if resolved.parent.name == "app-server-control":
        environment["CODEX_HOME"] = str(resolved.parent.parent)
    return environment


class CodexAppServer:
    def __init__(
        self,
        *,
        codex_binary: str,
        cwd: str,
        on_notification: NotificationHandler,
        on_server_request: ServerRequestHandler,
        socket_path: str | Path | None = None,
        compatible_versions: tuple[str, ...] = (),
        full_access: bool = False,
    ):
        self.codex_binary = codex_binary
        self.cwd = cwd
        self.on_notification = on_notification
        self.on_server_request = on_server_request
        self.socket_path = Path(socket_path).expanduser() if socket_path else None
        self.compatible_versions = frozenset(compatible_versions)
        self.full_access = full_access
        self.process: asyncio.subprocess.Process | None = None
        self._websocket: websocket.WebSocket | None = None
        self._reader_task: asyncio.Task[None] | None = None
        self._stderr_task: asyncio.Task[None] | None = None
        self._next_request_id = 10
        self._pending: dict[int, asyncio.Future[dict[str, Any]]] = {}
        self._write_lock = asyncio.Lock()
        self._loaded_threads: set[str] = set()
        self._thread_settings: dict[str, dict[str, Any]] = {}
        self._background_tasks: set[asyncio.Task[None]] = set()
        self._ordered_tails: dict[str, asyncio.Task[None]] = {}
        self._stopping = False
        self.server_version: str | None = None

    @property
    def is_connected(self) -> bool:
        if self._websocket is not None:
            return True
        return bool(self.process is not None and self.process.returncode is None)

    async def start(self) -> None:
        if self._websocket is not None or (
            self.process and self.process.returncode is None
        ):
            return
        self._stopping = False
        self._loaded_threads.clear()
        self._thread_settings.clear()
        if self.socket_path is not None:
            try:
                self._websocket = await asyncio.to_thread(
                    self._connect_unix_websocket,
                    self.socket_path,
                )
            except (OSError, websocket.WebSocketException) as error:
                raise CodexProtocolError(
                    f"Shared Codex app-server is unavailable at {self.socket_path}"
                ) from error
        else:
            self.process = await asyncio.create_subprocess_exec(
                self.codex_binary,
                "app-server",
                cwd=self.cwd,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                limit=APP_SERVER_STREAM_LIMIT,
            )
        self._reader_task = asyncio.create_task(self._reader_loop())
        if self.process is not None:
            self._stderr_task = asyncio.create_task(self._stderr_loop())
        try:
            initialize_result = await self.request(
                "initialize",
                {
                    "clientInfo": {
                        "name": "codex_telegram_forum_bridge",
                        "title": "Codex Telegram Forum Bridge",
                        "version": "0.4.0",
                    },
                    "capabilities": {"experimentalApi": True},
                },
                request_id=1,
            )
            server_info = (
                initialize_result.get("serverInfo")
                or initialize_result.get("server_info")
                or {}
            )
            version = (
                server_info.get("version")
                if isinstance(server_info, dict)
                else None
            ) or initialize_result.get("serverVersion")
            if version is not None:
                self.server_version = str(version)
            elif self.compatible_versions and self.socket_path is not None:
                self.server_version = await self._daemon_version()
            self._validate_protocol_version()
            await self.notify("initialized", {})
        except BaseException:
            await self.stop()
            raise

    async def _daemon_version(self) -> str | None:
        try:
            result = await asyncio.to_thread(
                subprocess.run,
                [
                    self.codex_binary,
                    "app-server",
                    "daemon",
                    "version",
                ],
                check=False,
                capture_output=True,
                text=True,
                timeout=10,
                env=codex_daemon_environment(self.socket_path),
            )
        except (OSError, subprocess.TimeoutExpired):
            return None
        if result.returncode != 0:
            return None
        try:
            payload = json.loads(result.stdout)
        except json.JSONDecodeError:
            return None
        cli_version = str(payload.get("cliVersion") or "")
        server_version = str(payload.get("appServerVersion") or "")
        if not cli_version or cli_version != server_version:
            return None
        return server_version

    def _validate_protocol_version(self) -> None:
        if not self.compatible_versions:
            return
        if not self.server_version:
            raise CodexProtocolCompatibilityError(
                "Codex app-server did not advertise a protocol version"
            )
        if self.server_version not in self.compatible_versions:
            raise CodexProtocolCompatibilityError(
                "Codex app-server version is not in the tested compatibility set"
            )

    @staticmethod
    def _connect_unix_websocket(socket_path: Path) -> websocket.WebSocket:
        unix_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            unix_socket.connect(str(socket_path))
            connection = websocket.create_connection(
                "ws://localhost/",
                socket=unix_socket,
                suppress_origin=True,
                timeout=APP_SERVER_IO_TIMEOUT,
                enable_multithread=True,
            )
            connection.settimeout(APP_SERVER_IO_TIMEOUT)
            return connection
        except BaseException:
            unix_socket.close()
            raise

    async def stop(self) -> None:
        self._stopping = True
        connection = self._websocket
        self._websocket = None
        process = self.process
        if process is not None and process.returncode is None:
            process.terminate()
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(
                    process.wait(),
                    timeout=APP_SERVER_STOP_TIMEOUT,
                )
            if process.returncode is None:
                process.kill()
                await process.wait()

        reader_task = self._reader_task
        if connection is not None and reader_task is not None:
            try:
                await asyncio.wait_for(
                    asyncio.shield(reader_task),
                    timeout=APP_SERVER_STOP_TIMEOUT,
                )
            except asyncio.TimeoutError:
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(
                        asyncio.to_thread(connection.shutdown),
                        timeout=APP_SERVER_STOP_TIMEOUT,
                    )

        for task in (self._reader_task, self._stderr_task):
            if task and not task.done():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
        if connection is not None:
            with contextlib.suppress(Exception):
                await asyncio.wait_for(
                    asyncio.to_thread(connection.close, timeout=1),
                    timeout=APP_SERVER_STOP_TIMEOUT,
                )
        for task in tuple(self._background_tasks):
            task.cancel()
        for task in tuple(self._background_tasks):
            with contextlib.suppress(asyncio.CancelledError):
                await task
        self._background_tasks.clear()
        self._ordered_tails.clear()
        self.process = None
        self._reader_task = None
        self._stderr_task = None
        self._loaded_threads.clear()
        self._thread_settings.clear()
        self.server_version = None

    async def wait_closed(self) -> None:
        """Raise when the child reader exits without an intentional stop."""
        reader_task = self._reader_task
        if reader_task is None:
            raise CodexProtocolError("Codex app-server is not running")
        await asyncio.shield(reader_task)
        if not self._stopping:
            raise CodexProtocolError("Codex app-server stopped unexpectedly")

    def _spawn_background(self, coroutine: Awaitable[None]) -> None:
        task = asyncio.create_task(coroutine)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    def _spawn_ordered_dispatch(
        self,
        message: dict[str, Any],
        coroutine_factory: Callable[[], Awaitable[None]],
    ) -> None:
        params = message.get("params") or {}
        thread = params.get("thread") or {}
        order_key = str(
            params.get("threadId")
            or params.get("conversationId")
            or (thread.get("id") if isinstance(thread, dict) else "")
            or "__global__"
        )
        previous = self._ordered_tails.get(order_key)

        async def run_after_previous() -> None:
            if previous is not None:
                with contextlib.suppress(
                    asyncio.CancelledError,
                    Exception,
                ):
                    await previous
            await coroutine_factory()

        task = asyncio.create_task(run_after_previous())
        self._ordered_tails[order_key] = task
        self._background_tasks.add(task)

        def clear(done: asyncio.Task[None]) -> None:
            self._background_tasks.discard(done)
            if self._ordered_tails.get(order_key) is done:
                self._ordered_tails.pop(order_key, None)

        task.add_done_callback(clear)

    async def _dispatch_server_request(self, message: dict[str, Any]) -> None:
        try:
            await self.on_server_request(message)
        except asyncio.CancelledError:
            raise
        except Exception:
            LOGGER.exception("Codex server request handler failed")
            # The shared App Server broadcasts client requests to every
            # subscribed connection and accepts the first response. A bridge
            # delivery failure must stay silent so a capable Desktop client
            # can still handle the request.

    async def _dispatch_notification(self, message: dict[str, Any]) -> None:
        try:
            await self.on_notification(message)
        except asyncio.CancelledError:
            raise
        except Exception:
            LOGGER.exception("Codex notification handler failed")

    async def _send(self, payload: dict[str, Any]) -> None:
        encoded = json.dumps(payload, ensure_ascii=False)
        connection = self._websocket
        if connection is not None:
            async with self._write_lock:
                try:
                    await asyncio.to_thread(connection.send, encoded)
                except (OSError, websocket.WebSocketException) as error:
                    with contextlib.suppress(Exception):
                        await asyncio.wait_for(
                            asyncio.to_thread(connection.shutdown),
                            timeout=APP_SERVER_STOP_TIMEOUT,
                        )
                    raise CodexProtocolError(
                        "Shared Codex app-server connection closed"
                    ) from error
            return
        if not self.process or not self.process.stdin:
            raise CodexProtocolError("Codex app-server is not running")
        async with self._write_lock:
            self.process.stdin.write((encoded + "\n").encode("utf-8"))
            await self.process.stdin.drain()

    async def request(
        self,
        method: str,
        params: dict[str, Any] | None,
        *,
        request_id: int | None = None,
        timeout: float = 60,
    ) -> dict[str, Any]:
        if request_id is None:
            request_id = self._next_request_id
            self._next_request_id += 1
        loop = asyncio.get_running_loop()
        future: asyncio.Future[dict[str, Any]] = loop.create_future()
        future.add_done_callback(self._consume_future_exception)
        self._pending[request_id] = future
        try:
            try:
                await asyncio.wait_for(
                    self._send(
                        {
                            "method": method,
                            "id": request_id,
                            "params": params or {},
                        }
                    ),
                    timeout=min(timeout, APP_SERVER_IO_TIMEOUT + 1),
                )
            except asyncio.TimeoutError as error:
                connection = self._websocket
                if connection is not None:
                    with contextlib.suppress(Exception):
                        await asyncio.wait_for(
                            asyncio.to_thread(connection.shutdown),
                            timeout=APP_SERVER_STOP_TIMEOUT,
                        )
                raise CodexProtocolError(
                    f"{method}: timed out sending to Codex app-server"
                ) from error
            try:
                message = await asyncio.wait_for(future, timeout=timeout)
            except asyncio.TimeoutError as error:
                connection = self._websocket
                if connection is not None:
                    with contextlib.suppress(Exception):
                        await asyncio.wait_for(
                            asyncio.to_thread(connection.shutdown),
                            timeout=APP_SERVER_STOP_TIMEOUT,
                        )
                raise CodexProtocolError(
                    f"{method}: timed out waiting for Codex app-server"
                ) from error
        finally:
            self._pending.pop(request_id, None)
        if "error" in message:
            error = message["error"] or {}
            raise CodexProtocolError(
                f"{method}: {error.get('message') or 'Codex protocol error'}"
            )
        return dict(message.get("result") or {})

    @staticmethod
    def _consume_future_exception(
        future: asyncio.Future[dict[str, Any]],
    ) -> None:
        if future.cancelled():
            return
        with contextlib.suppress(Exception):
            future.exception()

    async def notify(self, method: str, params: dict[str, Any]) -> None:
        await self._send({"method": method, "params": params})

    async def respond(
        self,
        request_id: int | str,
        *,
        result: dict[str, Any] | None = None,
        error: dict[str, Any] | None = None,
    ) -> None:
        payload: dict[str, Any] = {"id": request_id}
        if error is not None:
            payload["error"] = error
        else:
            payload["result"] = result or {}
        await self._send(payload)

    async def _reader_loop(self) -> None:
        connection = self._websocket
        process = self.process
        while True:
            if self._stopping:
                break
            try:
                if connection is not None:
                    raw = await asyncio.to_thread(connection.recv)
                else:
                    assert process and process.stdout
                    raw = await process.stdout.readline()
            except WebSocketTimeoutException:
                if self._stopping:
                    break
                continue
            except (OSError, WebSocketConnectionClosedException):
                break
            if not raw:
                break
            try:
                if isinstance(raw, bytes):
                    raw = raw.decode("utf-8")
                message = json.loads(raw)
            except (TypeError, UnicodeDecodeError, json.JSONDecodeError):
                LOGGER.warning("Ignored malformed Codex app-server message")
                continue
            request_id = message.get("id")
            if request_id in self._pending and (
                "result" in message or "error" in message
            ):
                future = self._pending[request_id]
                if not future.done():
                    future.set_result(message)
                continue
            if request_id is not None and message.get("method"):
                self._spawn_ordered_dispatch(
                    message,
                    lambda current=message: self._dispatch_server_request(
                        current
                    ),
                )
                continue
            if message.get("method"):
                self._spawn_ordered_dispatch(
                    message,
                    lambda current=message: self._dispatch_notification(
                        current
                    ),
                )

        error = CodexProtocolError("Codex app-server stopped")
        for future in self._pending.values():
            if not future.done():
                future.set_exception(error)

    async def _stderr_loop(self) -> None:
        assert self.process and self.process.stderr
        while True:
            raw = await self.process.stderr.readline()
            if not raw:
                break
            line = raw.decode("utf-8", errors="replace").strip()
            if line:
                LOGGER.debug("Codex app-server emitted a diagnostic line")

    async def list_threads(self, *, archived: bool) -> list[dict[str, Any]]:
        threads: list[dict[str, Any]] = []
        cursor: str | None = None
        while True:
            params: dict[str, Any] = {
                "cwd": self.cwd,
                "archived": archived,
                "limit": 100,
                "sortKey": "updated_at",
                "sortDirection": "desc",
            }
            if cursor:
                params["cursor"] = cursor
            result = await self.request("thread/list", params)
            threads.extend(result.get("data") or [])
            cursor = result.get("nextCursor")
            if not cursor:
                break
        return threads

    async def read_rate_limits(self) -> dict[str, Any]:
        return await self.request("account/rateLimits/read", {})

    async def read_thread(self, thread_id: str) -> dict[str, Any]:
        try:
            result = await self.request(
                "thread/read",
                {"threadId": thread_id, "includeTurns": True},
            )
            return dict(result["thread"])
        except CodexProtocolError as error:
            if "paginated" not in str(error).lower():
                raise

        result = await self.request(
            "thread/read",
            {"threadId": thread_id, "includeTurns": False},
        )
        thread = dict(result["thread"])
        turns: list[dict[str, Any]] = []
        cursor: str | None = None
        while True:
            params: dict[str, Any] = {
                "threadId": thread_id,
                "limit": 100,
                "itemsView": "full",
            }
            if cursor:
                params["cursor"] = cursor
            page = await self.request("thread/turns/list", params)
            turns.extend(page.get("data") or [])
            cursor = page.get("nextCursor")
            if not cursor:
                break
        thread["turns"] = turns
        return thread

    @staticmethod
    def _normalized_thread_settings(
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        settings: dict[str, Any] = {}
        if "model" in payload:
            settings["model"] = payload.get("model")
        if "reasoningEffort" in payload:
            settings["effort"] = payload.get("reasoningEffort")
        elif "effort" in payload:
            settings["effort"] = payload.get("effort")
        if "serviceTier" in payload:
            settings["serviceTier"] = payload.get("serviceTier")
        return settings

    def remember_thread_settings(
        self,
        thread_id: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        normalized = self._normalized_thread_settings(payload)
        if normalized:
            current = dict(self._thread_settings.get(thread_id) or {})
            current.update(normalized)
            self._thread_settings[thread_id] = current
        return dict(self._thread_settings.get(thread_id) or {})

    def cached_thread_settings(self, thread_id: str) -> dict[str, Any]:
        return dict(self._thread_settings.get(thread_id) or {})

    def _thread_permission_params(self) -> dict[str, Any]:
        if not self.full_access:
            return {}
        return {
            "approvalPolicy": "never",
            "sandbox": "danger-full-access",
        }

    def _turn_permission_params(self) -> dict[str, Any]:
        if not self.full_access:
            return {}
        return {
            "approvalPolicy": "never",
            "sandboxPolicy": {"type": "dangerFullAccess"},
        }

    async def resume_thread(
        self,
        thread_id: str,
        *,
        refresh_settings: bool = False,
    ) -> dict[str, Any]:
        if thread_id in self._loaded_threads:
            if not refresh_settings:
                return self.cached_thread_settings(thread_id)
        result = await self.request(
            "thread/resume",
            {
                "threadId": thread_id,
                **self._thread_permission_params(),
            },
        )
        self._loaded_threads.add(thread_id)
        return self.remember_thread_settings(thread_id, result)

    async def refresh_thread_settings(
        self,
        thread_id: str,
    ) -> dict[str, Any]:
        return await self.resume_thread(thread_id, refresh_settings=True)

    async def update_thread_settings(
        self,
        *,
        thread_id: str,
        effort: str | None = None,
        service_tier: str | None = None,
        update_effort: bool = False,
        update_service_tier: bool = False,
    ) -> dict[str, Any]:
        if not update_effort and not update_service_tier:
            raise ValueError("at least one thread setting must be selected")
        params: dict[str, Any] = {"threadId": thread_id}
        if update_effort:
            params["effort"] = effort
        if update_service_tier:
            params["serviceTier"] = service_tier
        await self.request("thread/settings/update", params)
        return await self.refresh_thread_settings(thread_id)

    async def list_models(
        self,
        *,
        include_hidden: bool = False,
    ) -> list[dict[str, Any]]:
        models: list[dict[str, Any]] = []
        cursor: str | None = None
        while True:
            params: dict[str, Any] = {
                "limit": 100,
                "includeHidden": include_hidden,
            }
            if cursor:
                params["cursor"] = cursor
            result = await self.request("model/list", params)
            models.extend(result.get("data") or [])
            cursor = result.get("nextCursor")
            if not cursor:
                return models

    async def start_thread(self) -> dict[str, Any]:
        result = await self.request(
            "thread/start",
            {
                "cwd": self.cwd,
                "ephemeral": False,
                "threadSource": "telegram_bridge",
                **self._thread_permission_params(),
            },
        )
        thread = dict(result["thread"])
        thread_id = str(thread["id"])
        self._loaded_threads.add(thread_id)
        self.remember_thread_settings(thread_id, result)
        return thread

    async def set_thread_name(self, thread_id: str, name: str) -> None:
        await self.request(
            "thread/name/set",
            {"threadId": thread_id, "name": name},
        )

    async def archive_thread(self, thread_id: str) -> dict[str, Any]:
        result = await self.request(
            "thread/archive",
            {"threadId": thread_id},
        )
        self._loaded_threads.discard(thread_id)
        self._thread_settings.pop(thread_id, None)
        return dict(result)

    async def unarchive_thread(self, thread_id: str) -> dict[str, Any]:
        # Archiving tears down the App Server subscription even though this
        # client may still remember a previous resume. Force the next access
        # to issue a fresh thread/resume.
        self._loaded_threads.discard(thread_id)
        self._thread_settings.pop(thread_id, None)
        result = await self.request(
            "thread/unarchive",
            {"threadId": thread_id},
        )
        return dict(result["thread"])

    def forget_thread(self, thread_id: str) -> None:
        self._loaded_threads.discard(thread_id)
        self._thread_settings.pop(thread_id, None)

    @staticmethod
    def _text_input(text: str) -> list[dict[str, Any]]:
        return [{"type": "text", "text": text, "text_elements": []}]

    @classmethod
    def _turn_input(
        cls,
        text: str,
        local_inputs: tuple[LocalInput, ...] = (),
    ) -> list[dict[str, Any]]:
        return [
            *cls._text_input(text),
            *[
                item.to_payload()
                for item in normalize_local_inputs(local_inputs)
            ],
        ]

    async def start_turn(
        self,
        *,
        thread_id: str,
        text: str,
        client_id: str,
        local_inputs: tuple[LocalInput, ...] = (),
    ) -> dict[str, Any]:
        await self.resume_thread(thread_id)
        result = await self.request(
            "turn/start",
            {
                "threadId": thread_id,
                "input": self._turn_input(text, local_inputs),
                "clientUserMessageId": client_id,
                **self._turn_permission_params(),
            },
        )
        return dict(result["turn"])

    async def steer_turn(
        self,
        *,
        thread_id: str,
        turn_id: str,
        text: str,
        client_id: str,
        local_inputs: tuple[LocalInput, ...] = (),
    ) -> str:
        result = await self.request(
            "turn/steer",
            {
                "threadId": thread_id,
                "expectedTurnId": turn_id,
                "input": self._turn_input(text, local_inputs),
                "clientUserMessageId": client_id,
            },
        )
        return str(result["turnId"])

    async def interrupt_turn(self, *, thread_id: str, turn_id: str) -> None:
        await self.request(
            "turn/interrupt",
            {"threadId": thread_id, "turnId": turn_id},
        )
