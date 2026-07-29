from __future__ import annotations

import asyncio
import json
import queue
import sys
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import websocket


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from codex_telegram_bridge.codex import (  # noqa: E402
    CodexAppServer,
    CodexProtocolCompatibilityError,
    CodexProtocolError,
)
from codex_telegram_bridge.input_types import LocalInput  # noqa: E402


async def ignore_message(_: dict[str, object]) -> None:
    return None


class EndOfFileStream:
    async def readline(self) -> bytes:
        return b""


class BlockingStream:
    def __init__(self) -> None:
        self.blocked = asyncio.Event()

    async def readline(self) -> bytes:
        await self.blocked.wait()
        return b""


class FakeChildProcess:
    def __init__(self) -> None:
        self.returncode: int | None = None
        self.stdin = SimpleNamespace()
        self.stdout = BlockingStream()
        self.stderr = BlockingStream()

    def terminate(self) -> None:
        self.returncode = 0

    def kill(self) -> None:
        self.returncode = -9

    async def wait(self) -> int:
        assert self.returncode is not None
        return self.returncode


class FakeUnixWebSocket:
    def __init__(
        self,
        *,
        answer_requests: bool = False,
        send_error: Exception | None = None,
        response_result: dict[str, object] | None = None,
    ) -> None:
        self.answer_requests = answer_requests
        self.send_error = send_error
        self.response_result = dict(response_result or {})
        self.sent: list[str] = []
        self.incoming: queue.Queue[str] = queue.Queue()
        self.close_calls = 0
        self.shutdown_calls = 0

    def send(self, payload: str) -> None:
        if self.send_error is not None:
            raise self.send_error
        self.sent.append(payload)
        message = json.loads(payload)
        if self.answer_requests and "id" in message:
            self.incoming.put(
                json.dumps(
                    {
                        "id": message["id"],
                        "result": self.response_result,
                    }
                )
            )

    def recv(self) -> str:
        return self.incoming.get()

    def close(self, *, timeout: int = 3) -> None:
        del timeout
        self.close_calls += 1
        self.incoming.put("")

    def shutdown(self) -> None:
        self.shutdown_calls += 1
        self.incoming.put("")


class BlockingSendWebSocket(FakeUnixWebSocket):
    def __init__(self) -> None:
        super().__init__()
        self.release_send = threading.Event()

    def send(self, payload: str) -> None:
        self.release_send.wait()

    def shutdown(self) -> None:
        self.release_send.set()
        super().shutdown()


class CodexUnixWebSocketTests(unittest.IsolatedAsyncioTestCase):
    def make_server(
        self,
        socket_path: str = "/tmp/codex-app-server.sock",
        *,
        compatible_versions: tuple[str, ...] = (),
    ) -> CodexAppServer:
        return CodexAppServer(
            codex_binary="/must/not/be/started",
            cwd="/tmp",
            on_notification=ignore_message,
            on_server_request=ignore_message,
            socket_path=socket_path,
            compatible_versions=compatible_versions,
        )

    async def test_start_connects_to_unix_websocket_and_initializes(self) -> None:
        server = self.make_server()
        client = FakeUnixWebSocket(answer_requests=True)

        with (
            patch.object(
                CodexAppServer,
                "_connect_unix_websocket",
                return_value=client,
            ) as connect,
            patch(
                "codex_telegram_bridge.codex.asyncio.create_subprocess_exec",
                new=AsyncMock(),
            ) as create_process,
        ):
            try:
                await server.start()
            finally:
                await server.stop()

        connect.assert_called_once_with(Path("/tmp/codex-app-server.sock"))
        create_process.assert_not_awaited()
        initialize = json.loads(client.sent[0])
        self.assertEqual(initialize["method"], "initialize")
        self.assertEqual(initialize["id"], 1)
        self.assertEqual(
            initialize["params"]["clientInfo"]["name"],
            "codex_telegram_forum_bridge",
        )
        self.assertEqual(
            json.loads(client.sent[1]),
            {"method": "initialized", "params": {}},
        )

    async def test_websocket_send_uses_one_json_frame_without_newline(self) -> None:
        server = self.make_server()
        client = FakeUnixWebSocket()
        server._websocket = client

        await server.notify("example/event", {"text": "Привет"})

        self.assertEqual(len(client.sent), 1)
        self.assertFalse(client.sent[0].endswith("\n"))
        self.assertEqual(
            json.loads(client.sent[0]),
            {
                "method": "example/event",
                "params": {"text": "Привет"},
            },
        )

    async def test_tested_protocol_version_is_accepted(self) -> None:
        server = self.make_server(compatible_versions=("tested-version",))
        client = FakeUnixWebSocket(
            answer_requests=True,
            response_result={
                "serverInfo": {"version": "tested-version"},
            },
        )

        with patch.object(
            CodexAppServer,
            "_connect_unix_websocket",
            return_value=client,
        ):
            try:
                await server.start()
                self.assertEqual(server.server_version, "tested-version")
            finally:
                await server.stop()

    async def test_unadvertised_protocol_version_fails_closed(self) -> None:
        server = self.make_server(compatible_versions=("tested-version",))
        client = FakeUnixWebSocket(answer_requests=True)

        with (
            patch.object(
                CodexAppServer,
                "_connect_unix_websocket",
                return_value=client,
            ),
            self.assertRaises(CodexProtocolCompatibilityError),
        ):
            await server.start()

        self.assertFalse(server.is_connected)

    async def test_daemon_version_fallback_accepts_matching_shared_server(
        self,
    ) -> None:
        server = self.make_server(compatible_versions=("tested-version",))
        client = FakeUnixWebSocket(answer_requests=True)

        with (
            patch.object(
                CodexAppServer,
                "_connect_unix_websocket",
                return_value=client,
            ),
            patch(
                "codex_telegram_bridge.codex.subprocess.run",
                return_value=SimpleNamespace(
                    returncode=0,
                    stdout=json.dumps(
                        {
                            "cliVersion": "tested-version",
                            "appServerVersion": "tested-version",
                        }
                    ),
                ),
            ),
        ):
            try:
                await server.start()
                self.assertEqual(server.server_version, "tested-version")
            finally:
                await server.stop()

    async def test_unknown_protocol_version_fails_closed(self) -> None:
        server = self.make_server(compatible_versions=("tested-version",))
        client = FakeUnixWebSocket(
            answer_requests=True,
            response_result={
                "serverInfo": {"version": "future-version"},
            },
        )

        with (
            patch.object(
                CodexAppServer,
                "_connect_unix_websocket",
                return_value=client,
            ),
            self.assertRaises(CodexProtocolCompatibilityError),
        ):
            await server.start()

        self.assertFalse(server.is_connected)

    async def test_unarchive_thread_uses_documented_app_server_method(self) -> None:
        server = self.make_server()
        server._loaded_threads.add("thread-archive")
        server.request = AsyncMock(
            return_value={"thread": {"id": "thread-archive"}}
        )

        thread = await server.unarchive_thread("thread-archive")

        self.assertEqual(thread["id"], "thread-archive")
        server.request.assert_awaited_once_with(
            "thread/unarchive",
            {"threadId": "thread-archive"},
        )
        self.assertNotIn("thread-archive", server._loaded_threads)

    async def test_archive_thread_uses_documented_app_server_method(self) -> None:
        server = self.make_server()
        server._loaded_threads.add("thread-archive")
        server.request = AsyncMock(return_value={})

        result = await server.archive_thread("thread-archive")

        self.assertEqual(result, {})
        server.request.assert_awaited_once_with(
            "thread/archive",
            {"threadId": "thread-archive"},
        )
        self.assertNotIn("thread-archive", server._loaded_threads)

    async def test_read_rate_limits_uses_documented_account_method(self) -> None:
        server = self.make_server()
        response = {
            "rateLimits": {
                "primary": {
                    "usedPercent": 6,
                    "windowDurationMins": 10_080,
                }
            }
        }
        server.request = AsyncMock(return_value=response)

        self.assertEqual(await server.read_rate_limits(), response)

        server.request.assert_awaited_once_with(
            "account/rateLimits/read",
            {},
        )

    async def test_turn_start_preserves_native_audio_and_image_inputs(
        self,
    ) -> None:
        server = self.make_server()
        server.resume_thread = AsyncMock()
        server.request = AsyncMock(return_value={"turn": {"id": "turn-media"}})
        local_inputs = (
            LocalInput("localAudio", "/private/tmp/voice.mp3"),
            LocalInput(
                "localImage",
                "/private/tmp/frame.jpg",
                detail="low",
            ),
        )

        turn = await server.start_turn(
            thread_id="thread-media",
            text="Telegram video note",
            client_id="tg:-100:90",
            local_inputs=local_inputs,
        )

        self.assertEqual(turn["id"], "turn-media")
        server.request.assert_awaited_once_with(
            "turn/start",
            {
                "threadId": "thread-media",
                "input": [
                    {
                        "type": "text",
                        "text": "Telegram video note",
                        "text_elements": [],
                    },
                    {
                        "type": "localAudio",
                        "path": "/private/tmp/voice.mp3",
                    },
                    {
                        "type": "localImage",
                        "path": "/private/tmp/frame.jpg",
                        "detail": "low",
                    },
                ],
                "clientUserMessageId": "tg:-100:90",
            },
        )

    async def test_turn_start_preserves_mentioned_file_input(self) -> None:
        server = self.make_server()
        server.resume_thread = AsyncMock()
        server.request = AsyncMock(
            return_value={"turn": {"id": "turn-document"}}
        )
        document = LocalInput(
            "mention",
            "/private/tmp/statement.csv",
            name="statement.csv",
        )

        turn = await server.start_turn(
            thread_id="thread-document",
            text="Telegram document",
            client_id="tg:-100:92",
            local_inputs=(document,),
        )

        self.assertEqual(turn["id"], "turn-document")
        server.request.assert_awaited_once_with(
            "turn/start",
            {
                "threadId": "thread-document",
                "input": [
                    {
                        "type": "text",
                        "text": "Telegram document",
                        "text_elements": [],
                    },
                    {
                        "type": "mention",
                        "path": "/private/tmp/statement.csv",
                        "name": "statement.csv",
                    },
                ],
                "clientUserMessageId": "tg:-100:92",
            },
        )

    async def test_turn_steer_preserves_native_audio_input(self) -> None:
        server = self.make_server()
        server.request = AsyncMock(return_value={"turnId": "turn-active"})

        turn_id = await server.steer_turn(
            thread_id="thread-media",
            turn_id="turn-active",
            text="Telegram voice",
            client_id="tg:-100:91",
            local_inputs=(
                LocalInput("localAudio", "/private/tmp/voice.mp3"),
            ),
        )

        self.assertEqual(turn_id, "turn-active")
        server.request.assert_awaited_once_with(
            "turn/steer",
            {
                "threadId": "thread-media",
                "expectedTurnId": "turn-active",
                "input": [
                    {
                        "type": "text",
                        "text": "Telegram voice",
                        "text_elements": [],
                    },
                    {
                        "type": "localAudio",
                        "path": "/private/tmp/voice.mp3",
                    },
                ],
                "clientUserMessageId": "tg:-100:91",
            },
        )

    async def test_paginated_thread_history_uses_turns_list(self) -> None:
        server = self.make_server()
        server.request = AsyncMock(
            side_effect=[
                CodexProtocolError(
                    "thread/read: paginated history rejects includeTurns"
                ),
                {"thread": {"id": "thread-paged", "historyMode": "paginated"}},
                {
                    "data": [{"id": "turn-1"}],
                    "nextCursor": "next",
                },
                {
                    "data": [{"id": "turn-2"}],
                    "nextCursor": None,
                },
            ]
        )

        thread = await server.read_thread("thread-paged")

        self.assertEqual(
            [turn["id"] for turn in thread["turns"]],
            ["turn-1", "turn-2"],
        )
        self.assertEqual(
            server.request.await_args_list[-2:],
            [
                unittest.mock.call(
                    "thread/turns/list",
                    {
                        "threadId": "thread-paged",
                        "limit": 100,
                        "itemsView": "full",
                    },
                ),
                unittest.mock.call(
                    "thread/turns/list",
                    {
                        "threadId": "thread-paged",
                        "limit": 100,
                        "itemsView": "full",
                        "cursor": "next",
                    },
                ),
            ],
        )

    async def test_unarchived_thread_is_resumed_again(self) -> None:
        server = self.make_server()
        server._loaded_threads.add("thread-archive")
        server.request = AsyncMock(
            side_effect=[
                {"thread": {"id": "thread-archive"}},
                {},
            ]
        )

        await server.unarchive_thread("thread-archive")
        await server.resume_thread("thread-archive")

        self.assertEqual(
            server.request.await_args_list,
            [
                unittest.mock.call(
                    "thread/unarchive",
                    {"threadId": "thread-archive"},
                ),
                unittest.mock.call(
                    "thread/resume",
                    {"threadId": "thread-archive"},
                ),
            ],
        )

    async def test_resume_caches_native_thread_mode_settings(self) -> None:
        server = self.make_server()
        server.request = AsyncMock(
            return_value={
                "model": "gpt-5.6-sol",
                "reasoningEffort": "xhigh",
                "serviceTier": "default",
            }
        )

        first = await server.resume_thread("thread-mode")
        second = await server.resume_thread("thread-mode")

        self.assertEqual(
            first,
            {
                "model": "gpt-5.6-sol",
                "effort": "xhigh",
                "serviceTier": "default",
            },
        )
        self.assertEqual(second, first)
        server.request.assert_awaited_once_with(
            "thread/resume",
            {"threadId": "thread-mode"},
        )

    async def test_update_thread_settings_uses_native_sticky_method(self) -> None:
        server = self.make_server()
        server._loaded_threads.add("thread-mode")
        server.request = AsyncMock(
            side_effect=[
                {},
                {
                    "model": "gpt-5.6-sol",
                    "reasoningEffort": "high",
                    "serviceTier": "default",
                },
            ]
        )

        settings = await server.update_thread_settings(
            thread_id="thread-mode",
            effort="high",
            service_tier=None,
            update_effort=True,
            update_service_tier=True,
        )

        self.assertEqual(settings["effort"], "high")
        self.assertEqual(settings["serviceTier"], "default")
        self.assertEqual(
            server.request.await_args_list,
            [
                unittest.mock.call(
                    "thread/settings/update",
                    {
                        "threadId": "thread-mode",
                        "effort": "high",
                        "serviceTier": None,
                    },
                ),
                unittest.mock.call(
                    "thread/resume",
                    {"threadId": "thread-mode"},
                ),
            ],
        )

    async def test_model_list_paginates_the_native_catalog(self) -> None:
        server = self.make_server()
        server.request = AsyncMock(
            side_effect=[
                {
                    "data": [{"id": "model-a"}],
                    "nextCursor": "next",
                },
                {
                    "data": [{"id": "model-b"}],
                    "nextCursor": None,
                },
            ]
        )

        models = await server.list_models()

        self.assertEqual(
            [model["id"] for model in models],
            ["model-a", "model-b"],
        )
        self.assertEqual(
            server.request.await_args_list,
            [
                unittest.mock.call(
                    "model/list",
                    {"limit": 100, "includeHidden": False},
                ),
                unittest.mock.call(
                    "model/list",
                    {
                        "limit": 100,
                        "includeHidden": False,
                        "cursor": "next",
                    },
                ),
            ],
        )

    async def test_stop_closes_client_without_terminating_shared_server(
        self,
    ) -> None:
        server = self.make_server()
        client = FakeUnixWebSocket()
        server._websocket = client

        await server.stop()

        self.assertEqual(client.shutdown_calls, 0)
        self.assertEqual(client.close_calls, 1)
        self.assertIsNone(server._websocket)
        self.assertIsNone(server.process)

    async def test_failed_send_cleans_pending_request_and_aborts_socket(
        self,
    ) -> None:
        server = self.make_server()
        client = FakeUnixWebSocket(
            send_error=websocket.WebSocketConnectionClosedException(),
        )
        server._websocket = client

        with self.assertRaises(CodexProtocolError):
            await server.request("thread/list", {})

        self.assertEqual(server._pending, {})
        self.assertEqual(client.shutdown_calls, 1)

    async def test_stalled_send_times_out_cleans_pending_and_aborts_socket(
        self,
    ) -> None:
        server = self.make_server()
        client = BlockingSendWebSocket()
        server._websocket = client

        with (
            patch(
                "codex_telegram_bridge.codex.APP_SERVER_IO_TIMEOUT",
                0.01,
            ),
            self.assertRaisesRegex(CodexProtocolError, "timed out sending"),
        ):
            await server.request("thread/list", {}, timeout=0.1)

        self.assertEqual(server._pending, {})
        self.assertEqual(client.shutdown_calls, 1)

    async def test_response_timeout_aborts_socket_and_cleans_pending(
        self,
    ) -> None:
        server = self.make_server()
        client = FakeUnixWebSocket()
        server._websocket = client
        send = AsyncMock()

        with (
            patch.object(server, "_send", send),
            self.assertRaisesRegex(
                CodexProtocolError,
                "timed out waiting",
            ),
        ):
            await server.request("thread/list", {}, timeout=0.01)

        send.assert_awaited_once()
        self.assertEqual(server._pending, {})
        self.assertEqual(client.shutdown_calls, 1)

    async def test_events_for_one_thread_are_dispatched_in_frame_order(
        self,
    ) -> None:
        seen: list[str] = []
        release_first = asyncio.Event()

        async def first() -> None:
            await release_first.wait()
            seen.append("first")

        async def second() -> None:
            seen.append("second")

        server = self.make_server()
        server._spawn_ordered_dispatch(
            {"params": {"threadId": "thread-1"}},
            lambda: first(),
        )
        server._spawn_ordered_dispatch(
            {"params": {"threadId": "thread-1"}},
            lambda: second(),
        )
        await asyncio.sleep(0)
        self.assertEqual(seen, [])

        release_first.set()
        await asyncio.gather(*tuple(server._background_tasks))

        self.assertEqual(seen, ["first", "second"])


class CodexChildLifecycleTests(unittest.IsolatedAsyncioTestCase):
    def make_server(self) -> CodexAppServer:
        return CodexAppServer(
            codex_binary="/not/started",
            cwd="/tmp",
            on_notification=ignore_message,
            on_server_request=ignore_message,
        )

    async def test_reader_eof_fails_every_pending_request(self) -> None:
        server = self.make_server()
        server.process = SimpleNamespace(stdout=EndOfFileStream())
        loop = asyncio.get_running_loop()
        first = loop.create_future()
        second = loop.create_future()
        server._pending = {10: first, 11: second}

        await server._reader_loop()

        for future in (first, second):
            with self.assertRaisesRegex(CodexProtocolError, "stopped"):
                await future

    async def test_wait_closed_raises_after_unexpected_reader_exit(self) -> None:
        server = self.make_server()
        server._reader_task = asyncio.create_task(asyncio.sleep(0))

        with self.assertRaises(CodexProtocolError):
            await server.wait_closed()

    async def test_start_allows_app_server_frames_of_at_least_one_megabyte(
        self,
    ) -> None:
        server = self.make_server()
        child = FakeChildProcess()
        server.request = AsyncMock(return_value={})
        server.notify = AsyncMock()

        with patch(
            "codex_telegram_bridge.codex.asyncio.create_subprocess_exec",
            new=AsyncMock(return_value=child),
        ) as create_process:
            try:
                await server.start()

                create_process.assert_awaited_once()
                _, kwargs = create_process.await_args
                self.assertIn("limit", kwargs)
                self.assertGreaterEqual(kwargs["limit"], 1024 * 1024)
                server.request.assert_awaited_once()
                initialize_call = server.request.await_args
                self.assertEqual(initialize_call.args[0], "initialize")
                server.notify.assert_awaited_once_with("initialized", {})
            finally:
                await server.stop()


if __name__ == "__main__":
    unittest.main()
