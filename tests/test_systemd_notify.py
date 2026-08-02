from __future__ import annotations

import asyncio
import os
import socket
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from codex_telegram_bridge.systemd_notify import SystemdNotifier


class SystemdNotifierTests(unittest.IsolatedAsyncioTestCase):
    def test_environment_parsing_is_bounded_and_pid_scoped(self) -> None:
        notifier = SystemdNotifier.from_environment(
            {
                "NOTIFY_SOCKET": "/run/user/1000/systemd/notify",
                "WATCHDOG_USEC": "120000000",
                "WATCHDOG_PID": str(os.getpid()),
            }
        )

        self.assertEqual(
            notifier.socket_address,
            "/run/user/1000/systemd/notify",
        )
        self.assertEqual(notifier.watchdog_interval_seconds, 40.0)

        wrong_pid = SystemdNotifier.from_environment(
            {
                "NOTIFY_SOCKET": "/run/user/1000/systemd/notify",
                "WATCHDOG_USEC": "120000000",
                "WATCHDOG_PID": str(os.getpid() + 1),
            }
        )
        self.assertIsNone(wrong_pid.watchdog_interval_seconds)

        malformed = SystemdNotifier.from_environment(
            {
                "NOTIFY_SOCKET": "/run/user/1000/systemd/notify",
                "WATCHDOG_USEC": "not-an-integer",
            }
        )
        self.assertIsNone(malformed.watchdog_interval_seconds)

    def test_notify_delivers_one_datagram_and_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            socket_path = Path(directory) / "notify.sock"
            receiver = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
            receiver.bind(str(socket_path))
            receiver.settimeout(1)
            try:
                notifier = SystemdNotifier(
                    socket_address=str(socket_path),
                    watchdog_usec=None,
                )
                self.assertTrue(notifier.notify("READY=1"))
                self.assertEqual(receiver.recv(128), b"READY=1")
            finally:
                receiver.close()

        missing = SystemdNotifier(
            socket_address=str(socket_path),
            watchdog_usec=None,
        )
        self.assertFalse(missing.notify("READY=1"))

    async def test_watchdog_sends_after_each_scheduled_interval(self) -> None:
        notifier = SystemdNotifier(
            socket_address="/unused",
            watchdog_usec=3_000_000,
        )
        sent = asyncio.Event()

        def record(message: str) -> bool:
            self.assertEqual(message, "WATCHDOG=1")
            sent.set()
            return True

        with mock.patch.object(notifier, "notify", side_effect=record):
            task = asyncio.create_task(notifier.watchdog_loop())
            try:
                await asyncio.wait_for(sent.wait(), timeout=2)
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    async def test_watchdog_suppresses_heartbeat_when_unhealthy(self) -> None:
        notifier = SystemdNotifier(
            socket_address="/unused",
            watchdog_usec=3_000_000,
        )
        checks = iter([False, True])
        sent = asyncio.Event()

        def record(_: str) -> bool:
            sent.set()
            return True

        with mock.patch.object(notifier, "notify", side_effect=record) as notify:
            task = asyncio.create_task(
                notifier.watchdog_loop(lambda: next(checks))
            )
            try:
                await asyncio.wait_for(sent.wait(), timeout=3)
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

        notify.assert_called_once_with("WATCHDOG=1")


if __name__ == "__main__":
    unittest.main()
