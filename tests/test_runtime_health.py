from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from codex_telegram_bridge.runtime_health import (
    read_telegram_update_health,
    write_telegram_update_health,
)


class RuntimeHealthTests(unittest.TestCase):
    def _write(self, state_dir: Path, **overrides: object) -> None:
        payload: dict[str, object] = {
            "pid": os.getpid(),
            "startedAt": 900.0,
            "lastSuccessAt": 990.0,
            "lastErrorAt": None,
            "lastErrorKind": None,
            "lastErrorType": None,
            "consecutiveFailures": 0,
            "updatedAt": 990.0,
        }
        payload.update(overrides)
        write_telegram_update_health(state_dir, payload)

    def test_recent_live_poll_is_healthy(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_dir = Path(directory)
            self._write(state_dir)

            health = read_telegram_update_health(state_dir, now=1000.0)

        self.assertTrue(health["telegramUpdateLoopObserved"])
        self.assertTrue(health["telegramUpdateLoopHealthy"])
        self.assertEqual(health["telegramUpdateLastSuccessAgeSeconds"], 10.0)

    def test_stale_unexpected_failures_are_a_local_fault(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_dir = Path(directory)
            self._write(
                state_dir,
                lastSuccessAt=800.0,
                lastErrorKind="unexpected",
                lastErrorType="RemoteDisconnected",
                consecutiveFailures=4,
            )

            health = read_telegram_update_health(state_dir, now=1000.0)

        self.assertFalse(health["telegramUpdateLoopHealthy"])
        self.assertTrue(health["telegramUpdateLoopLocalFault"])

    def test_external_network_failure_is_unhealthy_but_not_local_fault(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_dir = Path(directory)
            self._write(
                state_dir,
                lastSuccessAt=800.0,
                lastErrorKind="network_error",
                lastErrorType="URLError",
                consecutiveFailures=4,
            )

            health = read_telegram_update_health(state_dir, now=1000.0)

        self.assertFalse(health["telegramUpdateLoopHealthy"])
        self.assertFalse(health["telegramUpdateLoopLocalFault"])

    def test_dead_process_health_is_ignored(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_dir = Path(directory)
            self._write(state_dir, pid=999999)
            with patch(
                "codex_telegram_bridge.runtime_health._process_is_alive",
                return_value=False,
            ):
                health = read_telegram_update_health(state_dir, now=1000.0)

        self.assertFalse(health["telegramUpdateLoopObserved"])
        self.assertIsNone(health["telegramUpdateLoopHealthy"])


if __name__ == "__main__":
    unittest.main()
