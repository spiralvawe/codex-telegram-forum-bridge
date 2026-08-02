from __future__ import annotations

import asyncio
import os
import socket
from collections.abc import Callable, Mapping


class SystemdNotifier:
    def __init__(
        self,
        *,
        socket_address: str | None,
        watchdog_usec: int | None,
    ) -> None:
        self.socket_address = socket_address
        self.watchdog_usec = watchdog_usec

    @classmethod
    def from_environment(
        cls,
        environment: Mapping[str, str] | None = None,
    ) -> SystemdNotifier:
        current = os.environ if environment is None else environment
        socket_address = current.get("NOTIFY_SOCKET") or None
        watchdog_pid = current.get("WATCHDOG_PID")
        if watchdog_pid:
            try:
                if int(watchdog_pid) != os.getpid():
                    return cls(
                        socket_address=socket_address,
                        watchdog_usec=None,
                    )
            except ValueError:
                return cls(
                    socket_address=socket_address,
                    watchdog_usec=None,
                )
        try:
            watchdog_usec = int(current.get("WATCHDOG_USEC") or "0")
        except ValueError:
            watchdog_usec = 0
        return cls(
            socket_address=socket_address,
            watchdog_usec=watchdog_usec if watchdog_usec > 0 else None,
        )

    @property
    def watchdog_interval_seconds(self) -> float | None:
        if self.watchdog_usec is None:
            return None
        return max(1.0, self.watchdog_usec / 3_000_000)

    def notify(self, message: str) -> bool:
        if not self.socket_address:
            return False
        address = self.socket_address
        if address.startswith("@"):
            address = "\0" + address[1:]
        try:
            with socket.socket(
                socket.AF_UNIX,
                socket.SOCK_DGRAM | getattr(socket, "SOCK_CLOEXEC", 0),
            ) as connection:
                connection.connect(address)
                connection.sendall(message.encode("utf-8"))
        except OSError:
            return False
        return True

    async def watchdog_loop(
        self,
        healthy: Callable[[], bool] | None = None,
    ) -> None:
        interval = self.watchdog_interval_seconds
        if interval is None:
            return
        while True:
            await asyncio.sleep(interval)
            if healthy is None or healthy():
                self.notify("WATCHDOG=1")
