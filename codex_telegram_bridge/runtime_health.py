from __future__ import annotations

import json
import os
import stat
import time
from pathlib import Path
from typing import Any


TELEGRAM_UPDATE_HEALTH_FILE = "telegram-update-health.json"
TELEGRAM_UPDATE_STALE_SECONDS = 120.0
TELEGRAM_UPDATE_LOCAL_FAILURES = 3


def telegram_update_health_path(state_dir: Path) -> Path:
    return state_dir / TELEGRAM_UPDATE_HEALTH_FILE


def write_telegram_update_health(
    state_dir: Path,
    payload: dict[str, Any],
) -> None:
    state_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    path = telegram_update_health_path(state_dir)
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    encoded = json.dumps(
        payload,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )
    try:
        temporary.write_text(encoded + "\n", encoding="ascii")
        os.chmod(temporary, 0o600)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _process_is_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except (OSError, ValueError):
        return False
    return True


def read_telegram_update_health(
    state_dir: Path,
    *,
    now: float | None = None,
) -> dict[str, Any]:
    path = telegram_update_health_path(state_dir)
    result: dict[str, Any] = {
        "telegramUpdateLoopObserved": False,
        "telegramUpdateLoopHealthy": None,
        "telegramUpdateLoopLocalFault": False,
        "telegramUpdateLastSuccessAgeSeconds": None,
        "telegramUpdateLastErrorKind": None,
        "telegramUpdateLastErrorType": None,
        "telegramUpdateConsecutiveFailures": 0,
    }
    try:
        status = path.lstat()
        if (
            path.is_symlink()
            or not stat.S_ISREG(status.st_mode)
            or status.st_uid != os.getuid()
            or stat.S_IMODE(status.st_mode) & 0o077
        ):
            return result
        payload = json.loads(path.read_text(encoding="ascii"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return result
    if not isinstance(payload, dict):
        return result
    try:
        pid = int(payload.get("pid") or 0)
        started_at = float(payload.get("startedAt") or 0)
        last_success_raw = payload.get("lastSuccessAt")
        last_success = (
            None if last_success_raw is None else float(last_success_raw)
        )
        failures = max(0, int(payload.get("consecutiveFailures") or 0))
    except (TypeError, ValueError):
        return result
    if not _process_is_alive(pid):
        return result
    current = time.time() if now is None else float(now)
    reference = last_success if last_success is not None else started_at
    age = max(0.0, current - reference) if reference > 0 else None
    error_kind = str(payload.get("lastErrorKind") or "") or None
    error_type = str(payload.get("lastErrorType") or "") or None
    healthy = bool(
        age is not None
        and age <= TELEGRAM_UPDATE_STALE_SECONDS
        and failures < TELEGRAM_UPDATE_LOCAL_FAILURES
    )
    local_fault = bool(
        not healthy
        and failures >= TELEGRAM_UPDATE_LOCAL_FAILURES
        and error_kind == "unexpected"
    )
    result.update(
        {
            "telegramUpdateLoopObserved": True,
            "telegramUpdateLoopHealthy": healthy,
            "telegramUpdateLoopLocalFault": local_fault,
            "telegramUpdateLastSuccessAgeSeconds": (
                None if age is None else round(age, 1)
            ),
            "telegramUpdateLastErrorKind": error_kind,
            "telegramUpdateLastErrorType": error_type,
            "telegramUpdateConsecutiveFailures": failures,
        }
    )
    return result
