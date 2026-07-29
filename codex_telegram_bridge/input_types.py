from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable


LOCAL_INPUT_TYPES = frozenset({"localAudio", "localImage", "mention"})
IMAGE_DETAILS = frozenset({"auto", "low", "high", "original"})
MAX_LOCAL_INPUTS = 4


@dataclass(frozen=True)
class LocalInput:
    input_type: str
    path: str
    detail: str | None = None
    name: str | None = None

    def __post_init__(self) -> None:
        if self.input_type not in LOCAL_INPUT_TYPES:
            raise ValueError("unsupported local input type")
        if not self.path:
            raise ValueError("local input path is required")
        if self.input_type != "localImage" and self.detail is not None:
            raise ValueError("only image input supports detail")
        if self.detail is not None and self.detail not in IMAGE_DETAILS:
            raise ValueError("unsupported image detail")
        if self.input_type == "mention":
            if not self.name:
                raise ValueError("mentioned file name is required")
        elif self.name is not None:
            raise ValueError("only mentioned file input supports name")

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "type": self.input_type,
            "path": self.path,
        }
        if self.detail is not None:
            payload["detail"] = self.detail
        if self.name is not None:
            payload["name"] = self.name
        return payload

    @classmethod
    def from_payload(cls, value: dict[str, Any]) -> "LocalInput":
        if not isinstance(value, dict):
            raise ValueError("local input must be an object")
        return cls(
            input_type=str(value.get("type") or ""),
            path=str(value.get("path") or ""),
            detail=(
                None
                if value.get("detail") is None
                else str(value.get("detail"))
            ),
            name=(
                None
                if value.get("name") is None
                else str(value.get("name"))
            ),
        )


def normalize_local_inputs(
    values: Iterable[LocalInput | dict[str, Any]] | None,
) -> tuple[LocalInput, ...]:
    normalized: list[LocalInput] = []
    for value in values or ():
        normalized.append(
            value if isinstance(value, LocalInput) else LocalInput.from_payload(value)
        )
    if len(normalized) > MAX_LOCAL_INPUTS:
        raise ValueError("too many local inputs")
    audio_count = sum(
        item.input_type == "localAudio" for item in normalized
    )
    if audio_count > 1:
        raise ValueError("only one local audio input is supported")
    return tuple(normalized)
