"""Type conversion helpers for FinMind payloads."""

from __future__ import annotations

from typing import Any


def as_text(raw: Any) -> str | None:
    text = "" if raw is None else str(raw).strip()
    return text or None


def as_float(raw: Any) -> float | None:
    text = as_text(raw)
    if not text:
        return None
    return float(text)


def as_int(raw: Any) -> int | None:
    text = as_text(raw)
    if not text:
        return None
    return int(float(text))
