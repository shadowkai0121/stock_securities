"""Date parsing and validation utilities."""

from __future__ import annotations

from datetime import date, datetime


ISO_DATE_FMT = "%Y-%m-%d"


def parse_iso_date(value: str, option_name: str) -> date:
    try:
        return datetime.strptime(value, ISO_DATE_FMT).date()
    except ValueError as exc:
        raise ValueError(f"Invalid {option_name} '{value}'. Use YYYY-MM-DD.") from exc


def ensure_date_range(start: date, end: date, *, start_name: str, end_name: str) -> None:
    if end < start:
        raise ValueError(f"{end_name} must be greater than or equal to {start_name}.")


def to_iso(value: date) -> str:
    return value.strftime(ISO_DATE_FMT)
