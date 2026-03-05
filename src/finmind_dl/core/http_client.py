"""HTTP client wrappers for FinMind APIs."""

from __future__ import annotations

import json
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen

API_URL = "https://api.finmindtrade.com/api/v4/data"
BROKER_REPORT_URL = "https://api.finmindtrade.com/api/v4/taiwan_stock_trading_daily_report"


class APIError(RuntimeError):
    """Raised when FinMind API request or response is invalid."""


def _load_payload(url: str, params: dict[str, str], *, timeout: int = 60) -> dict[str, Any]:
    request_url = f"{url}?{urlencode(params)}"
    try:
        with urlopen(request_url, timeout=timeout) as response:
            payload = json.load(response)
    except HTTPError as exc:
        detail = ""
        try:
            detail = exc.read().decode("utf-8", errors="ignore")
        except Exception:
            detail = ""
        detail = detail.strip()
        if detail:
            raise APIError(f"HTTP error {exc.code}: {exc.reason}. {detail[:200]}") from exc
        raise APIError(f"HTTP error {exc.code}: {exc.reason}") from exc
    except URLError as exc:
        raise APIError(f"Network error: {exc.reason}") from exc

    if not isinstance(payload, dict):
        raise APIError("Invalid FinMind response payload type.")
    return payload


def _extract_data(payload: dict[str, Any]) -> list[dict[str, Any]]:
    data = payload.get("data")
    if isinstance(data, list):
        return [row for row in data if isinstance(row, dict)]
    message = payload.get("msg") or payload.get("message") or "Unknown API response"
    raise APIError(f"Invalid FinMind response: {message}")


def fetch_dataset(dataset: str, token: str, params: dict[str, str] | None = None) -> list[dict[str, Any]]:
    query_params: dict[str, str] = {"dataset": dataset, "token": token}
    if params:
        query_params.update({k: v for k, v in params.items() if v is not None})
    payload = _load_payload(API_URL, query_params)
    return _extract_data(payload)


def fetch_trading_daily_report(token: str, params: dict[str, str]) -> list[dict[str, Any]]:
    query_params: dict[str, str] = {"token": token}
    query_params.update({k: v for k, v in params.items() if v is not None})
    payload = _load_payload(BROKER_REPORT_URL, query_params)
    return _extract_data(payload)
