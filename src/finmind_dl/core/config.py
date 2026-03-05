"""Configuration and token resolution utilities."""

from __future__ import annotations

import os
from pathlib import Path


def load_env_file(path: Path | None = None) -> dict[str, str]:
    env_path = path or Path(".env")
    if not env_path.exists():
        return {}

    env_map: dict[str, str] = {}
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        env_map[key.strip()] = value.strip().strip("'").strip('"')
    return env_map


def resolve_token(cli_token: str | None, env_path: Path | None = None) -> str:
    token = (cli_token or "").strip()
    if token:
        return token

    env_map = load_env_file(env_path)
    token = (
        os.getenv("FINMIND_SPONSOR_API_KEY")
        or os.getenv("FINMIND_TOKEN")
        or env_map.get("FINMIND_SPONSOR_API_KEY")
        or env_map.get("FINMIND_TOKEN")
        or ""
    ).strip()

    if token:
        return token

    raise ValueError(
        "Missing FinMind token. Set --token or FINMIND_SPONSOR_API_KEY/FINMIND_TOKEN in env/.env."
    )
