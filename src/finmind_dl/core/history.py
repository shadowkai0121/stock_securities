"""Run history utilities for meta_runs."""

from __future__ import annotations

import argparse
import json
import sqlite3
import uuid
from pathlib import Path
from typing import Any

from finmind_dl.schema import init_schema


def new_run_id() -> str:
    return uuid.uuid4().hex


def build_requested_params(args: argparse.Namespace) -> str:
    data: dict[str, Any] = dict(vars(args))
    data.pop("handler", None)
    if data.get("token"):
        data["token"] = "***"
    return json.dumps(data, ensure_ascii=False, sort_keys=True)


def write_meta_run(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    dataset: str,
    stock_id: str,
    query_mode: str,
    start_date: str | None,
    end_date: str | None,
    requested_params_json: str,
    fetched_rows: int,
    inserted_rows: int,
    status: str,
    error_message: str | None,
) -> None:
    init_schema(conn)
    conn.execute(
        """
        INSERT INTO meta_runs (
            run_id, dataset, stock_id, query_mode, start_date, end_date,
            requested_params_json, fetched_rows, inserted_rows, status, error_message
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            dataset,
            stock_id,
            query_mode,
            start_date,
            end_date,
            requested_params_json,
            fetched_rows,
            inserted_rows,
            status,
            error_message,
        ),
    )


def try_log_meta_run(
    db_path: Path | None,
    *,
    run_id: str,
    dataset: str,
    stock_id: str,
    query_mode: str,
    start_date: str | None,
    end_date: str | None,
    requested_params_json: str,
    fetched_rows: int,
    inserted_rows: int,
    status: str,
    error_message: str | None,
) -> None:
    if db_path is None:
        return

    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        write_meta_run(
            conn,
            run_id=run_id,
            dataset=dataset,
            stock_id=stock_id,
            query_mode=query_mode,
            start_date=start_date,
            end_date=end_date,
            requested_params_json=requested_params_json,
            fetched_rows=fetched_rows,
            inserted_rows=inserted_rows,
            status=status,
            error_message=(error_message or "")[:2000] or None,
        )
        conn.commit()
    except Exception:
        # Meta logging should never break command result handling.
        pass
    finally:
        conn.close()
