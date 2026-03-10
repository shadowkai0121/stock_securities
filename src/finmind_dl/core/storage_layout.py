"""Storage layout enforcement and one-time migrations."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from finmind_dl.schema import init_market_schema, init_meta_schema, init_stock_schema


LEGACY_MARKET_FILENAMES = ("stock_info.sqlite", "holding_shares_per.sqlite")


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?",
        (table,),
    ).fetchone()
    return row is not None


def _list_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f'PRAGMA table_info("{table}")').fetchall()
    return {str(row[1]) for row in rows}


def _validate_single_id_column(
    conn: sqlite3.Connection,
    *,
    table: str,
    column: str,
    expected_stock_id: str,
) -> None:
    ids = [
        str(row[0]).strip()
        for row in conn.execute(
            f'SELECT DISTINCT "{column}" FROM "{table}" WHERE "{column}" IS NOT NULL'
        ).fetchall()
        if str(row[0]).strip()
    ]
    if not ids:
        return
    if len(ids) > 1:
        raise ValueError(
            f"{table} in this DB contains multiple ids ({', '.join(sorted(ids)[:5])}); "
            "per-stock DB must contain only one id."
        )
    if ids[0] != expected_stock_id:
        raise ValueError(
            f"{table} contains id {ids[0]} but expected {expected_stock_id}. "
            "Per-stock DB identity mismatch."
        )


def _migrate_price_like_table(conn: sqlite3.Connection, *, table: str, expected_stock_id: str) -> None:
    if not _table_exists(conn, table):
        return
    cols = _list_columns(conn, table)
    if "stock_id" not in cols:
        return

    _validate_single_id_column(
        conn,
        table=table,
        column="stock_id",
        expected_stock_id=expected_stock_id,
    )
    conn.execute(f'DROP TABLE IF EXISTS "__new_{table}"')
    conn.execute(
        f"""
        CREATE TABLE "__new_{table}" (
            date TEXT NOT NULL,
            open REAL,
            max REAL,
            min REAL,
            close REAL,
            trading_volume INTEGER,
            trading_money INTEGER,
            spread REAL,
            trading_turnover INTEGER,
            is_placeholder INTEGER NOT NULL DEFAULT 0,
            inserted_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE (date)
        )
        """
    )
    conn.execute(
        f"""
        INSERT INTO "__new_{table}" (
            date, open, max, min, close, trading_volume, trading_money, spread,
            trading_turnover, is_placeholder, inserted_at
        )
        SELECT
            date,
            open,
            max,
            min,
            close,
            trading_volume,
            trading_money,
            spread,
            trading_turnover,
            COALESCE(is_placeholder, 0),
            COALESCE(inserted_at, datetime('now'))
        FROM "{table}"
        """
    )
    conn.execute(f'DROP TABLE "{table}"')
    conn.execute(f'ALTER TABLE "__new_{table}" RENAME TO "{table}"')
    conn.execute(f'CREATE INDEX IF NOT EXISTS idx_{table}_date ON "{table}" (date)')


def _migrate_margin_table(conn: sqlite3.Connection, *, expected_stock_id: str) -> None:
    table = "margin_daily"
    if not _table_exists(conn, table):
        return
    cols = _list_columns(conn, table)
    if "stock_id" not in cols:
        return

    _validate_single_id_column(
        conn,
        table=table,
        column="stock_id",
        expected_stock_id=expected_stock_id,
    )
    conn.execute(f'DROP TABLE IF EXISTS "__new_{table}"')
    conn.execute(
        """
        CREATE TABLE "__new_margin_daily" (
            date TEXT NOT NULL,
            margin_purchase_buy INTEGER,
            margin_purchase_cash_repayment INTEGER,
            margin_purchase_limit INTEGER,
            margin_purchase_sell INTEGER,
            margin_purchase_today_balance INTEGER,
            margin_purchase_yesterday_balance INTEGER,
            offset_loan_and_short INTEGER,
            short_sale_buy INTEGER,
            short_sale_cash_repayment INTEGER,
            short_sale_limit INTEGER,
            short_sale_sell INTEGER,
            short_sale_today_balance INTEGER,
            short_sale_yesterday_balance INTEGER,
            note TEXT,
            is_placeholder INTEGER NOT NULL DEFAULT 0,
            inserted_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE (date)
        )
        """
    )
    conn.execute(
        """
        INSERT INTO "__new_margin_daily" (
            date,
            margin_purchase_buy,
            margin_purchase_cash_repayment,
            margin_purchase_limit,
            margin_purchase_sell,
            margin_purchase_today_balance,
            margin_purchase_yesterday_balance,
            offset_loan_and_short,
            short_sale_buy,
            short_sale_cash_repayment,
            short_sale_limit,
            short_sale_sell,
            short_sale_today_balance,
            short_sale_yesterday_balance,
            note,
            is_placeholder,
            inserted_at
        )
        SELECT
            date,
            margin_purchase_buy,
            margin_purchase_cash_repayment,
            margin_purchase_limit,
            margin_purchase_sell,
            margin_purchase_today_balance,
            margin_purchase_yesterday_balance,
            offset_loan_and_short,
            short_sale_buy,
            short_sale_cash_repayment,
            short_sale_limit,
            short_sale_sell,
            short_sale_today_balance,
            short_sale_yesterday_balance,
            note,
            COALESCE(is_placeholder, 0),
            COALESCE(inserted_at, datetime('now'))
        FROM "margin_daily"
        """
    )
    conn.execute('DROP TABLE "margin_daily"')
    conn.execute('ALTER TABLE "__new_margin_daily" RENAME TO "margin_daily"')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_margin_daily_date ON "margin_daily" (date)')


def _migrate_broker_table(conn: sqlite3.Connection, *, expected_stock_id: str) -> None:
    table = "broker_trades"
    if not _table_exists(conn, table):
        return
    cols = _list_columns(conn, table)
    if "stock_id" not in cols:
        return

    _validate_single_id_column(
        conn,
        table=table,
        column="stock_id",
        expected_stock_id=expected_stock_id,
    )
    conn.execute('DROP TABLE IF EXISTS "__new_broker_trades"')
    conn.execute(
        """
        CREATE TABLE "__new_broker_trades" (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            broker_id TEXT NOT NULL,
            broker_name TEXT NOT NULL,
            price REAL,
            buy REAL,
            sell REAL,
            is_placeholder INTEGER NOT NULL DEFAULT 0,
            inserted_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE (date, broker_id, broker_name, price, buy, sell, is_placeholder)
        )
        """
    )
    conn.execute(
        """
        INSERT INTO "__new_broker_trades" (
            date, broker_id, broker_name, price, buy, sell, is_placeholder, inserted_at
        )
        SELECT
            date,
            broker_id,
            broker_name,
            price,
            buy,
            sell,
            COALESCE(is_placeholder, 0),
            COALESCE(inserted_at, datetime('now'))
        FROM "broker_trades"
        """
    )
    conn.execute('DROP TABLE "broker_trades"')
    conn.execute('ALTER TABLE "__new_broker_trades" RENAME TO "broker_trades"')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_broker_trades_date ON "broker_trades" (date)')
    conn.execute(
        'CREATE INDEX IF NOT EXISTS idx_broker_trades_broker_date ON "broker_trades" (broker_id, date)'
    )


def _migrate_warrant_table(conn: sqlite3.Connection, *, expected_stock_id: str) -> None:
    table = "warrant_summary"
    if not _table_exists(conn, table):
        return
    cols = _list_columns(conn, table)
    if "target_stock_id" not in cols:
        return

    _validate_single_id_column(
        conn,
        table=table,
        column="target_stock_id",
        expected_stock_id=expected_stock_id,
    )
    conn.execute('DROP TABLE IF EXISTS "__new_warrant_summary"')
    conn.execute(
        """
        CREATE TABLE "__new_warrant_summary" (
            warrant_stock_id TEXT NOT NULL,
            warrant_type TEXT,
            date TEXT,
            end_date TEXT,
            exercise_ratio REAL,
            fulfillment_price REAL,
            fulfillment_method TEXT,
            fulfillment_start_date TEXT,
            fulfillment_end_date TEXT,
            close REAL,
            target_close REAL,
            inserted_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE (warrant_stock_id, date)
        )
        """
    )
    conn.execute(
        """
        INSERT INTO "__new_warrant_summary" (
            warrant_stock_id, warrant_type, date, end_date, exercise_ratio, fulfillment_price,
            fulfillment_method, fulfillment_start_date, fulfillment_end_date, close, target_close,
            inserted_at
        )
        SELECT
            warrant_stock_id,
            warrant_type,
            date,
            end_date,
            exercise_ratio,
            fulfillment_price,
            fulfillment_method,
            fulfillment_start_date,
            fulfillment_end_date,
            close,
            target_close,
            COALESCE(inserted_at, datetime('now'))
        FROM "warrant_summary"
        """
    )
    conn.execute('DROP TABLE "warrant_summary"')
    conn.execute('ALTER TABLE "__new_warrant_summary" RENAME TO "warrant_summary"')
    conn.execute(
        'CREATE INDEX IF NOT EXISTS idx_warrant_summary_warrant_date ON "warrant_summary" (warrant_stock_id, date)'
    )


def _migrate_stock_holding_table(conn: sqlite3.Connection, *, expected_stock_id: str) -> None:
    table = "holding_shares_per"
    if not _table_exists(conn, table):
        return
    cols = _list_columns(conn, table)
    if "stock_id" not in cols:
        return

    _validate_single_id_column(
        conn,
        table=table,
        column="stock_id",
        expected_stock_id=expected_stock_id,
    )
    conn.execute('DROP TABLE IF EXISTS "__new_holding_shares_per"')
    conn.execute(
        """
        CREATE TABLE "__new_holding_shares_per" (
            date TEXT NOT NULL,
            holding_shares_level TEXT NOT NULL,
            people INTEGER,
            percent REAL,
            unit INTEGER,
            query_mode TEXT NOT NULL,
            inserted_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE (date, holding_shares_level)
        )
        """
    )
    conn.execute(
        """
        INSERT INTO "__new_holding_shares_per" (
            date, holding_shares_level, people, percent, unit, query_mode, inserted_at
        )
        SELECT
            date,
            holding_shares_level,
            people,
            percent,
            unit,
            query_mode,
            COALESCE(inserted_at, datetime('now'))
        FROM "holding_shares_per"
        """
    )
    conn.execute('DROP TABLE "holding_shares_per"')
    conn.execute('ALTER TABLE "__new_holding_shares_per" RENAME TO "holding_shares_per"')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_holding_shares_per_date ON "holding_shares_per" (date)')


def _enforce_stock_identity(conn: sqlite3.Connection, *, stock_id: str) -> None:
    row = conn.execute('SELECT stock_id FROM "db_identity" WHERE id = 1').fetchone()
    if row is None:
        conn.execute(
            'INSERT INTO "db_identity" (id, stock_id) VALUES (1, ?)',
            (stock_id,),
        )
        return

    existing = str(row[0]).strip()
    if existing != stock_id:
        raise ValueError(
            f"DB identity stock_id={existing} does not match requested stock_id={stock_id}."
        )


def ensure_stock_db_layout(conn: sqlite3.Connection, *, stock_id: str) -> None:
    if _table_exists(conn, "stock_info"):
        stock_info_rows = int(conn.execute('SELECT COUNT(*) FROM "stock_info"').fetchone()[0] or 0)
        if stock_info_rows > 0:
            raise ValueError(
                "This SQLite file contains market-scoped stock_info rows. "
                "Per-stock commands must write to <stock_id>.sqlite."
            )
        conn.execute('DROP TABLE IF EXISTS "stock_info"')

    init_meta_schema(conn)
    _migrate_price_like_table(conn, table="price_daily", expected_stock_id=stock_id)
    _migrate_price_like_table(conn, table="price_adj_daily", expected_stock_id=stock_id)
    _migrate_margin_table(conn, expected_stock_id=stock_id)
    _migrate_broker_table(conn, expected_stock_id=stock_id)
    _migrate_warrant_table(conn, expected_stock_id=stock_id)
    _migrate_stock_holding_table(conn, expected_stock_id=stock_id)
    init_stock_schema(conn)
    _enforce_stock_identity(conn, stock_id=stock_id)


def _import_meta_runs(src_conn: sqlite3.Connection, dst_conn: sqlite3.Connection) -> None:
    if not _table_exists(src_conn, "meta_runs"):
        return
    cols = _list_columns(src_conn, "meta_runs")
    created_expr = "created_at" if "created_at" in cols else "datetime('now')"
    dst_conn.executemany(
        """
        INSERT INTO meta_runs (
            run_id, dataset, stock_id, query_mode, start_date, end_date,
            requested_params_json, fetched_rows, inserted_rows, status, error_message, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(run_id) DO NOTHING
        """,
        src_conn.execute(
            f"""
            SELECT run_id, dataset, stock_id, query_mode, start_date, end_date,
                   requested_params_json, fetched_rows, inserted_rows, status, error_message, {created_expr}
            FROM meta_runs
            """
        ).fetchall(),
    )


def _import_stock_info(src_conn: sqlite3.Connection, dst_conn: sqlite3.Connection) -> None:
    if not _table_exists(src_conn, "stock_info"):
        return
    cols = _list_columns(src_conn, "stock_info")
    inserted_expr = "inserted_at" if "inserted_at" in cols else "datetime('now')"
    dst_conn.executemany(
        """
        INSERT INTO stock_info (
            date, stock_id, stock_name, type, industry_category, inserted_at
        )
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(date, stock_id, stock_name, type, industry_category) DO NOTHING
        """,
        src_conn.execute(
            f"""
            SELECT
                date,
                stock_id,
                stock_name,
                type,
                industry_category,
                COALESCE({inserted_expr}, datetime('now'))
            FROM stock_info
            """
        ).fetchall(),
    )


def _import_market_holding(src_conn: sqlite3.Connection, dst_conn: sqlite3.Connection) -> None:
    if not _table_exists(src_conn, "holding_shares_per"):
        return
    cols = _list_columns(src_conn, "holding_shares_per")
    if "stock_id" not in cols:
        return
    inserted_expr = "inserted_at" if "inserted_at" in cols else "datetime('now')"
    dst_conn.executemany(
        """
        INSERT INTO holding_shares_per (
            date, stock_id, holding_shares_level, people, percent, unit, query_mode, inserted_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(date, stock_id, holding_shares_level) DO UPDATE SET
            people = COALESCE(excluded.people, holding_shares_per.people),
            percent = COALESCE(excluded.percent, holding_shares_per.percent),
            unit = COALESCE(excluded.unit, holding_shares_per.unit),
            query_mode = excluded.query_mode
        """,
        src_conn.execute(
            f"""
            SELECT
                date,
                stock_id,
                holding_shares_level,
                people,
                percent,
                unit,
                query_mode,
                COALESCE({inserted_expr}, datetime('now'))
            FROM holding_shares_per
            """
        ).fetchall(),
    )


def migrate_legacy_market_files(target_db_path: Path) -> None:
    target_dir = target_db_path.parent
    legacy_paths = [
        target_dir / filename
        for filename in LEGACY_MARKET_FILENAMES
        if (target_dir / filename).exists() and (target_dir / filename).resolve() != target_db_path.resolve()
    ]
    if not legacy_paths:
        return

    target_dir.mkdir(parents=True, exist_ok=True)
    dst_conn = sqlite3.connect(target_db_path)
    migrated_paths: list[Path] = []
    try:
        dst_conn.execute("PRAGMA journal_mode=WAL")
        init_meta_schema(dst_conn)
        init_market_schema(dst_conn)
        for legacy_path in legacy_paths:
            src_conn = sqlite3.connect(legacy_path)
            try:
                _import_meta_runs(src_conn, dst_conn)
                _import_stock_info(src_conn, dst_conn)
                _import_market_holding(src_conn, dst_conn)
            finally:
                src_conn.close()
            migrated_paths.append(legacy_path)
        dst_conn.commit()
        for path in migrated_paths:
            if path.exists():
                path.unlink()
    finally:
        dst_conn.close()


def ensure_market_db_layout(conn: sqlite3.Connection) -> None:
    if _table_exists(conn, "db_identity"):
        raise ValueError(
            "This SQLite file is bound to a stock_id (db_identity exists). "
            "Market-scoped commands must write to market.sqlite."
        )
    init_meta_schema(conn)
    init_market_schema(conn)
