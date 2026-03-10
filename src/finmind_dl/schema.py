"""SQLite schema definitions for finmind-dl."""

from __future__ import annotations

import sqlite3


META_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS meta_runs (
    run_id TEXT PRIMARY KEY,
    dataset TEXT NOT NULL,
    stock_id TEXT NOT NULL,
    query_mode TEXT NOT NULL,
    start_date TEXT,
    end_date TEXT,
    requested_params_json TEXT NOT NULL,
    fetched_rows INTEGER NOT NULL,
    inserted_rows INTEGER NOT NULL,
    status TEXT NOT NULL,
    error_message TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_meta_runs_dataset_created
ON meta_runs (dataset, stock_id, created_at);
"""


STOCK_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS db_identity (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    stock_id TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS price_daily (
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
);

CREATE INDEX IF NOT EXISTS idx_price_daily_date
ON price_daily (date);

CREATE TABLE IF NOT EXISTS price_adj_daily (
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
);

CREATE INDEX IF NOT EXISTS idx_price_adj_daily_date
ON price_adj_daily (date);

CREATE TABLE IF NOT EXISTS margin_daily (
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
);

CREATE INDEX IF NOT EXISTS idx_margin_daily_date
ON margin_daily (date);

CREATE TABLE IF NOT EXISTS broker_trades (
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
);

CREATE INDEX IF NOT EXISTS idx_broker_trades_date
ON broker_trades (date);

CREATE INDEX IF NOT EXISTS idx_broker_trades_broker_date
ON broker_trades (broker_id, date);

CREATE TABLE IF NOT EXISTS warrant_summary (
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
);

CREATE INDEX IF NOT EXISTS idx_warrant_summary_warrant_date
ON warrant_summary (warrant_stock_id, date);

CREATE TABLE IF NOT EXISTS holding_shares_per (
    date TEXT NOT NULL,
    holding_shares_level TEXT NOT NULL,
    people INTEGER,
    percent REAL,
    unit INTEGER,
    query_mode TEXT NOT NULL,
    inserted_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (date, holding_shares_level)
);

CREATE INDEX IF NOT EXISTS idx_holding_shares_per_date
ON holding_shares_per (date);
"""


MARKET_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS stock_info (
    date TEXT,
    stock_id TEXT NOT NULL,
    stock_name TEXT NOT NULL,
    type TEXT NOT NULL,
    industry_category TEXT,
    inserted_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (date, stock_id, stock_name, type, industry_category)
);

CREATE INDEX IF NOT EXISTS idx_stock_info_date_stock
ON stock_info (date, stock_id);

CREATE TABLE IF NOT EXISTS holding_shares_per (
    date TEXT NOT NULL,
    stock_id TEXT NOT NULL,
    holding_shares_level TEXT NOT NULL,
    people INTEGER,
    percent REAL,
    unit INTEGER,
    query_mode TEXT NOT NULL,
    inserted_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (date, stock_id, holding_shares_level)
);

CREATE INDEX IF NOT EXISTS idx_holding_shares_per_date_stock
ON holding_shares_per (date, stock_id);
"""


def init_meta_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(META_SCHEMA_SQL)


def init_stock_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(STOCK_SCHEMA_SQL)


def init_market_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(MARKET_SCHEMA_SQL)


def init_schema(conn: sqlite3.Connection) -> None:
    """Backward-compatible init used by tests and local helpers."""
    init_meta_schema(conn)
    init_stock_schema(conn)
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS stock_info (
            date TEXT,
            stock_id TEXT NOT NULL,
            stock_name TEXT NOT NULL,
            type TEXT NOT NULL,
            industry_category TEXT,
            inserted_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE (date, stock_id, stock_name, type, industry_category)
        );
        CREATE INDEX IF NOT EXISTS idx_stock_info_date_stock
        ON stock_info (date, stock_id);
        """
    )
