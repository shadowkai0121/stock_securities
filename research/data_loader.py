"""Research-facing local data access layer.

This module is intentionally separated from ingestion. It reads only local
persisted datasets and never invokes remote API calls.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

import numpy as np
import pandas as pd

from data.storage.sqlite_store import SQLiteStore


@dataclass(frozen=True, slots=True)
class DataRange:
    """Date range parameters for local dataset queries."""

    start_date: str | None = None
    end_date: str | None = None


class ResearchDataLoader:
    """Research-facing local dataset loader built on top of SQLite files."""

    def __init__(self, *, data_root: str | Path = "data") -> None:
        self.data_root = Path(data_root)

    def _resolve_stock_db(self, stock_id: str, db_path: str | Path | None = None) -> Path:
        if db_path is not None:
            return Path(db_path)
        return self.data_root / f"{stock_id}.sqlite"

    def _resolve_shared_db(self, filename: str, db_path: str | Path | None = None) -> Path:
        if db_path is not None:
            return Path(db_path)
        return self.data_root / filename

    def available_stock_ids(self) -> list[str]:
        if not self.data_root.exists():
            return []
        return sorted(path.stem for path in self.data_root.glob("*.sqlite") if path.stem.isdigit())

    def _query_stock_table(
        self,
        *,
        stock_id: str,
        table: str,
        columns: Sequence[str],
        date_range: DataRange,
        db_path: str | Path | None = None,
        extra_where: str | None = None,
        extra_params: Sequence[Any] | None = None,
        order_by: str = "date",
    ) -> pd.DataFrame:
        sqlite_path = self._resolve_stock_db(stock_id, db_path)
        if not sqlite_path.exists():
            return pd.DataFrame(columns=list(columns))

        store = SQLiteStore(sqlite_path)
        if not store.table_exists(table):
            return pd.DataFrame(columns=list(columns))

        where_parts = ["stock_id = ?"]
        params: list[Any] = [stock_id]
        if date_range.start_date:
            where_parts.append("date >= ?")
            params.append(date_range.start_date)
        if date_range.end_date:
            where_parts.append("date <= ?")
            params.append(date_range.end_date)
        if extra_where:
            where_parts.append(extra_where)
            if extra_params:
                params.extend(list(extra_params))

        frame = store.read_table(
            table,
            columns=columns,
            where=" AND ".join(where_parts),
            params=params,
            order_by=order_by,
        )
        return frame

    def _load_multi_stock(
        self,
        *,
        stock_ids: Iterable[str],
        table: str,
        columns: Sequence[str],
        date_range: DataRange,
        db_path_map: dict[str, str | Path] | None = None,
        extra_where: str | None = None,
        extra_params: Sequence[Any] | None = None,
    ) -> pd.DataFrame:
        frames: list[pd.DataFrame] = []
        for stock_id in stock_ids:
            frame = self._query_stock_table(
                stock_id=stock_id,
                table=table,
                columns=columns,
                date_range=date_range,
                db_path=(db_path_map or {}).get(stock_id),
                extra_where=extra_where,
                extra_params=extra_params,
            )
            if not frame.empty:
                frames.append(frame)
        if not frames:
            return pd.DataFrame(columns=list(columns))
        out = pd.concat(frames, axis=0, ignore_index=True)
        if "date" in out.columns:
            out["date"] = pd.to_datetime(out["date"], errors="coerce")
            out = out[out["date"].notna()].copy()
            out["date"] = out["date"].dt.strftime("%Y-%m-%d")
        return out.sort_values([c for c in ["date", "stock_id"] if c in out.columns]).reset_index(drop=True)

    def load_prices(
        self,
        *,
        stock_ids: Sequence[str],
        start_date: str | None = None,
        end_date: str | None = None,
        adjusted: bool = True,
        db_path_map: dict[str, str | Path] | None = None,
        include_placeholders: bool = False,
    ) -> pd.DataFrame:
        """Load daily price rows from local SQLite stock databases."""

        table = "price_adj_daily" if adjusted else "price_daily"
        columns = [
            "date",
            "stock_id",
            "open",
            "max",
            "min",
            "close",
            "trading_volume",
            "trading_money",
            "spread",
            "trading_turnover",
            "is_placeholder",
        ]
        frame = self._load_multi_stock(
            stock_ids=stock_ids,
            table=table,
            columns=columns,
            date_range=DataRange(start_date, end_date),
            db_path_map=db_path_map,
        )
        if frame.empty:
            return frame

        numeric_cols = [
            "open",
            "max",
            "min",
            "close",
            "trading_volume",
            "trading_money",
            "spread",
            "trading_turnover",
            "is_placeholder",
        ]
        for col in numeric_cols:
            if col in frame.columns:
                frame[col] = pd.to_numeric(frame[col], errors="coerce")
        frame["is_placeholder"] = frame["is_placeholder"].fillna(1).astype(int)

        if not include_placeholders:
            frame = frame[frame["is_placeholder"] == 0].copy()

        frame = frame[frame["close"].notna()]
        frame = frame.sort_values(["stock_id", "date"]).drop_duplicates(
            subset=["date", "stock_id"],
            keep="last",
        )
        return frame.reset_index(drop=True)

    def load_returns(
        self,
        *,
        stock_ids: Sequence[str],
        start_date: str | None = None,
        end_date: str | None = None,
        adjusted: bool = True,
        log_returns: bool = False,
    ) -> pd.DataFrame:
        """Load close-based returns from local price tables."""

        prices = self.load_prices(
            stock_ids=stock_ids,
            start_date=start_date,
            end_date=end_date,
            adjusted=adjusted,
        )
        if prices.empty:
            return pd.DataFrame(columns=["date", "stock_id", "return"])

        prices = prices.sort_values(["stock_id", "date"]).copy()
        prices["close"] = pd.to_numeric(prices["close"], errors="coerce")
        prices["return"] = prices.groupby("stock_id")["close"].pct_change()
        if log_returns:
            prices["return"] = np.log1p(prices["return"])
        return prices[["date", "stock_id", "return"]].dropna().reset_index(drop=True)

    def load_margin(
        self,
        *,
        stock_ids: Sequence[str],
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        columns = [
            "date",
            "stock_id",
            "margin_purchase_today_balance",
            "short_sale_today_balance",
            "margin_purchase_buy",
            "margin_purchase_sell",
            "short_sale_buy",
            "short_sale_sell",
            "note",
            "is_placeholder",
        ]
        frame = self._load_multi_stock(
            stock_ids=stock_ids,
            table="margin_daily",
            columns=columns,
            date_range=DataRange(start_date, end_date),
        )
        if frame.empty:
            return frame
        for col in columns:
            if col in {"date", "stock_id", "note"}:
                continue
            frame[col] = pd.to_numeric(frame[col], errors="coerce")
        frame = frame[frame["is_placeholder"].fillna(1).astype(int) == 0].copy()
        return frame.reset_index(drop=True)

    def load_broker_flows(
        self,
        *,
        stock_ids: Sequence[str],
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        """Load and aggregate broker buy/sell flows from local broker tables."""

        columns = ["date", "stock_id", "broker_id", "buy", "sell", "is_placeholder"]
        raw = self._load_multi_stock(
            stock_ids=stock_ids,
            table="broker_trades",
            columns=columns,
            date_range=DataRange(start_date, end_date),
        )
        if raw.empty:
            return pd.DataFrame(columns=["date", "stock_id", "broker_buy", "broker_sell", "net_flow", "imbalance", "broker_count"])

        for col in ["buy", "sell", "is_placeholder"]:
            raw[col] = pd.to_numeric(raw[col], errors="coerce")

        raw = raw[raw["is_placeholder"].fillna(1).astype(int) == 0].copy()
        grouped = (
            raw.groupby(["date", "stock_id"], as_index=False)
            .agg(
                broker_buy=("buy", "sum"),
                broker_sell=("sell", "sum"),
                broker_count=("broker_id", "nunique"),
            )
            .sort_values(["stock_id", "date"])
            .reset_index(drop=True)
        )
        grouped["net_flow"] = grouped["broker_buy"] - grouped["broker_sell"]
        denom = grouped["broker_buy"].abs() + grouped["broker_sell"].abs()
        grouped["imbalance"] = np.where(denom > 0, grouped["net_flow"] / denom, 0.0)
        return grouped

    def load_holding_shares(
        self,
        *,
        stock_ids: Sequence[str] | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        db_path: str | Path | None = None,
    ) -> pd.DataFrame:
        """Load holding-shares distribution from a shared local SQLite file."""

        path = self._resolve_shared_db("holding_shares_per.sqlite", db_path)
        if not path.exists():
            return pd.DataFrame(
                columns=["date", "stock_id", "holding_shares_level", "people", "percent", "unit", "query_mode"]
            )

        store = SQLiteStore(path)
        if not store.table_exists("holding_shares_per"):
            return pd.DataFrame(
                columns=["date", "stock_id", "holding_shares_level", "people", "percent", "unit", "query_mode"]
            )

        where_parts: list[str] = []
        params: list[Any] = []
        if stock_ids:
            placeholders = ", ".join("?" for _ in stock_ids)
            where_parts.append(f"stock_id IN ({placeholders})")
            params.extend(list(stock_ids))
        if start_date:
            where_parts.append("date >= ?")
            params.append(start_date)
        if end_date:
            where_parts.append("date <= ?")
            params.append(end_date)

        where = " AND ".join(where_parts) if where_parts else None
        frame = store.read_table(
            "holding_shares_per",
            where=where,
            params=params,
            order_by="date, stock_id, holding_shares_level",
        )
        if frame.empty:
            return frame
        for col in ["people", "percent", "unit"]:
            if col in frame.columns:
                frame[col] = pd.to_numeric(frame[col], errors="coerce")
        return frame

    def load_stock_info(
        self,
        *,
        start_date: str | None = None,
        end_date: str | None = None,
        db_path: str | Path | None = None,
    ) -> pd.DataFrame:
        """Load stock metadata from local stock_info SQLite."""

        path = self._resolve_shared_db("stock_info.sqlite", db_path)
        if not path.exists():
            return pd.DataFrame(columns=["date", "stock_id", "stock_name", "type", "industry_category"])

        store = SQLiteStore(path)
        if not store.table_exists("stock_info"):
            return pd.DataFrame(columns=["date", "stock_id", "stock_name", "type", "industry_category"])

        where_parts: list[str] = []
        params: list[Any] = []
        if start_date:
            where_parts.append("date >= ?")
            params.append(start_date)
        if end_date:
            where_parts.append("date <= ?")
            params.append(end_date)
        where = " AND ".join(where_parts) if where_parts else None

        frame = store.read_table(
            "stock_info",
            columns=["date", "stock_id", "stock_name", "type", "industry_category"],
            where=where,
            params=params,
            order_by="date, stock_id",
        )
        if frame.empty:
            return frame

        frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
        frame = frame[frame["date"].notna()].copy()
        frame["date"] = frame["date"].dt.strftime("%Y-%m-%d")
        frame["stock_id"] = frame["stock_id"].astype(str).str.strip()
        frame["stock_name"] = frame["stock_name"].astype(str).str.strip()
        frame["type"] = frame["type"].astype(str).str.strip().str.lower()
        frame["industry_category"] = frame["industry_category"].fillna("").astype(str).str.strip()
        return frame.reset_index(drop=True)

    def load_table(
        self,
        *,
        db_path: str | Path,
        table: str,
        where: str | None = None,
        params: Sequence[Any] | None = None,
        order_by: str | None = None,
    ) -> pd.DataFrame:
        """Generic local-table reader for ad-hoc research needs."""

        store = SQLiteStore(db_path)
        if not store.table_exists(table):
            return pd.DataFrame()
        return store.read_table(table, where=where, params=params, order_by=order_by)
