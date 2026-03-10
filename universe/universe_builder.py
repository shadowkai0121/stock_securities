"""Taiwan-equity universe construction from local persisted datasets."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Sequence

import numpy as np
import pandas as pd

from research.data_loader import ResearchDataLoader


FOUR_DIGIT_STOCK_ID = re.compile(r"^\d{4}$")


@dataclass(frozen=True, slots=True)
class UniverseConfig:
    """Configuration for tradable universe generation."""

    start_date: str
    end_date: str
    stock_ids: Sequence[str] | None = None
    exclude_etf: bool = True
    exclude_warrant: bool = True
    min_liquidity: float | None = None
    min_history_days: int = 120
    inactive_lookback_days: int = 60
    adjusted_price: bool = True


class TaiwanEquityUniverseBuilder:
    """Date-based tradable universe builder for Taiwan equities."""

    def __init__(self, data_loader: ResearchDataLoader) -> None:
        self.data_loader = data_loader

    @staticmethod
    def _prepare_latest_stock_info(stock_info: pd.DataFrame, end_date: str) -> pd.DataFrame:
        if stock_info.empty:
            return stock_info

        meta = stock_info.copy()
        meta["date"] = pd.to_datetime(meta["date"], errors="coerce")
        meta = meta[meta["date"].notna()].copy()
        meta = meta[meta["date"] <= pd.Timestamp(end_date)]
        if meta.empty:
            return meta

        meta = meta.sort_values(["stock_id", "date"]).drop_duplicates(subset=["stock_id"], keep="last")
        meta["stock_id"] = meta["stock_id"].astype(str).str.strip()
        meta["type"] = meta["type"].astype(str).str.strip().str.lower()
        meta["stock_name"] = meta["stock_name"].astype(str).str.strip()
        meta["industry_category"] = meta["industry_category"].fillna("").astype(str).str.strip()
        return meta.reset_index(drop=True)

    @staticmethod
    def _exclude_etf(meta: pd.DataFrame) -> pd.Series:
        name = meta["stock_name"].str.lower()
        industry = meta["industry_category"].str.lower()
        type_col = meta["type"].str.lower()
        return (
            name.str.contains("etf", na=False)
            | industry.str.contains("etf", na=False)
            | type_col.str.contains("etf", na=False)
            | industry.str.contains("指數", na=False)
        )

    @staticmethod
    def _exclude_warrant(meta: pd.DataFrame) -> pd.Series:
        name = meta["stock_name"].str.lower()
        industry = meta["industry_category"].str.lower()
        type_col = meta["type"].str.lower()
        return (
            name.str.contains("權證", na=False)
            | industry.str.contains("權證", na=False)
            | type_col.str.contains("warrant", na=False)
            | ~meta["stock_id"].astype(str).str.match(FOUR_DIGIT_STOCK_ID)
        )

    @staticmethod
    def _build_liquidity_column(frame: pd.DataFrame) -> pd.Series:
        trading_money = pd.to_numeric(frame.get("trading_money"), errors="coerce")
        if trading_money.notna().any():
            return trading_money.fillna(0.0)

        close = pd.to_numeric(frame.get("close"), errors="coerce").fillna(0.0)
        volume = pd.to_numeric(frame.get("trading_volume"), errors="coerce").fillna(0.0)
        return close * volume

    def build(self, **kwargs: object) -> pd.DataFrame:
        """Build normalized universe output: ``date, stock_id, tradable_flag``."""

        config = UniverseConfig(**kwargs)
        stock_info = self.data_loader.load_stock_info(end_date=config.end_date)
        meta = self._prepare_latest_stock_info(stock_info, config.end_date)
        if meta.empty:
            return pd.DataFrame(columns=["date", "stock_id", "tradable_flag"])

        meta = meta[meta["type"].isin({"twse", "tpex"})].copy()
        meta = meta[meta["stock_id"].str.match(FOUR_DIGIT_STOCK_ID)].copy()

        if config.exclude_etf:
            meta = meta[~self._exclude_etf(meta)].copy()
        if config.exclude_warrant:
            meta = meta[~self._exclude_warrant(meta)].copy()

        if config.stock_ids:
            target = {str(stock_id).strip() for stock_id in config.stock_ids}
            meta = meta[meta["stock_id"].isin(target)].copy()

        candidate_ids = sorted(meta["stock_id"].unique().tolist())
        if not candidate_ids:
            return pd.DataFrame(columns=["date", "stock_id", "tradable_flag"])

        prices = self.data_loader.load_prices(
            stock_ids=candidate_ids,
            start_date=config.start_date,
            end_date=config.end_date,
            adjusted=config.adjusted_price,
        )
        if prices.empty:
            return pd.DataFrame(columns=["date", "stock_id", "tradable_flag"])

        prices = prices.copy()
        prices["date"] = pd.to_datetime(prices["date"], errors="coerce")
        prices = prices[prices["date"].notna()].copy()
        prices["close"] = pd.to_numeric(prices["close"], errors="coerce")
        prices["liquidity"] = self._build_liquidity_column(prices)

        history_days = prices.groupby("stock_id")["date"].nunique()
        eligible_by_history = history_days[history_days >= int(config.min_history_days)].index.tolist()
        prices = prices[prices["stock_id"].isin(eligible_by_history)].copy()
        if prices.empty:
            return pd.DataFrame(columns=["date", "stock_id", "tradable_flag"])

        cutoff = pd.Timestamp(config.end_date) - pd.Timedelta(days=int(config.inactive_lookback_days))
        recent_ids = prices.loc[prices["date"] >= cutoff, "stock_id"].dropna().unique().tolist()
        prices = prices[prices["stock_id"].isin(recent_ids)].copy()
        if prices.empty:
            return pd.DataFrame(columns=["date", "stock_id", "tradable_flag"])

        if config.min_liquidity is not None:
            med_liq = prices.groupby("stock_id")["liquidity"].median()
            liquid_ids = med_liq[med_liq >= float(config.min_liquidity)].index.tolist()
            prices = prices[prices["stock_id"].isin(liquid_ids)].copy()
            if prices.empty:
                return pd.DataFrame(columns=["date", "stock_id", "tradable_flag"])

        prices["tradable_flag"] = np.where(
            prices["close"].notna() & (prices["close"] > 0),
            1,
            0,
        )

        all_dates = pd.DatetimeIndex(sorted(prices["date"].unique()))
        all_stock_ids = sorted(prices["stock_id"].unique())
        grid = pd.MultiIndex.from_product([all_dates, all_stock_ids], names=["date", "stock_id"]).to_frame(index=False)

        merged = grid.merge(
            prices[["date", "stock_id", "tradable_flag"]],
            on=["date", "stock_id"],
            how="left",
        )
        merged["tradable_flag"] = merged["tradable_flag"].fillna(0).astype(int)
        merged["date"] = merged["date"].dt.strftime("%Y-%m-%d")

        return merged[["date", "stock_id", "tradable_flag"]].sort_values(["date", "stock_id"]).reset_index(drop=True)
