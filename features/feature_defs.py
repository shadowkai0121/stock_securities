"""Feature definitions for Taiwan equity quantitative research."""

from __future__ import annotations

import re
from typing import Callable

import numpy as np
import pandas as pd


def _sorted_price_frame(price_df: pd.DataFrame) -> pd.DataFrame:
    frame = price_df.copy()
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame = frame[frame["date"].notna()].copy()
    frame = frame.sort_values(["stock_id", "date"]).reset_index(drop=True)
    frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
    return frame


def simple_returns(price_df: pd.DataFrame) -> pd.DataFrame:
    """Daily simple returns by stock."""

    frame = _sorted_price_frame(price_df)
    frame["simple_return"] = frame.groupby("stock_id")["close"].pct_change()
    return frame[["date", "stock_id", "simple_return"]]


def log_returns(price_df: pd.DataFrame) -> pd.DataFrame:
    """Daily log returns by stock."""

    frame = _sorted_price_frame(price_df)
    frame["log_return"] = np.log(frame["close"]).groupby(frame["stock_id"]).diff()
    return frame[["date", "stock_id", "log_return"]]


def rolling_volatility(price_df: pd.DataFrame, *, window: int = 20) -> pd.DataFrame:
    """Rolling volatility of simple returns."""

    frame = _sorted_price_frame(price_df)
    ret = frame.groupby("stock_id")["close"].pct_change()
    frame[f"volatility_{window}"] = ret.groupby(frame["stock_id"]).rolling(window=window, min_periods=window).std().reset_index(level=0, drop=True)
    return frame[["date", "stock_id", f"volatility_{window}"]]


def moving_average(price_df: pd.DataFrame, *, window: int = 20) -> pd.DataFrame:
    """Rolling moving-average level."""

    frame = _sorted_price_frame(price_df)
    frame[f"ma_{window}"] = (
        frame.groupby("stock_id")["close"]
        .rolling(window=window, min_periods=window)
        .mean()
        .reset_index(level=0, drop=True)
    )
    return frame[["date", "stock_id", f"ma_{window}"]]


def turnover_proxy(price_df: pd.DataFrame, *, window: int = 20) -> pd.DataFrame:
    """Turnover proxy based on trading money versus own rolling average."""

    frame = _sorted_price_frame(price_df)
    money = pd.to_numeric(frame.get("trading_money"), errors="coerce")
    if money.notna().sum() == 0:
        money = pd.to_numeric(frame.get("trading_volume"), errors="coerce") * frame["close"]
    frame["_money"] = money.fillna(0.0)

    rolling_mean = (
        frame.groupby("stock_id")["_money"]
        .rolling(window=window, min_periods=window)
        .mean()
        .reset_index(level=0, drop=True)
    )
    frame["turnover_proxy"] = np.where(rolling_mean > 0, frame["_money"] / rolling_mean, np.nan)
    return frame[["date", "stock_id", "turnover_proxy"]]


def margin_ratios(margin_df: pd.DataFrame) -> pd.DataFrame:
    """Margin balance ratio features."""

    if margin_df.empty:
        return pd.DataFrame(columns=["date", "stock_id", "margin_balance_ratio", "margin_net_buy"])

    frame = margin_df.copy()
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame = frame[frame["date"].notna()].copy()

    mp = pd.to_numeric(frame.get("margin_purchase_today_balance"), errors="coerce").fillna(0.0)
    ss = pd.to_numeric(frame.get("short_sale_today_balance"), errors="coerce").fillna(0.0)
    frame["margin_balance_ratio"] = np.where(ss + 1.0 > 0, mp / (ss + 1.0), np.nan)

    mp_buy = pd.to_numeric(frame.get("margin_purchase_buy"), errors="coerce").fillna(0.0)
    mp_sell = pd.to_numeric(frame.get("margin_purchase_sell"), errors="coerce").fillna(0.0)
    frame["margin_net_buy"] = mp_buy - mp_sell

    return frame[["date", "stock_id", "margin_balance_ratio", "margin_net_buy"]]


def broker_features(broker_flow_df: pd.DataFrame) -> pd.DataFrame:
    """Broker concentration and imbalance style features."""

    if broker_flow_df.empty:
        return pd.DataFrame(columns=["date", "stock_id", "broker_imbalance", "broker_concentration"])

    frame = broker_flow_df.copy()
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame = frame[frame["date"].notna()].copy()

    net = pd.to_numeric(frame.get("net_flow"), errors="coerce").fillna(0.0)
    buy = pd.to_numeric(frame.get("broker_buy"), errors="coerce").fillna(0.0)
    sell = pd.to_numeric(frame.get("broker_sell"), errors="coerce").fillna(0.0)
    denom = buy.abs() + sell.abs()

    frame["broker_imbalance"] = np.where(denom > 0, net / denom, 0.0)
    count = pd.to_numeric(frame.get("broker_count"), errors="coerce").fillna(0.0)
    frame["broker_concentration"] = np.where(count > 0, 1.0 / count, np.nan)

    return frame[["date", "stock_id", "broker_imbalance", "broker_concentration"]]


def _extract_bucket_weight(bucket: str) -> float:
    text = str(bucket).strip()
    numbers = [float(x) for x in re.findall(r"\d+\.?\d*", text)]
    if not numbers:
        return 0.0
    return max(numbers)


def holding_share_features(holding_df: pd.DataFrame) -> pd.DataFrame:
    """Holding-share concentration features (Herfindahl and large-holder ratio)."""

    if holding_df.empty:
        return pd.DataFrame(columns=["date", "stock_id", "holding_hhi", "large_holder_percent"])

    frame = holding_df.copy()
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame = frame[frame["date"].notna()].copy()

    frame["percent"] = pd.to_numeric(frame.get("percent"), errors="coerce").fillna(0.0)
    frame["bucket_weight"] = frame["holding_shares_level"].map(_extract_bucket_weight)

    hhi = (
        frame.assign(_p=lambda x: x["percent"] / 100.0)
        .groupby(["date", "stock_id"]) ["_p"]
        .apply(lambda x: float(np.square(x).sum()))
        .reset_index(name="holding_hhi")
    )

    large_holder = (
        frame.assign(weighted=lambda x: np.where(x["bucket_weight"] >= 400.0, x["percent"], 0.0))
        .groupby(["date", "stock_id"], as_index=False)["weighted"]
        .sum()
        .rename(columns={"weighted": "large_holder_percent"})
    )

    out = hhi.merge(large_holder, on=["date", "stock_id"], how="outer")
    return out


BUILTIN_FEATURE_FUNCTIONS: dict[str, Callable[..., pd.DataFrame]] = {
    "simple_returns": simple_returns,
    "log_returns": log_returns,
    "rolling_volatility": rolling_volatility,
    "moving_average": moving_average,
    "turnover_proxy": turnover_proxy,
    "margin_ratios": margin_ratios,
    "broker_features": broker_features,
    "holding_share_features": holding_share_features,
}
