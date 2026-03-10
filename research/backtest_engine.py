"""Shared long/cash backtest engine for strategy evaluation."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Protocol

import numpy as np
import pandas as pd


@dataclass(frozen=True, slots=True)
class BacktestConfig:
    """Backtest configuration for long/cash portfolio simulation."""

    transaction_cost_bps: float = 10.0
    slippage_bps: float = 0.0
    lag_positions: int = 1
    trading_days_per_year: int = 252


@dataclass(slots=True)
class BacktestResult:
    """Portfolio-level backtest output."""

    timeseries: pd.DataFrame
    metrics: dict[str, float | int]


class SlippageModel(Protocol):
    """Interface for pluggable slippage models."""

    def estimate(self, turnover: pd.Series) -> pd.Series:
        ...


class FixedSlippageModel:
    """Linear slippage model based on turnover."""

    def __init__(self, bps: float) -> None:
        self.bps = float(bps)

    def estimate(self, turnover: pd.Series) -> pd.Series:
        return turnover * (self.bps / 10000.0)


class LongCashBacktestEngine:
    """Backtest engine that supports long/cash strategy simulation."""

    def __init__(self, config: BacktestConfig) -> None:
        self.config = config
        self.slippage_model: SlippageModel = FixedSlippageModel(config.slippage_bps)

    @staticmethod
    def _pivot_price(price_df: pd.DataFrame) -> pd.DataFrame:
        required = {"date", "stock_id", "close"}
        missing = required - set(price_df.columns)
        if missing:
            raise ValueError(f"price_df missing required columns: {sorted(missing)}")

        work = price_df.copy()
        work["date"] = pd.to_datetime(work["date"], errors="coerce")
        work["close"] = pd.to_numeric(work["close"], errors="coerce")
        work = work[work["date"].notna() & work["close"].notna() & (work["close"] > 0)].copy()
        if work.empty:
            raise ValueError("price_df has no valid rows after preprocessing")

        wide = (
            work.pivot_table(index="date", columns="stock_id", values="close", aggfunc="last")
            .sort_index()
            .sort_index(axis=1)
        )
        return wide

    @staticmethod
    def _pivot_signals(signal_df: pd.DataFrame) -> pd.DataFrame:
        required = {"date", "stock_id", "signal"}
        missing = required - set(signal_df.columns)
        if missing:
            raise ValueError(f"signal_df missing required columns: {sorted(missing)}")

        work = signal_df.copy()
        work["date"] = pd.to_datetime(work["date"], errors="coerce")
        work["signal"] = pd.to_numeric(work["signal"], errors="coerce").fillna(0.0)
        work = work[work["date"].notna()].copy()

        wide = (
            work.pivot_table(index="date", columns="stock_id", values="signal", aggfunc="last")
            .sort_index()
            .sort_index(axis=1)
            .fillna(0.0)
        )
        return wide

    @staticmethod
    def _compute_max_drawdown(equity: pd.Series) -> float:
        if equity.empty:
            return math.nan
        roll_max = equity.cummax()
        drawdown = (equity / roll_max) - 1.0
        return float(drawdown.min())

    def _compute_metrics(self, timeseries: pd.DataFrame) -> dict[str, float | int]:
        if timeseries.empty:
            return {
                "annual_return": math.nan,
                "annual_volatility": math.nan,
                "sharpe_ratio": math.nan,
                "max_drawdown": math.nan,
                "calmar_ratio": math.nan,
                "turnover": math.nan,
                "hit_ratio": math.nan,
                "number_of_trades": 0,
            }

        ret = timeseries["net_return"].dropna()
        if ret.empty:
            annual_return = math.nan
            annual_vol = math.nan
            sharpe = math.nan
            hit_ratio = math.nan
        else:
            total_return = float(timeseries["equity"].iloc[-1] - 1.0)
            years = max(len(ret) / self.config.trading_days_per_year, 1.0 / self.config.trading_days_per_year)
            annual_return = (1.0 + total_return) ** (1.0 / years) - 1.0

            std = float(ret.std(ddof=0))
            annual_vol = std * math.sqrt(self.config.trading_days_per_year) if std > 0 else math.nan
            sharpe = (
                (float(ret.mean()) / std) * math.sqrt(self.config.trading_days_per_year)
                if std > 0
                else math.nan
            )
            hit_ratio = float((ret > 0).mean())

        max_dd = self._compute_max_drawdown(timeseries["equity"])
        calmar = annual_return / abs(max_dd) if np.isfinite(annual_return) and max_dd < 0 else math.nan

        return {
            "annual_return": float(annual_return) if np.isfinite(annual_return) else math.nan,
            "annual_volatility": float(annual_vol) if np.isfinite(annual_vol) else math.nan,
            "sharpe_ratio": float(sharpe) if np.isfinite(sharpe) else math.nan,
            "max_drawdown": float(max_dd) if np.isfinite(max_dd) else math.nan,
            "calmar_ratio": float(calmar) if np.isfinite(calmar) else math.nan,
            "turnover": float(timeseries["turnover"].mean()) if not timeseries.empty else math.nan,
            "hit_ratio": float(hit_ratio) if np.isfinite(hit_ratio) else math.nan,
            "number_of_trades": int((timeseries["turnover"] > 0).sum()),
        }

    def run(
        self,
        *,
        price_df: pd.DataFrame,
        signal_df: pd.DataFrame,
        benchmark_returns: pd.Series | None = None,
    ) -> BacktestResult:
        """Run long/cash backtest with lagged positions and transaction costs."""

        prices = self._pivot_price(price_df)
        signals = self._pivot_signals(signal_df)

        aligned_cols = sorted(set(prices.columns) & set(signals.columns))
        if not aligned_cols:
            raise ValueError("No overlapping stock_ids between price_df and signal_df")

        aligned_index = prices.index.intersection(signals.index)
        if aligned_index.empty:
            raise ValueError("No overlapping dates between price_df and signal_df")

        prices = prices.loc[aligned_index, aligned_cols]
        signals = signals.loc[aligned_index, aligned_cols]

        asset_returns = prices.pct_change().replace([np.inf, -np.inf], np.nan).fillna(0.0)

        # Long/cash with next-day execution to avoid look-ahead bias.
        positions = signals.clip(lower=0.0, upper=1.0).shift(self.config.lag_positions).fillna(0.0)
        active_counts = positions.sum(axis=1)
        weights = positions.div(active_counts.replace(0, np.nan), axis=0).fillna(0.0)

        gross_return = (weights * asset_returns).sum(axis=1)

        turnover = weights.diff().abs().sum(axis=1).fillna(weights.abs().sum(axis=1))
        tx_cost = turnover * (self.config.transaction_cost_bps / 10000.0)
        slippage = self.slippage_model.estimate(turnover)

        net_return = gross_return - tx_cost - slippage
        equity = (1.0 + net_return).cumprod()

        ts = pd.DataFrame(
            {
                "date": aligned_index,
                "gross_return": gross_return.values,
                "transaction_cost": tx_cost.values,
                "slippage_cost": slippage.values,
                "net_return": net_return.values,
                "turnover": turnover.values,
                "active_positions": active_counts.values,
                "equity": equity.values,
            }
        )

        if benchmark_returns is None:
            ts["benchmark_equity"] = np.nan
        else:
            benchmark = benchmark_returns.copy()
            benchmark.index = pd.to_datetime(benchmark.index)
            benchmark = benchmark.reindex(aligned_index).fillna(0.0)
            ts["benchmark_equity"] = (1.0 + benchmark).cumprod().values

        metrics = self._compute_metrics(ts)
        return BacktestResult(timeseries=ts, metrics=metrics)
