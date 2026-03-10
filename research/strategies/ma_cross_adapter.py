"""MA crossover signal adapter that reuses legacy strategy behavior."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Any

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[2]
LEGACY_MODULE_PATH = REPO_ROOT / "strategies" / "ma-cross" / "backtest.py"


def _load_legacy_module() -> Any | None:
    if not LEGACY_MODULE_PATH.exists():
        return None
    spec = importlib.util.spec_from_file_location("legacy_ma_cross_backtest", LEGACY_MODULE_PATH)
    if spec is None or spec.loader is None:
        return None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class MACrossoverSignalModel:
    """Signal model compatible with the new research pipeline.

    When available, the adapter delegates MA logic to the existing legacy module
    to preserve established strategy behavior.
    """

    def __init__(
        self,
        *,
        short_window: int = 20,
        long_window: int = 60,
        use_legacy_impl: bool = True,
    ) -> None:
        if short_window <= 0 or long_window <= 0:
            raise ValueError("MA windows must be positive")
        if long_window <= short_window:
            raise ValueError("long_window must be greater than short_window")

        self.short_window = int(short_window)
        self.long_window = int(long_window)
        self.use_legacy_impl = bool(use_legacy_impl)
        self._legacy = _load_legacy_module() if self.use_legacy_impl else None

    def _legacy_signals(self, stock_df: pd.DataFrame) -> pd.DataFrame:
        if self._legacy is None or not hasattr(self._legacy, "run_backtest"):
            raise RuntimeError("legacy MA module unavailable")

        work = stock_df.copy()
        if "open" not in work.columns:
            work["open"] = work["close"]

        backtest_df, _trades = self._legacy.run_backtest(
            price_df=work[["date", "stock_id", "open", "close"]],
            short_window=self.short_window,
            long_window=self.long_window,
            fee_bps=0.0,
        )
        return backtest_df[["date", "stock_id", "signal"]].copy()

    def _fallback_signals(self, stock_df: pd.DataFrame) -> pd.DataFrame:
        work = stock_df.copy()
        work = work.sort_values("date").reset_index(drop=True)
        work["short_ma"] = work["close"].rolling(window=self.short_window, min_periods=self.short_window).mean()
        work["long_ma"] = work["close"].rolling(window=self.long_window, min_periods=self.long_window).mean()
        work["signal"] = (work["short_ma"] > work["long_ma"]).astype(int)
        work.loc[work["short_ma"].isna() | work["long_ma"].isna(), "signal"] = 0
        return work[["date", "stock_id", "signal"]]

    def generate_signals(
        self,
        *,
        price_df: pd.DataFrame,
        features: pd.DataFrame | None = None,
        universe: pd.DataFrame | None = None,
        **_: object,
    ) -> pd.DataFrame:
        """Generate MA crossover binary signals per stock/date."""

        required = {"date", "stock_id", "close"}
        missing = required - set(price_df.columns)
        if missing:
            raise ValueError(f"price_df missing columns: {sorted(missing)}")

        frame = price_df.copy()
        frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
        frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
        frame = frame[frame["date"].notna() & frame["close"].notna() & (frame["close"] > 0)].copy()
        if frame.empty:
            return pd.DataFrame(columns=["date", "stock_id", "signal"])

        signal_parts: list[pd.DataFrame] = []
        for stock_id, group in frame.groupby("stock_id", sort=True):
            stock_df = group.sort_values("date").reset_index(drop=True)
            try:
                part = self._legacy_signals(stock_df) if self.use_legacy_impl else self._fallback_signals(stock_df)
            except Exception:
                part = self._fallback_signals(stock_df)
            signal_parts.append(part)

        signals = pd.concat(signal_parts, axis=0, ignore_index=True)
        signals["date"] = pd.to_datetime(signals["date"], errors="coerce")
        signals = signals[signals["date"].notna()].copy()
        signals["signal"] = pd.to_numeric(signals["signal"], errors="coerce").fillna(0.0)
        signals["signal"] = signals["signal"].clip(lower=0.0, upper=1.0)
        signals["date"] = signals["date"].dt.strftime("%Y-%m-%d")
        return signals[["date", "stock_id", "signal"]].sort_values(["date", "stock_id"]).reset_index(drop=True)
