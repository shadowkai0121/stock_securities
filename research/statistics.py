"""Statistical validation utilities for quantitative research."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Iterable, Sequence

import numpy as np
import pandas as pd
import statsmodels.api as sm


@dataclass(frozen=True, slots=True)
class WindowSplit:
    """Train/test split boundary for walk-forward evaluation."""

    train_start: pd.Timestamp
    train_end: pd.Timestamp
    test_start: pd.Timestamp
    test_end: pd.Timestamp


def _clean_series(values: Sequence[float] | pd.Series | np.ndarray) -> np.ndarray:
    arr = np.asarray(values, dtype=float)
    return arr[np.isfinite(arr)]


def newey_west_t_statistics(
    values: Sequence[float] | pd.Series | np.ndarray,
    *,
    max_lags: int | None = None,
) -> dict[str, float | int]:
    """Return Newey-West HAC-adjusted t-statistics for the mean."""

    y = _clean_series(values)
    n = int(len(y))
    if n < 5:
        return {
            "n_obs": n,
            "mean": float("nan"),
            "t_stat": float("nan"),
            "p_value": float("nan"),
            "max_lags": 0,
        }

    lags = max_lags
    if lags is None:
        lags = int(np.floor(4.0 * (n / 100.0) ** (2.0 / 9.0)))
        lags = max(lags, 1)

    model = sm.OLS(y, np.ones((n, 1), dtype=float)).fit(cov_type="HAC", cov_kwds={"maxlags": int(lags)})
    return {
        "n_obs": n,
        "mean": float(model.params[0]),
        "t_stat": float(model.tvalues[0]),
        "p_value": float(model.pvalues[0]),
        "max_lags": int(lags),
    }


def bootstrap_confidence_interval(
    values: Sequence[float] | pd.Series | np.ndarray,
    *,
    statistic: Callable[[np.ndarray], float] | None = None,
    n_bootstrap: int = 2000,
    ci: float = 0.95,
    seed: int = 42,
) -> dict[str, float]:
    """Bootstrap confidence interval for a statistic (default: mean)."""

    sample = _clean_series(values)
    if sample.size == 0:
        return {"point_estimate": float("nan"), "ci_low": float("nan"), "ci_high": float("nan")}

    stat_fn = statistic or (lambda x: float(np.mean(x)))
    rng = np.random.default_rng(seed)
    point = float(stat_fn(sample))

    draws = np.empty(int(n_bootstrap), dtype=float)
    n = sample.size
    for idx in range(int(n_bootstrap)):
        resample = rng.choice(sample, size=n, replace=True)
        draws[idx] = float(stat_fn(resample))

    alpha = (1.0 - float(ci)) / 2.0
    low = float(np.quantile(draws, alpha))
    high = float(np.quantile(draws, 1.0 - alpha))
    return {"point_estimate": point, "ci_low": low, "ci_high": high}


def subperiod_analysis(
    returns: pd.Series,
    *,
    periods: Iterable[tuple[str, str]] | None = None,
    trading_days_per_year: int = 252,
) -> pd.DataFrame:
    """Compute return metrics over subperiod windows."""

    if returns.empty:
        return pd.DataFrame(columns=["period_start", "period_end", "n_obs", "mean", "vol", "sharpe", "total_return"])

    work = returns.copy()
    work.index = pd.to_datetime(work.index)
    work = work.sort_index()

    if periods is None:
        years = sorted(work.index.year.unique())
        periods = [(f"{year}-01-01", f"{year}-12-31") for year in years]

    rows: list[dict[str, float | int | str]] = []
    for start, end in periods:
        start_ts = pd.Timestamp(start)
        end_ts = pd.Timestamp(end)
        seg = work[(work.index >= start_ts) & (work.index <= end_ts)].dropna()
        if seg.empty:
            rows.append(
                {
                    "period_start": start,
                    "period_end": end,
                    "n_obs": 0,
                    "mean": float("nan"),
                    "vol": float("nan"),
                    "sharpe": float("nan"),
                    "total_return": float("nan"),
                }
            )
            continue

        mean = float(seg.mean())
        std = float(seg.std(ddof=0))
        sharpe = (mean / std) * np.sqrt(trading_days_per_year) if std > 0 else np.nan
        total_return = float((1.0 + seg.fillna(0.0)).prod() - 1.0)
        rows.append(
            {
                "period_start": start,
                "period_end": end,
                "n_obs": int(len(seg)),
                "mean": mean,
                "vol": std,
                "sharpe": float(sharpe) if np.isfinite(sharpe) else float("nan"),
                "total_return": total_return,
            }
        )

    return pd.DataFrame(rows)


def train_valid_test_split_by_ratio(
    frame: pd.DataFrame,
    *,
    train_ratio: float = 0.6,
    valid_ratio: float = 0.2,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Split a time-sorted dataframe into train/validation/test subsets."""

    if not 0.0 < train_ratio < 1.0:
        raise ValueError("train_ratio must be in (0,1)")
    if not 0.0 <= valid_ratio < 1.0:
        raise ValueError("valid_ratio must be in [0,1)")
    if train_ratio + valid_ratio >= 1.0:
        raise ValueError("train_ratio + valid_ratio must be < 1")

    if frame.empty:
        return frame.copy(), frame.copy(), frame.copy()

    n = len(frame)
    train_end = int(np.floor(n * train_ratio))
    valid_end = int(np.floor(n * (train_ratio + valid_ratio)))

    train = frame.iloc[:train_end].copy()
    valid = frame.iloc[train_end:valid_end].copy()
    test = frame.iloc[valid_end:].copy()
    return train, valid, test


def generate_walk_forward_splits(
    dates: Sequence[pd.Timestamp] | pd.DatetimeIndex,
    *,
    train_window: int,
    test_window: int,
    step: int | None = None,
    expanding: bool = False,
) -> list[WindowSplit]:
    """Generate rolling/expanding walk-forward train/test windows."""

    if train_window <= 0 or test_window <= 0:
        raise ValueError("train_window and test_window must be positive integers")

    index = pd.DatetimeIndex(pd.to_datetime(list(dates))).sort_values().unique()
    if len(index) < train_window + test_window:
        return []

    step_size = step or test_window
    if step_size <= 0:
        raise ValueError("step must be a positive integer")

    splits: list[WindowSplit] = []
    start_train = 0
    while True:
        train_start_idx = 0 if expanding else start_train
        train_end_idx = start_train + train_window - 1
        test_start_idx = train_end_idx + 1
        test_end_idx = test_start_idx + test_window - 1
        if test_end_idx >= len(index):
            break

        splits.append(
            WindowSplit(
                train_start=index[train_start_idx],
                train_end=index[train_end_idx],
                test_start=index[test_start_idx],
                test_end=index[test_end_idx],
            )
        )
        start_train += step_size

    return splits


def walk_forward_validation(
    frame: pd.DataFrame,
    *,
    date_col: str,
    train_window: int,
    test_window: int,
    evaluator: Callable[[pd.DataFrame, pd.DataFrame], dict[str, float | int]],
    step: int | None = None,
    expanding: bool = False,
) -> pd.DataFrame:
    """Execute walk-forward validation using user-provided evaluator."""

    if frame.empty:
        return pd.DataFrame()

    work = frame.copy()
    work[date_col] = pd.to_datetime(work[date_col], errors="coerce")
    work = work[work[date_col].notna()].sort_values(date_col).reset_index(drop=True)

    splits = generate_walk_forward_splits(
        work[date_col],
        train_window=train_window,
        test_window=test_window,
        step=step,
        expanding=expanding,
    )
    rows: list[dict[str, float | int | str]] = []
    for split in splits:
        train = work[(work[date_col] >= split.train_start) & (work[date_col] <= split.train_end)].copy()
        test = work[(work[date_col] >= split.test_start) & (work[date_col] <= split.test_end)].copy()
        metrics = evaluator(train, test)
        rows.append(
            {
                "train_start": split.train_start.strftime("%Y-%m-%d"),
                "train_end": split.train_end.strftime("%Y-%m-%d"),
                "test_start": split.test_start.strftime("%Y-%m-%d"),
                "test_end": split.test_end.strftime("%Y-%m-%d"),
                **metrics,
            }
        )
    return pd.DataFrame(rows)


def rolling_window_evaluation(
    values: Sequence[float] | pd.Series,
    *,
    window: int,
    statistic: Callable[[np.ndarray], float] | None = None,
) -> pd.Series:
    """Apply a statistic over rolling windows."""

    if window <= 0:
        raise ValueError("window must be positive")

    stat_fn = statistic or (lambda x: float(np.mean(x)))
    series = pd.Series(values, dtype=float)
    return series.rolling(window=window, min_periods=window).apply(lambda x: stat_fn(np.asarray(x)), raw=False)


def expanding_window_evaluation(
    values: Sequence[float] | pd.Series,
    *,
    min_periods: int = 20,
    statistic: Callable[[np.ndarray], float] | None = None,
) -> pd.Series:
    """Apply a statistic over expanding windows."""

    if min_periods <= 0:
        raise ValueError("min_periods must be positive")

    stat_fn = statistic or (lambda x: float(np.mean(x)))
    series = pd.Series(values, dtype=float)
    return series.expanding(min_periods=min_periods).apply(lambda x: stat_fn(np.asarray(x)), raw=False)
