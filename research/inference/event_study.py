"""Event study inference utilities for abnormal return testing."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import numpy as np
import pandas as pd
import statsmodels.api as sm
from scipy import stats


ExpectedReturnModel = Literal["mean", "market"]


@dataclass(slots=True)
class EventStudyResult:
    """Container for event-study outputs."""

    abnormal_returns: pd.DataFrame
    window_summary: pd.DataFrame
    car_test: dict[str, float | int]
    n_events: int


def _build_market_model(estimation: pd.DataFrame, market_return_col: str) -> tuple[float, float]:
    if estimation.empty or market_return_col not in estimation.columns:
        return 0.0, float(estimation["return"].mean()) if "return" in estimation.columns else 0.0

    sample = estimation[["return", market_return_col]].dropna()
    if len(sample) < 5:
        return 0.0, float(sample["return"].mean()) if not sample.empty else 0.0

    x = sm.add_constant(sample[market_return_col].to_numpy(dtype=float), has_constant="add")
    y = sample["return"].to_numpy(dtype=float)
    fit = sm.OLS(y, x).fit()
    alpha = float(fit.params[0])
    beta = float(fit.params[1]) if len(fit.params) > 1 else 0.0
    return beta, alpha


def run_event_study(
    returns: pd.DataFrame,
    events: pd.DataFrame,
    *,
    event_window: tuple[int, int] = (-5, 5),
    estimation_window: tuple[int, int] = (-120, -20),
    model: ExpectedReturnModel = "mean",
    date_col: str = "date",
    entity_col: str = "stock_id",
    return_col: str = "return",
    event_date_col: str = "event_date",
    market_return_col: str = "market_return",
) -> EventStudyResult:
    """Compute abnormal returns, CAR, and significance around event windows."""

    required_returns = [date_col, entity_col, return_col]
    missing_returns = [col for col in required_returns if col not in returns.columns]
    if missing_returns:
        raise ValueError(f"returns missing required columns: {missing_returns}")

    required_events = [entity_col, event_date_col]
    missing_events = [col for col in required_events if col not in events.columns]
    if missing_events:
        raise ValueError(f"events missing required columns: {missing_events}")

    work = returns.copy()
    work[date_col] = pd.to_datetime(work[date_col], errors="coerce")
    work[return_col] = pd.to_numeric(work[return_col], errors="coerce")
    if market_return_col in work.columns:
        work[market_return_col] = pd.to_numeric(work[market_return_col], errors="coerce")
    work = work.dropna(subset=[date_col, entity_col, return_col]).sort_values([entity_col, date_col]).reset_index(drop=True)

    event_df = events.copy()
    event_df[event_date_col] = pd.to_datetime(event_df[event_date_col], errors="coerce")
    event_df[entity_col] = event_df[entity_col].astype(str)
    event_df = event_df.dropna(subset=[entity_col, event_date_col]).reset_index(drop=True)
    if "event_id" not in event_df.columns:
        event_df["event_id"] = [f"E{idx + 1:04d}" for idx in range(len(event_df))]

    ar_rows: list[dict[str, float | str | int]] = []
    for event in event_df.to_dict(orient="records"):
        stock_id = str(event[entity_col])
        event_date = pd.Timestamp(event[event_date_col])
        stock = work[work[entity_col].astype(str) == stock_id].sort_values(date_col).reset_index(drop=True)
        if stock.empty:
            continue

        matches = stock.index[stock[date_col] == event_date]
        if len(matches) == 0:
            continue
        event_idx = int(matches[0])

        est_start = event_idx + int(estimation_window[0])
        est_end = event_idx + int(estimation_window[1])
        if est_start < 0 or est_end < est_start:
            continue
        estimation = stock.iloc[est_start : est_end + 1].copy()
        if estimation.empty:
            continue

        beta = 0.0
        alpha = float(estimation[return_col].mean())
        if model == "market":
            beta, alpha = _build_market_model(estimation.rename(columns={return_col: "return"}), market_return_col)

        cumulative = 0.0
        window_start, window_end = int(event_window[0]), int(event_window[1])
        for tau in range(window_start, window_end + 1):
            idx = event_idx + tau
            if idx < 0 or idx >= len(stock):
                continue
            row = stock.iloc[idx]
            actual = float(row[return_col])
            expected = alpha
            if model == "market" and market_return_col in stock.columns:
                expected = alpha + beta * float(row.get(market_return_col, 0.0) or 0.0)
            abnormal = actual - expected
            cumulative += abnormal
            ar_rows.append(
                {
                    "event_id": str(event["event_id"]),
                    "stock_id": stock_id,
                    "event_date": event_date.strftime("%Y-%m-%d"),
                    "date": pd.Timestamp(row[date_col]).strftime("%Y-%m-%d"),
                    "event_time": int(tau),
                    "actual_return": actual,
                    "expected_return": expected,
                    "abnormal_return": abnormal,
                    "car": cumulative,
                }
            )

    abnormal_panel = pd.DataFrame(ar_rows)
    if abnormal_panel.empty:
        empty_summary = pd.DataFrame(columns=["event_time", "aar", "caar", "t_stat", "p_value", "n_events"])
        return EventStudyResult(
            abnormal_returns=abnormal_panel,
            window_summary=empty_summary,
            car_test={"car_mean": float("nan"), "t_stat": float("nan"), "p_value": float("nan"), "n_events": 0},
            n_events=0,
        )

    summary_rows: list[dict[str, float | int]] = []
    cumulative_aar = 0.0
    for tau, group in abnormal_panel.groupby("event_time", sort=True):
        sample = pd.to_numeric(group["abnormal_return"], errors="coerce").dropna()
        if sample.empty:
            continue
        aar = float(sample.mean())
        cumulative_aar += aar
        t_stat, p_value = stats.ttest_1samp(sample.to_numpy(dtype=float), popmean=0.0, nan_policy="omit")
        summary_rows.append(
            {
                "event_time": int(tau),
                "aar": aar,
                "caar": cumulative_aar,
                "t_stat": float(t_stat) if np.isfinite(t_stat) else float("nan"),
                "p_value": float(p_value) if np.isfinite(p_value) else float("nan"),
                "n_events": int(sample.size),
            }
        )

    car_sample = (
        abnormal_panel.sort_values(["event_id", "event_time"])
        .groupby("event_id", as_index=False)
        .tail(1)["car"]
    )
    car_t, car_p = stats.ttest_1samp(pd.to_numeric(car_sample, errors="coerce").dropna().to_numpy(), popmean=0.0)
    car_test = {
        "car_mean": float(pd.to_numeric(car_sample, errors="coerce").mean()),
        "t_stat": float(car_t) if np.isfinite(car_t) else float("nan"),
        "p_value": float(car_p) if np.isfinite(car_p) else float("nan"),
        "n_events": int(car_sample.shape[0]),
    }

    return EventStudyResult(
        abnormal_returns=abnormal_panel,
        window_summary=pd.DataFrame(summary_rows),
        car_test=car_test,
        n_events=int(abnormal_panel["event_id"].nunique()),
    )

