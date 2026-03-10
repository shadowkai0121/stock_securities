"""Portfolio sorting for cross-sectional factor research."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from .standard_errors import newey_west_mean_test


@dataclass(slots=True)
class PortfolioSortResult:
    """Container for portfolio-sort panel and summary outputs."""

    portfolio_returns: pd.DataFrame
    spread_returns: pd.DataFrame
    summary: pd.DataFrame
    weighting: str
    n_portfolios: int


def _assign_portfolios(values: pd.Series, n_portfolios: int) -> pd.Series:
    ranked = values.rank(method="first")
    n_bins = max(2, min(int(n_portfolios), int(ranked.notna().sum())))
    if n_bins < 2:
        return pd.Series(index=values.index, dtype="float64")
    labels = pd.qcut(ranked, q=n_bins, labels=False, duplicates="drop")
    if labels is None:
        return pd.Series(index=values.index, dtype="float64")
    return labels.astype(float) + 1.0


def _weighted_mean(values: pd.Series, weights: pd.Series | None = None) -> float:
    series = pd.to_numeric(values, errors="coerce")
    if weights is None:
        return float(series.mean())
    w = pd.to_numeric(weights, errors="coerce").clip(lower=0.0)
    mask = series.notna() & w.notna() & (w > 0)
    if not mask.any():
        return float(series.mean())
    return float(np.average(series[mask], weights=w[mask]))


def run_portfolio_sort(
    panel: pd.DataFrame,
    *,
    sort_col: str,
    return_col: str,
    date_col: str = "date",
    n_portfolios: int = 10,
    weighting: str = "equal",
    weight_col: str = "market_cap",
    newey_west_lags: int | None = None,
) -> PortfolioSortResult:
    """Run cross-sectional portfolio sorting and long-short spread inference."""

    if n_portfolios < 2:
        raise ValueError("n_portfolios must be at least 2")
    if weighting not in {"equal", "value"}:
        raise ValueError("weighting must be either 'equal' or 'value'")

    required = [date_col, sort_col, return_col]
    if weighting == "value":
        required.append(weight_col)
    missing = [col for col in required if col not in panel.columns]
    if missing:
        raise ValueError(f"panel missing required columns: {missing}")

    work = panel[required].copy()
    work[date_col] = pd.to_datetime(work[date_col], errors="coerce")
    work[sort_col] = pd.to_numeric(work[sort_col], errors="coerce")
    work[return_col] = pd.to_numeric(work[return_col], errors="coerce")
    if weighting == "value":
        work[weight_col] = pd.to_numeric(work[weight_col], errors="coerce")
    work = work.dropna(subset=[date_col, sort_col, return_col]).sort_values(date_col).reset_index(drop=True)

    portfolio_rows: list[dict[str, float | int | str]] = []
    spread_rows: list[dict[str, float | str]] = []
    for raw_date, group in work.groupby(date_col, sort=True):
        assigned = group.copy()
        assigned["portfolio"] = _assign_portfolios(assigned[sort_col], n_portfolios=n_portfolios)
        assigned = assigned.dropna(subset=["portfolio"]).copy()
        if assigned.empty:
            continue

        labels = sorted(int(x) for x in assigned["portfolio"].dropna().unique().tolist())
        if len(labels) < 2:
            continue

        for label in labels:
            bucket = assigned[assigned["portfolio"] == float(label)]
            bucket_ret = _weighted_mean(
                bucket[return_col],
                bucket[weight_col] if weighting == "value" else None,
            )
            portfolio_rows.append(
                {
                    "date": pd.Timestamp(raw_date).strftime("%Y-%m-%d"),
                    "portfolio": label,
                    "portfolio_label": f"P{label}",
                    "return": bucket_ret,
                    "n_assets": int(len(bucket)),
                }
            )

        low = assigned[assigned["portfolio"] == float(labels[0])]
        high = assigned[assigned["portfolio"] == float(labels[-1])]
        low_ret = _weighted_mean(low[return_col], low[weight_col] if weighting == "value" else None)
        high_ret = _weighted_mean(high[return_col], high[weight_col] if weighting == "value" else None)
        spread_rows.append(
            {
                "date": pd.Timestamp(raw_date).strftime("%Y-%m-%d"),
                "long_short": float(high_ret - low_ret),
            }
        )

    portfolio_returns = pd.DataFrame(portfolio_rows)
    spread_returns = pd.DataFrame(spread_rows)

    summary_rows: list[dict[str, float | int | str]] = []
    if not portfolio_returns.empty:
        for portfolio, group in portfolio_returns.groupby("portfolio", sort=True):
            inference = newey_west_mean_test(group["return"].to_numpy(), max_lags=newey_west_lags)
            summary_rows.append(
                {
                    "portfolio": f"P{int(portfolio)}",
                    "mean_return": float(inference["mean"]),
                    "std_err": float(inference["std_err"]),
                    "t_stat": float(inference["t_stat"]),
                    "p_value": float(inference["p_value"]),
                    "n_obs": int(inference["n_obs"]),
                    "weighting": weighting,
                }
            )

    if not spread_returns.empty:
        spread_inference = newey_west_mean_test(spread_returns["long_short"].to_numpy(), max_lags=newey_west_lags)
        summary_rows.append(
            {
                "portfolio": "Long-Short",
                "mean_return": float(spread_inference["mean"]),
                "std_err": float(spread_inference["std_err"]),
                "t_stat": float(spread_inference["t_stat"]),
                "p_value": float(spread_inference["p_value"]),
                "n_obs": int(spread_inference["n_obs"]),
                "weighting": weighting,
            }
        )

    summary = pd.DataFrame(summary_rows)
    return PortfolioSortResult(
        portfolio_returns=portfolio_returns,
        spread_returns=spread_returns,
        summary=summary,
        weighting=weighting,
        n_portfolios=n_portfolios,
    )

