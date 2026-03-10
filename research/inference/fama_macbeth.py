"""Fama-MacBeth cross-sectional regression utilities."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd
import statsmodels.api as sm

from .standard_errors import newey_west_mean_test


@dataclass(slots=True)
class FamaMacBethResult:
    """Container for two-step Fama-MacBeth outputs."""

    period_betas: pd.DataFrame
    summary: pd.DataFrame
    n_periods: int
    n_obs: int


def _prepare_panel(
    panel: pd.DataFrame,
    *,
    y_col: str,
    x_cols: list[str],
    date_col: str,
) -> pd.DataFrame:
    cols = [date_col, y_col, *x_cols]
    missing = [col for col in cols if col not in panel.columns]
    if missing:
        raise ValueError(f"panel missing required columns: {missing}")

    work = panel[cols].copy()
    work[date_col] = pd.to_datetime(work[date_col], errors="coerce")
    work[y_col] = pd.to_numeric(work[y_col], errors="coerce")
    for col in x_cols:
        work[col] = pd.to_numeric(work[col], errors="coerce")
    work = work.dropna(subset=[date_col, y_col, *x_cols]).sort_values(date_col).reset_index(drop=True)
    return work


def run_fama_macbeth(
    panel: pd.DataFrame,
    *,
    y_col: str,
    x_cols: list[str],
    date_col: str = "date",
    add_intercept: bool = True,
    min_obs_per_period: int | None = None,
    newey_west_lags: int | None = None,
) -> FamaMacBethResult:
    """Run two-pass Fama-MacBeth with HAC-adjusted second-step inference."""

    if not x_cols:
        raise ValueError("x_cols must contain at least one regressor")

    work = _prepare_panel(panel, y_col=y_col, x_cols=x_cols, date_col=date_col)
    if work.empty:
        empty = pd.DataFrame(columns=["date"])
        summary = pd.DataFrame(columns=["variable", "coef", "std_err", "t_stat", "p_value", "n_periods"])
        return FamaMacBethResult(period_betas=empty, summary=summary, n_periods=0, n_obs=0)

    min_obs = int(min_obs_per_period or (len(x_cols) + int(add_intercept) + 1))

    beta_rows: list[dict[str, float | str]] = []
    n_obs_total = 0
    for raw_date, group in work.groupby(date_col, sort=True):
        sample = group[[y_col, *x_cols]].dropna()
        if len(sample) < min_obs:
            continue

        y = sample[y_col].to_numpy(dtype=float)
        x = sample[x_cols].to_numpy(dtype=float)
        names = list(x_cols)
        if add_intercept:
            x = sm.add_constant(x, has_constant="add")
            names = ["const", *names]

        try:
            fit = sm.OLS(y, x).fit()
        except Exception:
            continue

        row: dict[str, float | str] = {"date": pd.Timestamp(raw_date).strftime("%Y-%m-%d")}
        for idx, name in enumerate(names):
            row[name] = float(fit.params[idx])
        beta_rows.append(row)
        n_obs_total += int(len(sample))

    if not beta_rows:
        empty = pd.DataFrame(columns=["date"])
        summary = pd.DataFrame(columns=["variable", "coef", "std_err", "t_stat", "p_value", "n_periods"])
        return FamaMacBethResult(period_betas=empty, summary=summary, n_periods=0, n_obs=0)

    betas = pd.DataFrame(beta_rows).sort_values("date").reset_index(drop=True)
    variables = [col for col in betas.columns if col != "date"]
    summary_rows: list[dict[str, float | int | str]] = []
    for variable in variables:
        inference = newey_west_mean_test(betas[variable].to_numpy(), max_lags=newey_west_lags)
        summary_rows.append(
            {
                "variable": variable,
                "coef": float(inference["mean"]),
                "std_err": float(inference["std_err"]),
                "t_stat": float(inference["t_stat"]),
                "p_value": float(inference["p_value"]),
                "n_periods": int(inference["n_obs"]),
            }
        )

    summary = pd.DataFrame(summary_rows)
    return FamaMacBethResult(
        period_betas=betas,
        summary=summary,
        n_periods=int(len(betas)),
        n_obs=n_obs_total,
    )

