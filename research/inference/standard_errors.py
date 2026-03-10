"""Standard-error helpers used by empirical inference modules."""

from __future__ import annotations

from typing import Iterable

import numpy as np
import pandas as pd
import statsmodels.api as sm
from scipy import stats
from statsmodels.stats import sandwich_covariance as sw


def default_newey_west_lags(n_obs: int) -> int:
    """Andrews-style default lag length for Newey-West HAC covariance."""

    if n_obs <= 1:
        return 0
    lags = int(np.floor(4.0 * (n_obs / 100.0) ** (2.0 / 9.0)))
    return max(lags, 1)


def newey_west_mean_test(values: Iterable[float], *, max_lags: int | None = None) -> dict[str, float | int]:
    """HAC-adjusted inference for the sample mean of a return series."""

    series = pd.to_numeric(pd.Series(list(values), dtype="float64"), errors="coerce").dropna()
    n_obs = int(len(series))
    if n_obs == 0:
        return {
            "n_obs": 0,
            "mean": float("nan"),
            "std_err": float("nan"),
            "t_stat": float("nan"),
            "p_value": float("nan"),
            "max_lags": 0,
        }

    lags = int(max_lags) if max_lags is not None else default_newey_west_lags(n_obs)
    lags = max(lags, 0)
    model = sm.OLS(series.to_numpy(), np.ones((n_obs, 1), dtype=float)).fit(
        cov_type="HAC",
        cov_kwds={"maxlags": lags},
    )
    coef = float(model.params[0])
    std_err = float(model.bse[0])
    t_stat = float(model.tvalues[0])
    p_value = float(model.pvalues[0])
    return {
        "n_obs": n_obs,
        "mean": coef,
        "std_err": std_err,
        "t_stat": t_stat,
        "p_value": p_value,
        "max_lags": lags,
    }


def cluster_covariance(
    ols_result: sm.regression.linear_model.RegressionResultsWrapper,
    *,
    cluster_a: Iterable[object],
    cluster_b: Iterable[object] | None = None,
) -> np.ndarray:
    """Cluster-robust covariance matrix for one-way or two-way clustering."""

    group_a = pd.Categorical(pd.Series(list(cluster_a))).codes
    if cluster_b is None:
        return np.asarray(sw.cov_cluster(ols_result, group_a), dtype=float)

    group_b = pd.Categorical(pd.Series(list(cluster_b))).codes
    cov, _cov_a, _cov_b = sw.cov_cluster_2groups(ols_result, group_a, group_b)
    return np.asarray(cov, dtype=float)


def summarize_coefficients(
    *,
    params: pd.Series,
    covariance: np.ndarray,
    n_obs: int,
    variable_order: list[str] | None = None,
) -> pd.DataFrame:
    """Build a coefficient summary table from parameter and covariance estimates."""

    if params.empty:
        return pd.DataFrame(columns=["variable", "coef", "std_err", "t_stat", "p_value", "n_obs"])

    names = list(params.index if variable_order is None else variable_order)
    coef_vec = np.asarray([float(params.get(name, np.nan)) for name in names], dtype=float)
    cov = np.asarray(covariance, dtype=float)
    if cov.shape[0] != cov.shape[1] or cov.shape[0] < len(names):
        raise ValueError("covariance matrix dimension does not match coefficients")

    diag = np.clip(np.diag(cov)[: len(names)], a_min=0.0, a_max=None)
    std_err = np.sqrt(diag)
    with np.errstate(divide="ignore", invalid="ignore"):
        t_stats = np.where(std_err > 0, coef_vec / std_err, np.nan)
    p_values = 2.0 * stats.norm.sf(np.abs(t_stats))

    table = pd.DataFrame(
        {
            "variable": names,
            "coef": coef_vec,
            "std_err": std_err,
            "t_stat": t_stats,
            "p_value": p_values,
            "n_obs": int(n_obs),
        }
    )
    return table
