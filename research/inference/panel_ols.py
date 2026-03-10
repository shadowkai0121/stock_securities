"""Panel OLS estimators with fixed effects and clustered standard errors."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import pandas as pd
import statsmodels.api as sm

from .standard_errors import cluster_covariance, summarize_coefficients


ClusterType = Literal["none", "entity", "time", "two_way"]


@dataclass(slots=True)
class PanelOLSResult:
    """Container for panel OLS regression outputs."""

    summary: pd.DataFrame
    params: pd.Series
    r_squared: float
    n_obs: int
    entity_effects: bool
    time_effects: bool
    cluster: ClusterType


def _prepare_design(
    panel: pd.DataFrame,
    *,
    y_col: str,
    x_cols: list[str],
    entity_col: str,
    time_col: str,
    add_intercept: bool,
    entity_effects: bool,
    time_effects: bool,
) -> tuple[pd.Series, pd.DataFrame, pd.DataFrame]:
    cols = [y_col, entity_col, time_col, *x_cols]
    missing = [col for col in cols if col not in panel.columns]
    if missing:
        raise ValueError(f"panel missing required columns: {missing}")

    work = panel[cols].copy()
    work[time_col] = pd.to_datetime(work[time_col], errors="coerce")
    work[y_col] = pd.to_numeric(work[y_col], errors="coerce")
    for col in x_cols:
        work[col] = pd.to_numeric(work[col], errors="coerce")
    work = work.dropna(subset=[y_col, entity_col, time_col, *x_cols]).copy()
    if work.empty:
        return pd.Series(dtype=float), pd.DataFrame(), work

    x = work[x_cols].copy()
    if add_intercept:
        x = sm.add_constant(x, has_constant="add")

    if entity_effects:
        entity_dummies = pd.get_dummies(work[entity_col].astype(str), prefix="fe_entity", drop_first=True, dtype=float)
        x = pd.concat([x, entity_dummies], axis=1)

    if time_effects:
        time_dummies = pd.get_dummies(
            work[time_col].dt.strftime("%Y-%m-%d"),
            prefix="fe_time",
            drop_first=True,
            dtype=float,
        )
        x = pd.concat([x, time_dummies], axis=1)

    y = work[y_col].astype(float)
    x = x.astype(float)
    return y, x, work


def run_panel_ols(
    panel: pd.DataFrame,
    *,
    y_col: str,
    x_cols: list[str],
    entity_col: str = "stock_id",
    time_col: str = "date",
    entity_effects: bool = False,
    time_effects: bool = False,
    cluster: ClusterType = "none",
    add_intercept: bool = True,
) -> PanelOLSResult:
    """Estimate panel OLS with optional fixed effects and cluster-robust covariance."""

    if not x_cols:
        raise ValueError("x_cols must contain at least one regressor")
    if cluster not in {"none", "entity", "time", "two_way"}:
        raise ValueError(f"Unsupported cluster value: {cluster}")

    y, x, prepared = _prepare_design(
        panel,
        y_col=y_col,
        x_cols=x_cols,
        entity_col=entity_col,
        time_col=time_col,
        add_intercept=add_intercept,
        entity_effects=entity_effects,
        time_effects=time_effects,
    )
    if y.empty or x.empty:
        return PanelOLSResult(
            summary=pd.DataFrame(columns=["variable", "coef", "std_err", "t_stat", "p_value", "n_obs"]),
            params=pd.Series(dtype=float),
            r_squared=float("nan"),
            n_obs=0,
            entity_effects=entity_effects,
            time_effects=time_effects,
            cluster=cluster,
        )

    model = sm.OLS(y.to_numpy(dtype=float), x.to_numpy(dtype=float)).fit()

    if cluster == "none":
        cov = model.cov_params()
    elif cluster == "entity":
        cov = cluster_covariance(model, cluster_a=prepared[entity_col].astype(str).to_numpy())
    elif cluster == "time":
        cov = cluster_covariance(
            model,
            cluster_a=prepared[time_col].dt.strftime("%Y-%m-%d").to_numpy(),
        )
    else:
        cov = cluster_covariance(
            model,
            cluster_a=prepared[entity_col].astype(str).to_numpy(),
            cluster_b=prepared[time_col].dt.strftime("%Y-%m-%d").to_numpy(),
        )

    params = pd.Series(model.params, index=x.columns, dtype=float)
    keep = [name for name in ["const", *x_cols] if name in params.index]
    summary = summarize_coefficients(
        params=params,
        covariance=cov,
        n_obs=int(len(prepared)),
        variable_order=keep,
    )

    return PanelOLSResult(
        summary=summary,
        params=params,
        r_squared=float(model.rsquared),
        n_obs=int(len(prepared)),
        entity_effects=entity_effects,
        time_effects=time_effects,
        cluster=cluster,
    )


def run_pooled_panel_ols(
    panel: pd.DataFrame,
    *,
    y_col: str,
    x_cols: list[str],
    entity_col: str = "stock_id",
    time_col: str = "date",
    cluster: ClusterType = "none",
) -> PanelOLSResult:
    """Convenience wrapper for pooled panel OLS."""

    return run_panel_ols(
        panel,
        y_col=y_col,
        x_cols=x_cols,
        entity_col=entity_col,
        time_col=time_col,
        entity_effects=False,
        time_effects=False,
        cluster=cluster,
        add_intercept=True,
    )


def run_firm_fe_ols(
    panel: pd.DataFrame,
    *,
    y_col: str,
    x_cols: list[str],
    entity_col: str = "stock_id",
    time_col: str = "date",
    cluster: ClusterType = "entity",
) -> PanelOLSResult:
    """Convenience wrapper for firm fixed-effects panel OLS."""

    return run_panel_ols(
        panel,
        y_col=y_col,
        x_cols=x_cols,
        entity_col=entity_col,
        time_col=time_col,
        entity_effects=True,
        time_effects=False,
        cluster=cluster,
        add_intercept=True,
    )


def run_time_fe_ols(
    panel: pd.DataFrame,
    *,
    y_col: str,
    x_cols: list[str],
    entity_col: str = "stock_id",
    time_col: str = "date",
    cluster: ClusterType = "time",
) -> PanelOLSResult:
    """Convenience wrapper for time fixed-effects panel OLS."""

    return run_panel_ols(
        panel,
        y_col=y_col,
        x_cols=x_cols,
        entity_col=entity_col,
        time_col=time_col,
        entity_effects=False,
        time_effects=True,
        cluster=cluster,
        add_intercept=True,
    )

