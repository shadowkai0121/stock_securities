"""統計與績效評估指標。"""

from __future__ import annotations

import math
from typing import Sequence

import numpy as np
import pandas as pd
from sklearn.metrics import davies_bouldin_score, silhouette_score


def _to_1d_array(values: Sequence[float] | pd.Series | np.ndarray) -> np.ndarray:
    arr = np.asarray(values, dtype=float)
    arr = arr[np.isfinite(arr)]
    return arr


def dunn_index(distance_matrix: pd.DataFrame, labels: Sequence[int]) -> float:
    dist = np.asarray(distance_matrix, dtype=float)
    y = np.asarray(labels)
    clusters = np.unique(y)
    if len(clusters) < 2:
        return math.nan

    max_intra = 0.0
    min_inter = np.inf
    for c in clusters:
        idx_c = np.where(y == c)[0]
        if len(idx_c) > 1:
            intra = dist[np.ix_(idx_c, idx_c)]
            max_intra = max(max_intra, float(np.nanmax(intra)))
        for d in clusters:
            if c >= d:
                continue
            idx_d = np.where(y == d)[0]
            inter = dist[np.ix_(idx_c, idx_d)]
            min_inter = min(min_inter, float(np.nanmin(inter)))

    if not np.isfinite(min_inter) or max_intra <= 0:
        return math.nan
    return float(min_inter / max_intra)


def evaluate_clustering_quality(
    *,
    feature_matrix: pd.DataFrame,
    distance_matrix: pd.DataFrame,
    labels: Sequence[int] | pd.Series,
) -> dict[str, float]:
    y = np.asarray(labels)
    if len(np.unique(y)) < 2:
        return {"silhouette": math.nan, "dunn": math.nan, "davies_bouldin": math.nan}

    dist = np.asarray(distance_matrix, dtype=float)
    x = np.asarray(feature_matrix, dtype=float)

    sil = float(silhouette_score(dist, y, metric="precomputed"))
    dunn = float(dunn_index(distance_matrix, y))
    dbi = float(davies_bouldin_score(x, y))
    return {"silhouette": sil, "dunn": dunn, "davies_bouldin": dbi}


def select_best_k(score_table: pd.DataFrame, *, criterion: str = "silhouette") -> int:
    if score_table.empty:
        raise ValueError("score_table is empty.")

    criterion = criterion.lower()
    df = score_table.copy()
    for col in ["silhouette", "dunn", "davies_bouldin"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if criterion == "dbi":
        ranked = df.sort_values(["davies_bouldin", "silhouette"], ascending=[True, False])
    elif criterion == "dunn":
        ranked = df.sort_values(["dunn", "silhouette"], ascending=[False, False])
    else:
        ranked = df.sort_values(["silhouette", "dunn"], ascending=[False, False])

    if ranked.empty:
        raise ValueError("No valid row in score_table.")
    return int(ranked.iloc[0]["k"])


def sharpe_ratio(
    returns: Sequence[float] | pd.Series | np.ndarray,
    *,
    risk_free_rate: float = 0.0,
    periods_per_year: int = 252,
) -> float:
    r = _to_1d_array(returns)
    if len(r) < 2:
        return math.nan

    rf_per_period = risk_free_rate / periods_per_year
    excess = r - rf_per_period
    sigma = float(np.std(excess, ddof=1))
    if sigma <= 0 or not np.isfinite(sigma):
        return math.nan
    mu = float(np.mean(excess))
    return float((mu / sigma) * math.sqrt(periods_per_year))


def sortino_ratio(
    returns: Sequence[float] | pd.Series | np.ndarray,
    *,
    mar: float = 0.0,
    periods_per_year: int = 252,
) -> float:
    r = _to_1d_array(returns)
    if len(r) < 2:
        return math.nan

    mar_per_period = mar / periods_per_year
    excess = r - mar_per_period
    downside = np.minimum(excess, 0.0)
    downside_dev = float(np.sqrt(np.mean(np.square(downside))))
    if downside_dev <= 0 or not np.isfinite(downside_dev):
        return math.nan
    return float((np.mean(excess) / downside_dev) * math.sqrt(periods_per_year))


def max_drawdown(returns: Sequence[float] | pd.Series | np.ndarray) -> float:
    r = _to_1d_array(returns)
    if len(r) == 0:
        return math.nan
    equity = np.cumprod(1.0 + r)
    running_max = np.maximum.accumulate(equity)
    drawdown = (equity / running_max) - 1.0
    return float(np.min(drawdown))


def conditional_drawdown_at_risk(
    returns: Sequence[float] | pd.Series | np.ndarray,
    *,
    alpha: float = 0.95,
) -> float:
    """CDaR: 最壞尾端回撤的平均值（回傳正值，越大代表風險越高）。"""
    if not 0.0 < alpha < 1.0:
        raise ValueError("alpha must be in (0, 1).")

    r = _to_1d_array(returns)
    if len(r) == 0:
        return math.nan

    equity = np.cumprod(1.0 + r)
    running_max = np.maximum.accumulate(equity)
    drawdown = 1.0 - (equity / running_max)  # 轉成正值損失比例

    threshold = float(np.quantile(drawdown, alpha))
    tail = drawdown[drawdown >= threshold]
    if len(tail) == 0:
        return 0.0
    return float(np.mean(tail))


def evaluate_performance(
    returns: Sequence[float] | pd.Series | np.ndarray,
    *,
    risk_free_rate: float = 0.0,
    mar: float = 0.0,
    periods_per_year: int = 252,
    cdar_alpha: float = 0.95,
) -> dict[str, float]:
    r = _to_1d_array(returns)
    if len(r) == 0:
        return {
            "total_return": math.nan,
            "annualized_return": math.nan,
            "annualized_volatility": math.nan,
            "max_drawdown": math.nan,
            "sharpe": math.nan,
            "sortino": math.nan,
            "cdar": math.nan,
        }

    equity = np.cumprod(1.0 + r)
    total_return = float(equity[-1] - 1.0)
    years = max(len(r) / periods_per_year, 1.0 / periods_per_year)
    annualized_return = float((1.0 + total_return) ** (1.0 / years) - 1.0)
    annualized_vol = float(np.std(r, ddof=1) * np.sqrt(periods_per_year)) if len(r) > 1 else math.nan

    return {
        "total_return": total_return,
        "annualized_return": annualized_return,
        "annualized_volatility": annualized_vol,
        "max_drawdown": max_drawdown(r),
        "sharpe": sharpe_ratio(r, risk_free_rate=risk_free_rate, periods_per_year=periods_per_year),
        "sortino": sortino_ratio(r, mar=mar, periods_per_year=periods_per_year),
        "cdar": conditional_drawdown_at_risk(r, alpha=cdar_alpha),
    }
