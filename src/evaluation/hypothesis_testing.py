"""策略統計顯著性檢定。"""

from __future__ import annotations

from typing import Mapping, Sequence

import numpy as np
from scipy.stats import f_oneway, wilcoxon


def _clean_array(values: Sequence[float]) -> np.ndarray:
    arr = np.asarray(values, dtype=float)
    return arr[np.isfinite(arr)]


def wilcoxon_signed_rank_test(
    strategy_excess: Sequence[float],
    random_excess: Sequence[float],
    *,
    alpha: float = 0.05,
    alternative: str = "greater",
) -> dict[str, float | int | bool]:
    """Wilcoxon signed-rank test（配對樣本）。

    建議用於：
    - 同期比較「策略超額報酬」vs「隨機策略超額報酬」。
    """
    s = _clean_array(strategy_excess)
    r = _clean_array(random_excess)
    n = min(len(s), len(r))
    if n < 10:
        raise ValueError(f"Need at least 10 paired observations; got {n}.")

    s = s[:n]
    r = r[:n]
    stat, p_value = wilcoxon(s, r, zero_method="wilcox", correction=False, alternative=alternative)
    return {
        "n_obs": int(n),
        "statistic": float(stat),
        "p_value": float(p_value),
        "is_significant": bool(p_value < alpha),
        "alpha": float(alpha),
    }


def one_way_anova_test(
    groups: Mapping[str, Sequence[float]],
    *,
    alpha: float = 0.05,
) -> dict[str, float | int | bool]:
    """One-way ANOVA（多組平均數差異檢定）。"""
    cleaned: dict[str, np.ndarray] = {}
    for name, values in groups.items():
        arr = _clean_array(values)
        if len(arr) >= 2:
            cleaned[name] = arr

    if len(cleaned) < 2:
        raise ValueError("Need at least two groups with >=2 observations.")

    stat, p_value = f_oneway(*cleaned.values())
    return {
        "n_groups": int(len(cleaned)),
        "statistic": float(stat),
        "p_value": float(p_value),
        "is_significant": bool(p_value < alpha),
        "alpha": float(alpha),
    }
