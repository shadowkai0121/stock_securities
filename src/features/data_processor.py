"""金融研究導向的資料預處理模組。

本模組聚焦在三件學術研究中最關鍵的事情：
1. 將價格轉換成對數報酬率（降低尺度問題，近似可加性）。
2. 透過 CAPM 估計系統性風險暴露（beta），抽出個股殘差報酬。
3. 用統計檢定（ADF / Anderson-Darling）檢查序列可用性。
"""

from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from scipy.stats import anderson
from statsmodels.tsa.stattools import adfuller


REPO_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(REPO_ROOT / ".env", override=False)


def _resolve_data_dir(data_dir: str | Path | None = None) -> Path:
    """解析資料目錄，支援 .env 的 DATA_DIR 設定。"""
    raw = data_dir or os.getenv("DATA_DIR") or (REPO_ROOT / "data")
    path = Path(raw)
    if not path.is_absolute():
        path = (REPO_ROOT / path).resolve()
    return path


@dataclass(frozen=True)
class CapmFitResult:
    """CAPM OLS 估計結果。

    alpha:
        個股無法由市場解釋的平均超額報酬（截距項）。
    beta:
        系統性風險暴露；beta > 1 代表對市場波動較敏感。
    r_squared:
        市場因子對個股報酬解釋力，僅作診斷參考。
    n_obs:
        用於估計的觀測值數量。
    """

    alpha: float
    beta: float
    r_squared: float
    n_obs: int


def resolve_db_path(stock_id: str, data_dir: str | Path | None = None) -> Path:
    """回傳 `data/<stock_id>.sqlite` 路徑。"""
    return _resolve_data_dir(data_dir) / f"{stock_id}.sqlite"


def load_price_adj_daily(
    stock_id: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    data_dir: str | Path | None = None,
    table: str = "price_adj_daily",
) -> pd.DataFrame:
    """從個股 SQLite 載入還原收盤價資料。

    資料品質處理原則：
    - 僅保留 `is_placeholder == 0` 的真實觀測。
    - 價格必須 > 0（對數報酬需要正值）。
    - 日期去重後按時間排序。
    """
    db_path = resolve_db_path(stock_id, data_dir=data_dir)
    if not db_path.exists():
        raise FileNotFoundError(f"SQLite DB not found: {db_path}")

    where_clauses: list[str] = []
    params: list[object] = []
    select_cols = ["date", "close", "is_placeholder"]
    has_stock_id = False

    with sqlite3.connect(db_path) as conn:
        table_cols = {
            str(row[1])
            for row in conn.execute(f'PRAGMA table_info("{table}")').fetchall()
        }
        if not table_cols:
            raise ValueError(f"Table not found: {table} in {db_path}")

        has_stock_id = "stock_id" in table_cols
        if has_stock_id:
            select_cols.insert(1, "stock_id")
            where_clauses.append("stock_id = ?")
            params.append(stock_id)
        if start_date:
            where_clauses.append("date >= ?")
            params.append(start_date)
        if end_date:
            where_clauses.append("date <= ?")
            params.append(end_date)

        sql = f"""
        SELECT {", ".join(select_cols)}
        FROM "{table}"
        {"WHERE " + " AND ".join(where_clauses) if where_clauses else ""}
        ORDER BY date
        """
        df = pd.read_sql_query(sql, conn, params=params)

    if not has_stock_id:
        df["stock_id"] = stock_id

    if df.empty:
        raise ValueError(
            f"No rows found for stock_id={stock_id}, table={table}, "
            f"range=({start_date}, {end_date})"
        )

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["is_placeholder"] = (
        pd.to_numeric(df["is_placeholder"], errors="coerce").fillna(1).astype(int)
    )

    df = df[
        (df["date"].notna())
        & (df["close"].notna())
        & (df["close"] > 0)
        & (df["is_placeholder"] == 0)
    ].copy()
    df = df.sort_values("date").drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)

    if df.empty:
        raise ValueError(f"No valid rows after cleaning for stock_id={stock_id} in {db_path}.")
    return df[["date", "stock_id", "close"]].copy()


def compute_log_returns(
    price_df: pd.DataFrame,
    *,
    price_col: str = "close",
    date_col: str = "date",
    dropna: bool = True,
) -> pd.DataFrame:
    """計算對數報酬率 r_t = ln(P_t / P_{t-1})。

    學術意義：
    - 對數報酬在小變動下近似一般報酬，但在多期可加總，適合時間序列建模。
    - 相較絕對價格，能避免不同價位股票產生尺度偏誤。
    """
    if price_col not in price_df.columns or date_col not in price_df.columns:
        raise KeyError(f"`{price_col}` or `{date_col}` is missing in price_df.")

    df = price_df.copy()
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df[price_col] = pd.to_numeric(df[price_col], errors="coerce")
    df = df[(df[date_col].notna()) & (df[price_col].notna()) & (df[price_col] > 0)].copy()
    df = df.sort_values(date_col).reset_index(drop=True)

    df["log_return"] = np.log(df[price_col]).diff()
    if dropna:
        df = df[df["log_return"].notna()].copy()
    return df


def _estimate_capm_ols(
    stock_returns: pd.Series,
    market_returns: pd.Series,
    *,
    min_obs: int = 60,
) -> tuple[pd.DataFrame, CapmFitResult]:
    """估計 CAPM：r_i,t = alpha_i + beta_i * r_m,t + epsilon_i,t。

    這裡用最小平方法估計 alpha/beta，並回傳每期殘差：
    epsilon_i,t = r_i,t - (alpha_i + beta_i * r_m,t)
    """
    aligned = pd.concat(
        [stock_returns.rename("stock_return"), market_returns.rename("market_return")],
        axis=1,
    ).dropna()
    if len(aligned) < min_obs:
        raise ValueError(
            f"Insufficient overlap for CAPM fit: got {len(aligned)} obs, need >= {min_obs}."
        )

    x = aligned["market_return"].to_numpy(dtype=float)
    y = aligned["stock_return"].to_numpy(dtype=float)
    x_var = float(np.var(x, ddof=1))
    if not np.isfinite(x_var) or x_var <= 0:
        raise ValueError("Market return variance is zero; beta cannot be identified.")

    x_mean = float(np.mean(x))
    y_mean = float(np.mean(y))
    beta = float(np.cov(y, x, ddof=1)[0, 1] / x_var)
    alpha = float(y_mean - beta * x_mean)

    aligned["expected_return_capm"] = alpha + beta * aligned["market_return"]
    aligned["residual_return"] = aligned["stock_return"] - aligned["expected_return_capm"]

    sse = float(np.square(aligned["residual_return"]).sum())
    tss = float(np.square(aligned["stock_return"] - y_mean).sum())
    r_squared = float(1.0 - (sse / tss)) if tss > 0 else np.nan

    return aligned, CapmFitResult(alpha=alpha, beta=beta, r_squared=r_squared, n_obs=len(aligned))


def compute_market_residual_returns(
    stock_return_df: pd.DataFrame,
    market_return_df: pd.DataFrame,
    *,
    return_col: str = "log_return",
    date_col: str = "date",
    min_obs: int = 60,
) -> tuple[pd.DataFrame, CapmFitResult]:
    """根據個股與市場報酬，回傳 CAPM 殘差報酬序列。"""
    for needed_col in (date_col, return_col):
        if needed_col not in stock_return_df.columns or needed_col not in market_return_df.columns:
            raise KeyError(f"`{needed_col}` must exist in both stock_return_df and market_return_df.")

    stock = stock_return_df[[date_col, return_col]].copy()
    market = market_return_df[[date_col, return_col]].copy()
    stock[date_col] = pd.to_datetime(stock[date_col], errors="coerce")
    market[date_col] = pd.to_datetime(market[date_col], errors="coerce")
    stock = stock.dropna(subset=[date_col, return_col]).set_index(date_col).sort_index()
    market = market.dropna(subset=[date_col, return_col]).set_index(date_col).sort_index()

    aligned, fit_result = _estimate_capm_ols(
        stock[return_col], market[return_col], min_obs=min_obs
    )
    aligned = aligned.reset_index().rename(columns={"index": date_col})
    aligned = aligned.sort_values(date_col).reset_index(drop=True)
    return aligned, fit_result


def adf_test(
    series: Sequence[float] | pd.Series,
    *,
    alpha: float = 0.05,
    autolag: str = "AIC",
) -> dict[str, object]:
    """Augmented Dickey-Fuller 檢定（單根檢定）。

    解讀方式（常見學術門檻）：
    - H0: 序列存在單根（非平穩）。
    - 若 p-value < 0.05，拒絕 H0，可視為平穩。
    """
    s = pd.Series(series, dtype="float64").replace([np.inf, -np.inf], np.nan).dropna()
    if len(s) < 20:
        return {
            "ok": False,
            "reason": f"Too few observations for ADF: {len(s)}",
            "adf_is_stationary": False,
        }

    try:
        stat, pvalue, usedlag, nobs, critical_values, _ = adfuller(s, autolag=autolag)
    except ValueError as exc:
        return {
            "ok": False,
            "reason": str(exc),
            "adf_is_stationary": False,
        }

    return {
        "ok": True,
        "adf_statistic": float(stat),
        "adf_pvalue": float(pvalue),
        "adf_used_lag": int(usedlag),
        "adf_nobs": int(nobs),
        "adf_critical_values": {k: float(v) for k, v in critical_values.items()},
        "adf_alpha": float(alpha),
        "adf_is_stationary": bool(pvalue < alpha),
    }


def anderson_darling_test(
    series: Sequence[float] | pd.Series,
    *,
    alpha: float = 0.05,
) -> dict[str, object]:
    """Anderson-Darling 常態性檢定（以 normal 分配為基準）。

    解讀：
    - H0: 樣本來自常態分配。
    - 當 statistic > critical_value(alpha) 時，拒絕 H0（非常態）。
    """
    s = pd.Series(series, dtype="float64").replace([np.inf, -np.inf], np.nan).dropna()
    if len(s) < 20:
        return {
            "ok": False,
            "reason": f"Too few observations for AD: {len(s)}",
            "ad_is_normal": False,
        }

    ad = anderson(s.to_numpy(dtype=float), dist="norm")
    significance_levels = np.asarray(ad.significance_level, dtype=float)
    critical_values = np.asarray(ad.critical_values, dtype=float)
    target_level = alpha * 100.0
    idx = int(np.argmin(np.abs(significance_levels - target_level)))
    critical_at_alpha = float(critical_values[idx])
    statistic = float(ad.statistic)

    return {
        "ok": True,
        "ad_statistic": statistic,
        "ad_significance_levels": [float(x) for x in significance_levels],
        "ad_critical_values": [float(x) for x in critical_values],
        "ad_alpha": float(alpha),
        "ad_critical_at_alpha": critical_at_alpha,
        "ad_is_normal": bool(statistic < critical_at_alpha),
    }


def run_series_quality_checks(
    series: Sequence[float] | pd.Series,
    *,
    alpha: float = 0.05,
) -> dict[str, object]:
    """一次回傳平穩性 + 常態性檢定結果。"""
    adf = adf_test(series, alpha=alpha)
    ad = anderson_darling_test(series, alpha=alpha)
    return {
        "adf": adf,
        "anderson_darling": ad,
        "qualified": bool(adf.get("adf_is_stationary", False)),
    }


def build_residual_dataset(
    stock_id: str,
    *,
    market_id: str = "0050",
    start_date: str | None = None,
    end_date: str | None = None,
    data_dir: str | Path | None = None,
    min_obs: int = 60,
) -> dict[str, object]:
    """完整研究流程：載入資料 -> 對數報酬 -> CAPM 殘差 -> 統計檢定。

    回傳欄位：
    - `residual_df`: 含 stock_return / market_return / residual_return 的時間序列。
    - `capm`: alpha / beta / r_squared。
    - `diagnostics`: ADF + AD 檢定，供樣本篩選。
    """
    stock_prices = load_price_adj_daily(
        stock_id=stock_id,
        start_date=start_date,
        end_date=end_date,
        data_dir=data_dir,
    )
    market_prices = load_price_adj_daily(
        stock_id=market_id,
        start_date=start_date,
        end_date=end_date,
        data_dir=data_dir,
    )

    stock_returns = compute_log_returns(stock_prices)
    market_returns = compute_log_returns(market_prices)

    residual_df, capm_fit = compute_market_residual_returns(
        stock_returns,
        market_returns,
        min_obs=min_obs,
    )
    diagnostics = run_series_quality_checks(residual_df["residual_return"])

    return {
        "stock_id": stock_id,
        "market_id": market_id,
        "residual_df": residual_df,
        "capm": capm_fit,
        "diagnostics": diagnostics,
    }


def build_panel_residuals(
    stock_ids: Iterable[str],
    *,
    market_id: str = "0050",
    start_date: str | None = None,
    end_date: str | None = None,
    data_dir: str | Path | None = None,
    min_obs: int = 60,
) -> pd.DataFrame:
    """批次建立多檔股票的殘差報酬面板（date x stock_id）。"""
    panel_parts: list[pd.DataFrame] = []
    for stock_id in stock_ids:
        item = build_residual_dataset(
            stock_id,
            market_id=market_id,
            start_date=start_date,
            end_date=end_date,
            data_dir=data_dir,
            min_obs=min_obs,
        )
        residual_df = item["residual_df"][["date", "residual_return"]].copy()
        residual_df = residual_df.rename(columns={"residual_return": stock_id})
        panel_parts.append(residual_df)

    if not panel_parts:
        return pd.DataFrame()

    panel = panel_parts[0]
    for part in panel_parts[1:]:
        panel = panel.merge(part, on="date", how="outer")
    panel = panel.sort_values("date").reset_index(drop=True)
    return panel
