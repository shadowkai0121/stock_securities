from __future__ import annotations

import argparse
import json
import math
import re
import sqlite3
import sys
import time
import warnings
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import matplotlib
import numpy as np
import pandas as pd

matplotlib.use("Agg")
import matplotlib.pyplot as plt

warnings.filterwarnings("ignore", message="Glyph .* missing from font")

plt.rcParams["font.family"] = "sans-serif"
plt.rcParams["font.sans-serif"] = [
    "Microsoft JhengHei",
    "Microsoft YaHei",
    "DejaVu Sans",
]
plt.rcParams["axes.unicode_minus"] = False


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_PATH = REPO_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from finmind_dl.core.config import resolve_token
from finmind_dl.datasets import price, price_adj, stock_info


TRADING_DAYS_PER_YEAR = 252
TRAIN_START = "2010-01-01"
TRAIN_END = "2018-12-31"
TEST_START = "2019-01-01"
TEST_END = "2025-12-31"
ALL_START = TRAIN_START
ALL_END = TEST_END
SHORT_WINDOWS = [5, 10, 20, 30, 40]
LONG_WINDOWS = [20, 60, 120, 200]
PARAM_GRID = [(s, l) for s in SHORT_WINDOWS for l in LONG_WINDOWS if l > s]
VALID_PRICE_TABLES = ["price_adj_daily", "price_daily"]
GENERIC_INDUSTRY_NAMES = {
    "電子工業",
    "電子類",
    "金融業",
    "其他",
    "創新板股票",
    "創新版股票",
}
STOCK_ID_PATTERN = re.compile(r"^\d{4}$")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ma-cross-thesis-pipeline",
        description="Download per-stock SQLite data and generate thesis outputs for MA cross.",
    )
    parser.add_argument("--token", default=None, help="FinMind token; env/.env fallback supported.")
    parser.add_argument(
        "--db-dir",
        default=str(REPO_ROOT / "data"),
        help="Directory for per-stock SQLite files (data/<stock_id>.sqlite).",
    )
    parser.add_argument(
        "--output-dir",
        default=str(REPO_ROOT / "strategies" / "ma-cross" / "outputs" / "thesis"),
        help="Output directory for thesis CSV/figures.",
    )
    parser.add_argument(
        "--universe-csv",
        default=None,
        help="Optional prebuilt universe.csv. When provided, skip stock-info fetch and reuse mapping.",
    )
    parser.add_argument(
        "--industry-target-date",
        default=TEST_START,
        help="Target industry snapshot date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--replace-db",
        action="store_true",
        help="Replace per-stock DB when downloading (default is incremental upsert).",
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip downloading and only run analysis from existing DB files.",
    )
    parser.add_argument(
        "--include-price-daily",
        action="store_true",
        help="Also download price_daily for robustness (may require higher API quota).",
    )
    parser.add_argument(
        "--table",
        default="price_adj_daily",
        choices=VALID_PRICE_TABLES,
        help="Price table used for analysis (and downloading when not using --skip-download).",
    )
    parser.add_argument(
        "--max-stocks",
        type=int,
        default=None,
        help="Optional cap for number of stocks to process (debug purpose).",
    )
    parser.add_argument(
        "--retry",
        type=int,
        default=3,
        help="Retry times for each stock download.",
    )
    parser.add_argument(
        "--sleep-sec",
        type=float,
        default=0.0,
        help="Sleep seconds between stock downloads.",
    )
    parser.add_argument(
        "--fee-bps",
        type=float,
        default=10.0,
        help="Cost per position-change event in bps.",
    )
    parser.add_argument(
        "--bootstrap-b",
        type=int,
        default=5000,
        help="Bootstrap repetitions.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for bootstrap.",
    )
    return parser


def _as_date(value: str) -> pd.Timestamp:
    return pd.Timestamp(value).normalize()


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _industry_specificity(industry: str) -> tuple[int, int, str]:
    text = (industry or "").strip()
    if not text:
        return (-1, 0, "")
    is_generic = int(text in GENERIC_INDUSTRY_NAMES)
    # Prefer non-generic and a slightly longer label.
    return (1 - is_generic, len(text), text)


def _pick_row_for_stock(stock_rows: pd.DataFrame, snapshot_date: pd.Timestamp) -> pd.Series:
    stock_rows = stock_rows.copy()
    stock_rows["date_ts"] = pd.to_datetime(stock_rows["date"], errors="coerce")

    on_snapshot = stock_rows[stock_rows["date_ts"] == snapshot_date]
    if not on_snapshot.empty:
        same_date = on_snapshot.copy()
        same_date["priority"] = same_date["industry_category"].map(_industry_specificity)
        return same_date.sort_values("priority", ascending=False).iloc[0]

    before = stock_rows[stock_rows["date_ts"].notna() & (stock_rows["date_ts"] < snapshot_date)]
    if not before.empty:
        last_date = before["date_ts"].max()
        same_date = before[before["date_ts"] == last_date].copy()
        same_date["priority"] = same_date["industry_category"].map(_industry_specificity)
        return same_date.sort_values("priority", ascending=False).iloc[0]

    after = stock_rows[stock_rows["date_ts"].notna() & (stock_rows["date_ts"] > snapshot_date)]
    if not after.empty:
        first_date = after["date_ts"].min()
        same_date = after[after["date_ts"] == first_date].copy()
        same_date["priority"] = same_date["industry_category"].map(_industry_specificity)
        return same_date.sort_values("priority", ascending=False).iloc[0]

    # No valid date; pick best by specificity only.
    stock_rows["priority"] = stock_rows["industry_category"].map(_industry_specificity)
    return stock_rows.sort_values("priority", ascending=False).iloc[0]


def build_industry_universe(
    stock_info_df: pd.DataFrame,
    *,
    target_date: str,
) -> tuple[pd.DataFrame, str]:
    df = stock_info_df.copy()
    df["stock_id"] = df["stock_id"].astype(str).str.strip()
    df["type"] = df["type"].astype(str).str.strip().str.lower()
    df["industry_category"] = df["industry_category"].astype(str).str.strip()
    df["stock_name"] = df["stock_name"].astype(str).str.strip()
    df["date_ts"] = pd.to_datetime(df["date"], errors="coerce")

    df = df[df["type"].isin({"twse", "tpex"})].copy()
    df = df[df["stock_id"].map(lambda x: bool(STOCK_ID_PATTERN.fullmatch(x)))].copy()
    df = df[df["industry_category"] != ""].copy()

    if df.empty:
        raise ValueError("No eligible rows found in stock_info for twse/tpex 4-digit stocks.")

    target_ts = _as_date(target_date)
    date_counts = (
        df[df["date_ts"].notna()]
        .groupby("date_ts")["stock_id"]
        .nunique()
        .sort_values(ascending=False)
    )
    if target_ts in date_counts.index:
        snapshot_ts = target_ts
    else:
        snapshot_ts = date_counts.index[0]

    rows: list[dict[str, Any]] = []
    for stock_id, group in df.groupby("stock_id", sort=True):
        selected = _pick_row_for_stock(group, snapshot_ts)
        stock_type = str(selected.get("type", "")).lower()
        rows.append(
            {
                "stock_id": stock_id,
                "stock_name": str(selected.get("stock_name", "")),
                "industry": str(selected.get("industry_category", "")),
                "type": stock_type,
                "is_listed": 1 if stock_type == "twse" else 0,
                "is_otc": 1 if stock_type == "tpex" else 0,
                "industry_snapshot_date": (
                    selected.get("date_ts").strftime("%Y-%m-%d")
                    if pd.notna(selected.get("date_ts"))
                    else None
                ),
                "industry_reference_date": snapshot_ts.strftime("%Y-%m-%d"),
            }
        )

    universe = pd.DataFrame(rows).sort_values("stock_id").reset_index(drop=True)
    return universe, snapshot_ts.strftime("%Y-%m-%d")


def load_industry_universe_from_csv(path: Path) -> tuple[pd.DataFrame, str | None]:
    df = pd.read_csv(path, dtype={"stock_id": str})
    missing = {c for c in ["stock_id", "industry"] if c not in df.columns}
    if missing:
        raise ValueError(f"universe-csv missing columns: {', '.join(sorted(missing))}")

    for col in [
        "stock_name",
        "is_listed",
        "is_otc",
        "industry_snapshot_date",
        "industry_reference_date",
    ]:
        if col not in df.columns:
            df[col] = None

    out = df[
        [
            "stock_id",
            "stock_name",
            "industry",
            "is_listed",
            "is_otc",
            "industry_snapshot_date",
            "industry_reference_date",
        ]
    ].copy()
    out["stock_id"] = out["stock_id"].astype(str).str.strip()
    out["stock_name"] = out["stock_name"].fillna("").astype(str).str.strip()
    out["industry"] = out["industry"].fillna("").astype(str).str.strip()
    out["is_listed"] = pd.to_numeric(out["is_listed"], errors="coerce").fillna(0).astype(int)
    out["is_otc"] = pd.to_numeric(out["is_otc"], errors="coerce").fillna(0).astype(int)

    ref = out["industry_reference_date"].dropna().astype(str)
    reference_date: str | None = None
    if not ref.empty:
        reference_date = str(ref.mode().iloc[0])
    return out, reference_date


def fetch_stock_info_to_db(
    *,
    token: str,
    db_path: Path,
    start_date: str,
) -> dict[str, Any]:
    args = SimpleNamespace(
        start_date=start_date,
        db_path=str(db_path),
        replace=False,
    )
    return stock_info.run(args, token)


def load_stock_info_from_db(db_path: Path) -> pd.DataFrame:
    conn = sqlite3.connect(db_path)
    try:
        return pd.read_sql_query(
            """
            SELECT date, stock_id, stock_name, type, industry_category
            FROM stock_info
            """,
            conn,
        )
    finally:
        conn.close()


def _download_one_stock(
    *,
    token: str,
    stock_id: str,
    db_path: Path,
    handler: Any,
    replace_db: bool,
    retry: int,
    sleep_sec: float,
) -> dict[str, Any]:
    last_error: Exception | None = None
    for attempt in range(1, retry + 1):
        try:
            args = SimpleNamespace(
                stock_id=stock_id,
                start_date=ALL_START,
                end_date=ALL_END,
                db_path=str(db_path),
                replace=replace_db and attempt == 1,
            )
            result = handler(args, token)
            return {
                "stock_id": stock_id,
                "status": "success",
                "attempt": attempt,
                "fetched_rows": int(result.get("fetched_rows", 0)),
                "inserted_rows": int(result.get("inserted_rows", 0)),
                "error": None,
            }
        except Exception as exc:  # pylint: disable=broad-except
            last_error = exc
            if attempt < retry:
                time.sleep(0.5 * attempt)
            else:
                break
    if sleep_sec > 0:
        time.sleep(sleep_sec)
    return {
        "stock_id": stock_id,
        "status": "error",
        "attempt": retry,
        "fetched_rows": 0,
        "inserted_rows": 0,
        "error": str(last_error) if last_error else "unknown error",
    }


def download_price_data(
    *,
    token: str,
    stock_ids: list[str],
    db_dir: Path,
    handler: Any,
    label: str,
    replace_db: bool,
    retry: int,
    sleep_sec: float,
) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    total = len(stock_ids)
    for idx, stock_id in enumerate(stock_ids, start=1):
        db_path = db_dir / f"{stock_id}.sqlite"
        result = _download_one_stock(
            token=token,
            stock_id=stock_id,
            db_path=db_path,
            handler=handler,
            replace_db=replace_db,
            retry=retry,
            sleep_sec=sleep_sec,
        )
        result["db_path"] = str(db_path)
        records.append(result)
        if idx % 50 == 0 or idx == total:
            ok = sum(1 for r in records if r["status"] == "success")
            print(f"[{label}] {idx}/{total} done, success={ok}, failed={idx - ok}")
    return pd.DataFrame(records)


def _load_price_series(db_path: Path, *, table: str) -> pd.DataFrame:
    if not db_path.exists():
        return pd.DataFrame(columns=["date", "close"])
    if table not in VALID_PRICE_TABLES:
        return pd.DataFrame(columns=["date", "close"])
    conn = sqlite3.connect(db_path)
    try:
        df = pd.read_sql_query(
            f"""
            SELECT date, close, is_placeholder
            FROM "{table}"
            WHERE date BETWEEN ? AND ?
            ORDER BY date
            """,
            conn,
            params=[ALL_START, ALL_END],
        )
    except sqlite3.Error:
        conn.close()
        return pd.DataFrame(columns=["date", "close"])
    finally:
        if conn:
            conn.close()
    if df.empty:
        return pd.DataFrame(columns=["date", "close"])

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["is_placeholder"] = pd.to_numeric(df["is_placeholder"], errors="coerce").fillna(1).astype(int)
    df = (
        df[
            (df["date"].notna())
            & (df["is_placeholder"] == 0)
            & (df["close"].notna())
            & (df["close"] > 0)
        ]
        .copy()
    )
    if df.empty:
        return pd.DataFrame(columns=["date", "close"])
    return df[["date", "close"]].drop_duplicates(subset=["date"], keep="last").sort_values("date")


def _sharpe_from_series(ret: pd.Series) -> float:
    s = ret.dropna()
    if s.empty:
        return math.nan
    std = float(s.std(ddof=0))
    if not np.isfinite(std) or std <= 0:
        return math.nan
    mean = float(s.mean())
    return (mean / std) * math.sqrt(TRADING_DAYS_PER_YEAR)


def _mdd_from_ret(ret: pd.Series) -> float:
    equity = (1.0 + ret.fillna(0.0)).cumprod()
    if equity.empty:
        return math.nan
    drawdown = (equity / equity.cummax()) - 1.0
    return float(drawdown.min())


def _total_return_from_ret(ret: pd.Series) -> float:
    equity = (1.0 + ret.fillna(0.0)).cumprod()
    if equity.empty:
        return math.nan
    return float(equity.iloc[-1] - 1.0)


def _slice_series(series: pd.Series, start: str, end: str) -> pd.Series:
    idx = series.index
    mask = (idx >= _as_date(start)) & (idx <= _as_date(end))
    return series[mask]


def _evaluate_stock_metrics(
    price_df: pd.DataFrame,
    *,
    fee_bps: float,
) -> dict[tuple[int, int], dict[str, float]]:
    if price_df.empty:
        return {}

    df = price_df.copy()
    df = df.sort_values("date").reset_index(drop=True)
    idx = pd.DatetimeIndex(df["date"])
    close = pd.Series(df["close"].values, index=idx, dtype="float64")
    ret = close.pct_change().replace([np.inf, -np.inf], np.nan)
    fee_ratio = fee_bps / 10000.0

    rolling_map: dict[int, pd.Series] = {}
    for window in sorted({*SHORT_WINDOWS, *LONG_WINDOWS}):
        rolling_map[window] = close.rolling(window=window, min_periods=window).mean()

    output: dict[tuple[int, int], dict[str, float]] = {}
    for short_w, long_w in PARAM_GRID:
        short_ma = rolling_map[short_w]
        long_ma = rolling_map[long_w]
        signal = (short_ma > long_ma).astype(int)
        signal[(short_ma.isna()) | (long_ma.isna())] = 0
        position = signal.shift(1).fillna(0).astype(int)
        cost = signal.diff().abs().fillna(0.0).shift(1).fillna(0.0) * fee_ratio
        strategy_ret = (position * ret) - cost

        train_strategy = _slice_series(strategy_ret, TRAIN_START, TRAIN_END)
        train_bh = _slice_series(ret, TRAIN_START, TRAIN_END)
        test_strategy = _slice_series(strategy_ret, TEST_START, TEST_END)
        test_bh = _slice_series(ret, TEST_START, TEST_END)

        output[(short_w, long_w)] = {
            "train_sharpe": _sharpe_from_series(train_strategy),
            "train_total_return_strategy": _total_return_from_ret(train_strategy),
            "train_total_return_bh": _total_return_from_ret(train_bh),
            "test_total_return_strategy": _total_return_from_ret(test_strategy),
            "test_total_return_bh": _total_return_from_ret(test_bh),
            "test_sharpe_strategy": _sharpe_from_series(test_strategy),
            "test_mdd_strategy": _mdd_from_ret(test_strategy),
            "test_excess_return": _total_return_from_ret(test_strategy) - _total_return_from_ret(test_bh),
        }
    return output


def _pick_best_params(industry_df: pd.DataFrame) -> dict[str, Any]:
    ranked_rows: list[dict[str, Any]] = []
    for (short_w, long_w), group in industry_df.groupby(["short_w", "long_w"], sort=False):
        valid_sharpe = group["train_sharpe"].replace([np.inf, -np.inf], np.nan).dropna()
        score = float(valid_sharpe.median()) if not valid_sharpe.empty else -np.inf
        outratio = float(
            (
                group["train_total_return_strategy"]
                > group["train_total_return_bh"]
            ).mean()
        )
        ranked_rows.append(
            {
                "short_w": int(short_w),
                "long_w": int(long_w),
                "score": score,
                "outratio": outratio,
            }
        )

    ranked = pd.DataFrame(ranked_rows)
    max_score = ranked["score"].max()
    candidates = ranked[ranked["score"] == max_score].copy()
    tie_break_rule = "score"

    if len(candidates) > 1:
        max_ratio = candidates["outratio"].max()
        candidates = candidates[candidates["outratio"] == max_ratio].copy()
        tie_break_rule = "ratio"
    if len(candidates) > 1:
        max_long = candidates["long_w"].max()
        candidates = candidates[candidates["long_w"] == max_long].copy()
        tie_break_rule = "long"
    if len(candidates) > 1:
        min_short = candidates["short_w"].min()
        candidates = candidates[candidates["short_w"] == min_short].copy()
        tie_break_rule = "short"

    best = candidates.sort_values(["short_w", "long_w"]).iloc[0]
    return {
        "best_short": int(best["short_w"]),
        "best_long": int(best["long_w"]),
        "train_score_sharpe_median": float(best["score"]),
        "train_outperform_ratio": float(best["outratio"]),
        "tie_break_rule": tie_break_rule,
    }


def _apply_bh_significance(p_values: pd.Series, q: float = 0.05) -> pd.Series:
    valid = p_values.dropna()
    if valid.empty:
        return pd.Series(False, index=p_values.index)

    order = valid.sort_values().index
    m = len(valid)
    threshold = pd.Series([(i / m) * q for i in range(1, m + 1)], index=order)
    passed = valid.loc[order] <= threshold
    if not passed.any():
        out = pd.Series(False, index=p_values.index)
        return out

    last_rank_idx = np.where(passed.values)[0].max()
    significant_indices = order[: last_rank_idx + 1]
    out = pd.Series(False, index=p_values.index)
    out.loc[significant_indices] = True
    return out


def _bootstrap_applicability(values: np.ndarray, *, b: int, rng: np.random.Generator) -> tuple[float, float, float]:
    n = int(values.size)
    if n == 0:
        return (math.nan, math.nan, math.nan)
    sampled = rng.choice(values, size=(b, n), replace=True)
    stat = sampled.mean(axis=1)
    ci_low, ci_high = np.quantile(stat, [0.025, 0.975])
    p_value = (int((stat <= 0.5).sum()) + 1) / (b + 1)
    return float(ci_low), float(ci_high), float(p_value)


def run_pipeline(args: argparse.Namespace) -> int:
    token = resolve_token(args.token)
    db_dir = Path(args.db_dir)
    output_dir = Path(args.output_dir)
    _ensure_dir(db_dir)
    _ensure_dir(output_dir)

    snapshot_date: str | None = None
    if args.universe_csv:
        universe_path = Path(args.universe_csv)
        print(f"[1/5] Loading universe from CSV: {universe_path}")
        industry_universe, snapshot_date = load_industry_universe_from_csv(universe_path)
    else:
        stock_info_db = output_dir / "stock_info.sqlite"
        print("[1/5] Fetching stock info snapshot candidates...")
        stock_info_result = fetch_stock_info_to_db(
            token=token,
            db_path=stock_info_db,
            start_date=args.industry_target_date,
        )
        print(
            "[stock-info] "
            f"fetched={stock_info_result.get('fetched_rows')} inserted={stock_info_result.get('inserted_rows')}"
        )
        stock_info_df = load_stock_info_from_db(stock_info_db)
        industry_universe, snapshot_date = build_industry_universe(
            stock_info_df, target_date=args.industry_target_date
        )

    if args.max_stocks is not None and args.max_stocks > 0:
        industry_universe = industry_universe.head(args.max_stocks).copy()

    stock_ids = industry_universe["stock_id"].tolist()
    snapshot_text = snapshot_date or "unknown"
    print(f"[2/5] Universe size={len(stock_ids)}, industry reference date={snapshot_text}")

    if args.skip_download:
        download_df = pd.DataFrame(
            {
                "stock_id": stock_ids,
                "status": "skipped",
                "attempt": 0,
                "fetched_rows": 0,
                "inserted_rows": 0,
                "error": None,
                "db_path": [str(db_dir / f"{sid}.sqlite") for sid in stock_ids],
            }
        )
    else:
        primary_table = str(args.table)
        primary_handler = price_adj.run if primary_table == "price_adj_daily" else price.run

        print(f"[3/5] Downloading {primary_table} into per-stock SQLite...")
        download_df = download_price_data(
            token=token,
            stock_ids=stock_ids,
            db_dir=db_dir,
            handler=primary_handler,
            label=f"download:{primary_table}",
            replace_db=bool(args.replace_db),
            retry=max(1, int(args.retry)),
            sleep_sec=max(0.0, float(args.sleep_sec)),
        )
        if bool(args.include_price_daily) and primary_table != "price_daily":
            print("[3.5/5] Downloading price_daily into per-stock SQLite...")
            daily_df = download_price_data(
                token=token,
                stock_ids=stock_ids,
                db_dir=db_dir,
                handler=price.run,
                label="download:price_daily",
                replace_db=False,
                retry=max(1, int(args.retry)),
                sleep_sec=max(0.0, float(args.sleep_sec)),
            )
            daily_df.to_csv(output_dir / "price_daily_download_summary.csv", index=False)
            if "error" in daily_df:
                daily_errors = daily_df[daily_df["status"] == "error"].copy()
                if not daily_errors.empty:
                    daily_errors[["stock_id", "error"]].to_csv(
                        output_dir / "price_daily_download_errors.csv",
                        index=False,
                    )
    download_df.to_csv(output_dir / "download_summary.csv", index=False)

    valid_ids = set(
        download_df[download_df["status"].isin(["success", "skipped"])]["stock_id"].astype(str).tolist()
    )
    industry_universe = industry_universe[industry_universe["stock_id"].isin(valid_ids)].copy()
    industry_universe = industry_universe.sort_values("stock_id").reset_index(drop=True)

    print("[4/5] Running train/test optimization and OOS evaluation...")
    train_rows: list[dict[str, Any]] = []
    universe_rows: list[dict[str, Any]] = []
    stock_combo_metrics: dict[str, dict[tuple[int, int], dict[str, float]]] = {}

    for idx, row in industry_universe.iterrows():
        stock_id = str(row["stock_id"])
        db_path = db_dir / f"{stock_id}.sqlite"
        price_df = _load_price_series(db_path, table=str(args.table))
        price_df = price_df[
            (price_df["date"] >= _as_date(ALL_START)) & (price_df["date"] <= _as_date(ALL_END))
        ].copy()

        train_days = int(
            ((price_df["date"] >= _as_date(TRAIN_START)) & (price_df["date"] <= _as_date(TRAIN_END))).sum()
        )
        test_days = int(
            ((price_df["date"] >= _as_date(TEST_START)) & (price_df["date"] <= _as_date(TEST_END))).sum()
        )
        included = int(train_days >= 504 and test_days >= 252)

        universe_rows.append(
            {
                "stock_id": stock_id,
                "industry": row["industry"],
                "is_listed": int(row["is_listed"]),
                "is_otc": int(row["is_otc"]),
                "train_trading_days": train_days,
                "test_trading_days": test_days,
                "included": included,
                "industry_snapshot_date": row["industry_snapshot_date"],
                "industry_reference_date": row["industry_reference_date"],
                "stock_name": row["stock_name"],
            }
        )
        if included == 0:
            continue

        combo_metrics = _evaluate_stock_metrics(price_df, fee_bps=float(args.fee_bps))
        stock_combo_metrics[stock_id] = combo_metrics
        for (short_w, long_w), metric in combo_metrics.items():
            train_rows.append(
                {
                    "stock_id": stock_id,
                    "industry": row["industry"],
                    "short_w": short_w,
                    "long_w": long_w,
                    **metric,
                }
            )
        if (idx + 1) % 100 == 0 or (idx + 1) == len(industry_universe):
            print(f"[analysis] processed {idx + 1}/{len(industry_universe)} stocks")

    universe_df = pd.DataFrame(universe_rows).sort_values("stock_id").reset_index(drop=True)
    universe_df.to_csv(output_dir / "universe.csv", index=False)

    if not train_rows:
        raise ValueError("No included stocks available after filtering; cannot continue.")

    train_df = pd.DataFrame(train_rows)
    industry_best_rows: list[dict[str, Any]] = []
    for industry, group in train_df.groupby("industry", sort=True):
        best = _pick_best_params(group)
        industry_best_rows.append(
            {
                "industry": industry,
                "best_short": best["best_short"],
                "best_long": best["best_long"],
                "train_score_sharpe_median": best["train_score_sharpe_median"],
                "train_outperform_ratio": best["train_outperform_ratio"],
                "tie_break_rule": best["tie_break_rule"],
            }
        )
    industry_best_df = pd.DataFrame(industry_best_rows).sort_values("industry").reset_index(drop=True)
    industry_best_df.to_csv(output_dir / "industry_best_params.csv", index=False)

    best_map = {
        str(r["industry"]): (int(r["best_short"]), int(r["best_long"]))
        for r in industry_best_df.to_dict(orient="records")
    }

    included_universe = universe_df[universe_df["included"] == 1].copy()
    stock_oos_rows: list[dict[str, Any]] = []
    for row in included_universe.to_dict(orient="records"):
        stock_id = str(row["stock_id"])
        industry = str(row["industry"])
        combo = best_map.get(industry)
        if combo is None:
            continue
        metric = stock_combo_metrics.get(stock_id, {}).get(combo)
        if metric is None:
            continue
        test_outperform = int(metric["test_total_return_strategy"] > metric["test_total_return_bh"])
        stock_oos_rows.append(
            {
                "stock_id": stock_id,
                "industry": industry,
                "best_short": combo[0],
                "best_long": combo[1],
                "fee_bps": float(args.fee_bps),
                "table": str(args.table),
                "test_total_return_strategy": float(metric["test_total_return_strategy"]),
                "test_total_return_bh": float(metric["test_total_return_bh"]),
                "test_outperform": test_outperform,
                "test_sharpe_strategy": float(metric["test_sharpe_strategy"]),
                "test_mdd_strategy": float(metric["test_mdd_strategy"]),
                "test_excess_return": float(metric["test_excess_return"]),
            }
        )
    stock_oos_df = pd.DataFrame(stock_oos_rows).sort_values(["industry", "stock_id"]).reset_index(drop=True)
    stock_oos_df.to_csv(output_dir / "stock_oos_results.csv", index=False)

    table1 = (
        universe_df[universe_df["included"] == 1]
        .groupby("industry", as_index=False)
        .agg(
            n_stocks=("stock_id", "count"),
            train_days_median=("train_trading_days", "median"),
            test_days_median=("test_trading_days", "median"),
        )
        .sort_values("industry")
        .reset_index(drop=True)
    )
    table1.to_csv(output_dir / "table1_sample_description.csv", index=False)

    table2 = industry_best_df[
        [
            "industry",
            "best_short",
            "best_long",
            "train_score_sharpe_median",
            "train_outperform_ratio",
        ]
    ].copy()
    table2.to_csv(output_dir / "table2_best_params.csv", index=False)

    rng = np.random.default_rng(int(args.seed))
    app_rows: list[dict[str, Any]] = []
    for industry, group in stock_oos_df.groupby("industry", sort=True):
        values = group["test_outperform"].astype(float).to_numpy()
        applicability = float(values.mean()) if len(values) > 0 else math.nan
        ci_low, ci_high, p_value = _bootstrap_applicability(values, b=int(args.bootstrap_b), rng=rng)
        app_rows.append(
            {
                "industry": industry,
                "n_stocks": int(len(values)),
                "applicability": applicability,
                "ci_low": ci_low,
                "ci_high": ci_high,
                "p_value": p_value,
            }
        )
    table3 = pd.DataFrame(app_rows).sort_values("industry").reset_index(drop=True)
    table3["bh_significant"] = _apply_bh_significance(table3["p_value"], q=0.05).astype(int)
    table3.to_csv(output_dir / "table3_applicability_oos.csv", index=False)

    print("[5/5] Writing figures and metadata...")
    fig1_df = table3.sort_values("applicability", ascending=False).reset_index(drop=True)
    fig, ax = plt.subplots(figsize=(14, 6))
    x = np.arange(len(fig1_df))
    y = fig1_df["applicability"].to_numpy(dtype=float)
    yerr_low = y - fig1_df["ci_low"].to_numpy(dtype=float)
    yerr_high = fig1_df["ci_high"].to_numpy(dtype=float) - y
    colors = ["#d9534f" if s == 1 else "#5bc0de" for s in fig1_df["bh_significant"].astype(int).tolist()]
    ax.bar(x, y, color=colors, alpha=0.9)
    ax.errorbar(x, y, yerr=[yerr_low, yerr_high], fmt="none", ecolor="black", capsize=3, linewidth=1)
    ax.set_xticks(x)
    ax.set_xticklabels(fig1_df["industry"], rotation=70, ha="right")
    ax.set_ylim(0.0, 1.0)
    ax.set_ylabel("Applicability (OOS Outperform Ratio)")
    ax.set_title("Figure 1. Industry Applicability with 95% CI")
    ax.grid(axis="y", alpha=0.2)
    fig.tight_layout()
    fig.savefig(output_dir / "figure1_applicability_bar.png", dpi=160)
    plt.close(fig)

    heat = (
        industry_best_df.groupby(["best_short", "best_long"])
        .size()
        .unstack(fill_value=0)
        .reindex(index=SHORT_WINDOWS, columns=LONG_WINDOWS, fill_value=0)
    )
    fig, ax = plt.subplots(figsize=(8, 5))
    im = ax.imshow(heat.values, cmap="YlOrRd")
    ax.set_xticks(np.arange(len(LONG_WINDOWS)))
    ax.set_yticks(np.arange(len(SHORT_WINDOWS)))
    ax.set_xticklabels(LONG_WINDOWS)
    ax.set_yticklabels(SHORT_WINDOWS)
    ax.set_xlabel("Long Window")
    ax.set_ylabel("Short Window")
    ax.set_title("Figure 2. Best Parameter Heatmap")
    for i in range(len(SHORT_WINDOWS)):
        for j in range(len(LONG_WINDOWS)):
            ax.text(j, i, str(int(heat.values[i, j])), ha="center", va="center", color="black")
    fig.colorbar(im, ax=ax, shrink=0.9, label="Industry Count")
    fig.tight_layout()
    fig.savefig(output_dir / "figure2_best_params_heatmap.png", dpi=160)
    plt.close(fig)

    metadata = {
        "run_at": pd.Timestamp.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "all_period": {"start": ALL_START, "end": ALL_END},
        "train_period": {"start": TRAIN_START, "end": TRAIN_END},
        "test_period": {"start": TEST_START, "end": TEST_END},
        "industry_target_date": args.industry_target_date,
        "industry_reference_date": snapshot_date,
        "fee_bps": float(args.fee_bps),
        "bootstrap_b": int(args.bootstrap_b),
        "seed": int(args.seed),
        "stock_count_total": int(len(industry_universe)),
        "stock_count_included": int((universe_df["included"] == 1).sum()),
        "download_success": int(download_df["status"].isin(["success", "skipped"]).sum()),
        "download_failed": int((download_df["status"] == "error").sum()),
        "download_skipped": int((download_df["status"] == "skipped").sum()),
        "include_price_daily": bool(args.include_price_daily),
        "table": str(args.table),
    }
    with (output_dir / "run_metadata.json").open("w", encoding="utf-8") as fp:
        json.dump(metadata, fp, ensure_ascii=False, indent=2)

    print(f"Done. Outputs at: {output_dir}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return run_pipeline(args)
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:  # pylint: disable=broad-except
        print(f"Error: {exc}", file=sys.stderr)
        return 4


if __name__ == "__main__":
    raise SystemExit(main())
