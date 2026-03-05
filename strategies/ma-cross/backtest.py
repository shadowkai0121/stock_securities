from __future__ import annotations

import argparse
import json
import math
import sqlite3
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import numpy as np
import pandas as pd

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_PATH = REPO_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from finmind_dl.core.config import resolve_token
from finmind_dl.core.date_utils import ensure_date_range, parse_iso_date
from finmind_dl.datasets import price, price_adj


TRADING_DAYS_PER_YEAR = 252
VALID_TABLES = {"price_daily", "price_adj_daily"}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ma-cross-backtest",
        description="MA crossover backtest (long/cash) for Taiwan stocks.",
    )
    parser.add_argument("--stock-id", required=True, help="Stock ID, e.g. 2330")
    parser.add_argument("--start-date", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="End date YYYY-MM-DD")
    parser.add_argument("--short-window", type=int, default=20, help="Short SMA window.")
    parser.add_argument("--long-window", type=int, default=60, help="Long SMA window.")
    parser.add_argument(
        "--table",
        default="price_adj_daily",
        choices=sorted(VALID_TABLES),
        help="SQLite price table.",
    )
    parser.add_argument(
        "--db-path",
        default=None,
        help="SQLite path. Default: data/<stock_id>.sqlite under repo root.",
    )
    parser.add_argument(
        "--ensure-data",
        action="store_true",
        help="Fetch data from FinMind before backtest.",
    )
    parser.add_argument(
        "--replace-db",
        action="store_true",
        help="Delete DB before fetching when --ensure-data is enabled.",
    )
    parser.add_argument(
        "--token",
        default=None,
        help="FinMind token. Falls back to env/.env when omitted.",
    )
    parser.add_argument(
        "--fee-bps",
        type=float,
        default=0.0,
        help="Cost per position-change event in basis points.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(REPO_ROOT / "strategies" / "ma-cross" / "outputs"),
        help="Output root directory.",
    )
    parser.add_argument(
        "--no-plot",
        action="store_true",
        help="Skip plot output.",
    )
    return parser


def resolve_db_path(stock_id: str, db_path: str | None) -> Path:
    if db_path:
        return Path(db_path)
    return REPO_ROOT / "data" / f"{stock_id}.sqlite"


def validate_args(args: argparse.Namespace) -> None:
    start_dt = parse_iso_date(args.start_date, "--start-date")
    end_dt = parse_iso_date(args.end_date, "--end-date")
    ensure_date_range(start_dt, end_dt, start_name="--start-date", end_name="--end-date")

    if args.short_window <= 0:
        raise ValueError("--short-window must be a positive integer.")
    if args.long_window <= 0:
        raise ValueError("--long-window must be a positive integer.")
    if args.long_window <= args.short_window:
        raise ValueError("--long-window must be greater than --short-window.")
    if args.fee_bps < 0:
        raise ValueError("--fee-bps must be greater than or equal to 0.")


def ensure_price_data(args: argparse.Namespace, db_path: Path) -> dict[str, Any]:
    token = resolve_token(args.token)
    dl_args = SimpleNamespace(
        stock_id=args.stock_id,
        start_date=args.start_date,
        end_date=args.end_date,
        db_path=str(db_path),
        replace=bool(args.replace_db),
    )
    if args.table == "price_adj_daily":
        return price_adj.run(dl_args, token)
    return price.run(dl_args, token)


def load_price_data(
    *,
    db_path: Path,
    table: str,
    stock_id: str,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    if table not in VALID_TABLES:
        raise ValueError(f"Invalid table '{table}'.")
    if not db_path.exists():
        raise ValueError(f"DB not found: {db_path}")

    sql = (
        f'SELECT date, stock_id, open, close, is_placeholder FROM "{table}" '
        "WHERE stock_id = ? AND date BETWEEN ? AND ? ORDER BY date"
    )

    try:
        conn = sqlite3.connect(db_path)
        try:
            df = pd.read_sql_query(sql, conn, params=[stock_id, start_date, end_date])
        finally:
            conn.close()
    except Exception as exc:
        raise ValueError(f"Failed to read table '{table}' from '{db_path}': {exc}") from exc

    if df.empty:
        raise ValueError(
            f"No data found in table '{table}' for stock '{stock_id}' between {start_date} and {end_date}."
        )

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["open"] = pd.to_numeric(df["open"], errors="coerce")
    df["is_placeholder"] = pd.to_numeric(df["is_placeholder"], errors="coerce").fillna(1).astype(int)

    df = df[(df["is_placeholder"] == 0) & df["date"].notna() & df["close"].notna()].copy()
    df = df.sort_values("date").drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)

    if df.empty:
        raise ValueError(
            f"No valid rows after filtering placeholders/null close for stock '{stock_id}' in table '{table}'."
        )

    return df[["date", "stock_id", "open", "close"]].copy()


def extract_trades(signal_df: pd.DataFrame, fee_bps: float) -> pd.DataFrame:
    trades: list[dict[str, Any]] = []
    open_idx: int | None = None
    prev_signal = 0
    total_cost_bps = 2.0 * fee_bps

    for row in signal_df.itertuples(index=True):
        idx = int(row.Index)
        signal = int(row.signal)

        if prev_signal == 0 and signal == 1:
            open_idx = idx
        elif prev_signal == 1 and signal == 0 and open_idx is not None:
            entry = signal_df.iloc[open_idx]
            exit_row = signal_df.iloc[idx]
            gross_return = (float(exit_row["close"]) / float(entry["close"])) - 1.0
            trade = {
                "entry_date": entry["date"],
                "entry_close": float(entry["close"]),
                "exit_date": exit_row["date"],
                "exit_close": float(exit_row["close"]),
                "holding_days": int((exit_row["date"] - entry["date"]).days),
                "gross_return": gross_return,
                "exit_reason": "signal",
            }
            if fee_bps > 0:
                trade["total_cost_bps"] = total_cost_bps
                trade["est_net_return"] = gross_return - (total_cost_bps / 10000.0)
            trades.append(trade)
            open_idx = None

        prev_signal = signal

    if open_idx is not None and not signal_df.empty:
        entry = signal_df.iloc[open_idx]
        exit_row = signal_df.iloc[-1]
        gross_return = (float(exit_row["close"]) / float(entry["close"])) - 1.0
        trade = {
            "entry_date": entry["date"],
            "entry_close": float(entry["close"]),
            "exit_date": exit_row["date"],
            "exit_close": float(exit_row["close"]),
            "holding_days": int((exit_row["date"] - entry["date"]).days),
            "gross_return": gross_return,
            "exit_reason": "eod",
        }
        if fee_bps > 0:
            trade["total_cost_bps"] = total_cost_bps
            trade["est_net_return"] = gross_return - (total_cost_bps / 10000.0)
        trades.append(trade)

    columns = [
        "entry_date",
        "entry_close",
        "exit_date",
        "exit_close",
        "holding_days",
        "gross_return",
        "exit_reason",
    ]
    if fee_bps > 0:
        columns += ["total_cost_bps", "est_net_return"]

    if not trades:
        return pd.DataFrame(columns=columns)
    return pd.DataFrame(trades)


def run_backtest(
    *,
    price_df: pd.DataFrame,
    short_window: int,
    long_window: int,
    fee_bps: float,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = price_df.sort_values("date").reset_index(drop=True).copy()

    df["short_ma"] = df["close"].rolling(window=short_window, min_periods=short_window).mean()
    df["long_ma"] = df["close"].rolling(window=long_window, min_periods=long_window).mean()
    df["signal"] = (df["short_ma"] > df["long_ma"]).astype(int)
    df.loc[df["short_ma"].isna() | df["long_ma"].isna(), "signal"] = 0

    df["position"] = df["signal"].shift(1).fillna(0).astype(int)
    df["ret"] = df["close"].pct_change()

    fee_ratio = fee_bps / 10000.0
    df["trade_event"] = df["signal"].diff().abs().fillna(0.0)
    df["cost"] = df["trade_event"].shift(1).fillna(0.0) * fee_ratio
    df["strategy_ret"] = (df["position"] * df["ret"]) - df["cost"]

    df["equity"] = (1.0 + df["strategy_ret"].fillna(0.0)).cumprod()
    df["bh_equity"] = (1.0 + df["ret"].fillna(0.0)).cumprod()

    trades_df = extract_trades(df, fee_bps=fee_bps)
    return df, trades_df


def _calc_cagr(equity: pd.Series) -> float:
    if equity.empty:
        return math.nan
    start_value = float(equity.iloc[0])
    end_value = float(equity.iloc[-1])
    periods = len(equity) - 1
    if periods <= 0 or start_value <= 0 or end_value <= 0:
        return math.nan
    years = periods / TRADING_DAYS_PER_YEAR
    if years <= 0:
        return math.nan
    return (end_value / start_value) ** (1.0 / years) - 1.0


def _calc_max_drawdown(equity: pd.Series) -> float:
    if equity.empty:
        return math.nan
    rolling_max = equity.cummax()
    drawdown = (equity / rolling_max) - 1.0
    return float(drawdown.min())


def compute_metrics(backtest_df: pd.DataFrame, trades_df: pd.DataFrame) -> dict[str, float | int]:
    ret = backtest_df["strategy_ret"].dropna()
    bh_ret = backtest_df["ret"].dropna()

    ret_std = float(ret.std(ddof=0)) if not ret.empty else math.nan
    mean_ret = float(ret.mean()) if not ret.empty else math.nan
    bh_std = float(bh_ret.std(ddof=0)) if not bh_ret.empty else math.nan

    ann_vol = ret_std * math.sqrt(TRADING_DAYS_PER_YEAR) if np.isfinite(ret_std) else math.nan
    sharpe = (
        (mean_ret / ret_std) * math.sqrt(TRADING_DAYS_PER_YEAR)
        if np.isfinite(ret_std) and ret_std > 0
        else math.nan
    )

    trade_count = int(len(trades_df))
    if trade_count > 0:
        win_rate = float((trades_df["gross_return"] > 0).mean())
        avg_trade_return = float(trades_df["gross_return"].mean())
    else:
        win_rate = math.nan
        avg_trade_return = math.nan

    return {
        "rows": int(len(backtest_df)),
        "trade_count": trade_count,
        "total_return": float(backtest_df["equity"].iloc[-1] - 1.0),
        "buy_hold_return": float(backtest_df["bh_equity"].iloc[-1] - 1.0),
        "cagr": _calc_cagr(backtest_df["equity"]),
        "buy_hold_cagr": _calc_cagr(backtest_df["bh_equity"]),
        "max_drawdown": _calc_max_drawdown(backtest_df["equity"]),
        "buy_hold_max_drawdown": _calc_max_drawdown(backtest_df["bh_equity"]),
        "annualized_volatility": ann_vol,
        "buy_hold_annualized_volatility": (
            bh_std * math.sqrt(TRADING_DAYS_PER_YEAR) if np.isfinite(bh_std) else math.nan
        ),
        "sharpe": sharpe,
        "win_rate": win_rate,
        "avg_trade_return": avg_trade_return,
    }


def _format_pct(value: float | int) -> str:
    if value is None or not np.isfinite(value):
        return "NaN"
    return f"{float(value) * 100:.2f}%"


def _serialize_dates(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    out = frame.copy()
    for col in columns:
        if col in out:
            out[col] = pd.to_datetime(out[col], errors="coerce").dt.strftime("%Y-%m-%d")
    return out


def write_outputs(
    *,
    args: argparse.Namespace,
    backtest_df: pd.DataFrame,
    trades_df: pd.DataFrame,
    metrics: dict[str, float | int],
) -> Path:
    run_name = (
        f"{args.stock_id}_{args.start_date}_{args.end_date}"
        f"_s{args.short_window}_l{args.long_window}_{args.table}"
    )
    run_dir = Path(args.output_dir) / run_name
    run_dir.mkdir(parents=True, exist_ok=True)

    equity_cols = [
        "date",
        "close",
        "short_ma",
        "long_ma",
        "signal",
        "position",
        "ret",
        "cost",
        "strategy_ret",
        "equity",
        "bh_equity",
    ]
    equity_df = _serialize_dates(backtest_df[equity_cols], columns=["date"])
    equity_df.to_csv(run_dir / "equity.csv", index=False)

    trades_out = _serialize_dates(trades_df, columns=["entry_date", "exit_date"])
    trades_out.to_csv(run_dir / "trades.csv", index=False)

    report = {
        "config": {
            "stock_id": args.stock_id,
            "start_date": args.start_date,
            "end_date": args.end_date,
            "short_window": args.short_window,
            "long_window": args.long_window,
            "table": args.table,
            "db_path": str(resolve_db_path(args.stock_id, args.db_path)),
            "ensure_data": bool(args.ensure_data),
            "replace_db": bool(args.replace_db),
            "fee_bps": float(args.fee_bps),
            "no_plot": bool(args.no_plot),
        },
        "metrics": metrics,
    }
    with (run_dir / "report.json").open("w", encoding="utf-8") as fp:
        json.dump(report, fp, ensure_ascii=False, indent=2, allow_nan=True)

    if not args.no_plot:
        plot_results(backtest_df, trades_df, run_dir / "plot.png")

    return run_dir


def plot_results(backtest_df: pd.DataFrame, trades_df: pd.DataFrame, output_path: Path) -> None:
    fig, (ax_price, ax_equity) = plt.subplots(
        2,
        1,
        figsize=(12, 8),
        sharex=True,
        gridspec_kw={"height_ratios": [2, 1]},
    )

    ax_price.plot(backtest_df["date"], backtest_df["close"], label="Close", linewidth=1.2)
    ax_price.plot(backtest_df["date"], backtest_df["short_ma"], label="Short SMA", linewidth=1.0)
    ax_price.plot(backtest_df["date"], backtest_df["long_ma"], label="Long SMA", linewidth=1.0)

    if not trades_df.empty:
        entries = pd.to_datetime(trades_df["entry_date"], errors="coerce")
        exits = pd.to_datetime(trades_df["exit_date"], errors="coerce")
        ax_price.scatter(entries, trades_df["entry_close"], marker="^", s=40, label="Entry")
        ax_price.scatter(exits, trades_df["exit_close"], marker="v", s=40, label="Exit")

    ax_price.set_title("Price and Moving Averages")
    ax_price.legend(loc="best")
    ax_price.grid(alpha=0.2)

    ax_equity.plot(backtest_df["date"], backtest_df["equity"], label="Strategy", linewidth=1.2)
    ax_equity.plot(backtest_df["date"], backtest_df["bh_equity"], label="Buy & Hold", linewidth=1.2)
    ax_equity.set_title("Equity Curve")
    ax_equity.legend(loc="best")
    ax_equity.grid(alpha=0.2)

    fig.tight_layout()
    fig.savefig(output_path, dpi=150)
    plt.close(fig)


def print_summary(args: argparse.Namespace, metrics: dict[str, float | int], run_dir: Path) -> None:
    print(f"Stock: {args.stock_id}")
    print(f"Date range: {args.start_date} to {args.end_date}")
    print(f"Table: {args.table}")
    print(f"MA windows: short={args.short_window}, long={args.long_window}")
    print(f"Trades: {metrics['trade_count']}")
    print(f"Total return: {_format_pct(float(metrics['total_return']))}")
    print(f"Buy/Hold return: {_format_pct(float(metrics['buy_hold_return']))}")
    print(f"CAGR: {_format_pct(float(metrics['cagr']))}")
    print(f"Max drawdown: {_format_pct(float(metrics['max_drawdown']))}")
    print(f"Annualized vol: {_format_pct(float(metrics['annualized_volatility']))}")
    sharpe = metrics["sharpe"]
    sharpe_text = "NaN" if sharpe is None or not np.isfinite(sharpe) else f"{float(sharpe):.4f}"
    print(f"Sharpe (rf=0): {sharpe_text}")
    print(f"Output dir: {run_dir}")


def run(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    validate_args(args)

    db_path = resolve_db_path(args.stock_id, args.db_path)

    if args.ensure_data:
        ensure_result = ensure_price_data(args, db_path)
        print(
            "[ensure-data] "
            f"dataset={ensure_result.get('dataset')} inserted={ensure_result.get('inserted_rows')}"
        )

    price_df = load_price_data(
        db_path=db_path,
        table=args.table,
        stock_id=args.stock_id,
        start_date=args.start_date,
        end_date=args.end_date,
    )
    backtest_df, trades_df = run_backtest(
        price_df=price_df,
        short_window=args.short_window,
        long_window=args.long_window,
        fee_bps=args.fee_bps,
    )
    metrics = compute_metrics(backtest_df, trades_df)
    run_dir = write_outputs(args=args, backtest_df=backtest_df, trades_df=trades_df, metrics=metrics)
    print_summary(args, metrics, run_dir)
    return 0


def main(argv: list[str] | None = None) -> int:
    try:
        return run(argv)
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2
    except sqlite3.Error as exc:
        print(f"Error: SQLite failure: {exc}", file=sys.stderr)
        return 4
    except Exception as exc:  # pylint: disable=broad-except
        print(f"Error: {exc}", file=sys.stderr)
        return 4


if __name__ == "__main__":
    raise SystemExit(main())
