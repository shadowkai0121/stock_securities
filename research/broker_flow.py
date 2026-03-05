from __future__ import annotations

import math
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

DEFAULT_BUY_COST = 0.002425
DEFAULT_SELL_COST = 0.005425


def find_repo_root(start: Path) -> Path:
    start = start.resolve()
    for candidate in [start, *start.parents]:
        if (candidate / ".git").exists():
            return candidate
    return start


def resolve_stock_db_path(stock_id: str, base_dir: Path) -> Path:
    return (base_dir / f"{stock_id}.sqlite").resolve()


def _read_sql(
    conn: sqlite3.Connection,
    query: str,
    params: tuple[object, ...] = (),
) -> pd.DataFrame:
    return pd.read_sql_query(query, conn, params=params)


def load_stock_price_table(
    db_path: Path,
    table_name: str,
    stock_id: str,
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    where: list[str] = ["stock_id = ?"]
    params: list[object] = [stock_id]
    if start_date:
        where.append("date >= ?")
        params.append(start_date)
    if end_date:
        where.append("date <= ?")
        params.append(end_date)
    where_sql = " AND ".join(where)

    query = f"""
        SELECT
            date,
            stock_id,
            open,
            "max" AS high,
            "min" AS low,
            close,
            Trading_Volume AS volume,
            Trading_money AS money,
            spread,
            Trading_turnover AS turnover
        FROM "{table_name}"
        WHERE {where_sql}
        ORDER BY date
    """

    conn = sqlite3.connect(db_path)
    try:
        df = _read_sql(conn, query, params=tuple(params))
    finally:
        conn.close()

    if df.empty:
        return df

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).set_index("date").sort_index()
    return df


@dataclass(frozen=True)
class BrokerDailyRow:
    date: pd.Timestamp
    broker_id: str
    broker_name: str
    total_buy: float
    total_sell: float
    net_volume: float
    buy_amount: float
    sell_amount: float


def load_broker_daily_agg(
    db_path: Path,
    start_date: str | None = None,
    end_date: str | None = None,
    *,
    exclude_no_data: bool = True,
    cache_path: Path | None = None,
    show_progress: bool = True,
) -> pd.DataFrame:
    if cache_path and cache_path.exists():
        df = pd.read_parquet(cache_path)
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        return df.dropna(subset=["date"]).sort_values(["date", "broker_id"])

    conn = sqlite3.connect(db_path)
    try:
        broker_tables = _read_sql(
            conn,
            """
            SELECT securities_trader_id AS broker_id,
                   securities_trader AS broker_name,
                   table_name
            FROM broker_tables
            ORDER BY securities_trader_id
            """,
        )
        if exclude_no_data:
            broker_tables = broker_tables[broker_tables["broker_id"] != "__NO_DATA__"]

        if broker_tables.empty:
            return pd.DataFrame(
                columns=[
                    "date",
                    "broker_id",
                    "broker_name",
                    "total_buy",
                    "total_sell",
                    "net_volume",
                    "buy_amount",
                    "sell_amount",
                ]
            )

        where: list[str] = []
        params: list[object] = []
        if start_date:
            where.append("date >= ?")
            params.append(start_date)
        if end_date:
            where.append("date <= ?")
            params.append(end_date)
        where_sql = f"WHERE {' AND '.join(where)}" if where else ""

        def _iter_rows() -> Iterable[BrokerDailyRow]:
            iterable = broker_tables.itertuples(index=False)
            if show_progress:
                try:
                    from tqdm import tqdm  # type: ignore[import-not-found]

                    iterable = tqdm(list(iterable), desc="Aggregate brokers", unit="broker")
                except Exception:
                    pass

            for broker_id, broker_name, table_name in iterable:
                query = f"""
                    SELECT
                        date,
                        COALESCE(SUM(buy), 0) AS total_buy,
                        COALESCE(SUM(sell), 0) AS total_sell,
                        COALESCE(SUM(price * buy), 0) AS buy_amount,
                        COALESCE(SUM(price * sell), 0) AS sell_amount
                    FROM "{table_name}"
                    {where_sql}
                    GROUP BY date
                    ORDER BY date
                """
                rows = conn.execute(query, tuple(params)).fetchall()
                for date_str, total_buy, total_sell, buy_amount, sell_amount in rows:
                    date_ts = pd.to_datetime(date_str, errors="coerce")
                    if pd.isna(date_ts):
                        continue
                    total_buy_f = float(total_buy or 0.0)
                    total_sell_f = float(total_sell or 0.0)
                    yield BrokerDailyRow(
                        date=date_ts,
                        broker_id=str(broker_id),
                        broker_name=str(broker_name),
                        total_buy=total_buy_f,
                        total_sell=total_sell_f,
                        net_volume=total_buy_f - total_sell_f,
                        buy_amount=float(buy_amount or 0.0),
                        sell_amount=float(sell_amount or 0.0),
                    )

        data = list(_iter_rows())
    finally:
        conn.close()

    df = pd.DataFrame([row.__dict__ for row in data])
    if df.empty:
        return df

    df = df.sort_values(["date", "broker_id"]).reset_index(drop=True)
    if cache_path:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(cache_path, index=False)
    return df


def rolling_percentile_rank(
    series: pd.Series,
    window: int,
    *,
    min_periods: int = 30,
) -> pd.Series:
    values = series.astype(float).to_numpy()
    out = np.full(values.shape, np.nan, dtype=float)
    for i in range(len(values)):
        current = values[i]
        if not np.isfinite(current):
            continue
        start = max(0, i - window + 1)
        w = values[start : i + 1]
        w = w[np.isfinite(w)]
        if len(w) < min_periods:
            continue
        out[i] = float(np.mean(w <= current))
    return pd.Series(out, index=series.index, name=f"{series.name}_rank{window}")


def build_day_features(
    price_raw: pd.DataFrame,
    price_adj: pd.DataFrame,
    broker_daily: pd.DataFrame,
    *,
    top_k: int = 20,
) -> pd.DataFrame:
    if price_adj.empty:
        raise ValueError("price_adj is empty; cannot build features.")

    df = price_adj.rename(
        columns={
            "open": "open_adj",
            "high": "high_adj",
            "low": "low_adj",
            "close": "close_adj",
            "volume": "volume_adj",
            "money": "money_adj",
            "turnover": "turnover_adj",
            "spread": "spread_adj",
        }
    )[
        [
            "stock_id",
            "open_adj",
            "high_adj",
            "low_adj",
            "close_adj",
            "volume_adj",
            "money_adj",
            "spread_adj",
            "turnover_adj",
        ]
    ].copy()

    if not price_raw.empty:
        raw_cols = price_raw.rename(
            columns={
                "open": "open_raw",
                "high": "high_raw",
                "low": "low_raw",
                "close": "close_raw",
                "volume": "volume_raw",
                "money": "money_raw",
                "turnover": "turnover_raw",
                "spread": "spread_raw",
            }
        )[
            [
                "open_raw",
                "high_raw",
                "low_raw",
                "close_raw",
                "volume_raw",
                "money_raw",
                "spread_raw",
                "turnover_raw",
            ]
        ]
        df = df.join(raw_cols, how="left")

    if broker_daily.empty:
        broker_features = pd.DataFrame(index=df.index)
    else:
        broker_daily = broker_daily.copy()
        broker_daily["date"] = pd.to_datetime(broker_daily["date"], errors="coerce")
        broker_daily = broker_daily.dropna(subset=["date"])
        broker_daily = broker_daily.set_index("date")

        g = broker_daily.groupby(level=0, sort=True)
        sum_buy = g["total_buy"].sum()
        sum_sell = g["total_sell"].sum()
        sum_net = g["net_volume"].sum()
        net_std = g["net_volume"].std(ddof=0)

        active = ((broker_daily["total_buy"] + broker_daily["total_sell"]) > 0).astype(int)
        active_brokers = active.groupby(level=0).sum()

        buy_sum = g["total_buy"].transform("sum")
        sell_sum = g["total_sell"].transform("sum")
        buy_share_sq = np.where(buy_sum > 0, (broker_daily["total_buy"] / buy_sum) ** 2, np.nan)
        sell_share_sq = np.where(sell_sum > 0, (broker_daily["total_sell"] / sell_sum) ** 2, np.nan)
        buy_hhi = pd.Series(buy_share_sq, index=broker_daily.index).groupby(level=0).sum(
            min_count=1
        )
        sell_hhi = pd.Series(sell_share_sq, index=broker_daily.index).groupby(level=0).sum(
            min_count=1
        )

        def _top_k_net_sum(s: pd.Series) -> float:
            v = s.to_numpy(dtype=float)
            v = v[np.isfinite(v)]
            if len(v) == 0:
                return float("nan")
            if len(v) <= top_k:
                return float(v.sum())
            return float(np.partition(v, -top_k)[-top_k:].sum())

        topk_net = g["net_volume"].apply(_top_k_net_sum)

        broker_features = pd.DataFrame(
            {
                "sum_buy": sum_buy,
                "sum_sell": sum_sell,
                "sum_net": sum_net,
                "active_brokers": active_brokers,
                "buy_hhi": buy_hhi,
                "sell_hhi": sell_hhi,
                "topk_net": topk_net,
                "net_std": net_std,
            }
        )

    df = df.join(broker_features, how="left")

    trading_volume = df["volume_adj"].astype(float)
    df["topk_net_share"] = df["topk_net"] / trading_volume
    df["net_std_share"] = df["net_std"] / trading_volume
    df["net_consistency"] = (df["sum_net"].abs() / trading_volume).astype(float)
    df["buy_sell_ratio"] = df["sum_buy"] / df["sum_sell"]

    close = df["close_adj"].astype(float)
    df["ret_1"] = close.pct_change(1)
    df["ret_3"] = close.pct_change(3)
    df["ret_5"] = close.pct_change(5)
    df["ret_20"] = close.pct_change(20)
    df["vol_5"] = df["ret_1"].rolling(5).std(ddof=0)
    df["vol_20"] = df["ret_1"].rolling(20).std(ddof=0)

    df["hl_range"] = (df["high_adj"] - df["low_adj"]) / df["close_adj"]
    df["oc_return"] = (df["close_adj"] - df["open_adj"]) / df["open_adj"]
    df["log_vol"] = np.log(df["volume_adj"].where(df["volume_adj"] > 0))

    vol_roll = df["volume_adj"].rolling(20)
    df["vol_z20"] = (df["volume_adj"] - vol_roll.mean()) / vol_roll.std(ddof=0)

    df["topk_net_share_3d"] = df["topk_net_share"].rolling(3).sum()

    df["volume_med20"] = df["volume_adj"].rolling(20).median()

    if "close_raw" in df.columns:
        df["prev_max20_raw"] = df["close_raw"].shift(1).rolling(20).max()

    return df


def make_rule_signals(features: pd.DataFrame) -> dict[str, pd.Series]:
    tradeable = (
        (features["volume_adj"].fillna(0) > 0)
        & features["topk_net_share"].notna()
        & features["ret_5"].notna()
    )

    topk_rank120 = rolling_percentile_rank(features["topk_net_share"], 120, min_periods=60)
    buy_hhi_rank120 = rolling_percentile_rank(features["buy_hhi"], 120, min_periods=60)

    r1 = (
        tradeable
        & (topk_rank120 >= 0.8)
        & (features["ret_5"] > 0)
        & (features["volume_adj"] > features["volume_med20"])
    )

    breakout = pd.Series(False, index=features.index)
    if "close_raw" in features.columns and "prev_max20_raw" in features.columns:
        breakout = features["close_raw"].notna() & (
            features["close_raw"] > features["prev_max20_raw"]
        )

    r2 = tradeable & (buy_hhi_rank120 >= 0.8) & (features["topk_net_share"] > 0) & breakout

    return {
        "R1_FlowBreakout": r1.astype(bool),
        "R2_ConcentrationBreakout": r2.astype(bool),
    }


@dataclass
class BacktestResult:
    equity: pd.Series
    daily_returns: pd.Series
    trades: pd.DataFrame
    metrics: dict[str, float]


def _max_drawdown(equity: pd.Series) -> float:
    running_max = equity.cummax()
    drawdown = equity / running_max - 1.0
    return float(drawdown.min())


def backtest_fixed_hold(
    features: pd.DataFrame,
    signal: pd.Series,
    *,
    hold_days: int,
    buy_cost: float = DEFAULT_BUY_COST,
    sell_cost: float = DEFAULT_SELL_COST,
    open_col: str = "open_adj",
    close_col: str = "close_adj",
) -> BacktestResult:
    if hold_days < 1:
        raise ValueError("hold_days must be >= 1")

    df = features[[open_col, close_col]].copy()
    df["signal"] = signal.reindex(df.index).fillna(False).astype(bool)
    df = df.dropna(subset=[open_col, close_col])
    if df.empty:
        return BacktestResult(
            equity=pd.Series(dtype=float),
            daily_returns=pd.Series(dtype=float),
            trades=pd.DataFrame(),
            metrics={},
        )

    dates = df.index.to_list()
    open_px = df[open_col].astype(float).to_numpy()
    close_px = df[close_col].astype(float).to_numpy()
    sig = df["signal"].to_numpy(dtype=bool)

    cash = 1.0
    shares = 0.0
    pending_entry: int | None = None
    scheduled_exit: int | None = None
    entry_cash: float | None = None
    entry_price: float | None = None
    entry_date: pd.Timestamp | None = None

    equity_curve: list[float] = []
    in_pos: list[int] = []
    trade_rows: list[dict[str, object]] = []

    for i in range(len(dates)):
        # Execute entry at open
        if pending_entry == i:
            px = open_px[i]
            cash_before = cash
            shares = cash_before / (px * (1.0 + buy_cost))
            cash = 0.0
            entry_cash = cash_before
            entry_price = px
            entry_date = dates[i]

        # Mark-to-market at close (and execute exit at close if scheduled)
        if shares > 0.0:
            if scheduled_exit == i:
                exit_px = close_px[i]
                cash = shares * exit_px * (1.0 - sell_cost)
                shares = 0.0

                if entry_cash is not None and entry_price is not None and entry_date is not None:
                    trade_rows.append(
                        {
                            "entry_date": entry_date,
                            "exit_date": dates[i],
                            "hold_days": hold_days,
                            "entry_open": float(entry_price),
                            "exit_close": float(exit_px),
                            "buy_cost": buy_cost,
                            "sell_cost": sell_cost,
                            "gross_return": float(exit_px / entry_price - 1.0),
                            "net_return": float(cash / entry_cash - 1.0),
                        }
                    )

                pending_entry = None
                scheduled_exit = None
                entry_cash = None
                entry_price = None
                entry_date = None

            equity_close = cash if shares == 0.0 else float(shares * close_px[i])
            in_pos.append(1)
        else:
            equity_close = float(cash)
            in_pos.append(0)

        # After close, decide whether to schedule a new trade (only if flat and no pending)
        if shares == 0.0 and pending_entry is None and sig[i]:
            if (i + 1) < len(dates) and (i + hold_days) < len(dates):
                pending_entry = i + 1
                scheduled_exit = i + hold_days

        equity_curve.append(float(equity_close))

    equity = pd.Series(equity_curve, index=dates, name="equity")
    daily_ret = equity.pct_change().fillna(0.0).rename("daily_return")

    trades = pd.DataFrame(trade_rows)
    if not trades.empty:
        trades["entry_date"] = pd.to_datetime(trades["entry_date"])
        trades["exit_date"] = pd.to_datetime(trades["exit_date"])

    total_days = len(equity)
    years = total_days / 252.0 if total_days > 0 else float("nan")
    cagr = float(equity.iloc[-1] ** (1.0 / years) - 1.0) if years and years > 0 else float("nan")
    vol = float(daily_ret.std(ddof=0))
    sharpe = float(math.sqrt(252.0) * daily_ret.mean() / vol) if vol > 0 else float("nan")
    max_dd = _max_drawdown(equity)
    exposure = float(np.mean(in_pos)) if in_pos else float("nan")

    win_rate = float((trades["net_return"] > 0).mean()) if not trades.empty else float("nan")
    avg_trade = float(trades["net_return"].mean()) if not trades.empty else float("nan")

    metrics = {
        "final_equity": float(equity.iloc[-1]),
        "cagr": cagr,
        "sharpe": sharpe,
        "max_drawdown": max_dd,
        "trades": float(len(trades)),
        "win_rate": win_rate,
        "avg_trade_return": avg_trade,
        "exposure": exposure,
    }

    return BacktestResult(equity=equity, daily_returns=daily_ret, trades=trades, metrics=metrics)


def compute_forward_return(
    features: pd.DataFrame,
    *,
    hold_days: int,
    buy_cost: float = DEFAULT_BUY_COST,
    sell_cost: float = DEFAULT_SELL_COST,
    open_col: str = "open_adj",
    close_col: str = "close_adj",
) -> pd.Series:
    open_next = features[open_col].shift(-1).astype(float)
    close_exit = features[close_col].shift(-hold_days).astype(float)
    y = (close_exit * (1.0 - sell_cost)) / (open_next * (1.0 + buy_cost)) - 1.0
    y.name = f"fwd_net_ret_h{hold_days}"
    return y


def walk_forward_ridge_predict(
    features: pd.DataFrame,
    target: pd.Series,
    *,
    feature_cols: list[str],
    hold_days: int,
    train_window: int = 504,
    min_train: int = 252,
    alpha: float = 1.0,
    show_progress: bool = True,
) -> pd.Series:
    df = features[feature_cols].copy()
    y = target.reindex(df.index).astype(float)
    n = len(df)
    preds = np.full(n, np.nan, dtype=float)

    model = Pipeline(
        steps=[
            ("scaler", StandardScaler()),
            ("ridge", Ridge(alpha=alpha)),
        ]
    )

    iterator: Iterable[int] = range(n)
    if show_progress:
        try:
            from tqdm import tqdm  # type: ignore[import-not-found]

            iterator = tqdm(iterator, desc=f"Walk-forward Ridge h={hold_days}", unit="day")
        except Exception:
            pass

    for i in iterator:
        # Need future prices for this decision day to be tradable.
        if (i + 1) >= n or (i + hold_days) >= n:
            continue

        # At time i (after close), the newest labeled sample we can use is i - hold_days.
        train_end = i - hold_days
        if train_end < (min_train - 1):
            continue

        train_start = max(0, train_end - train_window + 1)

        X_train = df.iloc[train_start : train_end + 1]
        y_train = y.iloc[train_start : train_end + 1]

        train_mask = y_train.notna() & X_train.notna().all(axis=1)
        X_train = X_train[train_mask]
        y_train = y_train[train_mask]
        if len(X_train) < min_train:
            continue

        model.fit(X_train.to_numpy(), y_train.to_numpy())

        X_pred = df.iloc[[i]]
        if X_pred.notna().all(axis=1).iloc[0] is False:
            continue
        preds[i] = float(model.predict(X_pred.to_numpy())[0])

    return pd.Series(preds, index=df.index, name=f"ridge_pred_h{hold_days}")

