"""Generate paper-ready figures from run and inference artifacts."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import matplotlib
import numpy as np
import pandas as pd

matplotlib.use("Agg")
import matplotlib.pyplot as plt


def _prepare_timeseries(backtest_timeseries: pd.DataFrame) -> pd.DataFrame:
    frame = backtest_timeseries.copy()
    if "date" in frame.columns:
        frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame = frame[frame.get("date").notna()] if "date" in frame.columns else frame
    return frame.sort_values("date").reset_index(drop=True) if "date" in frame.columns else frame


def _plot_cumulative_return_curve(frame: pd.DataFrame, output_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(10, 4))
    equity = pd.to_numeric(frame.get("equity"), errors="coerce")
    if equity.notna().any():
        ax.plot(frame["date"], equity, label="Strategy")
    if "benchmark_equity" in frame.columns and pd.to_numeric(frame["benchmark_equity"], errors="coerce").notna().any():
        ax.plot(frame["date"], pd.to_numeric(frame["benchmark_equity"], errors="coerce"), label="Benchmark")
    ax.set_title("Cumulative Return Curve")
    ax.grid(alpha=0.2)
    ax.legend(loc="best")
    fig.tight_layout()
    fig.savefig(output_path, dpi=150)
    plt.close(fig)


def _plot_rolling_performance(frame: pd.DataFrame, output_path: Path) -> None:
    returns = pd.to_numeric(frame.get("net_return"), errors="coerce")
    rolling_mean = returns.rolling(window=20, min_periods=20).mean()
    rolling_vol = returns.rolling(window=20, min_periods=20).std(ddof=0)
    rolling_sharpe = np.where(rolling_vol > 0, rolling_mean / rolling_vol * np.sqrt(252.0), np.nan)

    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot(frame["date"], rolling_mean, label="20D Mean Return")
    ax.plot(frame["date"], rolling_sharpe, label="20D Sharpe")
    ax.set_title("Rolling Performance")
    ax.grid(alpha=0.2)
    ax.legend(loc="best")
    fig.tight_layout()
    fig.savefig(output_path, dpi=150)
    plt.close(fig)


def _plot_coefficient_summary(inference_results: dict[str, Any], output_path: Path) -> None:
    rows = list(inference_results.get("fama_macbeth", {}).get("summary", []))
    frame = pd.DataFrame(rows)
    if frame.empty:
        fig, ax = plt.subplots(figsize=(8, 3))
        ax.text(0.5, 0.5, "No coefficient estimates", ha="center", va="center")
        ax.axis("off")
        fig.tight_layout()
        fig.savefig(output_path, dpi=150)
        plt.close(fig)
        return

    frame = frame[frame["variable"] != "const"].copy()
    if frame.empty:
        frame = pd.DataFrame(rows).head(6)

    y_pos = np.arange(len(frame))
    coef = pd.to_numeric(frame["coef"], errors="coerce")
    se = pd.to_numeric(frame["std_err"], errors="coerce").fillna(0.0)

    fig, ax = plt.subplots(figsize=(8, max(3, len(frame) * 0.5)))
    ax.errorbar(coef, y_pos, xerr=1.96 * se, fmt="o", capsize=3)
    ax.axvline(0.0, color="black", linewidth=1.0, linestyle="--")
    ax.set_yticks(y_pos)
    ax.set_yticklabels(frame["variable"].astype(str))
    ax.set_title("Coefficient Plot (Fama-MacBeth)")
    ax.grid(alpha=0.2, axis="x")
    fig.tight_layout()
    fig.savefig(output_path, dpi=150)
    plt.close(fig)


def _plot_portfolio_spread(inference_results: dict[str, Any], output_path: Path) -> None:
    spread = pd.DataFrame(list(inference_results.get("portfolio_sort", {}).get("equal_weight", {}).get("spread_returns", [])))
    if spread.empty:
        fig, ax = plt.subplots(figsize=(8, 3))
        ax.text(0.5, 0.5, "No portfolio spread series", ha="center", va="center")
        ax.axis("off")
        fig.tight_layout()
        fig.savefig(output_path, dpi=150)
        plt.close(fig)
        return

    spread["date"] = pd.to_datetime(spread["date"], errors="coerce")
    spread = spread.dropna(subset=["date"]).sort_values("date")
    spread["cum_spread"] = (1.0 + pd.to_numeric(spread["long_short"], errors="coerce").fillna(0.0)).cumprod()

    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot(spread["date"], spread["cum_spread"], label="Long-Short")
    ax.set_title("Portfolio Spread Chart")
    ax.grid(alpha=0.2)
    ax.legend(loc="best")
    fig.tight_layout()
    fig.savefig(output_path, dpi=150)
    plt.close(fig)


def write_figures(
    *,
    backtest_timeseries: pd.DataFrame,
    inference_results: dict[str, Any],
    figures_dir: str | Path,
) -> dict[str, str]:
    """Generate all required paper figure artifacts."""

    output_dir = Path(figures_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    frame = _prepare_timeseries(backtest_timeseries)

    cumulative_path = output_dir / "figure_cumulative_returns.png"
    coefficient_path = output_dir / "figure_coefficients.png"
    spread_path = output_dir / "figure_portfolio_spread.png"
    rolling_path = output_dir / "figure_rolling_performance.png"

    if not frame.empty and {"date", "equity"}.issubset(frame.columns):
        _plot_cumulative_return_curve(frame, cumulative_path)
        _plot_rolling_performance(frame, rolling_path)
    else:
        for path in (cumulative_path, rolling_path):
            fig, ax = plt.subplots(figsize=(8, 3))
            ax.text(0.5, 0.5, "No backtest timeseries available", ha="center", va="center")
            ax.axis("off")
            fig.tight_layout()
            fig.savefig(path, dpi=150)
            plt.close(fig)

    _plot_coefficient_summary(inference_results, coefficient_path)
    _plot_portfolio_spread(inference_results, spread_path)

    return {
        "cumulative_returns": str(cumulative_path),
        "coefficient_plot": str(coefficient_path),
        "portfolio_spread": str(spread_path),
        "rolling_performance": str(rolling_path),
    }


def write_appendix_figures(*, inference_results: dict[str, Any], appendix_dir: str | Path) -> dict[str, str]:
    """Generate additional appendix figures (event-study CAAR)."""

    output_dir = Path(appendix_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    caar_path = output_dir / "appendix_event_study_caar.png"

    summary = pd.DataFrame(list(inference_results.get("event_study", {}).get("window_summary", [])))
    if summary.empty:
        fig, ax = plt.subplots(figsize=(8, 3))
        ax.text(0.5, 0.5, "No event-study window summary", ha="center", va="center")
        ax.axis("off")
        fig.tight_layout()
        fig.savefig(caar_path, dpi=150)
        plt.close(fig)
    else:
        summary["event_time"] = pd.to_numeric(summary["event_time"], errors="coerce")
        summary["caar"] = pd.to_numeric(summary["caar"], errors="coerce")
        summary = summary.dropna(subset=["event_time", "caar"]).sort_values("event_time")
        fig, ax = plt.subplots(figsize=(8, 4))
        ax.plot(summary["event_time"], summary["caar"], marker="o")
        ax.axvline(0.0, color="black", linestyle="--", linewidth=1.0)
        ax.axhline(0.0, color="black", linestyle=":", linewidth=0.8)
        ax.set_title("Event Study CAAR")
        ax.set_xlabel("Event Time")
        ax.set_ylabel("CAAR")
        ax.grid(alpha=0.2)
        fig.tight_layout()
        fig.savefig(caar_path, dpi=150)
        plt.close(fig)

    return {"event_study_caar": str(caar_path)}
