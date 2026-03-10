"""Generate reproducible markdown and plot artifacts for experiments."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import matplotlib
import numpy as np
import pandas as pd

matplotlib.use("Agg")
import matplotlib.pyplot as plt


class MarkdownReportGenerator:
    """Markdown report writer with simple equity and drawdown plots."""

    def __init__(self, *, default_output_dir: str | Path = "experiments") -> None:
        self.default_output_dir = Path(default_output_dir)

    @staticmethod
    def _drawdown_series(equity: pd.Series) -> pd.Series:
        roll_max = equity.cummax()
        return (equity / roll_max) - 1.0

    def _plot_equity(self, frame: pd.DataFrame, output_path: Path) -> None:
        fig, ax = plt.subplots(figsize=(10, 4))
        ax.plot(frame["date"], frame["equity"], label="Strategy", linewidth=1.4)
        if "benchmark_equity" in frame and frame["benchmark_equity"].notna().any():
            ax.plot(frame["date"], frame["benchmark_equity"], label="Benchmark", linewidth=1.2)
        ax.set_title("Equity Curve")
        ax.grid(alpha=0.2)
        ax.legend(loc="best")
        fig.tight_layout()
        fig.savefig(output_path, dpi=140)
        plt.close(fig)

    def _plot_drawdown(self, frame: pd.DataFrame, output_path: Path) -> None:
        equity = pd.to_numeric(frame["equity"], errors="coerce").ffill()
        dd = self._drawdown_series(equity)
        fig, ax = plt.subplots(figsize=(10, 3.2))
        ax.fill_between(frame["date"], dd, 0.0, color="#d9534f", alpha=0.6)
        ax.plot(frame["date"], dd, color="#b52b27", linewidth=1.0)
        ax.set_title("Drawdown")
        ax.grid(alpha=0.2)
        fig.tight_layout()
        fig.savefig(output_path, dpi=140)
        plt.close(fig)

    def generate(
        self,
        *,
        experiment_id: str,
        metrics: dict[str, Any],
        backtest_timeseries: pd.DataFrame,
        strategy_config: dict[str, Any],
        statistics: dict[str, Any] | None = None,
        output_dir: str | Path | None = None,
        extra_notes: str | None = None,
    ) -> tuple[str, dict[str, Any]]:
        """Generate report markdown and associated plots."""

        root = Path(output_dir) if output_dir else (self.default_output_dir / experiment_id)
        root.mkdir(parents=True, exist_ok=True)
        plots_dir = root / "plots"
        plots_dir.mkdir(parents=True, exist_ok=True)

        frame = backtest_timeseries.copy()
        if "date" in frame.columns:
            frame["date"] = pd.to_datetime(frame["date"], errors="coerce")

        equity_png = plots_dir / "equity_curve.png"
        drawdown_png = plots_dir / "drawdown.png"
        if not frame.empty and {"date", "equity"}.issubset(frame.columns):
            self._plot_equity(frame, equity_png)
            self._plot_drawdown(frame, drawdown_png)

        metric_lines = []
        for key, value in sorted(metrics.items()):
            if isinstance(value, float):
                if np.isfinite(value):
                    metric_lines.append(f"- `{key}`: {value:.6f}")
                else:
                    metric_lines.append(f"- `{key}`: NaN")
            else:
                metric_lines.append(f"- `{key}`: {value}")

        stats_text = "{}"
        if statistics:
            stats_text = json.dumps(statistics, ensure_ascii=False, indent=2)

        report_md = root / "report.md"
        report_md.write_text(
            "\n".join(
                [
                    f"# Experiment Report: {experiment_id}",
                    "",
                    "## Strategy Configuration",
                    "```json",
                    json.dumps(strategy_config, ensure_ascii=False, indent=2),
                    "```",
                    "",
                    "## Metrics",
                    *metric_lines,
                    "",
                    "## Statistical Validation",
                    "```json",
                    stats_text,
                    "```",
                    "",
                    "## Artifacts",
                    f"- equity curve: `plots/{equity_png.name}`" if equity_png.exists() else "- equity curve: not generated",
                    f"- drawdown: `plots/{drawdown_png.name}`" if drawdown_png.exists() else "- drawdown: not generated",
                    "",
                    "## Notes",
                    extra_notes or "No additional notes.",
                    "",
                ]
            ),
            encoding="utf-8",
        )

        artifacts = {
            "report": str(report_md),
            "equity_curve": str(equity_png) if equity_png.exists() else "",
            "drawdown": str(drawdown_png) if drawdown_png.exists() else "",
        }
        return str(report_md), artifacts
