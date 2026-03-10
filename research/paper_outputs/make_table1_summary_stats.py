"""Generate Table 1 summary statistics from inference panel data."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd

from .common import export_table


def build_table1_summary_stats(inference_panel: pd.DataFrame) -> pd.DataFrame:
    """Build canonical summary-statistics table for paper output."""

    if inference_panel.empty:
        return pd.DataFrame(columns=["variable", "count", "mean", "std", "p25", "median", "p75"])

    work = inference_panel.copy()
    numeric_cols = [col for col in work.columns if col not in {"date", "stock_id"}]
    rows: list[dict[str, float | str | int]] = []
    for col in numeric_cols:
        series = pd.to_numeric(work[col], errors="coerce").dropna()
        if series.empty:
            continue
        rows.append(
            {
                "variable": col,
                "count": int(series.shape[0]),
                "mean": float(series.mean()),
                "std": float(series.std(ddof=0)),
                "p25": float(series.quantile(0.25)),
                "median": float(series.quantile(0.50)),
                "p75": float(series.quantile(0.75)),
            }
        )

    return pd.DataFrame(rows)


def write_table1_summary_stats(
    inference_panel: pd.DataFrame,
    *,
    tables_dir: str | Path,
    formats: Iterable[str] = ("csv", "md", "tex"),
) -> tuple[pd.DataFrame, dict[str, str]]:
    table = build_table1_summary_stats(inference_panel)
    outputs = export_table(table, output_dir=Path(tables_dir), stem="table1_summary_stats", formats=formats)
    return table, outputs

