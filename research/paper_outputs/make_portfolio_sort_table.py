"""Generate portfolio-sort table outputs."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from .common import export_table


def build_portfolio_sort_table(inference_results: dict[str, Any]) -> pd.DataFrame:
    """Combine equal-weight and value-weight portfolio-sort summaries."""

    rows: list[dict[str, Any]] = []
    for label, key in [("Equal Weight", "equal_weight"), ("Value Weight", "value_weight")]:
        summary = list(inference_results.get("portfolio_sort", {}).get(key, {}).get("summary", []))
        for row in summary:
            rows.append(
                {
                    "weighting_scheme": label,
                    "portfolio": row.get("portfolio"),
                    "mean_return": row.get("mean_return"),
                    "std_err": row.get("std_err"),
                    "t_stat": row.get("t_stat"),
                    "p_value": row.get("p_value"),
                    "n_obs": row.get("n_obs"),
                }
            )
    return pd.DataFrame(rows)


def write_portfolio_sort_table(
    inference_results: dict[str, Any],
    *,
    tables_dir: str | Path,
    formats: Iterable[str] = ("csv", "md", "tex"),
) -> tuple[pd.DataFrame, dict[str, str]]:
    table = build_portfolio_sort_table(inference_results)
    outputs = export_table(table, output_dir=Path(tables_dir), stem="table_portfolio_sort", formats=formats)
    return table, outputs

