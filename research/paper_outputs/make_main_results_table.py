"""Generate the paper main-results regression table."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from .common import export_table


def _annotate_significance(p_value: float) -> str:
    if pd.isna(p_value):
        return ""
    if p_value < 0.01:
        return "***"
    if p_value < 0.05:
        return "**"
    if p_value < 0.1:
        return "*"
    return ""


def _normalize_rows(rows: list[dict[str, Any]], *, method: str) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for row in rows:
        coef = float(row.get("coef")) if row.get("coef") is not None else float("nan")
        p_value = float(row.get("p_value")) if row.get("p_value") is not None else float("nan")
        output.append(
            {
                "method": method,
                "variable": row.get("variable"),
                "coef": coef,
                "std_err": row.get("std_err"),
                "t_stat": row.get("t_stat"),
                "p_value": p_value,
                "sig": _annotate_significance(p_value),
            }
        )
    return output


def build_main_results_table(inference_results: dict[str, Any]) -> pd.DataFrame:
    """Create the main paper table from run-level inference outputs."""

    rows: list[dict[str, Any]] = []
    fm_rows = list(inference_results.get("fama_macbeth", {}).get("summary", []))
    rows.extend(_normalize_rows(fm_rows, method="Fama-MacBeth"))

    pooled_rows = list(inference_results.get("panel_ols", {}).get("pooled", {}).get("summary", []))
    rows.extend(_normalize_rows(pooled_rows, method="Panel OLS (Pooled, Cluster Entity)"))

    firm_rows = list(inference_results.get("panel_ols", {}).get("firm_fe", {}).get("summary", []))
    rows.extend(_normalize_rows(firm_rows, method="Panel OLS (Firm FE, Cluster Entity)"))

    time_rows = list(inference_results.get("panel_ols", {}).get("time_fe", {}).get("summary", []))
    rows.extend(_normalize_rows(time_rows, method="Panel OLS (Time FE, Cluster Time)"))

    return pd.DataFrame(rows)


def write_main_results_table(
    inference_results: dict[str, Any],
    *,
    tables_dir: str | Path,
    formats: Iterable[str] = ("csv", "md", "tex"),
) -> tuple[pd.DataFrame, dict[str, str]]:
    table = build_main_results_table(inference_results)
    outputs = export_table(table, output_dir=Path(tables_dir), stem="table_main_results", formats=formats)
    return table, outputs

