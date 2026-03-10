"""Generate appendix tables from secondary inference outputs."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from .common import export_table


def build_appendix_event_table(inference_results: dict[str, Any]) -> pd.DataFrame:
    """Event-study window table for appendix material."""

    return pd.DataFrame(list(inference_results.get("event_study", {}).get("window_summary", [])))


def build_appendix_fmb_betas_table(inference_results: dict[str, Any]) -> pd.DataFrame:
    """Fama-MacBeth period beta series for appendix diagnostics."""

    return pd.DataFrame(list(inference_results.get("fama_macbeth", {}).get("period_betas", [])))


def write_appendix_tables(
    inference_results: dict[str, Any],
    *,
    appendix_dir: str | Path,
    formats: Iterable[str] = ("csv", "md", "tex"),
) -> dict[str, dict[str, str]]:
    appendix_path = Path(appendix_dir)
    event_table = build_appendix_event_table(inference_results)
    beta_table = build_appendix_fmb_betas_table(inference_results)
    outputs = {
        "appendix_event_study": export_table(
            event_table,
            output_dir=appendix_path,
            stem="appendix_event_study",
            formats=formats,
        ),
        "appendix_fama_macbeth_period_betas": export_table(
            beta_table,
            output_dir=appendix_path,
            stem="appendix_fama_macbeth_period_betas",
            formats=formats,
        ),
    }
    return outputs

