"""Generate robustness appendix tables from robustness run outputs."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from .common import export_table


def _load_robustness_payload(run_bundle: dict[str, Any]) -> dict[str, Any]:
    artifacts = dict(run_bundle.get("artifacts", {}))
    run_path = Path(run_bundle["run_path"])
    candidate_paths = []
    if artifacts.get("robustness_results"):
        candidate_paths.append(Path(str(artifacts["robustness_results"])))
    candidate_paths.append(run_path / "robustness" / "robustness_results.json")
    for path in candidate_paths:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    return {"scenarios": [], "errors": []}


def build_robustness_table(run_bundle: dict[str, Any]) -> pd.DataFrame:
    """Build robustness-scenario summary table."""

    payload = _load_robustness_payload(run_bundle)
    rows: list[dict[str, Any]] = []
    for scenario in payload.get("scenarios", []):
        metrics = dict(scenario.get("metrics", {}))
        rows.append(
            {
                "scenario_id": scenario.get("scenario_id"),
                "transaction_cost_bps": scenario.get("scenario", {}).get("transaction_costs"),
                "holding_period_days": scenario.get("scenario", {}).get("holding_periods"),
                "winsorization_level": scenario.get("scenario", {}).get("winsorization_levels"),
                "status": scenario.get("status", "succeeded"),
                "annual_return": metrics.get("annual_return"),
                "sharpe_ratio": metrics.get("sharpe_ratio"),
                "max_drawdown": metrics.get("max_drawdown"),
                "turnover": metrics.get("turnover"),
            }
        )

    for error in payload.get("errors", []):
        rows.append(
            {
                "scenario_id": error.get("scenario_id"),
                "transaction_cost_bps": error.get("scenario", {}).get("transaction_costs"),
                "holding_period_days": error.get("scenario", {}).get("holding_periods"),
                "winsorization_level": error.get("scenario", {}).get("winsorization_levels"),
                "status": "failed",
                "annual_return": float("nan"),
                "sharpe_ratio": float("nan"),
                "max_drawdown": float("nan"),
                "turnover": float("nan"),
            }
        )

    return pd.DataFrame(rows)


def write_robustness_table(
    run_bundle: dict[str, Any],
    *,
    tables_dir: str | Path,
    formats: Iterable[str] = ("csv", "md", "tex"),
) -> tuple[pd.DataFrame, dict[str, str]]:
    table = build_robustness_table(run_bundle)
    outputs = export_table(table, output_dir=Path(tables_dir), stem="table_robustness", formats=formats)
    return table, outputs

