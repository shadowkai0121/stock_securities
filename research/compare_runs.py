"""Compare append-only runs of the same research study over time."""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any

from experiments.registry import ResearchRunRegistry


COMPARISON_METRICS = (
    "annual_return",
    "annual_volatility",
    "sharpe_ratio",
    "max_drawdown",
    "turnover",
    "number_of_trades",
    "universe_size",
)


def _numeric_delta(base: Any, target: Any) -> float | None:
    try:
        base_value = float(base)
        target_value = float(target)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(base_value) or not math.isfinite(target_value):
        return None
    return target_value - base_value


def _dataset_summary_map(manifest: dict[str, Any]) -> dict[str, dict[str, Any]]:
    output: dict[str, dict[str, Any]] = {}
    for item in manifest.get("dataset_summary", []):
        name = str(item.get("dataset_name", "")).strip()
        if name:
            output[name] = dict(item)
    return output


def compare_research_runs(
    *,
    research_id: str,
    base_run_id: str | None = None,
    target_run_id: str | None = None,
    experiments_root: str | Path = "experiments",
) -> dict[str, Any]:
    """Compare two runs of the same research_id and return markdown + JSON."""

    registry = ResearchRunRegistry(root_dir=experiments_root)
    latest = registry.latest_run(research_id)
    previous = registry.previous_run(research_id)

    resolved_target = target_run_id or (latest or {}).get("run_id")
    resolved_base = base_run_id or (previous or {}).get("run_id")
    if not resolved_target or not resolved_base:
        raise FileNotFoundError("At least two successful runs are required for comparison")

    base_bundle = registry.load_run_bundle(research_id=research_id, run_id=resolved_base)
    target_bundle = registry.load_run_bundle(research_id=research_id, run_id=resolved_target)

    base_metrics = base_bundle["metrics"]
    target_metrics = target_bundle["metrics"]

    metric_rows = []
    for name in COMPARISON_METRICS:
        metric_rows.append(
            {
                "metric": name,
                "base": base_metrics.get(name),
                "target": target_metrics.get(name),
                "delta": _numeric_delta(base_metrics.get(name), target_metrics.get(name)),
            }
        )

    base_datasets = _dataset_summary_map(base_bundle["data_manifest"])
    target_datasets = _dataset_summary_map(target_bundle["data_manifest"])
    dataset_rows = []
    for name in sorted(set(base_datasets) | set(target_datasets)):
        left = base_datasets.get(name, {})
        right = target_datasets.get(name, {})
        dataset_rows.append(
            {
                "dataset_name": name,
                "base_row_count_as_of_total": left.get("row_count_as_of_total"),
                "target_row_count_as_of_total": right.get("row_count_as_of_total"),
                "base_max_date": left.get("max_date"),
                "target_max_date": right.get("max_date"),
                "base_latest_requested_end_date": left.get("latest_requested_end_date"),
                "target_latest_requested_end_date": right.get("latest_requested_end_date"),
            }
        )

    comparison = {
        "research_id": research_id,
        "base_run_id": resolved_base,
        "target_run_id": resolved_target,
        "base_data_as_of": base_bundle["resolved_spec"].get("data_as_of"),
        "target_data_as_of": target_bundle["resolved_spec"].get("data_as_of"),
        "metrics": metric_rows,
        "datasets": dataset_rows,
    }

    markdown_lines = [
        f"# Run Comparison: {research_id}",
        "",
        f"- base run: `{resolved_base}` (data_as_of={comparison['base_data_as_of']})",
        f"- target run: `{resolved_target}` (data_as_of={comparison['target_data_as_of']})",
        "",
        "## Metrics",
    ]
    for row in metric_rows:
        markdown_lines.append(
            f"- `{row['metric']}`: base={row['base']} target={row['target']} delta={row['delta']}"
        )

    markdown_lines.append("")
    markdown_lines.append("## Data Coverage")
    for row in dataset_rows:
        markdown_lines.append(
            f"- `{row['dataset_name']}`: rows {row['base_row_count_as_of_total']} -> {row['target_row_count_as_of_total']}, "
            f"max_date {row['base_max_date']} -> {row['target_max_date']}, "
            f"requested_end {row['base_latest_requested_end_date']} -> {row['target_latest_requested_end_date']}"
        )

    comparison["markdown"] = "\n".join(markdown_lines) + "\n"
    return comparison


def build_parser() -> argparse.ArgumentParser:
    """Create CLI parser for run comparison."""

    parser = argparse.ArgumentParser(
        prog="python -m research.compare_runs",
        description="Compare two append-only runs of the same research_id.",
    )
    parser.add_argument("--research-id", required=True, help="Research identifier under experiments/<research_id>/runs.")
    parser.add_argument("--base-run", default=None, help="Older run_id. Defaults to the previous successful run.")
    parser.add_argument("--target-run", default=None, help="Newer run_id. Defaults to the latest successful run.")
    parser.add_argument("--experiments-root", default="experiments", help="Root directory containing run history.")
    parser.add_argument("--output-json", default=None, help="Optional file path for machine-readable comparison JSON.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    comparison = compare_research_runs(
        research_id=args.research_id,
        base_run_id=args.base_run,
        target_run_id=args.target_run,
        experiments_root=args.experiments_root,
    )
    print(comparison["markdown"], end="")
    if args.output_json:
        Path(args.output_json).write_text(
            json.dumps(comparison, ensure_ascii=False, indent=2, allow_nan=True),
            encoding="utf-8",
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
