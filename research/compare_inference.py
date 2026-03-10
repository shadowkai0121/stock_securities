"""Compare inference outputs across append-only runs."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd

from experiments.registry import ResearchRunRegistry
from research.paper_outputs.common import load_run_bundle
from research.paper_outputs.inference_pipeline import load_or_compute_inference_results


def _table_from_summary(rows: list[dict[str, Any]]) -> pd.DataFrame:
    frame = pd.DataFrame(rows)
    if frame.empty or "variable" not in frame.columns:
        return pd.DataFrame(columns=["variable", "coef", "t_stat", "p_value"])
    return frame[["variable", "coef", "t_stat", "p_value"]].copy()


def _extract_long_short(summary_rows: list[dict[str, Any]]) -> dict[str, float]:
    frame = pd.DataFrame(summary_rows)
    if frame.empty:
        return {"mean_return": float("nan"), "t_stat": float("nan"), "p_value": float("nan")}
    target = frame[frame["portfolio"].astype(str) == "Long-Short"]
    if target.empty:
        return {"mean_return": float("nan"), "t_stat": float("nan"), "p_value": float("nan")}
    row = target.iloc[0]
    return {
        "mean_return": float(row.get("mean_return", float("nan"))),
        "t_stat": float(row.get("t_stat", float("nan"))),
        "p_value": float(row.get("p_value", float("nan"))),
    }


def compare_inference_runs(
    *,
    research_id: str,
    base_run_id: str | None = None,
    target_run_id: str | None = None,
    experiments_root: str | Path = "experiments",
    force_recompute: bool = False,
) -> dict[str, Any]:
    """Compare coefficient and spread inference stability across two runs."""

    registry = ResearchRunRegistry(root_dir=experiments_root)
    latest = registry.latest_run(research_id)
    previous = registry.previous_run(research_id)

    resolved_target = target_run_id or (latest or {}).get("run_id")
    resolved_base = base_run_id or (previous or {}).get("run_id")
    if not resolved_target or not resolved_base:
        raise FileNotFoundError("At least two successful runs are required for inference comparison")

    base_bundle = load_run_bundle(
        experiment=resolved_base,
        research_id=research_id,
        experiments_root=experiments_root,
    )
    target_bundle = load_run_bundle(
        experiment=resolved_target,
        research_id=research_id,
        experiments_root=experiments_root,
    )

    base_inf = load_or_compute_inference_results(base_bundle, force=force_recompute)
    target_inf = load_or_compute_inference_results(target_bundle, force=force_recompute)

    base_fm = _table_from_summary(list(base_inf.get("fama_macbeth", {}).get("summary", [])))
    target_fm = _table_from_summary(list(target_inf.get("fama_macbeth", {}).get("summary", [])))
    merged = base_fm.merge(target_fm, on="variable", how="outer", suffixes=("_base", "_target"))
    if not merged.empty:
        merged["coef_delta"] = pd.to_numeric(merged["coef_target"], errors="coerce") - pd.to_numeric(
            merged["coef_base"], errors="coerce"
        )
        merged["t_stat_delta"] = pd.to_numeric(merged["t_stat_target"], errors="coerce") - pd.to_numeric(
            merged["t_stat_base"], errors="coerce"
        )
        merged["significant_base"] = pd.to_numeric(merged["p_value_base"], errors="coerce") < 0.05
        merged["significant_target"] = pd.to_numeric(merged["p_value_target"], errors="coerce") < 0.05
        merged["significance_persistent"] = merged["significant_base"] & merged["significant_target"]

    base_spread_equal = _extract_long_short(list(base_inf.get("portfolio_sort", {}).get("equal_weight", {}).get("summary", [])))
    target_spread_equal = _extract_long_short(list(target_inf.get("portfolio_sort", {}).get("equal_weight", {}).get("summary", [])))
    base_spread_value = _extract_long_short(list(base_inf.get("portfolio_sort", {}).get("value_weight", {}).get("summary", [])))
    target_spread_value = _extract_long_short(list(target_inf.get("portfolio_sort", {}).get("value_weight", {}).get("summary", [])))

    spread_changes = [
        {
            "weighting": "equal_weight",
            "base_mean_return": base_spread_equal["mean_return"],
            "target_mean_return": target_spread_equal["mean_return"],
            "delta_mean_return": target_spread_equal["mean_return"] - base_spread_equal["mean_return"],
            "base_t_stat": base_spread_equal["t_stat"],
            "target_t_stat": target_spread_equal["t_stat"],
            "delta_t_stat": target_spread_equal["t_stat"] - base_spread_equal["t_stat"],
        },
        {
            "weighting": "value_weight",
            "base_mean_return": base_spread_value["mean_return"],
            "target_mean_return": target_spread_value["mean_return"],
            "delta_mean_return": target_spread_value["mean_return"] - base_spread_value["mean_return"],
            "base_t_stat": base_spread_value["t_stat"],
            "target_t_stat": target_spread_value["t_stat"],
            "delta_t_stat": target_spread_value["t_stat"] - base_spread_value["t_stat"],
        },
    ]

    comparison = {
        "research_id": research_id,
        "base_run_id": resolved_base,
        "target_run_id": resolved_target,
        "coefficient_stability": merged[
            [
                "variable",
                "coef_base",
                "coef_target",
                "coef_delta",
                "t_stat_base",
                "t_stat_target",
                "t_stat_delta",
                "significance_persistent",
            ]
        ].to_dict(orient="records")
        if not merged.empty
        else [],
        "spread_changes": spread_changes,
    }

    markdown_lines = [
        f"# Inference Comparison: {research_id}",
        "",
        f"- base run: `{resolved_base}`",
        f"- target run: `{resolved_target}`",
        "",
        "## Coefficient Stability (Fama-MacBeth)",
    ]
    if merged.empty:
        markdown_lines.append("- No overlapping coefficients available.")
    else:
        for row in comparison["coefficient_stability"]:
            markdown_lines.append(
                f"- `{row['variable']}` coef {row['coef_base']} -> {row['coef_target']} "
                f"(delta={row['coef_delta']}), t {row['t_stat_base']} -> {row['t_stat_target']} "
                f"(delta={row['t_stat_delta']}), persistent_sig={row['significance_persistent']}"
            )

    markdown_lines.append("")
    markdown_lines.append("## Portfolio Spread Changes")
    for row in spread_changes:
        markdown_lines.append(
            f"- `{row['weighting']}` mean {row['base_mean_return']} -> {row['target_mean_return']} "
            f"(delta={row['delta_mean_return']}), t {row['base_t_stat']} -> {row['target_t_stat']} "
            f"(delta={row['delta_t_stat']})"
        )

    comparison["markdown"] = "\n".join(markdown_lines) + "\n"
    return comparison


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m research.compare_inference",
        description="Compare inference outputs between two runs of the same research study.",
    )
    parser.add_argument("--research-id", required=True, help="Research identifier under experiments/<research_id>/runs.")
    parser.add_argument("--base-run", default=None, help="Older run_id; defaults to previous successful run.")
    parser.add_argument("--target-run", default=None, help="Newer run_id; defaults to latest successful run.")
    parser.add_argument("--experiments-root", default="experiments", help="Root directory containing run history.")
    parser.add_argument("--output-json", default=None, help="Optional path to write comparison JSON.")
    parser.add_argument("--force-recompute", action="store_true", help="Recompute inference outputs before comparing.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    payload = compare_inference_runs(
        research_id=args.research_id,
        base_run_id=args.base_run,
        target_run_id=args.target_run,
        experiments_root=args.experiments_root,
        force_recompute=bool(args.force_recompute),
    )
    print(payload["markdown"], end="")
    if args.output_json:
        Path(args.output_json).write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=True),
            encoding="utf-8",
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

