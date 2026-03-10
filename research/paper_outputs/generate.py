"""CLI and orchestration for experiment-to-paper artifact generation."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd

from .common import ensure_paper_workspace, export_table, load_csv, load_run_bundle, write_reproducibility_artifacts
from .inference_pipeline import load_or_compute_inference_results
from .make_appendix_tables import write_appendix_tables
from .make_figures import write_appendix_figures, write_figures
from .make_main_results_table import write_main_results_table
from .make_portfolio_sort_table import write_portfolio_sort_table
from .make_robustness_tables import write_robustness_table
from .make_table1_summary_stats import write_table1_summary_stats


def _resolve_timeseries(run_bundle: dict[str, Any]) -> pd.DataFrame:
    run_path = Path(run_bundle["run_path"])
    artifacts = dict(run_bundle.get("artifacts", {}))
    candidate = Path(str(artifacts.get("backtest_timeseries") or ""))
    if candidate.exists():
        return load_csv(candidate)
    return load_csv(run_path / "backtest_timeseries.csv")


def _resolve_inference_panel(run_bundle: dict[str, Any]) -> pd.DataFrame:
    run_path = Path(run_bundle["run_path"])
    artifacts = dict(run_bundle.get("artifacts", {}))
    candidate = Path(str(artifacts.get("inference_panel") or ""))
    if candidate.exists():
        return load_csv(candidate)
    return load_csv(run_path / "inference_panel.csv")


def generate_paper_outputs(
    *,
    experiment: str,
    paper_id: str,
    research_id: str | None = None,
    experiments_root: str | Path = "experiments",
    papers_root: str | Path = "papers",
    table_formats: tuple[str, ...] = ("csv", "md", "tex"),
    force_inference: bool = False,
) -> dict[str, Any]:
    """Generate full paper-ready artifacts for one experiment run."""

    run_bundle = load_run_bundle(
        experiment=experiment,
        experiments_root=experiments_root,
        research_id=research_id,
    )
    workspace = ensure_paper_workspace(papers_root=papers_root, paper_id=paper_id)

    inference_results = load_or_compute_inference_results(run_bundle, force=force_inference)
    inference_results_path = Path(run_bundle["run_path"]) / "inference_results.json"

    backtest_timeseries = _resolve_timeseries(run_bundle)
    inference_panel = _resolve_inference_panel(run_bundle)

    table1, table1_outputs = write_table1_summary_stats(
        inference_panel,
        tables_dir=workspace["tables"],
        formats=table_formats,
    )
    main_table, main_table_outputs = write_main_results_table(
        inference_results,
        tables_dir=workspace["tables"],
        formats=table_formats,
    )
    portfolio_table, portfolio_outputs = write_portfolio_sort_table(
        inference_results,
        tables_dir=workspace["tables"],
        formats=table_formats,
    )
    robustness_table, robustness_outputs = write_robustness_table(
        run_bundle,
        tables_dir=workspace["tables"],
        formats=table_formats,
    )
    appendix_robustness_outputs = export_table(
        robustness_table,
        output_dir=workspace["appendix"],
        stem="appendix_robustness",
        formats=table_formats,
    )
    appendix_outputs = write_appendix_tables(
        inference_results,
        appendix_dir=workspace["appendix"],
        formats=table_formats,
    )

    figure_outputs = write_figures(
        backtest_timeseries=backtest_timeseries,
        inference_results=inference_results,
        figures_dir=workspace["figures"],
    )
    appendix_figures = write_appendix_figures(
        inference_results=inference_results,
        appendix_dir=workspace["appendix"],
    )

    run_ids = [str(run_bundle["run_id"])]
    robustness_payload_path = Path(run_bundle["run_path"]) / "robustness" / "robustness_results.json"
    if robustness_payload_path.exists():
        robustness_payload = json.loads(robustness_payload_path.read_text(encoding="utf-8"))
        for scenario in robustness_payload.get("scenarios", []):
            run_ids.append(str(scenario.get("scenario_id")))

    reproducibility_outputs = write_reproducibility_artifacts(
        workspace=workspace,
        run_bundle=run_bundle,
        run_ids=run_ids,
    )

    manifest = {
        "paper_id": paper_id,
        "research_id": run_bundle["research_id"],
        "run_id": run_bundle["run_id"],
        "run_path": str(run_bundle["run_path"]),
        "inference_results": str(inference_results_path),
        "tables": {
            "table1_summary_stats": table1_outputs,
            "table_main_results": main_table_outputs,
            "table_portfolio_sort": portfolio_outputs,
            "table_robustness": robustness_outputs,
        },
        "appendix_tables": {**appendix_outputs, "appendix_robustness": appendix_robustness_outputs},
        "appendix_figures": appendix_figures,
        "figures": figure_outputs,
        "reproducibility": reproducibility_outputs,
        "table_shapes": {
            "table1_summary_stats": [int(table1.shape[0]), int(table1.shape[1])],
            "table_main_results": [int(main_table.shape[0]), int(main_table.shape[1])],
            "table_portfolio_sort": [int(portfolio_table.shape[0]), int(portfolio_table.shape[1])],
            "table_robustness": [int(robustness_table.shape[0]), int(robustness_table.shape[1])],
        },
    }

    manifest_path = workspace["paper_root"] / "paper_artifacts.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, allow_nan=True), encoding="utf-8")
    manifest["manifest_path"] = str(manifest_path)
    return manifest


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m research.paper_outputs.generate",
        description="Generate paper-ready tables, figures, and reproducibility artifacts from one experiment run.",
    )
    parser.add_argument("--experiment", required=True, help="Run identifier, or research_id:run_id.")
    parser.add_argument("--paper", required=True, help="Paper identifier under papers/<paper_id>/.")
    parser.add_argument("--research-id", default=None, help="Optional research id disambiguation.")
    parser.add_argument("--experiments-root", default="experiments", help="Root directory of run history.")
    parser.add_argument("--papers-root", default="papers", help="Root directory for paper workspaces.")
    parser.add_argument(
        "--table-formats",
        default="csv,md,tex",
        help="Comma-separated table formats: csv,md,tex.",
    )
    parser.add_argument(
        "--force-inference",
        action="store_true",
        help="Recompute inference outputs even if inference_results.json already exists.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    formats = tuple(part.strip() for part in str(args.table_formats).split(",") if part.strip())
    manifest = generate_paper_outputs(
        experiment=args.experiment,
        paper_id=args.paper,
        research_id=args.research_id,
        experiments_root=args.experiments_root,
        papers_root=args.papers_root,
        table_formats=formats,
        force_inference=bool(args.force_inference),
    )
    print(json.dumps(manifest, ensure_ascii=False, indent=2, allow_nan=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
