"""CLI entrypoint for append-only research-spec reruns."""

from __future__ import annotations

import argparse
import copy
import itertools
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from data.loaders.finmind_loader import FinMindLoader
from experiments.registry import ResearchRunRegistry
from research.data_loader import ResearchDataLoader
from research.data_state import (
    build_data_manifest,
    ensure_local_datasets,
    load_data_catalog,
    resolve_dataset_targets,
    validate_dataset_targets,
)
from research.specs import SpecValidationError, load_research_spec, resolve_research_spec
from research.studies import StudyExecutionError, get_study_executor


EXIT_SUCCESS = 0
EXIT_VALIDATION_ERROR = 2
EXIT_RUNTIME_ERROR = 1
MAX_ROBUSTNESS_SCENARIOS = 128


def build_parser() -> argparse.ArgumentParser:
    """Create the non-interactive research rerun CLI."""

    parser = argparse.ArgumentParser(
        prog="python -m research.run",
        description="Execute a stable research spec against local data as of a cutoff date.",
    )
    parser.add_argument("--spec", required=True, help="Path to a research spec JSON file.")
    parser.add_argument("--data-as-of", required=True, help="Latest local data date allowed for the run.")
    parser.add_argument("--run-id", default=None, help="Optional explicit run_id. Must be unique within the research.")
    parser.add_argument("--data-root", default="data", help="Root directory containing local SQLite databases.")
    parser.add_argument(
        "--experiments-root",
        default="experiments",
        help="Root directory for append-only experiment and run outputs.",
    )
    parser.add_argument(
        "--catalog-path",
        default="data/catalog/data_catalog.yaml",
        help="Path to the data catalog used for validation and manifests.",
    )
    parser.add_argument(
        "--feature-store-version",
        default="v1",
        help="Version tag used for cached feature panels.",
    )
    parser.add_argument(
        "--feature-cache-dir",
        default=None,
        help="Optional override for feature cache directory. Defaults to <data-root>/feature_cache.",
    )
    parser.add_argument("--token", default=None, help="Optional FinMind token override.")
    parser.add_argument(
        "--use-subprocess-finmind",
        action="store_true",
        help="Invoke the finmind-dl CLI in subprocess mode instead of in-process handlers.",
    )
    return parser


def _log_line(message: str) -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return f"{stamp} {message}"


def _resolve_stock_ids(resolved_spec: dict[str, Any]) -> list[str]:
    universe_definition = dict(resolved_spec["universe_definition"])
    stock_ids = [str(item).strip() for item in universe_definition.get("stock_ids", []) if str(item).strip()]
    if stock_ids:
        return stock_ids

    data_loader = ResearchDataLoader(data_root=resolved_spec["runtime"]["data_root"])
    return data_loader.available_stock_ids()


def _success_payload(*, record_path: Path, resolved_spec: dict[str, Any], metrics: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": "success",
        "research_id": resolved_spec["research_id"],
        "run_id": resolved_spec["run_id"],
        "data_as_of": resolved_spec["data_as_of"],
        "run_path": str(record_path),
        "metrics": {
            key: metrics.get(key)
            for key in (
                "annual_return",
                "annual_volatility",
                "sharpe_ratio",
                "max_drawdown",
                "turnover",
                "number_of_trades",
                "universe_size",
            )
            if key in metrics
        },
    }


def _safe_fragment(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in str(value))


def _expand_robustness_grid(robustness_definition: dict[str, Any]) -> list[dict[str, Any]]:
    """Build Cartesian-product robustness scenarios from a spec definition."""

    dimensions: list[tuple[str, list[Any]]] = []
    for key, raw_values in robustness_definition.items():
        if not isinstance(raw_values, list):
            continue
        values = [value for value in raw_values]
        if not values:
            continue
        dimensions.append((str(key), values))

    if not dimensions:
        return []

    cardinality = 1
    for _, values in dimensions:
        cardinality *= len(values)
    if cardinality > MAX_ROBUSTNESS_SCENARIOS:
        raise SpecValidationError(
            f"robustness grid too large ({cardinality} scenarios); max is {MAX_ROBUSTNESS_SCENARIOS}"
        )

    keys = [key for key, _ in dimensions]
    products = itertools.product(*[values for _, values in dimensions])
    scenarios: list[dict[str, Any]] = []
    for combo in products:
        scenarios.append(dict(zip(keys, combo)))
    return scenarios


def _apply_robustness_overrides(resolved_spec: dict[str, Any], scenario: dict[str, Any], scenario_id: str) -> dict[str, Any]:
    """Apply scenario-level overrides onto a resolved spec snapshot."""

    updated = copy.deepcopy(resolved_spec)
    updated["run_id"] = f"{resolved_spec['run_id']}__{scenario_id}"
    updated.setdefault("robustness_context", {})
    updated["robustness_context"] = {"scenario_id": scenario_id, **scenario}

    if "transaction_costs" in scenario:
        updated.setdefault("backtest_definition", {})
        updated["backtest_definition"]["transaction_cost_bps"] = float(scenario["transaction_costs"])

    if "holding_periods" in scenario:
        updated.setdefault("strategy_definition", {})
        updated["strategy_definition"]["holding_period_days"] = int(scenario["holding_periods"])

    if "winsorization_levels" in scenario:
        updated.setdefault("evaluation_definition", {})
        updated["evaluation_definition"]["winsorization_level"] = float(scenario["winsorization_levels"])

    return updated


def _run_robustness_scenarios(
    *,
    executor: Any,
    resolved_spec: dict[str, Any],
    run_dir: Path,
) -> tuple[dict[str, Any], list[str]]:
    """Execute robustness scenarios and persist append-only sub-run artifacts."""

    robustness_definition = dict(resolved_spec.get("robustness", {}) or {})
    scenarios = _expand_robustness_grid(robustness_definition)
    if not scenarios:
        return {"scenarios": [], "errors": []}, []

    robustness_root = run_dir / "robustness"
    robustness_root.mkdir(parents=True, exist_ok=True)
    logs = [_log_line(f"[robustness] scenarios={len(scenarios)}")]

    results: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    for idx, scenario in enumerate(scenarios, start=1):
        fragments = [f"{key}_{_safe_fragment(value)}" for key, value in scenario.items()]
        scenario_id = f"rb_{idx:03d}__{'__'.join(fragments)}"
        scenario_dir = robustness_root / scenario_id
        scenario_dir.mkdir(parents=True, exist_ok=False)

        scenario_spec = _apply_robustness_overrides(resolved_spec, scenario, scenario_id)
        (scenario_dir / "resolved_spec.json").write_text(
            json.dumps(scenario_spec, ensure_ascii=False, indent=2, allow_nan=True),
            encoding="utf-8",
        )
        (scenario_dir / "scenario.json").write_text(
            json.dumps(scenario, ensure_ascii=False, indent=2, allow_nan=True),
            encoding="utf-8",
        )

        try:
            outcome = executor.execute(resolved_spec=scenario_spec, run_dir=scenario_dir)
            (scenario_dir / "metrics.json").write_text(
                json.dumps(outcome.metrics, ensure_ascii=False, indent=2, allow_nan=True),
                encoding="utf-8",
            )
            (scenario_dir / "artifacts.json").write_text(
                json.dumps(outcome.artifacts, ensure_ascii=False, indent=2, allow_nan=True),
                encoding="utf-8",
            )
            results.append(
                {
                    "scenario_id": scenario_id,
                    "scenario": scenario,
                    "status": "succeeded",
                    "run_dir": str(scenario_dir),
                    "metrics": outcome.metrics,
                    "artifacts": outcome.artifacts,
                }
            )
            logs.append(_log_line(f"[robustness] succeeded scenario={scenario_id}"))
        except Exception as exc:  # pragma: no cover - exercised in integration tests with safe inputs
            errors.append(
                {
                    "scenario_id": scenario_id,
                    "scenario": scenario,
                    "status": "failed",
                    "run_dir": str(scenario_dir),
                    "error": str(exc),
                }
            )
            logs.append(_log_line(f"[robustness] failed scenario={scenario_id} error={exc}"))

    payload = {
        "research_id": resolved_spec["research_id"],
        "run_id": resolved_spec["run_id"],
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "scenarios": results,
        "errors": errors,
    }
    payload_path = robustness_root / "robustness_results.json"
    payload_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=True), encoding="utf-8")
    payload["path"] = str(payload_path)
    return payload, logs


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    registry = ResearchRunRegistry(root_dir=args.experiments_root)
    run_record = None
    run_log_lines: list[str] = []
    resolved_spec: dict[str, Any] | None = None
    data_manifest: dict[str, Any] | None = None

    try:
        spec = load_research_spec(args.spec)
        run_id = registry.allocate_run_id(data_as_of=args.data_as_of, explicit_id=args.run_id)
        resolved_spec = resolve_research_spec(
            spec,
            data_as_of=args.data_as_of,
            run_id=run_id,
            data_root=args.data_root,
            experiments_root=args.experiments_root,
            catalog_path=args.catalog_path,
            feature_cache_dir=args.feature_cache_dir,
            feature_store_version=args.feature_store_version,
            git_commit_hash=registry._git_commit_hash(),
        )
        analysis_start = str(resolved_spec["analysis_period"]["start_date"])
        data_as_of = str(resolved_spec["data_as_of"])

        stock_ids = _resolve_stock_ids(resolved_spec)
        if not stock_ids and any(name not in {"stock_info"} for name in resolved_spec["required_datasets"]):
            raise SpecValidationError(
                "No stock_ids were resolved. Add universe_definition.stock_ids or seed local per-stock SQLite data first."
            )

        run_log_lines.append(_log_line(f"[spec] research_id={resolved_spec['research_id']} run_id={run_id}"))
        run_log_lines.append(_log_line(f"[spec] data_as_of={data_as_of} analysis_start={analysis_start}"))
        if stock_ids:
            run_log_lines.append(_log_line(f"[spec] stock_ids={','.join(stock_ids)}"))

        catalog = load_data_catalog(args.catalog_path)
        targets = resolve_dataset_targets(
            required_datasets=list(resolved_spec["required_datasets"]),
            stock_ids=stock_ids,
            data_root=resolved_spec["runtime"]["data_root"],
            catalog=catalog,
        )

        loader = FinMindLoader(
            token=args.token,
            use_subprocess=bool(args.use_subprocess_finmind or resolved_spec["data_update_policy"].get("use_subprocess")),
        )
        ensure_result = ensure_local_datasets(
            targets=targets,
            analysis_start=analysis_start,
            data_as_of=data_as_of,
            data_update_policy=resolved_spec["data_update_policy"],
            finmind_loader=loader,
        )
        run_log_lines.extend(_log_line(message) for message in ensure_result["logs"])

        validation_reports = validate_dataset_targets(targets=targets, catalog=catalog)
        failed_reports = [item for item in validation_reports if not bool(item.get("passed", False))]
        run_log_lines.append(_log_line(f"[validation] checked={len(validation_reports)} failed={len(failed_reports)}"))
        if failed_reports:
            raise RuntimeError(f"Dataset validation failed. First failure: {failed_reports[0]}")

        data_manifest = build_data_manifest(
            research_id=resolved_spec["research_id"],
            run_id=resolved_spec["run_id"],
            data_as_of=data_as_of,
            analysis_start=analysis_start,
            targets=targets,
        )
        run_log_lines.append(_log_line(f"[manifest] dataset_fingerprint={data_manifest['dataset_fingerprint']}"))

        run_record = registry.create_run(
            research_id=resolved_spec["research_id"],
            data_as_of=data_as_of,
            spec_path=args.spec,
            rerun_mode=resolved_spec["rerun_mode"],
            run_id=resolved_spec["run_id"],
        )
        registry.write_run_snapshot(record=run_record, resolved_spec=resolved_spec, data_manifest=data_manifest)
        registry.write_run_log(run_record, run_log_lines, append=False)

        executor = get_study_executor(resolved_spec["pipeline_id"])
        study_result = executor.execute(resolved_spec=resolved_spec, run_dir=run_record.path)
        registry.write_run_log(run_record, [_log_line(line) for line in study_result.log_lines], append=True)

        robustness_payload, robustness_logs = _run_robustness_scenarios(
            executor=executor,
            resolved_spec=resolved_spec,
            run_dir=run_record.path,
        )
        if robustness_logs:
            registry.write_run_log(run_record, robustness_logs, append=True)
        if robustness_payload.get("scenarios") or robustness_payload.get("errors"):
            study_result.artifacts["robustness_results"] = str(robustness_payload.get("path", ""))
            study_result.metrics["robustness_summary"] = {
                "succeeded": int(len(robustness_payload.get("scenarios", []))),
                "failed": int(len(robustness_payload.get("errors", []))),
            }

        registry.mark_succeeded(
            record=run_record,
            metrics=study_result.metrics,
            artifacts=study_result.artifacts,
            data_manifest=data_manifest,
        )

        print(
            json.dumps(
                _success_payload(
                    record_path=run_record.path,
                    resolved_spec=resolved_spec,
                    metrics=study_result.metrics,
                ),
                ensure_ascii=False,
                indent=2,
                allow_nan=True,
            )
        )
        return EXIT_SUCCESS

    except (SpecValidationError, FileNotFoundError, KeyError) as exc:
        if run_record is not None:
            registry.write_run_log(run_record, [_log_line(f"[error] {exc}")], append=True)
            registry.mark_failed(record=run_record, error_message=str(exc), data_manifest=data_manifest)
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return EXIT_VALIDATION_ERROR
    except (StudyExecutionError, RuntimeError, ValueError) as exc:
        if run_record is not None:
            registry.write_run_log(run_record, [_log_line(f"[error] {exc}")], append=True)
            registry.mark_failed(record=run_record, error_message=str(exc), data_manifest=data_manifest)
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return EXIT_RUNTIME_ERROR


if __name__ == "__main__":
    raise SystemExit(main())
