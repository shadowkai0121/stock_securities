"""Research spec parsing and run-specific resolution."""

from __future__ import annotations

import copy
import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


VALID_IDENTIFIER = re.compile(r"^[A-Za-z0-9_-]+$")
SUPPORTED_RERUN_MODES = {"fixed_spec"}

DEFAULT_DATA_UPDATE_POLICY = {
    "mode": "ensure_local",
    "auto_update_missing": True,
    "auto_update_stale": True,
    "stock_info_start_date": None,
    "use_subprocess": False,
}
DEFAULT_FEATURE_DEFINITION = {
    "ma_windows": [5, 20, 60],
    "vol_windows": [20, 60],
    "use_margin": False,
    "use_broker": False,
    "use_holding_shares": False,
}
DEFAULT_BACKTEST_DEFINITION = {
    "transaction_cost_bps": 10.0,
    "slippage_bps": 0.0,
    "lag_positions": 1,
    "trading_days_per_year": 252,
}
DEFAULT_EVALUATION_DEFINITION = {
    "newey_west_lags": None,
    "bootstrap_samples": 2000,
    "seed": 42,
    "walk_forward_train_window": 126,
    "walk_forward_test_window": 63,
    "walk_forward_step": 63,
    "walk_forward_expanding": False,
    "train_ratio": 0.6,
    "valid_ratio": 0.2,
    "rolling_window": 20,
    "expanding_min_periods": 20,
}
DEFAULT_REPORT_DEFINITION = {
    "write_timeseries_csv": True,
    "write_universe_csv": True,
    "write_features_csv": False,
}


class SpecValidationError(ValueError):
    """Raised when a research spec is missing required structure."""


def _iso_date(value: str, *, field_name: str) -> str:
    text = str(value).strip()
    try:
        return datetime.strptime(text, "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError as exc:
        raise SpecValidationError(f"{field_name} must be YYYY-MM-DD, got {value!r}") from exc


def _as_string_list(value: Any, *, field_name: str, allow_empty: bool = False) -> list[str]:
    if value is None:
        if allow_empty:
            return []
        raise SpecValidationError(f"{field_name} is required")
    if not isinstance(value, list):
        raise SpecValidationError(f"{field_name} must be a list")
    items = [str(item).strip() for item in value if str(item).strip()]
    if not items and not allow_empty:
        raise SpecValidationError(f"{field_name} must not be empty")
    return items


def _as_mapping(value: Any, *, field_name: str, required: bool = True) -> dict[str, Any]:
    if value is None:
        if required:
            raise SpecValidationError(f"{field_name} is required")
        return {}
    if not isinstance(value, dict):
        raise SpecValidationError(f"{field_name} must be an object")
    return copy.deepcopy(value)


def _merge_defaults(defaults: dict[str, Any], values: dict[str, Any]) -> dict[str, Any]:
    merged = copy.deepcopy(defaults)
    merged.update(copy.deepcopy(values))
    return merged


@dataclass(frozen=True, slots=True)
class ResearchSpec:
    """Stable research-study definition independent from any single run."""

    research_id: str
    title: str
    description: str
    pipeline_id: str
    required_datasets: tuple[str, ...]
    data_update_policy: dict[str, Any]
    analysis_period: dict[str, Any]
    universe_definition: dict[str, Any]
    feature_definition: dict[str, Any]
    strategy_definition: dict[str, Any]
    backtest_definition: dict[str, Any]
    evaluation_definition: dict[str, Any]
    rerun_mode: str
    report_definition: dict[str, Any] = field(default_factory=dict)
    source_path: str | None = None

    @classmethod
    def from_payload(cls, payload: dict[str, Any], *, source_path: str | Path | None = None) -> "ResearchSpec":
        """Validate and normalize a raw JSON payload."""

        research_id = str(payload.get("research_id", "")).strip()
        if not research_id:
            raise SpecValidationError("research_id is required")
        if not VALID_IDENTIFIER.match(research_id):
            raise SpecValidationError("research_id may contain only letters, digits, '-' and '_'")

        title = str(payload.get("title", "")).strip()
        if not title:
            raise SpecValidationError("title is required")

        description = str(payload.get("description", "")).strip()
        strategy_payload = payload.get("strategy_definition")
        strategy_name = strategy_payload.get("name", "") if isinstance(strategy_payload, dict) else ""
        pipeline_id = str(payload.get("pipeline_id") or strategy_name).strip()
        if not pipeline_id:
            raise SpecValidationError("pipeline_id is required")

        rerun_mode = str(payload.get("rerun_mode", "fixed_spec")).strip() or "fixed_spec"
        if rerun_mode not in SUPPORTED_RERUN_MODES:
            raise SpecValidationError(
                f"Unsupported rerun_mode {rerun_mode!r}; supported modes: {sorted(SUPPORTED_RERUN_MODES)}"
            )

        analysis_period = _as_mapping(payload.get("analysis_period"), field_name="analysis_period")
        start_date = _iso_date(analysis_period.get("start_date"), field_name="analysis_period.start_date")
        analysis_period["start_date"] = start_date

        data_update_policy = _merge_defaults(
            DEFAULT_DATA_UPDATE_POLICY,
            _as_mapping(payload.get("data_update_policy"), field_name="data_update_policy"),
        )
        if data_update_policy.get("stock_info_start_date"):
            data_update_policy["stock_info_start_date"] = _iso_date(
                data_update_policy["stock_info_start_date"],
                field_name="data_update_policy.stock_info_start_date",
            )
        else:
            data_update_policy["stock_info_start_date"] = start_date

        universe_definition = _as_mapping(payload.get("universe_definition"), field_name="universe_definition")
        if "stock_ids" in universe_definition:
            universe_definition["stock_ids"] = _as_string_list(
                universe_definition.get("stock_ids"),
                field_name="universe_definition.stock_ids",
                allow_empty=True,
            )

        feature_definition = _merge_defaults(
            DEFAULT_FEATURE_DEFINITION,
            _as_mapping(payload.get("feature_definition"), field_name="feature_definition"),
        )
        backtest_definition = _merge_defaults(
            DEFAULT_BACKTEST_DEFINITION,
            _as_mapping(payload.get("backtest_definition"), field_name="backtest_definition"),
        )
        evaluation_definition = _merge_defaults(
            DEFAULT_EVALUATION_DEFINITION,
            _as_mapping(payload.get("evaluation_definition"), field_name="evaluation_definition"),
        )
        report_definition = _merge_defaults(
            DEFAULT_REPORT_DEFINITION,
            _as_mapping(payload.get("report_definition"), field_name="report_definition", required=False),
        )
        strategy_definition = _as_mapping(payload.get("strategy_definition"), field_name="strategy_definition")

        required_datasets = tuple(
            _as_string_list(payload.get("required_datasets"), field_name="required_datasets")
        )

        return cls(
            research_id=research_id,
            title=title,
            description=description,
            pipeline_id=pipeline_id,
            required_datasets=required_datasets,
            data_update_policy=data_update_policy,
            analysis_period=analysis_period,
            universe_definition=universe_definition,
            feature_definition=feature_definition,
            strategy_definition=strategy_definition,
            backtest_definition=backtest_definition,
            evaluation_definition=evaluation_definition,
            rerun_mode=rerun_mode,
            report_definition=report_definition,
            source_path=str(Path(source_path)) if source_path is not None else None,
        )

    def to_dict(self) -> dict[str, Any]:
        """Serialize the normalized spec back to a JSON-safe dictionary."""

        return {
            "research_id": self.research_id,
            "title": self.title,
            "description": self.description,
            "pipeline_id": self.pipeline_id,
            "required_datasets": list(self.required_datasets),
            "data_update_policy": copy.deepcopy(self.data_update_policy),
            "analysis_period": copy.deepcopy(self.analysis_period),
            "universe_definition": copy.deepcopy(self.universe_definition),
            "feature_definition": copy.deepcopy(self.feature_definition),
            "strategy_definition": copy.deepcopy(self.strategy_definition),
            "backtest_definition": copy.deepcopy(self.backtest_definition),
            "evaluation_definition": copy.deepcopy(self.evaluation_definition),
            "rerun_mode": self.rerun_mode,
            "report_definition": copy.deepcopy(self.report_definition),
            "source_path": self.source_path,
        }


def load_research_spec(path: str | Path) -> ResearchSpec:
    """Load and validate a research spec JSON file."""

    spec_path = Path(path)
    if not spec_path.exists():
        raise FileNotFoundError(f"Research spec not found: {spec_path}")

    try:
        payload = json.loads(spec_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise SpecValidationError(f"Research spec is not valid JSON: {spec_path}") from exc

    return ResearchSpec.from_payload(payload, source_path=spec_path)


def resolve_research_spec(
    spec: ResearchSpec,
    *,
    data_as_of: str,
    run_id: str,
    data_root: str | Path = "data",
    experiments_root: str | Path = "experiments",
    catalog_path: str | Path = "data/catalog/data_catalog.yaml",
    feature_cache_dir: str | Path | None = None,
    feature_store_version: str = "v1",
    git_commit_hash: str = "unknown",
) -> dict[str, Any]:
    """Resolve a stable research spec into an exact run-time snapshot."""

    normalized_as_of = _iso_date(data_as_of, field_name="data_as_of")
    start_date = str(spec.analysis_period["start_date"])
    if normalized_as_of < start_date:
        raise SpecValidationError("data_as_of must be on or after analysis_period.start_date")

    spec_path = Path(spec.source_path) if spec.source_path else None
    spec_sha256 = ""
    if spec_path is not None and spec_path.exists():
        spec_sha256 = hashlib.sha256(spec_path.read_bytes()).hexdigest()

    feature_cache = Path(feature_cache_dir) if feature_cache_dir is not None else Path(data_root) / "feature_cache"
    resolved = spec.to_dict()
    resolved["run_id"] = run_id
    resolved["data_as_of"] = normalized_as_of
    resolved["analysis_period"] = {
        "start_date": start_date,
        "data_as_of": normalized_as_of,
    }
    resolved["runtime"] = {
        "spec_path": str(spec_path) if spec_path is not None else "",
        "data_root": str(Path(data_root)),
        "experiments_root": str(Path(experiments_root)),
        "catalog_path": str(Path(catalog_path)),
        "feature_cache_dir": str(feature_cache),
        "feature_store_version": str(feature_store_version),
    }
    resolved["code_version"] = {
        "git_commit_hash": git_commit_hash,
    }
    resolved["spec_sha256"] = spec_sha256
    resolved["resolved_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return resolved
