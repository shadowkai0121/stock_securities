"""Experiment and research-run registries for reproducible research artifacts."""

from __future__ import annotations

import hashlib
import json
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


RUN_SUMMARY_METRICS = (
    "annual_return",
    "annual_volatility",
    "sharpe_ratio",
    "max_drawdown",
    "turnover",
    "number_of_trades",
    "universe_size",
)


@dataclass(slots=True)
class ExperimentRecord:
    """Registry record for one legacy experiment run."""

    experiment_id: str
    path: Path
    metadata: dict[str, Any]


@dataclass(slots=True)
class ResearchRunRecord:
    """Registry record for one append-only research run."""

    research_id: str
    run_id: str
    path: Path
    metadata: dict[str, Any]


class ExperimentRegistry:
    """Manage legacy experiment folders and metadata for reproducibility."""

    def __init__(self, *, root_dir: str | Path = "experiments") -> None:
        self.root_dir = Path(root_dir)
        self.root_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _timestamp_utc() -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    @staticmethod
    def _git_commit_hash() -> str:
        try:
            result = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                capture_output=True,
                text=True,
                check=False,
            )
            if result.returncode == 0:
                return result.stdout.strip()
        except Exception:
            pass
        return "unknown"

    @staticmethod
    def dataset_fingerprint(paths: Iterable[str | Path]) -> str:
        """Compute a stable fingerprint from local dataset file metadata."""

        hasher = hashlib.sha256()
        for path in sorted(Path(p) for p in paths):
            if not path.exists():
                continue
            stat = path.stat()
            payload = f"{path.resolve()}|{stat.st_size}|{int(stat.st_mtime)}".encode("utf-8")
            hasher.update(payload)
        return hasher.hexdigest()

    def _allocate_experiment_id(self, *, strategy_name: str, explicit_id: str | None = None) -> str:
        if explicit_id:
            return explicit_id
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        safe_name = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in strategy_name)
        return f"{stamp}_{safe_name}"

    def start_experiment(
        self,
        *,
        strategy_name: str,
        config: dict[str, Any],
        parameters: dict[str, Any],
        universe_definition: dict[str, Any],
        feature_definition: dict[str, Any],
        dataset_hash: str,
        experiment_id: str | None = None,
    ) -> ExperimentRecord:
        """Create a legacy experiment folder and seed reproducibility metadata."""

        exp_id = self._allocate_experiment_id(strategy_name=strategy_name, explicit_id=experiment_id)
        path = self.root_dir / exp_id
        if path.exists():
            raise FileExistsError(f"Experiment already exists: {path}")

        path.mkdir(parents=True, exist_ok=False)
        (path / "plots").mkdir(parents=True, exist_ok=False)

        metadata = {
            "experiment_id": exp_id,
            "timestamp": self._timestamp_utc(),
            "git_commit_hash": self._git_commit_hash(),
            "dataset_fingerprint": dataset_hash,
            "strategy_name": strategy_name,
            "parameters": parameters,
            "universe_definition": universe_definition,
            "feature_definition": feature_definition,
        }

        (path / "config.json").write_text(
            json.dumps({"config": config, "metadata": metadata}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (path / "metrics.json").write_text("{}\n", encoding="utf-8")
        (path / "artifacts.json").write_text("{}\n", encoding="utf-8")
        (path / "report.md").write_text("# Pending Report\n", encoding="utf-8")

        return ExperimentRecord(experiment_id=exp_id, path=path, metadata=metadata)

    def finalize_experiment(
        self,
        *,
        record: ExperimentRecord,
        metrics: dict[str, Any],
        artifacts: dict[str, Any],
        report_text: str | None = None,
    ) -> None:
        """Write final legacy experiment outputs."""

        (record.path / "metrics.json").write_text(
            json.dumps(metrics, ensure_ascii=False, indent=2, allow_nan=True),
            encoding="utf-8",
        )
        (record.path / "artifacts.json").write_text(
            json.dumps(artifacts, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        if report_text is not None:
            (record.path / "report.md").write_text(report_text, encoding="utf-8")

    def list_experiments(self) -> list[str]:
        return sorted(path.name for path in self.root_dir.iterdir() if path.is_dir())


class ResearchRunRegistry:
    """Manage append-only research runs under ``experiments/<research_id>/runs``."""

    def __init__(self, *, root_dir: str | Path = "experiments") -> None:
        self.root_dir = Path(root_dir)
        self.root_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _timestamp_utc() -> str:
        return ExperimentRegistry._timestamp_utc()

    @staticmethod
    def _git_commit_hash() -> str:
        return ExperimentRegistry._git_commit_hash()

    @staticmethod
    def _safe_name(value: str) -> str:
        safe = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in value.strip())
        if not safe:
            raise ValueError("Identifier cannot be empty")
        return safe

    @staticmethod
    def _read_json(path: Path, *, default: dict[str, Any] | None = None) -> dict[str, Any]:
        if not path.exists():
            return dict(default or {})
        return json.loads(path.read_text(encoding="utf-8"))

    @staticmethod
    def _write_json(path: Path, payload: dict[str, Any]) -> None:
        path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=True),
            encoding="utf-8",
        )

    def research_root(self, research_id: str) -> Path:
        return self.root_dir / self._safe_name(research_id)

    def runs_root(self, research_id: str) -> Path:
        return self.research_root(research_id) / "runs"

    def run_path(self, research_id: str, run_id: str) -> Path:
        return self.runs_root(research_id) / self._safe_name(run_id)

    def latest_path(self, research_id: str) -> Path:
        return self.research_root(research_id) / "latest.json"

    def index_path(self, research_id: str) -> Path:
        return self.research_root(research_id) / "run_index.json"

    def allocate_run_id(self, *, data_as_of: str, explicit_id: str | None = None) -> str:
        if explicit_id:
            return self._safe_name(explicit_id)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        as_of = data_as_of.replace("-", "")
        return f"{stamp}__asof_{as_of}"

    def create_run(
        self,
        *,
        research_id: str,
        data_as_of: str,
        spec_path: str | Path,
        rerun_mode: str,
        run_id: str | None = None,
    ) -> ResearchRunRecord:
        """Create a new append-only run directory and seed metadata files."""

        safe_research_id = self._safe_name(research_id)
        run_identifier = self.allocate_run_id(data_as_of=data_as_of, explicit_id=run_id)

        research_root = self.research_root(safe_research_id)
        runs_root = self.runs_root(safe_research_id)
        runs_root.mkdir(parents=True, exist_ok=True)

        path = runs_root / run_identifier
        if path.exists():
            raise FileExistsError(f"Research run already exists: {path}")

        path.mkdir(parents=True, exist_ok=False)
        (path / "plots").mkdir(parents=True, exist_ok=False)

        metadata = {
            "research_id": safe_research_id,
            "run_id": run_identifier,
            "status": "running",
            "created_at": self._timestamp_utc(),
            "completed_at": None,
            "data_as_of": data_as_of,
            "spec_path": str(Path(spec_path)),
            "rerun_mode": rerun_mode,
            "git_commit_hash": self._git_commit_hash(),
            "path": str(path),
        }
        self._write_json(path / "run_metadata.json", metadata)
        (path / "resolved_spec.json").write_text("{}\n", encoding="utf-8")
        (path / "data_manifest.json").write_text("{}\n", encoding="utf-8")
        (path / "metrics.json").write_text("{}\n", encoding="utf-8")
        (path / "artifacts.json").write_text("{}\n", encoding="utf-8")
        (path / "report.md").write_text("# Pending Research Run\n", encoding="utf-8")
        (path / "run.log").write_text("", encoding="utf-8")

        return ResearchRunRecord(research_id=safe_research_id, run_id=run_identifier, path=path, metadata=metadata)

    def write_run_log(self, record: ResearchRunRecord, lines: Iterable[str], *, append: bool = True) -> None:
        """Write plain-text run log lines into ``run.log``."""

        payload = "\n".join(str(line) for line in lines if str(line).strip())
        if not payload:
            return

        log_path = record.path / "run.log"
        if append and log_path.exists() and log_path.read_text(encoding="utf-8"):
            log_path.write_text(log_path.read_text(encoding="utf-8") + payload + "\n", encoding="utf-8")
            return
        log_path.write_text(payload.rstrip() + "\n", encoding="utf-8")

    def write_run_snapshot(
        self,
        *,
        record: ResearchRunRecord,
        resolved_spec: dict[str, Any],
        data_manifest: dict[str, Any],
    ) -> None:
        """Persist resolved run inputs before the study executes."""

        self._write_json(record.path / "resolved_spec.json", resolved_spec)
        self._write_json(record.path / "data_manifest.json", data_manifest)

    @staticmethod
    def _summary_metrics(metrics: dict[str, Any]) -> dict[str, Any]:
        return {key: metrics.get(key) for key in RUN_SUMMARY_METRICS if key in metrics}

    def _load_index(self, research_id: str) -> dict[str, Any]:
        return self._read_json(
            self.index_path(research_id),
            default={"research_id": research_id, "updated_at": None, "runs": []},
        )

    def _save_index(self, research_id: str, payload: dict[str, Any]) -> None:
        payload["research_id"] = research_id
        payload["updated_at"] = self._timestamp_utc()
        self._write_json(self.index_path(research_id), payload)

    def _build_index_entry(
        self,
        *,
        metadata: dict[str, Any],
        metrics: dict[str, Any],
        data_manifest: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "research_id": metadata["research_id"],
            "run_id": metadata["run_id"],
            "status": metadata["status"],
            "created_at": metadata["created_at"],
            "completed_at": metadata["completed_at"],
            "data_as_of": metadata["data_as_of"],
            "spec_path": metadata["spec_path"],
            "path": metadata["path"],
            "git_commit_hash": metadata.get("git_commit_hash", "unknown"),
            "dataset_fingerprint": data_manifest.get("dataset_fingerprint", ""),
            "metrics": self._summary_metrics(metrics),
        }

    def mark_succeeded(
        self,
        *,
        record: ResearchRunRecord,
        metrics: dict[str, Any],
        artifacts: dict[str, Any],
        data_manifest: dict[str, Any],
        report_text: str | None = None,
    ) -> None:
        """Mark a run as succeeded and update registry pointers."""

        if report_text is not None:
            (record.path / "report.md").write_text(report_text, encoding="utf-8")

        self._write_json(record.path / "metrics.json", metrics)
        self._write_json(record.path / "artifacts.json", artifacts)

        metadata = self._read_json(record.path / "run_metadata.json", default=record.metadata)
        metadata["status"] = "succeeded"
        metadata["completed_at"] = self._timestamp_utc()
        metadata["dataset_fingerprint"] = data_manifest.get("dataset_fingerprint", "")
        metadata["summary_metrics"] = self._summary_metrics(metrics)
        self._write_json(record.path / "run_metadata.json", metadata)

        index_payload = self._load_index(record.research_id)
        entry = self._build_index_entry(metadata=metadata, metrics=metrics, data_manifest=data_manifest)
        runs = [item for item in index_payload.get("runs", []) if item.get("run_id") != record.run_id]
        runs.append(entry)
        runs.sort(key=lambda item: (str(item.get("created_at", "")), str(item.get("run_id", ""))))
        index_payload["runs"] = runs
        self._save_index(record.research_id, index_payload)
        self._write_json(self.latest_path(record.research_id), entry)

    def mark_failed(
        self,
        *,
        record: ResearchRunRecord,
        error_message: str,
        metrics: dict[str, Any] | None = None,
        data_manifest: dict[str, Any] | None = None,
    ) -> None:
        """Mark a run as failed while preserving append-only artifacts."""

        metadata = self._read_json(record.path / "run_metadata.json", default=record.metadata)
        metadata["status"] = "failed"
        metadata["completed_at"] = self._timestamp_utc()
        metadata["error_message"] = error_message
        self._write_json(record.path / "run_metadata.json", metadata)

        index_payload = self._load_index(record.research_id)
        entry = self._build_index_entry(
            metadata=metadata,
            metrics=metrics or {},
            data_manifest=data_manifest or {},
        )
        entry["error_message"] = error_message
        runs = [item for item in index_payload.get("runs", []) if item.get("run_id") != record.run_id]
        runs.append(entry)
        runs.sort(key=lambda item: (str(item.get("created_at", "")), str(item.get("run_id", ""))))
        index_payload["runs"] = runs
        self._save_index(record.research_id, index_payload)

    def list_runs(self, research_id: str) -> list[dict[str, Any]]:
        """Return recorded runs sorted by creation time."""

        payload = self._load_index(self._safe_name(research_id))
        runs = list(payload.get("runs", []))
        runs.sort(key=lambda item: (str(item.get("created_at", "")), str(item.get("run_id", ""))))
        return runs

    def latest_run(self, research_id: str) -> dict[str, Any] | None:
        """Return the latest successful run summary if present."""

        payload = self._read_json(self.latest_path(self._safe_name(research_id)))
        return payload or None

    def previous_run(self, research_id: str) -> dict[str, Any] | None:
        """Return the successful run immediately before the latest one."""

        succeeded = [item for item in self.list_runs(research_id) if item.get("status") == "succeeded"]
        if len(succeeded) < 2:
            return None
        return succeeded[-2]

    def load_run_bundle(self, *, research_id: str, run_id: str) -> dict[str, Any]:
        """Load metadata and core artifacts for a single run."""

        run_dir = self.run_path(research_id, run_id)
        if not run_dir.exists():
            raise FileNotFoundError(f"Run not found: {run_dir}")

        return {
            "metadata": self._read_json(run_dir / "run_metadata.json"),
            "resolved_spec": self._read_json(run_dir / "resolved_spec.json"),
            "data_manifest": self._read_json(run_dir / "data_manifest.json"),
            "metrics": self._read_json(run_dir / "metrics.json"),
            "artifacts": self._read_json(run_dir / "artifacts.json"),
            "report_path": str(run_dir / "report.md"),
            "run_path": str(run_dir),
        }
