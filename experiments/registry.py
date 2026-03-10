"""Experiment registry for reproducible quantitative research runs."""

from __future__ import annotations

import hashlib
import json
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


@dataclass(slots=True)
class ExperimentRecord:
    """Registry record for one experiment run."""

    experiment_id: str
    path: Path
    metadata: dict[str, Any]


class ExperimentRegistry:
    """Manage experiment folders and metadata for reproducibility."""

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
        """Create experiment folder and seed reproducibility metadata files."""

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
        """Write final metrics/artifacts and optional report content."""

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
