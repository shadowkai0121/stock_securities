"""Shared utilities for paper artifact generation."""

from __future__ import annotations

import json
import platform
import sys
from datetime import datetime, timezone
from importlib import metadata
from pathlib import Path
from typing import Any, Iterable

import pandas as pd


DEFAULT_MANUSCRIPT_FILES = (
    "abstract.md",
    "introduction.md",
    "literature_review.md",
    "empirical_design.md",
    "results.md",
    "conclusion.md",
)


def _read_json(path: Path, *, default: dict[str, Any] | None = None) -> dict[str, Any]:
    if not path.exists():
        return dict(default or {})
    return json.loads(path.read_text(encoding="utf-8"))


def resolve_run_path(
    *,
    experiment: str,
    experiments_root: str | Path = "experiments",
    research_id: str | None = None,
) -> tuple[str, str, Path]:
    """Resolve an experiment run path from run_id (or research_id:run_id)."""

    root = Path(experiments_root)
    exp_text = str(experiment).strip()
    if ":" in exp_text:
        left, right = exp_text.split(":", 1)
        candidate = root / left / "runs" / right
        if candidate.exists():
            return left, right, candidate

    if research_id:
        candidate = root / str(research_id) / "runs" / exp_text
        if not candidate.exists():
            raise FileNotFoundError(f"Run not found: {candidate}")
        return str(research_id), exp_text, candidate

    matches = list(root.glob(f"*/runs/{exp_text}"))
    if not matches:
        raise FileNotFoundError(f"Run id {exp_text!r} not found under {root}")
    if len(matches) > 1:
        raise ValueError(f"Run id {exp_text!r} is ambiguous; pass --research-id explicitly")

    path = matches[0]
    return path.parents[1].name, exp_text, path


def load_run_bundle(
    *,
    experiment: str,
    experiments_root: str | Path = "experiments",
    research_id: str | None = None,
) -> dict[str, Any]:
    """Load the canonical run bundle from an append-only run directory."""

    resolved_research_id, run_id, run_path = resolve_run_path(
        experiment=experiment,
        experiments_root=experiments_root,
        research_id=research_id,
    )
    return {
        "research_id": resolved_research_id,
        "run_id": run_id,
        "run_path": run_path,
        "run_metadata": _read_json(run_path / "run_metadata.json"),
        "resolved_spec": _read_json(run_path / "resolved_spec.json"),
        "data_manifest": _read_json(run_path / "data_manifest.json"),
        "metrics": _read_json(run_path / "metrics.json"),
        "artifacts": _read_json(run_path / "artifacts.json"),
        "report_path": run_path / "report.md",
    }


def load_csv(path: Path | str | None) -> pd.DataFrame:
    if path is None:
        return pd.DataFrame()
    target = Path(path)
    if not target.exists():
        return pd.DataFrame()
    return pd.read_csv(target)


def ensure_paper_workspace(*, papers_root: str | Path, paper_id: str) -> dict[str, Path]:
    """Ensure canonical paper workspace folders and manuscript placeholders."""

    root = Path(papers_root) / paper_id
    manuscript = root / "manuscript"
    tables = root / "tables"
    figures = root / "figures"
    appendix = root / "appendix"
    reproducibility = root / "reproducibility"

    for path in (root, manuscript, tables, figures, appendix, reproducibility):
        path.mkdir(parents=True, exist_ok=True)

    for filename in DEFAULT_MANUSCRIPT_FILES:
        target = manuscript / filename
        if not target.exists():
            section = filename.replace(".md", "").replace("_", " ").title()
            target.write_text(f"# {section}\n\nTODO: fill in this section.\n", encoding="utf-8")

    return {
        "paper_root": root,
        "manuscript": manuscript,
        "tables": tables,
        "figures": figures,
        "appendix": appendix,
        "reproducibility": reproducibility,
    }


def export_table(
    table: pd.DataFrame,
    *,
    output_dir: Path,
    stem: str,
    formats: Iterable[str] = ("csv", "md", "tex"),
) -> dict[str, str]:
    """Write one table into one or more export formats."""

    output_dir.mkdir(parents=True, exist_ok=True)
    written: dict[str, str] = {}
    for fmt in formats:
        fmt_clean = str(fmt).strip().lower()
        if fmt_clean not in {"csv", "md", "tex"}:
            continue
        target = output_dir / f"{stem}.{fmt_clean}"
        if fmt_clean == "csv":
            table.to_csv(target, index=False)
        elif fmt_clean == "md":
            try:
                target.write_text(table.to_markdown(index=False), encoding="utf-8")
            except ImportError:
                target.write_text(_table_to_markdown(table), encoding="utf-8")
        else:
            target.write_text(table.to_latex(index=False, escape=False), encoding="utf-8")
        written[fmt_clean] = str(target)
    return written


def _table_to_markdown(table: pd.DataFrame) -> str:
    """Lightweight markdown renderer that does not require optional dependencies."""

    if table.empty and len(table.columns) == 0:
        return "| |\n| --- |\n| |\n"

    headers = [str(col) for col in table.columns]
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    if table.empty:
        return "\n".join(lines) + "\n"

    for row in table.itertuples(index=False, name=None):
        values: list[str] = []
        for value in row:
            if value is None or (isinstance(value, float) and pd.isna(value)):
                values.append("")
            else:
                values.append(str(value))
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines) + "\n"


def write_reproducibility_artifacts(
    *,
    workspace: dict[str, Path],
    run_bundle: dict[str, Any],
    run_ids: list[str],
) -> dict[str, str]:
    """Persist reproducibility payloads under papers/<paper_id>/reproducibility."""

    repro = workspace["reproducibility"]
    spec_path = repro / "research_spec.json"
    run_ids_path = repro / "experiment_run_ids.txt"
    manifest_path = repro / "data_manifest.json"
    env_path = repro / "environment_info.json"

    spec_path.write_text(
        json.dumps(run_bundle.get("resolved_spec", {}), ensure_ascii=False, indent=2, allow_nan=True),
        encoding="utf-8",
    )
    run_ids_path.write_text("\n".join(str(item) for item in run_ids) + "\n", encoding="utf-8")
    manifest_path.write_text(
        json.dumps(run_bundle.get("data_manifest", {}), ensure_ascii=False, indent=2, allow_nan=True),
        encoding="utf-8",
    )

    package_names = ["pandas", "numpy", "statsmodels", "matplotlib", "scipy"]
    package_versions: dict[str, str] = {}
    for name in package_names:
        try:
            package_versions[name] = metadata.version(name)
        except metadata.PackageNotFoundError:
            package_versions[name] = "not-installed"

    environment_info = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "python_version": sys.version,
        "platform": platform.platform(),
        "package_versions": package_versions,
    }
    env_path.write_text(
        json.dumps(environment_info, ensure_ascii=False, indent=2, allow_nan=True),
        encoding="utf-8",
    )

    return {
        "research_spec": str(spec_path),
        "experiment_run_ids": str(run_ids_path),
        "data_manifest": str(manifest_path),
        "environment_info": str(env_path),
    }
