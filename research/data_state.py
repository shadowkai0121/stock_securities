"""Dataset coverage, validation, and manifest helpers for research reruns."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from data.loaders.finmind_loader import FinMindLoader, IngestionResult
from data.storage.sqlite_store import SQLiteStore
from data.validation.data_checks import run_dataset_checks
from experiments.registry import ExperimentRegistry


REMOTE_DATASET_IDS = {
    "price": "TaiwanStockPrice",
    "price_adj": "TaiwanStockPriceAdj",
    "margin": "TaiwanStockMarginPurchaseShortSale",
    "broker": "TaiwanStockTradingDailyReport",
    "holding_shares": "TaiwanStockHoldingSharesPer",
    "stock_info": "TaiwanStockInfo",
    "warrant": "TaiwanStockInfoWithWarrantSummary",
}

SHARED_DB_FILENAMES = {
    "market": "market.sqlite",
}


@dataclass(frozen=True, slots=True)
class DatasetTarget:
    """One local table that a research run depends on."""

    dataset_name: str
    table: str
    db_path: Path
    stock_id: str | None
    scope: str
    remote_dataset: str

    def history_stock_id(self) -> str:
        if self.stock_id:
            return self.stock_id
        return "__ALL__"

    def to_dict(self) -> dict[str, Any]:
        return {
            "dataset_name": self.dataset_name,
            "table": self.table,
            "db_path": str(self.db_path),
            "stock_id": self.stock_id,
            "scope": self.scope,
            "remote_dataset": self.remote_dataset,
        }


@dataclass(frozen=True, slots=True)
class DatasetCoverage:
    """Coverage and freshness snapshot for one dataset target."""

    target: DatasetTarget
    row_count_as_of: int
    min_date: str | None
    max_date: str | None
    db_mtime_utc: str | None
    db_file_size_bytes: int
    db_fingerprint: str
    latest_meta_run_at: str | None
    latest_requested_end_date: str | None
    table_last_inserted_at: str | None
    covers_data_as_of: bool
    needs_update: bool
    detail: str

    def to_dict(self) -> dict[str, Any]:
        return {
            **self.target.to_dict(),
            "row_count_as_of": self.row_count_as_of,
            "min_date": self.min_date,
            "max_date": self.max_date,
            "db_mtime_utc": self.db_mtime_utc,
            "db_file_size_bytes": self.db_file_size_bytes,
            "db_fingerprint": self.db_fingerprint,
            "latest_meta_run_at": self.latest_meta_run_at,
            "latest_requested_end_date": self.latest_requested_end_date,
            "table_last_inserted_at": self.table_last_inserted_at,
            "covers_data_as_of": self.covers_data_as_of,
            "needs_update": self.needs_update,
            "detail": self.detail,
        }


def load_data_catalog(path: str | Path) -> dict[str, Any]:
    """Load the JSON-formatted data catalog."""

    catalog_path = Path(path)
    if not catalog_path.exists():
        raise FileNotFoundError(f"Data catalog not found: {catalog_path}")
    return json.loads(catalog_path.read_text(encoding="utf-8"))


def dataset_catalog_map(catalog: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Index catalog rows by dataset name."""

    output: dict[str, dict[str, Any]] = {}
    for item in catalog.get("datasets", []):
        name = str(item.get("dataset_name", "")).strip()
        if name:
            output[name] = item
    return output


def resolve_dataset_targets(
    *,
    required_datasets: list[str],
    stock_ids: list[str],
    data_root: str | Path,
    catalog: dict[str, Any],
) -> list[DatasetTarget]:
    """Expand required dataset names into concrete local tables."""

    data_root_path = Path(data_root)
    dataset_map = dataset_catalog_map(catalog)
    targets: list[DatasetTarget] = []

    for dataset_name in required_datasets:
        item = dataset_map.get(dataset_name)
        if item is None:
            raise KeyError(f"Dataset {dataset_name!r} is not defined in the data catalog")

        table = str(item.get("storage_table", "")).strip()
        remote_dataset = REMOTE_DATASET_IDS.get(dataset_name, dataset_name)

        if dataset_name == "stock_info":
            targets.append(
                DatasetTarget(
                    dataset_name=dataset_name,
                    table=table,
                    db_path=data_root_path / SHARED_DB_FILENAMES["market"],
                    stock_id=None,
                    scope="market",
                    remote_dataset=remote_dataset,
                )
            )
            continue

        if not stock_ids:
            raise ValueError(
                f"Dataset {dataset_name!r} requires explicit stock_ids in the spec or local data_root"
            )

        for stock_id in stock_ids:
            db_path = data_root_path / f"{stock_id}.sqlite"
            targets.append(
                DatasetTarget(
                    dataset_name=dataset_name,
                    table=table,
                    db_path=db_path,
                    stock_id=stock_id,
                    scope="stock",
                    remote_dataset=remote_dataset,
                )
            )

    return targets


def _file_mtime_utc(path: Path) -> str | None:
    if not path.exists():
        return None
    return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _latest_meta_run(store: SQLiteStore, target: DatasetTarget) -> dict[str, Any] | None:
    if not store.table_exists("meta_runs"):
        return None

    frame = store.read_table(
        "meta_runs",
        columns=["created_at", "end_date", "status", "fetched_rows", "inserted_rows"],
        where="dataset = ? AND stock_id = ?",
        params=[target.remote_dataset, target.history_stock_id()],
        order_by="created_at DESC",
        limit=1,
    )
    if frame.empty:
        return None

    row = frame.iloc[0].to_dict()
    return {
        "created_at": row.get("created_at"),
        "end_date": row.get("end_date"),
        "status": row.get("status"),
        "fetched_rows": int(row.get("fetched_rows", 0) or 0),
        "inserted_rows": int(row.get("inserted_rows", 0) or 0),
    }


def _table_last_inserted_at(store: SQLiteStore, table: str) -> str | None:
    if "inserted_at" not in store.list_columns(table):
        return None
    row = store.fetch_one(f'SELECT MAX("inserted_at") FROM "{table}"')
    if row is None:
        return None
    value = row[0]
    return str(value) if value else None


def inspect_dataset_target(
    target: DatasetTarget,
    *,
    analysis_start: str,
    data_as_of: str,
) -> DatasetCoverage:
    """Inspect current local coverage for a dataset target."""

    path = target.db_path
    if not path.exists():
        return DatasetCoverage(
            target=target,
            row_count_as_of=0,
            min_date=None,
            max_date=None,
            db_mtime_utc=None,
            db_file_size_bytes=0,
            db_fingerprint="",
            latest_meta_run_at=None,
            latest_requested_end_date=None,
            table_last_inserted_at=None,
            covers_data_as_of=False,
            needs_update=True,
            detail="database file missing",
        )

    store = SQLiteStore(path)
    if not store.table_exists(target.table):
        return DatasetCoverage(
            target=target,
            row_count_as_of=0,
            min_date=None,
            max_date=None,
            db_mtime_utc=_file_mtime_utc(path),
            db_file_size_bytes=int(path.stat().st_size),
            db_fingerprint=ExperimentRegistry.dataset_fingerprint([path]),
            latest_meta_run_at=None,
            latest_requested_end_date=None,
            table_last_inserted_at=None,
            covers_data_as_of=False,
            needs_update=True,
            detail=f"table missing: {target.table}",
        )

    row_count_as_of = store.row_count(target.table, where='date <= ?', params=[data_as_of])
    min_date, max_date = store.min_max_date(target.table)
    latest_meta = _latest_meta_run(store, target)
    latest_requested_end_date = str(latest_meta.get("end_date")) if latest_meta and latest_meta.get("end_date") else None
    latest_meta_run_at = str(latest_meta.get("created_at")) if latest_meta and latest_meta.get("created_at") else None

    has_start_coverage = bool(min_date and str(min_date) <= analysis_start)
    if target.dataset_name == "stock_info":
        covers = row_count_as_of > 0 and has_start_coverage
        detail = "metadata available on or before data_as_of"
        if row_count_as_of <= 0:
            detail = "no metadata rows up to data_as_of"
        elif not has_start_coverage:
            detail = "metadata does not cover analysis start"
    else:
        requested_through_cutoff = bool(
            latest_requested_end_date
            and str(latest_requested_end_date) >= data_as_of
            and latest_meta
            and str(latest_meta.get("status", "")).lower() == "success"
        )
        max_reaches_cutoff = bool(max_date and str(max_date) >= data_as_of)
        covers = row_count_as_of > 0 and has_start_coverage and (max_reaches_cutoff or requested_through_cutoff)

        detail = "coverage satisfied"
        if row_count_as_of <= 0:
            detail = "no rows up to data_as_of"
        elif not has_start_coverage:
            detail = "table does not cover analysis start"
        elif not (max_reaches_cutoff or requested_through_cutoff):
            detail = "local data does not yet cover data_as_of"

    return DatasetCoverage(
        target=target,
        row_count_as_of=int(row_count_as_of),
        min_date=min_date,
        max_date=max_date,
        db_mtime_utc=_file_mtime_utc(path),
        db_file_size_bytes=int(path.stat().st_size),
        db_fingerprint=ExperimentRegistry.dataset_fingerprint([path]),
        latest_meta_run_at=latest_meta_run_at,
        latest_requested_end_date=latest_requested_end_date,
        table_last_inserted_at=_table_last_inserted_at(store, target.table),
        covers_data_as_of=covers,
        needs_update=not covers,
        detail=detail,
    )


def _next_date(value: str) -> str:
    return (date.fromisoformat(value) + timedelta(days=1)).strftime("%Y-%m-%d")


def _ingestion_start(coverage: DatasetCoverage, *, analysis_start: str) -> str:
    if coverage.min_date is None or coverage.min_date > analysis_start:
        return analysis_start
    if coverage.max_date is None:
        return analysis_start
    return _next_date(str(coverage.max_date))


def _run_ingestion(
    loader: FinMindLoader,
    target: DatasetTarget,
    *,
    coverage: DatasetCoverage,
    data_as_of: str,
    data_update_policy: dict[str, Any],
    analysis_start: str,
) -> IngestionResult:
    if target.dataset_name == "stock_info":
        return loader.download_stock_info(
            start_date=str(data_update_policy.get("stock_info_start_date") or analysis_start),
            db_path=target.db_path,
        )

    start_date = _ingestion_start(coverage, analysis_start=analysis_start)
    if target.dataset_name == "price":
        return loader.download_price(
            stock_id=str(target.stock_id),
            start_date=start_date,
            end_date=data_as_of,
            db_path=target.db_path,
        )
    if target.dataset_name == "price_adj":
        return loader.download_price_adj(
            stock_id=str(target.stock_id),
            start_date=start_date,
            end_date=data_as_of,
            db_path=target.db_path,
        )
    if target.dataset_name == "margin":
        return loader.download_margin(
            stock_id=str(target.stock_id),
            start_date=start_date,
            end_date=data_as_of,
            db_path=target.db_path,
        )
    if target.dataset_name == "broker":
        return loader.download_broker(
            stock_id=str(target.stock_id),
            start_date=start_date,
            end_date=data_as_of,
            db_path=target.db_path,
        )
    if target.dataset_name == "holding_shares":
        return loader.download_holding_shares(
            stock_id=str(target.stock_id) if target.stock_id else None,
            start_date=start_date,
            end_date=data_as_of,
            db_path=target.db_path,
        )

    raise ValueError(f"No ingestion handler registered for dataset {target.dataset_name!r}")


def ensure_local_datasets(
    *,
    targets: list[DatasetTarget],
    analysis_start: str,
    data_as_of: str,
    data_update_policy: dict[str, Any],
    finmind_loader: FinMindLoader,
) -> dict[str, Any]:
    """Ensure required local datasets cover the requested cutoff."""

    auto_update_missing = bool(data_update_policy.get("auto_update_missing", True))
    auto_update_stale = bool(data_update_policy.get("auto_update_stale", True))

    updates: list[dict[str, Any]] = []
    logs: list[str] = []
    final_coverages: list[DatasetCoverage] = []

    for target in targets:
        coverage = inspect_dataset_target(target, analysis_start=analysis_start, data_as_of=data_as_of)
        if not coverage.needs_update:
            final_coverages.append(coverage)
            logs.append(
                f"[coverage] {target.dataset_name}:{target.stock_id or '__ALL__'} {coverage.detail} max_date={coverage.max_date}"
            )
            continue

        if coverage.row_count_as_of == 0 and not auto_update_missing:
            raise FileNotFoundError(f"Missing local dataset and auto_update_missing disabled: {target.to_dict()}")
        if coverage.row_count_as_of > 0 and not auto_update_stale:
            raise FileNotFoundError(f"Stale local dataset and auto_update_stale disabled: {target.to_dict()}")

        result = _run_ingestion(
            finmind_loader,
            target,
            coverage=coverage,
            data_as_of=data_as_of,
            data_update_policy=data_update_policy,
            analysis_start=analysis_start,
        )
        update_payload = {
            "dataset_name": target.dataset_name,
            "stock_id": target.stock_id,
            "table": result.table,
            "db_path": str(result.db_path),
            "start_date": result.start_date,
            "end_date": result.end_date,
            "fetched_rows": result.fetched_rows,
            "inserted_rows": result.inserted_rows,
        }
        updates.append(update_payload)
        logs.append(
            f"[ingest] {target.dataset_name}:{target.stock_id or '__ALL__'} fetched={result.fetched_rows} inserted={result.inserted_rows}"
        )

        refreshed = inspect_dataset_target(target, analysis_start=analysis_start, data_as_of=data_as_of)
        if not refreshed.covers_data_as_of:
            raise RuntimeError(
                f"Local coverage still incomplete after finmind-dl update for {target.dataset_name}:{target.stock_id or '__ALL__'}: {refreshed.detail}"
            )
        final_coverages.append(refreshed)

    return {
        "coverages": [item.to_dict() for item in final_coverages],
        "updates": updates,
        "logs": logs,
    }


def _required_non_null_columns(quality_checks: list[str]) -> list[str]:
    cols: list[str] = []
    if "date_not_null" in quality_checks:
        cols.append("date")
    if "stock_id_not_null" in quality_checks:
        cols.append("stock_id")
    return cols


def validate_dataset_targets(
    *,
    targets: list[DatasetTarget],
    catalog: dict[str, Any],
) -> list[dict[str, Any]]:
    """Run data-catalog quality checks against local tables."""

    dataset_map = dataset_catalog_map(catalog)
    reports: list[dict[str, Any]] = []

    for target in targets:
        item = dataset_map.get(target.dataset_name)
        if item is None:
            continue

        if not target.db_path.exists():
            reports.append(
                {
                    **target.to_dict(),
                    "passed": False,
                    "detail": "database file missing",
                }
            )
            continue

        store = SQLiteStore(target.db_path)
        quality_checks = [str(value) for value in item.get("quality_checks", [])]
        enabled_checks = [
            check
            for check in quality_checks
            if check in {"table_exists", "row_count_positive", "primary_key_unique", "required_non_null"}
        ]
        if "date_not_null" in quality_checks or "stock_id_not_null" in quality_checks:
            if "required_non_null" not in enabled_checks:
                enabled_checks.append("required_non_null")

        primary_keys = [str(value) for value in item.get("primary_keys", []) if str(value) != "dataset_specific"]
        report = run_dataset_checks(
            store=store,
            dataset_name=target.dataset_name,
            table=target.table,
            primary_keys=primary_keys,
            required_non_null_columns=_required_non_null_columns(quality_checks),
            enabled_checks=enabled_checks,
        )
        reports.append(
            {
                **target.to_dict(),
                "passed": report.passed,
                "checks": [
                    {"name": check.name, "passed": check.passed, "detail": check.detail}
                    for check in report.checks
                ],
            }
        )

    return reports


def build_data_manifest(
    *,
    research_id: str,
    run_id: str,
    data_as_of: str,
    analysis_start: str,
    targets: list[DatasetTarget],
) -> dict[str, Any]:
    """Build an auditable manifest of the local datasets used for a run."""

    entries = [
        inspect_dataset_target(target, analysis_start=analysis_start, data_as_of=data_as_of).to_dict()
        for target in targets
    ]

    db_files: dict[str, dict[str, Any]] = {}
    summary_by_dataset: dict[str, dict[str, Any]] = {}
    for entry in entries:
        db_path = str(entry["db_path"])
        if db_path not in db_files:
            path = Path(db_path)
            db_files[db_path] = {
                "db_path": db_path,
                "db_file_size_bytes": int(path.stat().st_size) if path.exists() else 0,
                "db_mtime_utc": _file_mtime_utc(path),
                "db_fingerprint": ExperimentRegistry.dataset_fingerprint([path]) if path.exists() else "",
            }

        dataset_summary = summary_by_dataset.setdefault(
            str(entry["dataset_name"]),
            {
                "dataset_name": entry["dataset_name"],
                "table": entry["table"],
                "db_count": 0,
                "row_count_as_of_total": 0,
                "min_date": None,
                "max_date": None,
                "latest_meta_run_at": None,
                "latest_requested_end_date": None,
            },
        )
        dataset_summary["db_count"] += 1
        dataset_summary["row_count_as_of_total"] += int(entry["row_count_as_of"])

        min_date = entry.get("min_date")
        max_date = entry.get("max_date")
        if min_date and (dataset_summary["min_date"] is None or str(min_date) < str(dataset_summary["min_date"])):
            dataset_summary["min_date"] = min_date
        if max_date and (dataset_summary["max_date"] is None or str(max_date) > str(dataset_summary["max_date"])):
            dataset_summary["max_date"] = max_date

        latest_meta = entry.get("latest_meta_run_at")
        latest_requested_end_date = entry.get("latest_requested_end_date")
        if latest_meta and (
            dataset_summary["latest_meta_run_at"] is None
            or str(latest_meta) > str(dataset_summary["latest_meta_run_at"])
        ):
            dataset_summary["latest_meta_run_at"] = latest_meta
        if latest_requested_end_date and (
            dataset_summary["latest_requested_end_date"] is None
            or str(latest_requested_end_date) > str(dataset_summary["latest_requested_end_date"])
        ):
            dataset_summary["latest_requested_end_date"] = latest_requested_end_date

    db_paths = [Path(item["db_path"]) for item in db_files.values()]
    return {
        "research_id": research_id,
        "run_id": run_id,
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "data_as_of": data_as_of,
        "analysis_start": analysis_start,
        "dataset_fingerprint": ExperimentRegistry.dataset_fingerprint(db_paths),
        "db_files": list(db_files.values()),
        "datasets": entries,
        "dataset_summary": list(summary_by_dataset.values()),
    }
