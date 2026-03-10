"""Research orchestrator for end-to-end local quantitative experiments."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd

from data.loaders.finmind_loader import FinMindLoader
from data.storage.sqlite_store import SQLiteStore
from data.validation.data_checks import run_dataset_checks
from experiments.registry import ExperimentRegistry
from features.feature_store import FeatureStore
from research.backtest_engine import BacktestConfig, LongCashBacktestEngine
from research.data_loader import ResearchDataLoader
from research.report_generator import MarkdownReportGenerator
from research.statistics import (
    bootstrap_confidence_interval,
    expanding_window_evaluation,
    newey_west_t_statistics,
    rolling_window_evaluation,
    subperiod_analysis,
    train_valid_test_split_by_ratio,
    walk_forward_validation,
)
from research.strategies.ma_cross_adapter import MACrossoverSignalModel
from universe.universe_builder import TaiwanEquityUniverseBuilder


DEFAULT_REQUIRED_DATASETS = ["stock_info", "price_adj"]


class ResearchOrchestrator:
    """End-to-end orchestrator that preserves finmind-dl as ingestion boundary."""

    def __init__(
        self,
        *,
        catalog_path: str | Path = "data/catalog/data_catalog.yaml",
        finmind_loader: FinMindLoader | None = None,
        registry: ExperimentRegistry | None = None,
        report_generator: MarkdownReportGenerator | None = None,
    ) -> None:
        self.catalog_path = Path(catalog_path)
        self._external_loader = finmind_loader
        self._external_registry = registry
        self._external_reporter = report_generator

    @staticmethod
    def _load_config(config: str | Path | dict[str, Any]) -> dict[str, Any]:
        if isinstance(config, dict):
            return config
        path = Path(config)
        if not path.exists():
            raise FileNotFoundError(f"Config not found: {path}")
        return json.loads(path.read_text(encoding="utf-8"))

    def _load_catalog(self) -> dict[str, Any]:
        if not self.catalog_path.exists():
            raise FileNotFoundError(f"Data catalog not found: {self.catalog_path}")
        # The catalog file is JSON-formatted YAML for stdlib-only parsing.
        return json.loads(self.catalog_path.read_text(encoding="utf-8"))

    @staticmethod
    def _dataset_map(catalog: dict[str, Any]) -> dict[str, dict[str, Any]]:
        output: dict[str, dict[str, Any]] = {}
        for item in catalog.get("datasets", []):
            name = str(item.get("dataset_name", "")).strip()
            if name:
                output[name] = item
        return output

    @staticmethod
    def _table_has_rows(db_path: Path, table: str, *, start_date: str | None = None, end_date: str | None = None) -> bool:
        if not db_path.exists():
            return False
        store = SQLiteStore(db_path)
        if not store.table_exists(table):
            return False

        where_parts: list[str] = []
        params: list[Any] = []
        if start_date:
            where_parts.append("date >= ?")
            params.append(start_date)
        if end_date:
            where_parts.append("date <= ?")
            params.append(end_date)
        where = " AND ".join(where_parts) if where_parts else None
        count = store.row_count(table, where=where, params=params)
        return count > 0

    def _resolve_loader(self, *, token: str | None) -> FinMindLoader:
        if self._external_loader is not None:
            return self._external_loader
        return FinMindLoader(token=token)

    def _resolve_registry(self, *, root_dir: Path) -> ExperimentRegistry:
        if self._external_registry is not None:
            return self._external_registry
        return ExperimentRegistry(root_dir=root_dir)

    def _resolve_reporter(self) -> MarkdownReportGenerator:
        if self._external_reporter is not None:
            return self._external_reporter
        return MarkdownReportGenerator()

    def _ensure_required_datasets(
        self,
        *,
        cfg: dict[str, Any],
        stock_ids: list[str],
        data_root: Path,
        required_datasets: list[str],
    ) -> list[str]:
        logs: list[str] = []

        ingestion_cfg = cfg.get("ingestion", {})
        auto_ingest = bool(ingestion_cfg.get("auto_ingest_missing", True))
        token = ingestion_cfg.get("token")

        start_date = cfg["start_date"]
        end_date = cfg["end_date"]
        stock_info_db = Path(ingestion_cfg.get("stock_info_db", data_root / "stock_info.sqlite"))
        holding_db = Path(ingestion_cfg.get("holding_shares_db", data_root / "holding_shares_per.sqlite"))

        # Determine missing datasets first.
        missing: list[str] = []

        if "stock_info" in required_datasets:
            if not self._table_has_rows(stock_info_db, "stock_info"):
                missing.append("stock_info")

        for dataset_name, table in [
            ("price", "price_daily"),
            ("price_adj", "price_adj_daily"),
            ("margin", "margin_daily"),
            ("broker", "broker_trades"),
        ]:
            if dataset_name not in required_datasets:
                continue
            for stock_id in stock_ids:
                db_path = data_root / f"{stock_id}.sqlite"
                if not self._table_has_rows(db_path, table, start_date=start_date, end_date=end_date):
                    missing.append(f"{dataset_name}:{stock_id}")

        if "holding_shares" in required_datasets and stock_ids:
            if not self._table_has_rows(holding_db, "holding_shares_per", start_date=start_date, end_date=end_date):
                missing.append("holding_shares")

        if not missing:
            logs.append("All required datasets already available locally.")
            return logs

        if not auto_ingest:
            raise FileNotFoundError(
                "Missing required local datasets and auto ingestion disabled: " + ", ".join(sorted(missing))
            )

        loader = self._resolve_loader(token=token)

        if "stock_info" in [item.split(":", 1)[0] for item in missing]:
            result = loader.download_stock_info(
                start_date=str(ingestion_cfg.get("stock_info_start_date", start_date)),
                db_path=stock_info_db,
            )
            logs.append(f"[ingest] stock_info inserted={result.inserted_rows} db={result.db_path}")

        for item in missing:
            if ":" not in item:
                continue
            dataset_name, stock_id = item.split(":", 1)
            db_path = data_root / f"{stock_id}.sqlite"
            if dataset_name == "price_adj":
                result = loader.download_price_adj(
                    stock_id=stock_id,
                    start_date=start_date,
                    end_date=end_date,
                    db_path=db_path,
                )
            elif dataset_name == "price":
                result = loader.download_price(
                    stock_id=stock_id,
                    start_date=start_date,
                    end_date=end_date,
                    db_path=db_path,
                )
            elif dataset_name == "margin":
                result = loader.download_margin(
                    stock_id=stock_id,
                    start_date=start_date,
                    end_date=end_date,
                    db_path=db_path,
                )
            elif dataset_name == "broker":
                result = loader.download_broker(
                    stock_id=stock_id,
                    start_date=start_date,
                    end_date=end_date,
                    db_path=db_path,
                )
            else:
                continue
            logs.append(f"[ingest] {dataset_name}:{stock_id} inserted={result.inserted_rows} db={result.db_path}")

        if "holding_shares" in [item.split(":", 1)[0] for item in missing]:
            for stock_id in stock_ids:
                result = loader.download_holding_shares(
                    stock_id=stock_id,
                    start_date=start_date,
                    end_date=end_date,
                    db_path=holding_db,
                )
                logs.append(f"[ingest] holding_shares:{stock_id} inserted={result.inserted_rows} db={result.db_path}")

        return logs

    @staticmethod
    def _required_non_null_columns(quality_checks: list[str]) -> list[str]:
        cols: list[str] = []
        if "date_not_null" in quality_checks:
            cols.append("date")
        if "stock_id_not_null" in quality_checks:
            cols.append("stock_id")
        return cols

    def _validate_required_datasets(
        self,
        *,
        catalog: dict[str, Any],
        data_root: Path,
        stock_ids: list[str],
        required_datasets: list[str],
    ) -> list[dict[str, Any]]:
        dataset_map = self._dataset_map(catalog)
        reports: list[dict[str, Any]] = []

        for dataset_name in required_datasets:
            if dataset_name not in dataset_map:
                continue

            item = dataset_map[dataset_name]
            table = str(item.get("storage_table", ""))
            if not table or "bundle" in table:
                continue

            quality_checks = [str(x) for x in item.get("quality_checks", [])]
            enabled_checks = [
                check
                for check in quality_checks
                if check in {"table_exists", "row_count_positive", "primary_key_unique", "required_non_null"}
            ]
            if "date_not_null" in quality_checks or "stock_id_not_null" in quality_checks:
                if "required_non_null" not in enabled_checks:
                    enabled_checks.append("required_non_null")

            required_non_null_cols = self._required_non_null_columns(quality_checks)
            primary_keys = [str(x) for x in item.get("primary_keys", []) if str(x) != "dataset_specific"]

            if dataset_name in {"stock_info", "holding_shares"}:
                db_path = data_root / ("stock_info.sqlite" if dataset_name == "stock_info" else "holding_shares_per.sqlite")
                if not db_path.exists():
                    reports.append(
                        {
                            "dataset": dataset_name,
                            "table": table,
                            "db_path": str(db_path),
                            "passed": False,
                            "detail": "database file missing",
                        }
                    )
                    continue

                store = SQLiteStore(db_path)
                report = run_dataset_checks(
                    store=store,
                    dataset_name=dataset_name,
                    table=table,
                    primary_keys=primary_keys,
                    required_non_null_columns=required_non_null_cols,
                    enabled_checks=enabled_checks,
                )
                reports.append(
                    {
                        "dataset": dataset_name,
                        "table": table,
                        "db_path": str(db_path),
                        "passed": report.passed,
                        "checks": [
                            {"name": check.name, "passed": check.passed, "detail": check.detail}
                            for check in report.checks
                        ],
                    }
                )
                continue

            for stock_id in stock_ids:
                db_path = data_root / f"{stock_id}.sqlite"
                if not db_path.exists():
                    reports.append(
                        {
                            "dataset": dataset_name,
                            "table": table,
                            "db_path": str(db_path),
                            "stock_id": stock_id,
                            "passed": False,
                            "detail": "database file missing",
                        }
                    )
                    continue

                store = SQLiteStore(db_path)
                report = run_dataset_checks(
                    store=store,
                    dataset_name=dataset_name,
                    table=table,
                    primary_keys=primary_keys,
                    required_non_null_columns=required_non_null_cols,
                    enabled_checks=enabled_checks,
                )
                reports.append(
                    {
                        "dataset": dataset_name,
                        "table": table,
                        "db_path": str(db_path),
                        "stock_id": stock_id,
                        "passed": report.passed,
                        "checks": [
                            {"name": check.name, "passed": check.passed, "detail": check.detail}
                            for check in report.checks
                        ],
                    }
                )

        return reports

    @staticmethod
    def _walk_forward_evaluator(train: pd.DataFrame, test: pd.DataFrame) -> dict[str, float | int]:
        train_ret = pd.to_numeric(train.get("net_return"), errors="coerce").dropna()
        test_ret = pd.to_numeric(test.get("net_return"), errors="coerce").dropna()
        return {
            "train_mean": float(train_ret.mean()) if not train_ret.empty else float("nan"),
            "test_mean": float(test_ret.mean()) if not test_ret.empty else float("nan"),
            "train_obs": int(len(train_ret)),
            "test_obs": int(len(test_ret)),
        }

    def run(self, config: str | Path | dict[str, Any]) -> dict[str, Any]:
        """Run end-to-end workflow:

        config -> data checks/ingestion -> universe -> features -> strategy ->
        backtest -> statistical validation -> report -> experiment registry.
        """

        cfg = self._load_config(config)
        catalog = self._load_catalog()

        start_date = str(cfg["start_date"])
        end_date = str(cfg["end_date"])
        data_root = Path(cfg.get("data_root", "data"))
        data_root.mkdir(parents=True, exist_ok=True)

        data_loader = ResearchDataLoader(data_root=data_root)

        stock_ids = [str(x) for x in cfg.get("stock_ids", []) if str(x).strip()]
        if not stock_ids:
            stock_ids = data_loader.available_stock_ids()

        required_datasets = [str(x) for x in cfg.get("required_datasets", DEFAULT_REQUIRED_DATASETS)]

        ingestion_logs = self._ensure_required_datasets(
            cfg=cfg,
            stock_ids=stock_ids,
            data_root=data_root,
            required_datasets=required_datasets,
        )

        validation_reports = self._validate_required_datasets(
            catalog=catalog,
            data_root=data_root,
            stock_ids=stock_ids,
            required_datasets=required_datasets,
        )

        if validation_reports and not all(bool(item.get("passed", False)) for item in validation_reports):
            failed = [item for item in validation_reports if not bool(item.get("passed", False))]
            raise RuntimeError(f"Dataset validation failed for {len(failed)} checks. First failure: {failed[0]}")

        universe_cfg = {
            "start_date": start_date,
            "end_date": end_date,
            "stock_ids": stock_ids or None,
            **cfg.get("universe", {}),
        }
        universe_builder = TaiwanEquityUniverseBuilder(data_loader)
        universe = universe_builder.build(**universe_cfg)
        if universe.empty:
            raise RuntimeError("Universe builder returned no tradable stocks.")

        active_stock_ids = sorted(
            universe.loc[universe["tradable_flag"] == 1, "stock_id"].astype(str).unique().tolist()
        )
        if not active_stock_ids:
            raise RuntimeError("No active stock_ids available after tradable filter.")

        strategy_cfg = dict(cfg.get("strategy", {}))
        use_adjusted = bool(strategy_cfg.get("use_adjusted_price", True))

        prices = data_loader.load_prices(
            stock_ids=active_stock_ids,
            start_date=start_date,
            end_date=end_date,
            adjusted=use_adjusted,
        )
        if prices.empty:
            raise RuntimeError("No local price data available for active universe.")

        feature_cfg = dict(cfg.get("features", {}))
        use_margin = bool(feature_cfg.get("use_margin", False))
        use_broker = bool(feature_cfg.get("use_broker", False))
        use_holding = bool(feature_cfg.get("use_holding_shares", False))

        margin_df = data_loader.load_margin(stock_ids=active_stock_ids, start_date=start_date, end_date=end_date) if use_margin else pd.DataFrame()
        broker_df = data_loader.load_broker_flows(stock_ids=active_stock_ids, start_date=start_date, end_date=end_date) if use_broker else pd.DataFrame()
        holding_df = data_loader.load_holding_shares(stock_ids=active_stock_ids, start_date=start_date, end_date=end_date) if use_holding else pd.DataFrame()

        feature_store = FeatureStore(
            cache_dir=cfg.get("feature_cache_dir", data_root / "feature_cache"),
            version=str(cfg.get("feature_store_version", "v1")),
        )
        feature_definition = {
            "ma_windows": feature_cfg.get("ma_windows", [5, 20, 60]),
            "vol_windows": feature_cfg.get("vol_windows", [20, 60]),
            "use_margin": use_margin,
            "use_broker": use_broker,
            "use_holding_shares": use_holding,
        }
        feature_key = feature_store.build_cache_key(
            feature_set_name="default",
            universe_definition=universe_cfg,
            feature_definition=feature_definition,
        )

        def _feature_builder() -> pd.DataFrame:
            return feature_store.build_features(
                price_df=prices,
                margin_df=margin_df,
                broker_flow_df=broker_df,
                holding_df=holding_df,
                ma_windows=feature_definition["ma_windows"],
                vol_windows=feature_definition["vol_windows"],
            )

        feature_panel = feature_store.get_or_create(
            key=feature_key,
            builder=_feature_builder,
            metadata={
                "feature_key": feature_key,
                "feature_store_version": feature_store.version,
                "universe_definition": universe_cfg,
                "feature_definition": feature_definition,
            },
        )

        strategy_name = str(strategy_cfg.get("name", "ma_crossover"))
        if strategy_name != "ma_crossover":
            raise ValueError(f"Unsupported strategy: {strategy_name}")

        signal_model = MACrossoverSignalModel(
            short_window=int(strategy_cfg.get("short_window", 20)),
            long_window=int(strategy_cfg.get("long_window", 60)),
            use_legacy_impl=bool(strategy_cfg.get("use_legacy_impl", True)),
        )
        signals = signal_model.generate_signals(
            price_df=prices,
            features=feature_panel,
            universe=universe,
        )

        backtest_cfg = cfg.get("backtest", {})
        engine = LongCashBacktestEngine(
            BacktestConfig(
                transaction_cost_bps=float(backtest_cfg.get("transaction_cost_bps", 10.0)),
                slippage_bps=float(backtest_cfg.get("slippage_bps", 0.0)),
                lag_positions=int(backtest_cfg.get("lag_positions", 1)),
                trading_days_per_year=int(backtest_cfg.get("trading_days_per_year", 252)),
            )
        )
        backtest = engine.run(
            price_df=prices[["date", "stock_id", "close"]],
            signal_df=signals,
        )

        ts = backtest.timeseries.copy()
        ts["date"] = pd.to_datetime(ts["date"], errors="coerce")
        returns = ts.set_index("date")["net_return"].dropna()

        stats_cfg = cfg.get("statistics", {})
        nw = newey_west_t_statistics(returns.values, max_lags=stats_cfg.get("newey_west_lags"))
        boot = bootstrap_confidence_interval(
            returns.values,
            n_bootstrap=int(stats_cfg.get("bootstrap_samples", 2000)),
            seed=int(stats_cfg.get("seed", 42)),
        )
        sub = subperiod_analysis(returns)

        wf = walk_forward_validation(
            ts[["date", "net_return"]],
            date_col="date",
            train_window=int(stats_cfg.get("walk_forward_train_window", 126)),
            test_window=int(stats_cfg.get("walk_forward_test_window", 63)),
            evaluator=self._walk_forward_evaluator,
            step=int(stats_cfg.get("walk_forward_step", 63)),
            expanding=bool(stats_cfg.get("walk_forward_expanding", False)),
        )

        train_split, valid_split, test_split = train_valid_test_split_by_ratio(
            ts[["date", "net_return"]],
            train_ratio=float(stats_cfg.get("train_ratio", 0.6)),
            valid_ratio=float(stats_cfg.get("valid_ratio", 0.2)),
        )

        rolling_mean = rolling_window_evaluation(
            returns.values,
            window=int(stats_cfg.get("rolling_window", 20)),
        )
        expanding_mean = expanding_window_evaluation(
            returns.values,
            min_periods=int(stats_cfg.get("expanding_min_periods", 20)),
        )

        statistics = {
            "newey_west": nw,
            "bootstrap": boot,
            "subperiod_analysis": sub.to_dict(orient="records"),
            "walk_forward": wf.to_dict(orient="records"),
            "split_summary": {
                "train_rows": int(len(train_split)),
                "valid_rows": int(len(valid_split)),
                "test_rows": int(len(test_split)),
            },
            "rolling_mean_tail": rolling_mean.dropna().tail(10).tolist(),
            "expanding_mean_tail": expanding_mean.dropna().tail(10).tolist(),
        }

        experiments_root = Path(cfg.get("experiments_root", "experiments"))
        registry = self._resolve_registry(root_dir=experiments_root)

        shared_files = [data_root / "stock_info.sqlite", data_root / "holding_shares_per.sqlite"]
        stock_files = [data_root / f"{stock_id}.sqlite" for stock_id in active_stock_ids]
        dataset_hash = ExperimentRegistry.dataset_fingerprint(shared_files + stock_files)

        record = registry.start_experiment(
            strategy_name=strategy_name,
            config=cfg,
            parameters={
                "strategy": strategy_cfg,
                "backtest": backtest_cfg,
                "statistics": stats_cfg,
            },
            universe_definition=universe_cfg,
            feature_definition=feature_definition,
            dataset_hash=dataset_hash,
            experiment_id=cfg.get("experiment_id"),
        )

        reporter = self._resolve_reporter()
        report_path, report_artifacts = reporter.generate(
            experiment_id=record.experiment_id,
            metrics=backtest.metrics,
            backtest_timeseries=backtest.timeseries,
            strategy_config=strategy_cfg,
            statistics=statistics,
            output_dir=record.path,
            extra_notes="\n".join(ingestion_logs),
        )

        artifact_manifest = {
            **report_artifacts,
            "feature_cache_key": feature_key,
            "feature_cache_path": str(feature_store.feature_path(feature_key)),
            "universe_rows": int(len(universe)),
            "signal_rows": int(len(signals)),
        }

        metrics_payload = {
            **backtest.metrics,
            "statistics": statistics,
            "dataset_hash": dataset_hash,
        }
        registry.finalize_experiment(
            record=record,
            metrics=metrics_payload,
            artifacts=artifact_manifest,
        )

        return {
            "experiment_id": record.experiment_id,
            "experiment_path": str(record.path),
            "report_path": report_path,
            "metrics": backtest.metrics,
            "statistics": statistics,
            "validation_reports": validation_reports,
            "ingestion_logs": ingestion_logs,
            "dataset_hash": dataset_hash,
            "feature_key": feature_key,
        }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="research-orchestrator",
        description="Run end-to-end local quantitative research experiment.",
    )
    parser.add_argument("--config", required=True, help="Path to JSON config.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    orchestrator = ResearchOrchestrator()
    result = orchestrator.run(args.config)
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
