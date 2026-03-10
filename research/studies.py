"""Research study executors used by the rerun orchestration layer."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

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


@dataclass(slots=True)
class StudyRunResult:
    """Normalized output from one study execution."""

    metrics: dict[str, Any]
    artifacts: dict[str, Any]
    report_path: str
    log_lines: list[str]


class StudyExecutionError(RuntimeError):
    """Raised when a study cannot be executed on the local datasets."""


class MACrossoverStudyExecutor:
    """Cutoff-aware MA crossover study built on the local research components."""

    pipeline_id = "ma_crossover"

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

    @staticmethod
    def _write_frame(path: Path, frame: pd.DataFrame) -> None:
        frame.to_csv(path, index=False)

    @staticmethod
    def _apply_holding_period(signals: pd.DataFrame, holding_period_days: int) -> pd.DataFrame:
        """Hold signals between rebalance dates to support holding-period robustness."""

        period = max(int(holding_period_days), 1)
        if period == 1 or signals.empty:
            return signals

        work = signals.copy()
        work["date"] = pd.to_datetime(work["date"], errors="coerce")
        work = work[work["date"].notna()].sort_values(["stock_id", "date"]).reset_index(drop=True)

        output_parts: list[pd.DataFrame] = []
        for _, group in work.groupby("stock_id", sort=True):
            ordered = group.sort_values("date").reset_index(drop=True)
            ordered["rebalance_signal"] = ordered["signal"].where((ordered.index % period) == 0)
            ordered["signal"] = (
                pd.to_numeric(ordered["rebalance_signal"], errors="coerce")
                .ffill()
                .fillna(pd.to_numeric(ordered["signal"], errors="coerce").fillna(0.0))
            )
            output_parts.append(ordered[["date", "stock_id", "signal"]])

        out = pd.concat(output_parts, axis=0, ignore_index=True)
        out["date"] = out["date"].dt.strftime("%Y-%m-%d")
        out["signal"] = pd.to_numeric(out["signal"], errors="coerce").fillna(0.0).clip(lower=0.0, upper=1.0)
        return out.sort_values(["date", "stock_id"]).reset_index(drop=True)

    @staticmethod
    def _winsorize_series(series: pd.Series, level: float) -> pd.Series:
        if level <= 0.0:
            return series
        lower = float(series.quantile(level))
        upper = float(series.quantile(1.0 - level))
        return series.clip(lower=lower, upper=upper)

    def _build_inference_panel(
        self,
        *,
        prices: pd.DataFrame,
        features: pd.DataFrame,
        signals: pd.DataFrame,
        universe: pd.DataFrame,
        winsorization_level: float,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Create stock-date panel for empirical inference modules."""

        px = prices.copy()
        px["date"] = pd.to_datetime(px["date"], errors="coerce")
        px["close"] = pd.to_numeric(px["close"], errors="coerce")
        px["trading_money"] = pd.to_numeric(px.get("trading_money"), errors="coerce")
        px = px.dropna(subset=["date", "stock_id", "close"]).sort_values(["stock_id", "date"]).reset_index(drop=True)
        if px.empty:
            return pd.DataFrame(), pd.DataFrame(columns=["event_date", "stock_id", "event_type"])

        px["ret"] = px.groupby("stock_id")["close"].pct_change()
        px["ret_next"] = px.groupby("stock_id")["ret"].shift(-1)
        px["market_cap_proxy"] = px["trading_money"].fillna(px["close"].abs())

        merged = px[["date", "stock_id", "ret", "ret_next", "market_cap_proxy"]].copy()
        if not features.empty:
            feat = features.copy()
            feat["date"] = pd.to_datetime(feat["date"], errors="coerce")
            merged = merged.merge(feat, on=["date", "stock_id"], how="left", suffixes=("", "_feature"))

        if not signals.empty:
            sig = signals.copy()
            sig["date"] = pd.to_datetime(sig["date"], errors="coerce")
            sig["signal"] = pd.to_numeric(sig["signal"], errors="coerce")
            merged = merged.merge(sig[["date", "stock_id", "signal"]], on=["date", "stock_id"], how="left")
            merged["signal"] = pd.to_numeric(merged["signal"], errors="coerce").fillna(0.0)
        else:
            merged["signal"] = 0.0

        if not universe.empty:
            uni = universe.copy()
            uni["date"] = pd.to_datetime(uni["date"], errors="coerce")
            merged = merged.merge(uni[["date", "stock_id", "tradable_flag"]], on=["date", "stock_id"], how="left")
            merged["tradable_flag"] = pd.to_numeric(merged["tradable_flag"], errors="coerce").fillna(0).astype(int)
        else:
            merged["tradable_flag"] = 1

        merged = merged.dropna(subset=["ret_next"]).reset_index(drop=True)
        if winsorization_level > 0:
            merged["ret_next"] = self._winsorize_series(merged["ret_next"], winsorization_level)

        merged = merged.sort_values(["date", "stock_id"]).reset_index(drop=True)

        events = merged[["date", "stock_id", "signal"]].copy()
        events["lag_signal"] = events.groupby("stock_id")["signal"].shift(1).fillna(0.0)
        entries = events[(events["signal"] > 0) & (events["lag_signal"] <= 0)].copy()
        event_candidates = entries.rename(columns={"date": "event_date"})[["event_date", "stock_id"]]
        event_candidates["event_type"] = "signal_entry"

        merged["date"] = pd.to_datetime(merged["date"], errors="coerce").dt.strftime("%Y-%m-%d")
        event_candidates["event_date"] = pd.to_datetime(event_candidates["event_date"], errors="coerce").dt.strftime("%Y-%m-%d")
        return merged, event_candidates

    def execute(self, *, resolved_spec: dict[str, Any], run_dir: str | Path) -> StudyRunResult:
        """Execute the MA crossover study using only local persisted data."""

        root = Path(run_dir)
        runtime = resolved_spec["runtime"]
        start_date = str(resolved_spec["analysis_period"]["start_date"])
        data_as_of = str(resolved_spec["data_as_of"])

        data_loader = ResearchDataLoader(data_root=runtime["data_root"])
        universe_cfg = {
            "start_date": start_date,
            "end_date": data_as_of,
            **resolved_spec["universe_definition"],
        }
        universe_builder = TaiwanEquityUniverseBuilder(data_loader)
        universe = universe_builder.build(**universe_cfg)
        if universe.empty:
            raise StudyExecutionError("Universe builder returned no tradable stocks for the requested data_as_of.")

        active_stock_ids = sorted(
            universe.loc[universe["tradable_flag"] == 1, "stock_id"].astype(str).unique().tolist()
        )
        if not active_stock_ids:
            raise StudyExecutionError("No active stock_ids remained after universe filtering.")

        strategy_cfg = dict(resolved_spec["strategy_definition"])
        feature_cfg = dict(resolved_spec["feature_definition"])
        backtest_cfg = dict(resolved_spec["backtest_definition"])
        evaluation_cfg = dict(resolved_spec["evaluation_definition"])
        report_cfg = dict(resolved_spec["report_definition"])
        holding_period_days = int(strategy_cfg.get("holding_period_days", 1))
        winsorization_level = float(evaluation_cfg.get("winsorization_level", 0.0) or 0.0)

        prices = data_loader.load_prices(
            stock_ids=active_stock_ids,
            start_date=start_date,
            end_date=data_as_of,
            adjusted=bool(strategy_cfg.get("use_adjusted_price", True)),
        )
        if prices.empty:
            raise StudyExecutionError("No local price rows were available after applying data_as_of.")

        use_margin = bool(feature_cfg.get("use_margin", False))
        use_broker = bool(feature_cfg.get("use_broker", False))
        use_holding = bool(feature_cfg.get("use_holding_shares", False))

        margin_df = (
            data_loader.load_margin(stock_ids=active_stock_ids, start_date=start_date, end_date=data_as_of)
            if use_margin
            else pd.DataFrame()
        )
        broker_df = (
            data_loader.load_broker_flows(stock_ids=active_stock_ids, start_date=start_date, end_date=data_as_of)
            if use_broker
            else pd.DataFrame()
        )
        holding_df = (
            data_loader.load_holding_shares(stock_ids=active_stock_ids, start_date=start_date, end_date=data_as_of)
            if use_holding
            else pd.DataFrame()
        )

        feature_store = FeatureStore(
            cache_dir=runtime["feature_cache_dir"],
            version=str(runtime["feature_store_version"]),
        )
        feature_key = feature_store.build_cache_key(
            feature_set_name=resolved_spec["research_id"],
            universe_definition=universe_cfg,
            feature_definition=feature_cfg,
        )

        def _feature_builder() -> pd.DataFrame:
            return feature_store.build_features(
                price_df=prices,
                margin_df=margin_df,
                broker_flow_df=broker_df,
                holding_df=holding_df,
                ma_windows=feature_cfg.get("ma_windows", [5, 20, 60]),
                vol_windows=feature_cfg.get("vol_windows", [20, 60]),
            )

        features = feature_store.get_or_create(
            key=feature_key,
            builder=_feature_builder,
            metadata={
                "research_id": resolved_spec["research_id"],
                "run_id": resolved_spec["run_id"],
                "feature_key": feature_key,
                "data_as_of": data_as_of,
                "universe_definition": universe_cfg,
                "feature_definition": feature_cfg,
            },
        )

        signal_model = MACrossoverSignalModel(
            short_window=int(strategy_cfg.get("short_window", 20)),
            long_window=int(strategy_cfg.get("long_window", 60)),
            use_legacy_impl=bool(strategy_cfg.get("use_legacy_impl", True)),
        )
        signals = signal_model.generate_signals(
            price_df=prices,
            features=features,
            universe=universe,
        )
        signals = self._apply_holding_period(signals, holding_period_days)
        if signals.empty:
            raise StudyExecutionError("Signal model returned no signals for the requested date range.")

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

        timeseries = backtest.timeseries.copy()
        timeseries["date"] = pd.to_datetime(timeseries["date"], errors="coerce")
        returns = timeseries.set_index("date")["net_return"].dropna()
        if winsorization_level > 0:
            returns = self._winsorize_series(returns, winsorization_level)

        nw = newey_west_t_statistics(returns.values, max_lags=evaluation_cfg.get("newey_west_lags"))
        boot = bootstrap_confidence_interval(
            returns.values,
            n_bootstrap=int(evaluation_cfg.get("bootstrap_samples", 2000)),
            seed=int(evaluation_cfg.get("seed", 42)),
        )
        sub = subperiod_analysis(
            returns,
            trading_days_per_year=int(backtest_cfg.get("trading_days_per_year", 252)),
        )
        wf = walk_forward_validation(
            timeseries[["date", "net_return"]],
            date_col="date",
            train_window=int(evaluation_cfg.get("walk_forward_train_window", 126)),
            test_window=int(evaluation_cfg.get("walk_forward_test_window", 63)),
            evaluator=self._walk_forward_evaluator,
            step=int(evaluation_cfg.get("walk_forward_step", 63)),
            expanding=bool(evaluation_cfg.get("walk_forward_expanding", False)),
        )
        train_split, valid_split, test_split = train_valid_test_split_by_ratio(
            timeseries[["date", "net_return"]],
            train_ratio=float(evaluation_cfg.get("train_ratio", 0.6)),
            valid_ratio=float(evaluation_cfg.get("valid_ratio", 0.2)),
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
            "rolling_mean": rolling_window_evaluation(
                returns.values,
                window=int(evaluation_cfg.get("rolling_window", 20)),
            ).tolist(),
            "expanding_mean": expanding_window_evaluation(
                returns.values,
                min_periods=int(evaluation_cfg.get("expanding_min_periods", 20)),
            ).tolist(),
        }

        scalar_metrics = dict(backtest.metrics)
        scalar_metrics.update(
            {
                "research_id": resolved_spec["research_id"],
                "run_id": resolved_spec["run_id"],
                "data_as_of": data_as_of,
                "analysis_start": start_date,
                "universe_size": int(len(active_stock_ids)),
                "universe_rows": int(len(universe)),
                "price_rows": int(len(prices)),
                "feature_rows": int(len(features)),
                "signal_rows": int(len(signals)),
                "backtest_rows": int(len(backtest.timeseries)),
            }
        )
        metrics = dict(scalar_metrics)
        metrics["statistics"] = statistics

        reporter = MarkdownReportGenerator(default_output_dir=root.parent)
        report_path, report_artifacts = reporter.generate(
            experiment_id=resolved_spec["run_id"],
            metrics=scalar_metrics,
            backtest_timeseries=backtest.timeseries,
            strategy_config={
                "pipeline_id": resolved_spec["pipeline_id"],
                "strategy_definition": strategy_cfg,
                "backtest_definition": backtest_cfg,
                "data_as_of": data_as_of,
            },
            statistics=statistics,
            output_dir=root,
            extra_notes=(
                f"Research ID: {resolved_spec['research_id']}\n"
                f"Data as of: {data_as_of}\n"
                f"Feature cache key: {feature_key}"
            ),
        )

        artifacts: dict[str, Any] = dict(report_artifacts)

        timeseries_csv = root / "backtest_timeseries.csv"
        self._write_frame(timeseries_csv, backtest.timeseries)
        artifacts["backtest_timeseries"] = str(timeseries_csv)

        inference_panel, event_candidates = self._build_inference_panel(
            prices=prices,
            features=features,
            signals=signals,
            universe=universe,
            winsorization_level=winsorization_level,
        )
        if not inference_panel.empty:
            inference_csv = root / "inference_panel.csv"
            self._write_frame(inference_csv, inference_panel)
            artifacts["inference_panel"] = str(inference_csv)
        if not event_candidates.empty:
            events_csv = root / "event_candidates.csv"
            self._write_frame(events_csv, event_candidates)
            artifacts["event_candidates"] = str(events_csv)

        if report_cfg.get("write_universe_csv", True):
            universe_csv = root / "universe_snapshot.csv"
            self._write_frame(universe_csv, universe)
            artifacts["universe_snapshot"] = str(universe_csv)

        if report_cfg.get("write_features_csv", False):
            features_csv = root / "features_snapshot.csv"
            self._write_frame(features_csv, features)
            artifacts["features_snapshot"] = str(features_csv)

        log_lines = [
            f"[study] pipeline={self.pipeline_id}",
            f"[study] research_id={resolved_spec['research_id']} run_id={resolved_spec['run_id']}",
            f"[study] data_as_of={data_as_of} start_date={start_date}",
            f"[study] universe_size={len(active_stock_ids)} price_rows={len(prices)} feature_rows={len(features)}",
            f"[study] holding_period_days={holding_period_days} winsorization_level={winsorization_level}",
        ]

        return StudyRunResult(
            metrics=metrics,
            artifacts=artifacts,
            report_path=report_path,
            log_lines=log_lines,
        )


def get_study_executor(pipeline_id: str) -> MACrossoverStudyExecutor:
    """Resolve a study executor from the stable pipeline identifier."""

    if pipeline_id == MACrossoverStudyExecutor.pipeline_id:
        return MACrossoverStudyExecutor()
    raise ValueError(f"Unsupported pipeline_id: {pipeline_id}")
