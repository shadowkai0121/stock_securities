"""Composable research pipeline interfaces and default glue components."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

import pandas as pd


@dataclass(slots=True)
class PipelineContext:
    """Shared context passed across pipeline stages."""

    start_date: str
    end_date: str
    config: dict[str, Any]


@dataclass(slots=True)
class PipelineArtifacts:
    """Normalized output container for end-to-end research runs."""

    universe: pd.DataFrame
    features: pd.DataFrame
    signals: pd.DataFrame
    positions: pd.DataFrame
    backtest_timeseries: pd.DataFrame
    metrics: dict[str, Any]
    statistics: dict[str, Any]
    report_path: str


class DataLoader(Protocol):
    def load_prices(self, **kwargs: Any) -> pd.DataFrame:
        ...


class UniverseBuilder(Protocol):
    def build(self, **kwargs: Any) -> pd.DataFrame:
        ...


class FeaturePipeline(Protocol):
    def build_features(self, **kwargs: Any) -> pd.DataFrame:
        ...


class SignalModel(Protocol):
    def generate_signals(self, **kwargs: Any) -> pd.DataFrame:
        ...


class PortfolioConstructor(Protocol):
    def construct_positions(self, **kwargs: Any) -> pd.DataFrame:
        ...


class CostModel(Protocol):
    def estimate_costs(self, **kwargs: Any) -> pd.DataFrame:
        ...


class BacktestEngine(Protocol):
    def run(self, **kwargs: Any) -> Any:
        ...


class Evaluator(Protocol):
    def evaluate(self, **kwargs: Any) -> dict[str, Any]:
        ...


class ReportGenerator(Protocol):
    def generate(self, **kwargs: Any) -> tuple[str, dict[str, Any]]:
        ...


class PassThroughPortfolioConstructor:
    """Convert binary signal to long/cash positions without leverage."""

    def construct_positions(self, *, signals: pd.DataFrame) -> pd.DataFrame:
        required = {"date", "stock_id", "signal"}
        missing = required - set(signals.columns)
        if missing:
            raise ValueError(f"signals missing required columns: {sorted(missing)}")
        positions = signals[["date", "stock_id", "signal"]].copy()
        positions["weight"] = positions["signal"].clip(lower=0.0, upper=1.0)
        return positions[["date", "stock_id", "weight"]]


class FixedBpsCostModel:
    """Linear transaction-cost model on portfolio turnover."""

    def __init__(self, *, transaction_cost_bps: float) -> None:
        self.transaction_cost_bps = float(transaction_cost_bps)

    def estimate_costs(self, *, turnover: pd.Series) -> pd.DataFrame:
        costs = pd.DataFrame({"turnover": turnover.copy()})
        costs["cost"] = costs["turnover"] * (self.transaction_cost_bps / 10000.0)
        return costs


class DefaultEvaluator:
    """Combine backtest metrics with optional supplemental statistics."""

    def evaluate(self, *, backtest_metrics: dict[str, Any], statistics: dict[str, Any]) -> dict[str, Any]:
        out = dict(backtest_metrics)
        out["statistics"] = statistics
        return out


class ResearchPipeline:
    """Composable execution pipeline from local data to report artifacts."""

    def __init__(
        self,
        *,
        data_loader: DataLoader,
        universe_builder: UniverseBuilder,
        feature_pipeline: FeaturePipeline,
        signal_model: SignalModel,
        portfolio_constructor: PortfolioConstructor,
        cost_model: CostModel,
        backtest_engine: BacktestEngine,
        evaluator: Evaluator,
        report_generator: ReportGenerator,
    ) -> None:
        self.data_loader = data_loader
        self.universe_builder = universe_builder
        self.feature_pipeline = feature_pipeline
        self.signal_model = signal_model
        self.portfolio_constructor = portfolio_constructor
        self.cost_model = cost_model
        self.backtest_engine = backtest_engine
        self.evaluator = evaluator
        self.report_generator = report_generator

    def run(self, context: PipelineContext) -> PipelineArtifacts:
        universe = self.universe_builder.build(
            start_date=context.start_date,
            end_date=context.end_date,
            **context.config.get("universe", {}),
        )
        features = self.feature_pipeline.build_features(
            universe=universe,
            start_date=context.start_date,
            end_date=context.end_date,
            **context.config.get("features", {}),
        )
        signals = self.signal_model.generate_signals(
            features=features,
            universe=universe,
            start_date=context.start_date,
            end_date=context.end_date,
            **context.config.get("strategy", {}),
        )
        positions = self.portfolio_constructor.construct_positions(signals=signals)

        backtest_result = self.backtest_engine.run(
            positions=positions,
            start_date=context.start_date,
            end_date=context.end_date,
            **context.config.get("backtest", {}),
        )

        costs = self.cost_model.estimate_costs(turnover=backtest_result.timeseries["turnover"])
        statistics = context.config.get("statistics", {})
        metrics = self.evaluator.evaluate(
            backtest_metrics=backtest_result.metrics,
            statistics={**statistics, "cost_summary": costs["cost"].describe().to_dict()},
        )
        report_path, _artifacts = self.report_generator.generate(
            metrics=metrics,
            backtest_timeseries=backtest_result.timeseries,
            strategy_config=context.config.get("strategy", {}),
            experiment_id=context.config.get("experiment_id", "adhoc"),
        )

        return PipelineArtifacts(
            universe=universe,
            features=features,
            signals=signals,
            positions=positions,
            backtest_timeseries=backtest_result.timeseries,
            metrics=metrics,
            statistics=metrics.get("statistics", {}),
            report_path=report_path,
        )
