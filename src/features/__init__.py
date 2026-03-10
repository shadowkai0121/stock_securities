"""Feature engineering utilities for academic finance workflows."""

from .data_processor import (
    CapmFitResult,
    build_residual_dataset,
    compute_log_returns,
    compute_market_residual_returns,
    load_price_adj_daily,
    run_series_quality_checks,
)

__all__ = [
    "CapmFitResult",
    "build_residual_dataset",
    "compute_log_returns",
    "compute_market_residual_returns",
    "load_price_adj_daily",
    "run_series_quality_checks",
]
