"""Run-level empirical inference pipeline for paper artifacts."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from research.inference.event_study import run_event_study
from research.inference.fama_macbeth import run_fama_macbeth
from research.inference.panel_ols import run_firm_fe_ols, run_pooled_panel_ols, run_time_fe_ols
from research.inference.portfolio_sort import run_portfolio_sort

from .common import load_csv


def _resolve_artifact_path(run_path: Path, artifacts: dict[str, Any], key: str, fallback: str) -> Path:
    raw = str(artifacts.get(key) or "").strip()
    if raw:
        candidate = Path(raw)
        if candidate.exists():
            return candidate
    return run_path / fallback


def _pick_regressors(panel: pd.DataFrame) -> list[str]:
    reserved = {
        "date",
        "stock_id",
        "ret",
        "ret_next",
        "signal",
        "tradable_flag",
        "market_cap_proxy",
    }
    candidates = [col for col in panel.columns if col not in reserved]
    numeric_candidates = [col for col in candidates if pd.to_numeric(panel[col], errors="coerce").notna().sum() > 0]

    ranked: list[str] = []
    if "signal" in panel.columns and panel["signal"].nunique(dropna=True) > 1:
        ranked.append("signal")
    for prefix in ("ma_", "volatility_", "turnover_proxy", "simple_return", "log_return", "margin_", "broker_"):
        for col in numeric_candidates:
            if col in ranked:
                continue
            if col.startswith(prefix):
                ranked.append(col)
    for col in numeric_candidates:
        if col not in ranked:
            ranked.append(col)

    stable = [col for col in ranked if pd.to_numeric(panel[col], errors="coerce").nunique(dropna=True) > 1]
    return stable[:4]


def _derive_event_candidates(panel: pd.DataFrame) -> pd.DataFrame:
    if not {"date", "stock_id", "signal"}.issubset(panel.columns):
        return pd.DataFrame(columns=["event_date", "stock_id"])
    events = panel[["date", "stock_id", "signal"]].copy()
    events["signal"] = pd.to_numeric(events["signal"], errors="coerce").fillna(0.0)
    events["lag_signal"] = events.groupby("stock_id")["signal"].shift(1).fillna(0.0)
    entries = events[(events["signal"] > 0) & (events["lag_signal"] <= 0)].copy()
    output = entries.rename(columns={"date": "event_date"})[["event_date", "stock_id"]]
    output["event_id"] = [f"E{idx + 1:04d}" for idx in range(len(output))]
    return output.reset_index(drop=True)


def _safe_summary(frame: pd.DataFrame) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    out = frame.copy()
    for col in out.columns:
        if pd.api.types.is_datetime64_any_dtype(out[col]):
            out[col] = out[col].dt.strftime("%Y-%m-%d")
    return out.to_dict(orient="records")


def compute_inference_results(run_bundle: dict[str, Any]) -> dict[str, Any]:
    """Compute empirical inference outputs from one run bundle."""

    run_path = Path(run_bundle["run_path"])
    artifacts = dict(run_bundle.get("artifacts", {}))
    resolved_spec = dict(run_bundle.get("resolved_spec", {}))

    inference_panel_path = _resolve_artifact_path(run_path, artifacts, "inference_panel", "inference_panel.csv")
    event_candidates_path = _resolve_artifact_path(run_path, artifacts, "event_candidates", "event_candidates.csv")
    panel = load_csv(inference_panel_path)
    if not panel.empty and "date" in panel.columns:
        panel["date"] = pd.to_datetime(panel["date"], errors="coerce").dt.strftime("%Y-%m-%d")

    regressors = _pick_regressors(panel) if not panel.empty else []
    sort_variable = "signal" if "signal" in panel.columns else (regressors[0] if regressors else "")
    nw_lags = (
        dict(resolved_spec.get("evaluation_definition", {})).get("newey_west_lags")
        if isinstance(resolved_spec.get("evaluation_definition"), dict)
        else None
    )

    warnings: list[str] = []
    fm_summary: list[dict[str, Any]] = []
    fm_betas: list[dict[str, Any]] = []
    if panel.empty or not regressors:
        warnings.append("inference panel missing or no usable regressors for Fama-MacBeth/panel OLS")
    else:
        fm_result = run_fama_macbeth(
            panel,
            y_col="ret_next",
            x_cols=regressors,
            date_col="date",
            add_intercept=True,
            newey_west_lags=nw_lags,
        )
        fm_summary = _safe_summary(fm_result.summary)
        fm_betas = _safe_summary(fm_result.period_betas)

    pooled_summary: list[dict[str, Any]] = []
    firm_fe_summary: list[dict[str, Any]] = []
    time_fe_summary: list[dict[str, Any]] = []
    if not panel.empty and regressors:
        pooled = run_pooled_panel_ols(
            panel,
            y_col="ret_next",
            x_cols=regressors,
            entity_col="stock_id",
            time_col="date",
            cluster="entity",
        )
        pooled_summary = _safe_summary(pooled.summary)

        firm_fe = run_firm_fe_ols(
            panel,
            y_col="ret_next",
            x_cols=regressors,
            entity_col="stock_id",
            time_col="date",
            cluster="entity",
        )
        firm_fe_summary = _safe_summary(firm_fe.summary)

        time_fe = run_time_fe_ols(
            panel,
            y_col="ret_next",
            x_cols=regressors,
            entity_col="stock_id",
            time_col="date",
            cluster="time",
        )
        time_fe_summary = _safe_summary(time_fe.summary)

    equal_summary: list[dict[str, Any]] = []
    equal_spread: list[dict[str, Any]] = []
    value_summary: list[dict[str, Any]] = []
    value_spread: list[dict[str, Any]] = []
    if not panel.empty and sort_variable and "ret_next" in panel.columns:
        equal = run_portfolio_sort(
            panel,
            sort_col=sort_variable,
            return_col="ret_next",
            date_col="date",
            n_portfolios=10,
            weighting="equal",
            newey_west_lags=nw_lags,
        )
        equal_summary = _safe_summary(equal.summary)
        equal_spread = _safe_summary(equal.spread_returns)

        if "market_cap_proxy" in panel.columns:
            value = run_portfolio_sort(
                panel,
                sort_col=sort_variable,
                return_col="ret_next",
                date_col="date",
                n_portfolios=10,
                weighting="value",
                weight_col="market_cap_proxy",
                newey_west_lags=nw_lags,
            )
            value_summary = _safe_summary(value.summary)
            value_spread = _safe_summary(value.spread_returns)

    event_summary: list[dict[str, Any]] = []
    event_abnormal: list[dict[str, Any]] = []
    car_test: dict[str, Any] = {"car_mean": float("nan"), "t_stat": float("nan"), "p_value": float("nan"), "n_events": 0}
    n_events = 0
    if not panel.empty and {"date", "stock_id", "ret"}.issubset(panel.columns):
        event_candidates = load_csv(event_candidates_path)
        if event_candidates.empty:
            event_candidates = _derive_event_candidates(panel)
        if not event_candidates.empty:
            returns = panel[["date", "stock_id", "ret"]].copy().rename(columns={"ret": "return"})
            market = (
                returns.groupby("date", as_index=False)["return"]
                .mean()
                .rename(columns={"return": "market_return"})
            )
            returns = returns.merge(market, on="date", how="left")
            event_result = run_event_study(
                returns=returns,
                events=event_candidates.rename(columns={"event_date": "event_date"}),
                event_window=(-5, 5),
                estimation_window=(-120, -20),
                model="market",
                date_col="date",
                entity_col="stock_id",
                return_col="return",
                event_date_col="event_date",
                market_return_col="market_return",
            )
            event_summary = _safe_summary(event_result.window_summary)
            event_abnormal = _safe_summary(event_result.abnormal_returns)
            car_test = dict(event_result.car_test)
            n_events = int(event_result.n_events)

    output = {
        "research_id": run_bundle.get("research_id"),
        "run_id": run_bundle.get("run_id"),
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "metadata": {
            "regressors": regressors,
            "sort_variable": sort_variable,
            "inference_panel_path": str(inference_panel_path),
        },
        "warnings": warnings,
        "fama_macbeth": {
            "summary": fm_summary,
            "period_betas": fm_betas,
        },
        "panel_ols": {
            "pooled": {"summary": pooled_summary},
            "firm_fe": {"summary": firm_fe_summary},
            "time_fe": {"summary": time_fe_summary},
        },
        "portfolio_sort": {
            "equal_weight": {"summary": equal_summary, "spread_returns": equal_spread},
            "value_weight": {"summary": value_summary, "spread_returns": value_spread},
        },
        "event_study": {
            "window_summary": event_summary,
            "abnormal_returns": event_abnormal,
            "car_test": car_test,
            "n_events": n_events,
        },
    }
    return output


def load_or_compute_inference_results(run_bundle: dict[str, Any], *, force: bool = False) -> dict[str, Any]:
    """Load cached run-level inference outputs or compute and persist them."""

    run_path = Path(run_bundle["run_path"])
    inference_json = run_path / "inference_results.json"
    if inference_json.exists() and not force:
        return json.loads(inference_json.read_text(encoding="utf-8"))

    output = compute_inference_results(run_bundle)
    inference_json.write_text(
        json.dumps(output, ensure_ascii=False, indent=2, allow_nan=True),
        encoding="utf-8",
    )

    inference_dir = run_path / "inference"
    inference_dir.mkdir(parents=True, exist_ok=True)

    fm_summary = pd.DataFrame(output["fama_macbeth"]["summary"])
    if not fm_summary.empty:
        fm_summary.to_csv(inference_dir / "fama_macbeth_summary.csv", index=False)

    pooled_summary = pd.DataFrame(output["panel_ols"]["pooled"]["summary"])
    if not pooled_summary.empty:
        pooled_summary.to_csv(inference_dir / "panel_ols_pooled_summary.csv", index=False)

    spread_equal = pd.DataFrame(output["portfolio_sort"]["equal_weight"]["spread_returns"])
    if not spread_equal.empty:
        spread_equal.to_csv(inference_dir / "portfolio_spread_equal_weight.csv", index=False)

    event_summary = pd.DataFrame(output["event_study"]["window_summary"])
    if not event_summary.empty:
        event_summary.to_csv(inference_dir / "event_study_window_summary.csv", index=False)

    return output

