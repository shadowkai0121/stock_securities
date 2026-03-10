from __future__ import annotations

import unittest

import numpy as np
import pandas as pd

from _bootstrap import ROOT  # noqa: F401
from research.inference.event_study import run_event_study
from research.inference.fama_macbeth import run_fama_macbeth
from research.inference.panel_ols import run_panel_ols
from research.inference.portfolio_sort import run_portfolio_sort


class InferenceModuleTests(unittest.TestCase):
    def test_fama_macbeth_panel_ols_and_portfolio_sort(self) -> None:
        rng = np.random.default_rng(7)
        dates = pd.date_range("2021-01-01", periods=90, freq="D")
        stock_ids = [f"{1000 + idx}" for idx in range(25)]

        rows = []
        for date in dates:
            for stock_id in stock_ids:
                x1 = float(rng.normal())
                noise = float(rng.normal(scale=0.03))
                ret_next = 0.002 + 0.02 * x1 + noise
                rows.append(
                    {
                        "date": date.strftime("%Y-%m-%d"),
                        "stock_id": stock_id,
                        "x1": x1,
                        "signal": float(x1 > 0),
                        "market_cap_proxy": float(np.exp(rng.normal(6.0, 0.5))),
                        "ret_next": ret_next,
                    }
                )
        panel = pd.DataFrame(rows)

        fm = run_fama_macbeth(panel, y_col="ret_next", x_cols=["x1"], date_col="date")
        self.assertFalse(fm.summary.empty)
        fm_x1 = fm.summary.loc[fm.summary["variable"] == "x1"].iloc[0]
        self.assertGreater(float(fm_x1["coef"]), 0.0)

        pooled = run_panel_ols(
            panel,
            y_col="ret_next",
            x_cols=["x1"],
            entity_col="stock_id",
            time_col="date",
            entity_effects=True,
            time_effects=True,
            cluster="two_way",
        )
        self.assertFalse(pooled.summary.empty)
        pooled_x1 = pooled.summary.loc[pooled.summary["variable"] == "x1"].iloc[0]
        self.assertTrue(np.isfinite(float(pooled_x1["coef"])))

        equal_sort = run_portfolio_sort(
            panel,
            sort_col="x1",
            return_col="ret_next",
            date_col="date",
            n_portfolios=10,
            weighting="equal",
        )
        self.assertFalse(equal_sort.summary.empty)
        long_short = equal_sort.summary.loc[equal_sort.summary["portfolio"] == "Long-Short"].iloc[0]
        self.assertGreater(float(long_short["mean_return"]), 0.0)

        value_sort = run_portfolio_sort(
            panel,
            sort_col="x1",
            return_col="ret_next",
            date_col="date",
            n_portfolios=10,
            weighting="value",
            weight_col="market_cap_proxy",
        )
        self.assertFalse(value_sort.summary.empty)

    def test_event_study_significance(self) -> None:
        rng = np.random.default_rng(11)
        dates = pd.date_range("2020-01-01", periods=220, freq="D")
        stock_ids = ["1101", "1102", "1103", "1104", "1108"]
        market = rng.normal(loc=0.0003, scale=0.01, size=len(dates))

        rows = []
        events = []
        event_idx = 150
        for stock_id in stock_ids:
            alpha = rng.normal(loc=0.0002, scale=0.0005)
            beta = rng.normal(loc=1.0, scale=0.1)
            noise = rng.normal(loc=0.0, scale=0.01, size=len(dates))
            returns = alpha + beta * market + noise
            returns[event_idx] += 0.04
            returns[event_idx + 1] += 0.02
            for idx, date in enumerate(dates):
                rows.append(
                    {
                        "date": date.strftime("%Y-%m-%d"),
                        "stock_id": stock_id,
                        "return": float(returns[idx]),
                        "market_return": float(market[idx]),
                    }
                )
            events.append(
                {
                    "event_id": f"EV_{stock_id}",
                    "stock_id": stock_id,
                    "event_date": dates[event_idx].strftime("%Y-%m-%d"),
                }
            )

        result = run_event_study(
            returns=pd.DataFrame(rows),
            events=pd.DataFrame(events),
            event_window=(-3, 3),
            estimation_window=(-120, -20),
            model="market",
            date_col="date",
            entity_col="stock_id",
            return_col="return",
            event_date_col="event_date",
            market_return_col="market_return",
        )

        self.assertGreater(result.n_events, 0)
        self.assertFalse(result.window_summary.empty)
        self.assertGreater(float(result.car_test["car_mean"]), 0.0)


if __name__ == "__main__":
    unittest.main()

