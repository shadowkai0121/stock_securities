# Empirical Design: ma_cross_example_v1

## Baseline Specification

- Dependent variable: next-day stock return (`ret_next`).
- Core regressor: moving-average crossover signal (`signal`).
- Controls: available local feature columns from the feature store (MA levels, volatility proxies, turnover proxy).

## Regression Designs

1. Fama-MacBeth cross-sectional regressions by date, then time-series average of slopes with Newey-West errors.
2. Panel OLS (pooled).
3. Panel OLS with firm fixed effects.
4. Panel OLS with time fixed effects.

## Portfolio Design

- Decile sorting on `signal` or first available predictive characteristic.
- Equal-weight and value-weight (market-cap proxy from trading-money).
- Long-short spread between top and bottom portfolios.

## Identification Assumptions

1. Signal formation uses information available at or before formation date.
2. Strategy evaluation uses lagged execution in backtest to avoid look-ahead bias.
3. Inference is based on locally persisted datasets fixed by `data_as_of`.

