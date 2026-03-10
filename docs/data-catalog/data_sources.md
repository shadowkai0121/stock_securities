# Data Sources

Primary source: FinMind APIs, accessed only via `finmind-dl`.

Core datasets tracked in `data/catalog/data_catalog.yaml` include:

- `price` (`price_daily`)
- `price_adj` (`price_adj_daily`)
- `margin` (`margin_daily`)
- `broker` (`broker_trades`)
- `holding_shares` (`holding_shares_per`)
- `stock_info` (`stock_info`)
- `warrant` (`warrant_summary`)

## Rationale

Centralizing source metadata in a catalog enables:

- orchestrator dataset prerequisite checks
- explicit quality check mapping
- reproducibility and auditability
