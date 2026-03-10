# Example MA Crossover Experiment

This example demonstrates the complete orchestrated workflow:

1. check required local datasets
2. invoke `finmind-dl` via `data/loaders/finmind_loader.py` if missing
3. load local SQLite data through `research/data_loader.py`
4. build universe and features
5. run MA crossover signal model (legacy-compatible adapter)
6. run standardized backtest engine
7. compute statistical validation outputs
8. generate report and plots
9. register experiment outputs under `experiments/<experiment_id>/`

## Run

```bash
python experiments/example_ma_cross/run_experiment.py --config experiments/example_ma_cross/config.json
```

If required local data is missing, set a token first:

```bash
set FINMIND_SPONSOR_API_KEY=your_token_here
```

## Output

A completed experiment directory is created with:

- `config.json`
- `metrics.json`
- `artifacts.json`
- `report.md`
- `plots/`
