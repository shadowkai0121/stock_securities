.PHONY: install test lint download-sample-data run-example-experiment run-example-research rerun-spec compare-example-runs compare-example-inference generate-paper

install:
	python -m pip install --upgrade pip
	python -m pip install -r requirements.txt
	python -m pip install -e .

test:
	python -m unittest discover -s tests -p "test_*.py"

lint:
	python -m compileall src data research universe features experiments tests

download-sample-data:
	python scripts/download_sample_data.py

run-example-experiment:
	python experiments/example_ma_cross/run_experiment.py --config experiments/example_ma_cross/config.json

run-example-research:
	python -m research.run --spec research_specs/ma_cross_example_v1.json --data-as-of 2025-12-31

rerun-spec:
	python -m research.run --spec $(SPEC) --data-as-of $(DATA_AS_OF)

compare-example-runs:
	python -m research.compare_runs --research-id ma_cross_example_v1

compare-example-inference:
	python -m research.compare_inference --research-id ma_cross_example_v1

generate-paper:
	python -m research.paper_outputs.generate --experiment $(RUN_ID) --paper $(PAPER_ID) --research-id $(RESEARCH_ID)
