# 2026-03-10 Empirical Platform Extension

## Decision

Implemented empirical inference and paper-generation as additive modules without changing the ingestion boundary or run registry semantics.

## Rationale

1. Preserve existing `research.run` behavior and append-only history.
2. Keep `finmind-dl` as the only ingestion pathway.
3. Add reusable run-level inference outputs (`inference_results.json`) to support paper tables, figures, and run-to-run inference comparison.

## Key Additions

1. `research/inference/` modules for Fama-MacBeth, panel OLS, portfolio sorts, and event studies.
2. `research/paper_outputs/` generators and CLI for paper-ready artifacts.
3. Robustness scenario expansion from research specs into append-only sub-runs.
4. `research.compare_inference` for coefficient and spread stability analysis.

