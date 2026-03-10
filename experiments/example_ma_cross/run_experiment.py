from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_PATH = REPO_ROOT / "src"
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from research.orchestrator import ResearchOrchestrator


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="run-example-ma-cross",
        description="Run example MA crossover orchestrated experiment.",
    )
    parser.add_argument(
        "--config",
        default=str(REPO_ROOT / "experiments" / "example_ma_cross" / "config.json"),
        help="Path to experiment config JSON.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    orchestrator = ResearchOrchestrator()
    result = orchestrator.run(args.config)

    summary = {
        "experiment_id": result["experiment_id"],
        "experiment_path": result["experiment_path"],
        "report_path": result["report_path"],
        "metrics": result["metrics"],
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
