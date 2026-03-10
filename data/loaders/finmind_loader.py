"""Official ingestion wrapper around the existing ``finmind-dl`` tool.

This module does not re-implement FinMind API calls. It orchestrates existing
``finmind_dl`` dataset handlers (internal mode) or the CLI command
(subprocess mode) as the first-class ingestion boundary for the research
platform.
"""

from __future__ import annotations

import json
import subprocess
import sys
from argparse import Namespace
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from finmind_dl.core.config import resolve_token
from finmind_dl.datasets import broker, daily, holding_shares, margin, price, price_adj, stock_info, warrant


@dataclass(slots=True)
class IngestionResult:
    """Normalized result for dataset ingestion commands."""

    dataset: str
    table: str
    stock_id: str
    query_mode: str
    start_date: str | None
    end_date: str | None
    db_path: Path
    fetched_rows: int
    inserted_rows: int
    extra_lines: list[str]

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "IngestionResult":
        return cls(
            dataset=str(payload.get("dataset", "")),
            table=str(payload.get("table", "")),
            stock_id=str(payload.get("stock_id", "")),
            query_mode=str(payload.get("query_mode", "")),
            start_date=payload.get("start_date"),
            end_date=payload.get("end_date"),
            db_path=Path(payload.get("db_path")),
            fetched_rows=int(payload.get("fetched_rows", 0)),
            inserted_rows=int(payload.get("inserted_rows", 0)),
            extra_lines=[str(x) for x in payload.get("extra_lines", [])],
        )


class FinMindLoader:
    """Python-facing ingestion gateway backed by the existing downloader.

    Parameters
    ----------
    token:
        FinMind token. If omitted, resolve using the same environment rules as
        ``finmind-dl``.
    use_subprocess:
        If true, invoke the CLI command. Otherwise call handler modules
        directly in-process.
    finmind_command:
        CLI command when ``use_subprocess=True``.
    working_dir:
        Working directory for subprocess mode.
    """

    def __init__(
        self,
        *,
        token: str | None = None,
        use_subprocess: bool = False,
        finmind_command: str = "finmind-dl",
        working_dir: str | Path | None = None,
    ) -> None:
        self._token = resolve_token(token)
        self._use_subprocess = bool(use_subprocess)
        self._finmind_command = finmind_command
        self._working_dir = Path(working_dir).resolve() if working_dir else None

    @property
    def token(self) -> str:
        """Return resolved FinMind token used by this loader."""

        return self._token

    def _run_internal(self, handler: Any, args: Namespace) -> IngestionResult:
        payload = handler(args, self._token)
        return IngestionResult.from_payload(payload)

    def _run_subprocess(self, command: list[str]) -> IngestionResult:
        full_cmd = [self._finmind_command, *command, "--token", self._token]
        process = subprocess.run(
            full_cmd,
            check=False,
            capture_output=True,
            text=True,
            cwd=str(self._working_dir) if self._working_dir else None,
        )
        if process.returncode != 0:
            message = process.stderr.strip() or process.stdout.strip() or "ingestion command failed"
            raise RuntimeError(f"finmind-dl subprocess failed ({process.returncode}): {message}")

        # CLI output is human-readable; keep deterministic return format.
        lines = [line.strip() for line in process.stdout.splitlines() if line.strip()]
        data: dict[str, Any] = {
            "dataset": "subprocess",
            "table": "",
            "stock_id": "",
            "query_mode": "",
            "start_date": None,
            "end_date": None,
            "db_path": Path("."),
            "fetched_rows": 0,
            "inserted_rows": 0,
            "extra_lines": lines,
        }
        for line in lines:
            if line.startswith("Dataset:"):
                data["dataset"] = line.split(":", 1)[1].strip()
            elif line.startswith("DB:"):
                data["db_path"] = Path(line.split(":", 1)[1].strip())
            elif line.startswith("Table:"):
                data["table"] = line.split(":", 1)[1].strip()
            elif line.startswith("Fetched rows:"):
                data["fetched_rows"] = int(line.split(":", 1)[1].strip())
            elif line.startswith("Inserted rows:"):
                data["inserted_rows"] = int(line.split(":", 1)[1].strip())
        return IngestionResult.from_payload(data)

    def download_price(
        self,
        *,
        stock_id: str,
        start_date: str,
        end_date: str,
        db_path: str | Path | None = None,
        replace: bool = False,
    ) -> IngestionResult:
        if self._use_subprocess:
            cmd = [
                "price",
                "--stock-id",
                stock_id,
                "--start-date",
                start_date,
                "--end-date",
                end_date,
            ]
            if db_path:
                cmd.extend(["--db-path", str(db_path)])
            if replace:
                cmd.append("--replace")
            return self._run_subprocess(cmd)

        args = Namespace(
            stock_id=stock_id,
            start_date=start_date,
            end_date=end_date,
            db_path=str(db_path) if db_path else None,
            replace=bool(replace),
        )
        return self._run_internal(price.run, args)

    def download_price_adj(
        self,
        *,
        stock_id: str,
        start_date: str,
        end_date: str,
        db_path: str | Path | None = None,
        replace: bool = False,
    ) -> IngestionResult:
        if self._use_subprocess:
            cmd = [
                "price-adj",
                "--stock-id",
                stock_id,
                "--start-date",
                start_date,
                "--end-date",
                end_date,
            ]
            if db_path:
                cmd.extend(["--db-path", str(db_path)])
            if replace:
                cmd.append("--replace")
            return self._run_subprocess(cmd)

        args = Namespace(
            stock_id=stock_id,
            start_date=start_date,
            end_date=end_date,
            db_path=str(db_path) if db_path else None,
            replace=bool(replace),
        )
        return self._run_internal(price_adj.run, args)

    def download_margin(
        self,
        *,
        stock_id: str,
        start_date: str,
        end_date: str,
        db_path: str | Path | None = None,
        replace: bool = False,
    ) -> IngestionResult:
        if self._use_subprocess:
            cmd = [
                "margin",
                "--stock-id",
                stock_id,
                "--start-date",
                start_date,
                "--end-date",
                end_date,
            ]
            if db_path:
                cmd.extend(["--db-path", str(db_path)])
            if replace:
                cmd.append("--replace")
            return self._run_subprocess(cmd)

        args = Namespace(
            stock_id=stock_id,
            start_date=start_date,
            end_date=end_date,
            db_path=str(db_path) if db_path else None,
            replace=bool(replace),
        )
        return self._run_internal(margin.run, args)

    def download_broker(
        self,
        *,
        stock_id: str,
        start_date: str,
        end_date: str,
        db_path: str | Path | None = None,
        replace: bool = False,
    ) -> IngestionResult:
        if self._use_subprocess:
            cmd = [
                "broker",
                "--stock-id",
                stock_id,
                "--start-date",
                start_date,
                "--end-date",
                end_date,
            ]
            if db_path:
                cmd.extend(["--db-path", str(db_path)])
            if replace:
                cmd.append("--replace")
            return self._run_subprocess(cmd)

        args = Namespace(
            stock_id=stock_id,
            start_date=start_date,
            end_date=end_date,
            db_path=str(db_path) if db_path else None,
            replace=bool(replace),
        )
        return self._run_internal(broker.run, args)

    def download_holding_shares(
        self,
        *,
        stock_id: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        all_market_date: str | None = None,
        db_path: str | Path | None = None,
        replace: bool = False,
    ) -> IngestionResult:
        if self._use_subprocess:
            cmd = ["holding-shares"]
            if stock_id:
                cmd.extend(["--stock-id", stock_id])
            if start_date:
                cmd.extend(["--start-date", start_date])
            if end_date:
                cmd.extend(["--end-date", end_date])
            if all_market_date:
                cmd.extend(["--all-market-date", all_market_date])
            if db_path:
                cmd.extend(["--db-path", str(db_path)])
            if replace:
                cmd.append("--replace")
            return self._run_subprocess(cmd)

        args = Namespace(
            stock_id=stock_id,
            start_date=start_date,
            end_date=end_date,
            all_market_date=all_market_date,
            db_path=str(db_path) if db_path else None,
            replace=bool(replace),
        )
        return self._run_internal(holding_shares.run, args)

    def download_stock_info(
        self,
        *,
        start_date: str | None = None,
        db_path: str | Path | None = None,
        replace: bool = False,
    ) -> IngestionResult:
        if self._use_subprocess:
            cmd = ["stock-info"]
            if start_date:
                cmd.extend(["--start-date", start_date])
            if db_path:
                cmd.extend(["--db-path", str(db_path)])
            if replace:
                cmd.append("--replace")
            return self._run_subprocess(cmd)

        args = Namespace(
            start_date=start_date,
            db_path=str(db_path) if db_path else None,
            replace=bool(replace),
        )
        return self._run_internal(stock_info.run, args)

    def download_warrant(
        self,
        *,
        stock_id: str,
        start_date: str | None = None,
        active_only: bool = False,
        print_limit: int = 0,
        output_csv: str | Path | None = None,
        db_path: str | Path | None = None,
        replace: bool = False,
    ) -> IngestionResult:
        if self._use_subprocess:
            cmd = ["warrant", "--stock-id", stock_id]
            if start_date:
                cmd.extend(["--start-date", start_date])
            if active_only:
                cmd.append("--active-only")
            if print_limit:
                cmd.extend(["--print-limit", str(print_limit)])
            if output_csv:
                cmd.extend(["--output-csv", str(output_csv)])
            if db_path:
                cmd.extend(["--db-path", str(db_path)])
            if replace:
                cmd.append("--replace")
            return self._run_subprocess(cmd)

        args = Namespace(
            stock_id=stock_id,
            start_date=start_date,
            active_only=bool(active_only),
            print_limit=int(print_limit),
            output_csv=str(output_csv) if output_csv else None,
            db_path=str(db_path) if db_path else None,
            replace=bool(replace),
        )
        return self._run_internal(warrant.run, args)

    def download_daily_bundle(
        self,
        *,
        stock_id: str,
        start_date: str,
        end_date: str,
        include_holding_shares: bool = False,
        db_path: str | Path | None = None,
        replace: bool = False,
    ) -> IngestionResult:
        if self._use_subprocess:
            cmd = [
                "daily",
                "--stock-id",
                stock_id,
                "--start-date",
                start_date,
                "--end-date",
                end_date,
            ]
            if include_holding_shares:
                cmd.append("--include-holding-shares")
            if db_path:
                cmd.extend(["--db-path", str(db_path)])
            if replace:
                cmd.append("--replace")
            return self._run_subprocess(cmd)

        args = Namespace(
            stock_id=stock_id,
            start_date=start_date,
            end_date=end_date,
            include_holding_shares=bool(include_holding_shares),
            db_path=str(db_path) if db_path else None,
            replace=bool(replace),
        )
        return self._run_internal(daily.run, args)

    def to_json(self, result: IngestionResult) -> str:
        """Serialize an ingestion result for structured logs."""

        payload = {
            "dataset": result.dataset,
            "table": result.table,
            "stock_id": result.stock_id,
            "query_mode": result.query_mode,
            "start_date": result.start_date,
            "end_date": result.end_date,
            "db_path": str(result.db_path),
            "fetched_rows": result.fetched_rows,
            "inserted_rows": result.inserted_rows,
            "extra_lines": result.extra_lines,
        }
        return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def _cli_main(argv: list[str] | None = None) -> int:
    """Debug utility for manual loader invocation.

    This helper is intentionally lightweight and not used by orchestration code.
    """

    args = argv or []
    if len(args) < 4:
        print(
            "Usage: python -m data.loaders.finmind_loader <stock_id> <start_date> <end_date> <db_path>",
            file=sys.stderr,
        )
        return 2
    stock_id, start_date, end_date, db_path = args[0], args[1], args[2], args[3]
    loader = FinMindLoader()
    result = loader.download_price_adj(
        stock_id=stock_id,
        start_date=start_date,
        end_date=end_date,
        db_path=db_path,
    )
    print(loader.to_json(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(_cli_main(sys.argv[1:]))
