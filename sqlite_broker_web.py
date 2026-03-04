#!/usr/bin/env python3
"""
Local web UI for browsing stock broker SQLite data.

Features:
- Select stock SQLite DB (by stock id filename, e.g. 8271.sqlite)
- List brokers from broker_tables
- Query selected broker with date range
- Show daily buy/sell aggregated volume
"""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
from datetime import date
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

HOST = "127.0.0.1"
PORT = 8765
BASE_DIR = Path(__file__).resolve().parent
DB_PATTERN = re.compile(r"^(\d+)\.sqlite$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Broker SQLite local web viewer")
    parser.add_argument("--host", default=HOST, help=f"Bind host. Default: {HOST}")
    parser.add_argument("--port", type=int, default=PORT, help=f"Bind port. Default: {PORT}")
    return parser.parse_args()


def list_stock_dbs() -> list[dict[str, str]]:
    result: list[dict[str, str]] = []
    for path in sorted(BASE_DIR.glob("*.sqlite")):
        match = DB_PATTERN.match(path.name)
        if not match:
            continue
        result.append(
            {
                "stock_id": match.group(1),
                "db_file": path.name,
            }
        )
    return result


def resolve_db_path(db_file: str) -> Path:
    for item in list_stock_dbs():
        if item["db_file"] == db_file:
            return BASE_DIR / db_file
    raise ValueError(f"Unknown db_file: {db_file}")


def query_brokers(db_path: Path) -> list[dict[str, Any]]:
    conn = sqlite3.connect(db_path)
    try:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT securities_trader_id, securities_trader, table_name, stock_id
            FROM broker_tables
            ORDER BY securities_trader_id
            """
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def query_db_date_range(db_path: Path) -> dict[str, str]:
    conn = sqlite3.connect(db_path)
    try:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT MIN(start_date) AS min_date, MAX(end_date) AS max_date
            FROM fetch_history
            """
        ).fetchone()
        if not row or (row["min_date"] is None and row["max_date"] is None):
            return {"min_date": "", "max_date": ""}
        return {
            "min_date": row["min_date"] or "",
            "max_date": row["max_date"] or "",
        }
    finally:
        conn.close()


def query_daily_buy_sell(
    db_path: Path,
    table_name: str,
    start_date: str,
    end_date: str,
) -> list[dict[str, Any]]:
    conn = sqlite3.connect(db_path)
    try:
        conn.row_factory = sqlite3.Row
        known = conn.execute(
            "SELECT 1 FROM broker_tables WHERE table_name = ? LIMIT 1",
            (table_name,),
        ).fetchone()
        if not known:
            raise ValueError(f"Unknown broker table: {table_name}")

        rows = conn.execute(
            f"""
            SELECT
                date,
                ROUND(SUM(buy), 4) AS total_buy,
                ROUND(SUM(sell), 4) AS total_sell,
                ROUND(
                    CASE WHEN SUM(buy) > 0 THEN SUM(price * buy) / SUM(buy) ELSE 0 END,
                    4
                ) AS avg_buy_price,
                ROUND(
                    CASE WHEN SUM(sell) > 0 THEN SUM(price * sell) / SUM(sell) ELSE 0 END,
                    4
                ) AS avg_sell_price
            FROM "{table_name}"
            WHERE date >= ? AND date <= ?
            GROUP BY date
            ORDER BY date
            """,
            (start_date, end_date),
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def valid_iso_date(value: str) -> bool:
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        return False
    try:
        y, m, d = value.split("-")
        date(int(y), int(m), int(d))
        return True
    except ValueError:
        return False


def build_index_html() -> str:
    return """<!doctype html>
<html lang="zh-Hant">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Broker Volume Viewer</title>
  <style>
    :root {
      --bg-a: #f7f1e5;
      --bg-b: #d6e4f0;
      --panel: rgba(255, 255, 255, 0.86);
      --ink: #1e2a39;
      --muted: #5a6675;
      --accent: #0f766e;
      --accent-2: #f59e0b;
      --line: #ccd6e2;
      --buy: #0f766e;
      --sell: #b45309;
    }

    * { box-sizing: border-box; }

    body {
      margin: 0;
      min-height: 100vh;
      font-family: "IBM Plex Sans", "Noto Sans TC", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at 8% 12%, #ffe7bf 0%, transparent 35%),
        radial-gradient(circle at 92% 88%, #c3ecf3 0%, transparent 38%),
        linear-gradient(130deg, var(--bg-a), var(--bg-b));
      display: grid;
      place-items: start center;
      padding: 24px 16px;
    }

    .wrap {
      width: min(980px, 100%);
      background: var(--panel);
      border: 1px solid #f2f4f7;
      border-radius: 18px;
      backdrop-filter: blur(4px);
      box-shadow: 0 18px 42px rgba(30, 42, 57, 0.12);
      overflow: hidden;
      animation: rise .45s ease both;
    }

    @keyframes rise {
      from { opacity: 0; transform: translateY(10px); }
      to { opacity: 1; transform: translateY(0); }
    }

    header {
      padding: 20px 24px 12px;
      border-bottom: 1px solid var(--line);
      background: linear-gradient(90deg, rgba(15,118,110,.08), rgba(245,158,11,.08));
    }

    h1 {
      margin: 0;
      font-size: 24px;
      letter-spacing: .2px;
    }

    .sub {
      margin-top: 6px;
      color: var(--muted);
      font-size: 14px;
    }

    .controls {
      display: grid;
      grid-template-columns: repeat(5, minmax(0, 1fr));
      gap: 12px;
      padding: 16px 24px;
      border-bottom: 1px solid var(--line);
      align-items: end;
    }

    .field label {
      display: block;
      font-size: 12px;
      color: var(--muted);
      margin-bottom: 5px;
    }

    .field select,
    .field input {
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 10px;
      background: #fff;
      color: var(--ink);
      padding: 10px 11px;
      font-size: 14px;
      outline: none;
    }

    .field select:focus,
    .field input:focus {
      border-color: var(--accent);
      box-shadow: 0 0 0 3px rgba(15, 118, 110, 0.12);
    }

    button {
      height: 42px;
      border: 0;
      border-radius: 11px;
      cursor: pointer;
      background: linear-gradient(90deg, var(--accent), #0d9488);
      color: #fff;
      font-weight: 600;
      transition: transform .14s ease, box-shadow .14s ease;
    }

    button:hover {
      transform: translateY(-1px);
      box-shadow: 0 8px 18px rgba(15, 118, 110, .24);
    }

    .meta {
      display: flex;
      gap: 18px;
      flex-wrap: wrap;
      padding: 12px 24px;
      color: var(--muted);
      font-size: 13px;
      border-bottom: 1px solid var(--line);
    }

    .table-wrap {
      padding: 12px 24px 24px;
      overflow: auto;
    }

    table {
      width: 100%;
      border-collapse: collapse;
      min-width: 560px;
      background: #fff;
      border: 1px solid var(--line);
      border-radius: 10px;
      overflow: hidden;
    }

    thead th {
      background: #eef4fa;
      text-align: left;
      font-size: 13px;
      color: #364153;
      padding: 10px 12px;
      border-bottom: 1px solid var(--line);
    }

    tbody td {
      font-size: 14px;
      padding: 10px 12px;
      border-bottom: 1px solid #edf1f6;
    }

    tbody tr:nth-child(even) { background: #fcfdff; }
    .num { text-align: right; font-variant-numeric: tabular-nums; }
    .buy { color: var(--buy); font-weight: 600; }
    .sell { color: var(--sell); font-weight: 600; }
    .status { color: var(--muted); }
    .status.error { color: #b91c1c; font-weight: 600; }

    @media (max-width: 900px) {
      .controls { grid-template-columns: 1fr 1fr; }
      .controls .field:last-child { grid-column: 1 / -1; }
    }
  </style>
</head>
<body>
  <main class="wrap">
    <header>
      <h1>Broker Daily Buy/Sell Viewer</h1>
      <div class="sub">選擇 stock_id 的 SQLite、分點與日期區間，查詢每日買進/賣出成交量</div>
    </header>

    <section class="controls">
      <div class="field">
        <label for="dbSelect">Stock DB</label>
        <select id="dbSelect"></select>
      </div>
      <div class="field">
        <label for="brokerFilter">Broker</label>
        <input id="brokerFilter" type="text" placeholder="輸入分點代號或名稱" />
        <select id="brokerSelect"></select>
      </div>
      <div class="field">
        <label for="startDate">Start Date</label>
        <input id="startDate" type="date" />
      </div>
      <div class="field">
        <label for="endDate">End Date</label>
        <input id="endDate" type="date" />
      </div>
      <div class="field">
        <label>&nbsp;</label>
        <button id="runBtn">查詢</button>
      </div>
    </section>

    <section class="meta">
      <div id="dbRange">資料範圍：-</div>
      <div id="rowInfo">筆數：-</div>
      <div id="sumInfo">合計：-</div>
      <div id="status" class="status">狀態：初始化中</div>
    </section>

    <section class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>日期</th>
            <th class="num">買進量</th>
            <th class="num">賣出量</th>
            <th class="num">買進均價</th>
            <th class="num">賣出均價</th>
            <th class="num">淨買賣</th>
          </tr>
        </thead>
        <tbody id="tbody">
          <tr><td colspan="6" class="status">尚未查詢</td></tr>
        </tbody>
      </table>
    </section>
  </main>

  <script>
    const dbSelect = document.getElementById("dbSelect");
    const brokerFilter = document.getElementById("brokerFilter");
    const brokerSelect = document.getElementById("brokerSelect");
    const startDate = document.getElementById("startDate");
    const endDate = document.getElementById("endDate");
    const runBtn = document.getElementById("runBtn");
    const tbody = document.getElementById("tbody");
    const dbRange = document.getElementById("dbRange");
    const rowInfo = document.getElementById("rowInfo");
    const sumInfo = document.getElementById("sumInfo");
    const statusEl = document.getElementById("status");
    let brokerRawList = [];
    let selectedBrokerTableName = "";

    function brokerDisplayText(broker) {
      return `${broker.securities_trader_id} - ${broker.securities_trader}`;
    }

    function renderBrokerOptions(items, keepTableName = "") {
      brokerSelect.innerHTML = "";
      for (const b of items) {
        const opt = document.createElement("option");
        opt.value = b.table_name;
        opt.textContent = brokerDisplayText(b);
        brokerSelect.appendChild(opt);
      }

      if (keepTableName) {
        const target = items.find((b) => b.table_name == keepTableName);
        if (target) {
          selectedBrokerTableName = target.table_name;
          brokerSelect.value = target.table_name;
          return;
        }
      }

      if (items.length > 0) {
        selectedBrokerTableName = items[0].table_name;
        brokerSelect.value = items[0].table_name;
      } else {
        selectedBrokerTableName = "";
      }
    }

    function filterBrokersByKeyword() {
      const keyword = (brokerFilter.value || "").trim().toLowerCase();
      const currentTable = selectedBrokerTableName;
      if (!keyword) {
        renderBrokerOptions(brokerRawList, currentTable);
        return;
      }

      const filtered = brokerRawList.filter((b) => {
        const id = (b.securities_trader_id || "").toLowerCase();
        const name = (b.securities_trader || "").toLowerCase();
        return id.includes(keyword) || name.includes(keyword);
      });
      renderBrokerOptions(filtered, currentTable);
    }

    function setStatus(text, isError = false) {
      statusEl.textContent = "狀態：" + text;
      statusEl.className = isError ? "status error" : "status";
    }

    function fmtNum(v) {
      const n = Number(v || 0);
      return n.toLocaleString("zh-TW", { maximumFractionDigits: 4 });
    }

    async function getJson(url) {
      const res = await fetch(url);
      const body = await res.json();
      if (!res.ok || !body.ok) {
        throw new Error(body.error || "Request failed");
      }
      return body.data;
    }

    async function loadDbList() {
      const dbs = await getJson("/api/stocks");
      dbSelect.innerHTML = "";
      for (const item of dbs) {
        const opt = document.createElement("option");
        opt.value = item.db_file;
        opt.textContent = `${item.stock_id} (${item.db_file})`;
        dbSelect.appendChild(opt);
      }
      if (!dbs.length) throw new Error("找不到任何 <stock_id>.sqlite");
    }

    async function loadBrokersAndRange() {
      const db = encodeURIComponent(dbSelect.value);
      const [brokers, range] = await Promise.all([
        getJson(`/api/brokers?db_file=${db}`),
        getJson(`/api/date_range?db_file=${db}`),
      ]);

      brokerRawList = brokers;
      brokerFilter.value = "";
      renderBrokerOptions(brokerRawList);

      if (range.min_date) {
        startDate.value = range.min_date;
        startDate.min = range.min_date;
        startDate.max = range.max_date;
      }
      if (range.max_date) {
        endDate.value = range.max_date;
        endDate.min = range.min_date;
        endDate.max = range.max_date;
      }
      dbRange.textContent = `資料範圍：${range.min_date || "-"} ~ ${range.max_date || "-"}`;
      setStatus(`已載入分點 ${brokers.length} 筆`);
    }

    async function runQuery() {
      const db = dbSelect.value;
      const table = selectedBrokerTableName;
      const s = startDate.value;
      const e = endDate.value;
      if (!db || !table || !s || !e) {
        setStatus("請完整選擇 DB、分點與日期", true);
        return;
      }
      if (s > e) {
        setStatus("開始日期不能晚於結束日期", true);
        return;
      }

      setStatus("查詢中...");
      try {
        const data = await getJson(
          `/api/daily_volume?db_file=${encodeURIComponent(db)}&table_name=${encodeURIComponent(table)}&start_date=${encodeURIComponent(s)}&end_date=${encodeURIComponent(e)}`
        );
        renderRows(data);
        setStatus("完成");
      } catch (err) {
        setStatus(err.message, true);
      }
    }

    function renderRows(rows) {
      if (!rows.length) {
        tbody.innerHTML = `<tr><td colspan="6" class="status">此條件查無資料</td></tr>`;
        rowInfo.textContent = "筆數：0";
        sumInfo.textContent = "合計：買進 0 / 賣出 0 / 淨買賣 0";
        return;
      }

      let sumBuy = 0;
      let sumSell = 0;
      const html = rows.map(r => {
        const b = Number(r.total_buy || 0);
        const s = Number(r.total_sell || 0);
        const bp = Number(r.avg_buy_price || 0);
        const sp = Number(r.avg_sell_price || 0);
        const net = b - s;
        sumBuy += b;
        sumSell += s;
        return `<tr>
          <td>${r.date}</td>
          <td class="num buy">${fmtNum(b)}</td>
          <td class="num sell">${fmtNum(s)}</td>
          <td class="num">${fmtNum(bp)}</td>
          <td class="num">${fmtNum(sp)}</td>
          <td class="num">${fmtNum(net)}</td>
        </tr>`;
      }).join("");
      tbody.innerHTML = html;

      rowInfo.textContent = `筆數：${rows.length}`;
      sumInfo.textContent = `合計：買進 ${fmtNum(sumBuy)} / 賣出 ${fmtNum(sumSell)} / 淨買賣 ${fmtNum(sumBuy - sumSell)}`;
    }

    async function init() {
      try {
        await loadDbList();
        await loadBrokersAndRange();
        await runQuery();
      } catch (err) {
        setStatus(err.message, true);
      }
    }

    dbSelect.addEventListener("change", async () => {
      try {
        await loadBrokersAndRange();
        await runQuery();
      } catch (err) {
        setStatus(err.message, true);
      }
    });
    brokerFilter.addEventListener("input", filterBrokersByKeyword);
    brokerSelect.addEventListener("change", () => {
      selectedBrokerTableName = brokerSelect.value || "";
    });
    runBtn.addEventListener("click", runQuery);
    init();
  </script>
</body>
</html>
"""


class RequestHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path
        params = parse_qs(parsed.query)

        try:
            if path == "/":
                self._send_html(build_index_html())
                return
            if path == "/api/stocks":
                self._send_json({"ok": True, "data": list_stock_dbs()})
                return
            if path == "/api/brokers":
                db_file = self._required(params, "db_file")
                db_path = resolve_db_path(db_file)
                data = query_brokers(db_path)
                self._send_json({"ok": True, "data": data})
                return
            if path == "/api/date_range":
                db_file = self._required(params, "db_file")
                db_path = resolve_db_path(db_file)
                data = query_db_date_range(db_path)
                self._send_json({"ok": True, "data": data})
                return
            if path == "/api/daily_volume":
                db_file = self._required(params, "db_file")
                table_name = self._required(params, "table_name")
                start_date = self._required(params, "start_date")
                end_date = self._required(params, "end_date")
                if not valid_iso_date(start_date) or not valid_iso_date(end_date):
                    raise ValueError("Date format must be YYYY-MM-DD")
                if start_date > end_date:
                    raise ValueError("start_date must be <= end_date")
                db_path = resolve_db_path(db_file)
                data = query_daily_buy_sell(db_path, table_name, start_date, end_date)
                self._send_json({"ok": True, "data": data})
                return

            self._send_json({"ok": False, "error": f"Not found: {path}"}, status=404)
        except Exception as exc:  # pylint: disable=broad-except
            self._send_json({"ok": False, "error": str(exc)}, status=400)

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
        return

    def _required(self, params: dict[str, list[str]], key: str) -> str:
        values = params.get(key)
        if not values or not values[0].strip():
            raise ValueError(f"Missing query param: {key}")
        return values[0].strip()

    def _send_json(self, payload: dict[str, Any], status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_html(self, html: str, status: int = 200) -> None:
        body = html.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main() -> int:
    args = parse_args()
    server = ThreadingHTTPServer((args.host, args.port), RequestHandler)
    print(f"Broker viewer running at http://{args.host}:{args.port}")
    print("Press Ctrl+C to stop")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
