#!/usr/bin/env python3
"""
Local web UI for browsing stock broker SQLite data.

Features:
- Select stock SQLite DB (by stock id filename, e.g. 8271.sqlite)
- Query interval buy/sell volume and weighted average price
- Sort by selected column
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


def query_range_summary(
    db_path: Path,
    start_date: str,
    end_date: str,
    broker_keyword: str = "",
) -> list[dict[str, Any]]:
    conn = sqlite3.connect(db_path)
    try:
        conn.row_factory = sqlite3.Row
        brokers = conn.execute(
            """
            SELECT securities_trader_id, securities_trader, table_name
            FROM broker_tables
            ORDER BY securities_trader_id
            """
        ).fetchall()
        summary_rows: list[dict[str, Any]] = []
        keyword = broker_keyword.strip().lower()

        for broker in brokers:
            trader_id = broker["securities_trader_id"]
            trader_name = broker["securities_trader"]
            table_name = broker["table_name"]
            label = f"{trader_id} {trader_name}".lower()
            if keyword and keyword not in label:
                continue

            row = conn.execute(
                f"""
                SELECT
                    COALESCE(SUM(buy), 0) AS total_buy,
                    COALESCE(SUM(sell), 0) AS total_sell,
                    COALESCE(SUM(price * buy), 0) AS sum_buy_amount,
                    COALESCE(SUM(price * sell), 0) AS sum_sell_amount
                FROM "{table_name}"
                WHERE date >= ? AND date <= ?
                """,
                (start_date, end_date),
            ).fetchone()
            total_buy = float(row["total_buy"] or 0.0)
            total_sell = float(row["total_sell"] or 0.0)
            sum_buy_amount = float(row["sum_buy_amount"] or 0.0)
            sum_sell_amount = float(row["sum_sell_amount"] or 0.0)
            avg_buy_price = round(sum_buy_amount / total_buy, 4) if total_buy > 0 else 0.0
            avg_sell_price = round(sum_sell_amount / total_sell, 4) if total_sell > 0 else 0.0
            net_volume = round(total_buy - total_sell, 4)

            summary_rows.append(
                {
                    "securities_trader_id": trader_id,
                    "securities_trader": trader_name,
                    "table_name": table_name,
                    "total_buy": round(total_buy, 4),
                    "total_sell": round(total_sell, 4),
                    "avg_buy_price": avg_buy_price,
                    "avg_sell_price": avg_sell_price,
                    "net_volume": net_volume,
                }
            )
        summary_rows.sort(key=lambda x: x["securities_trader_id"])
        return summary_rows
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
                ) AS avg_sell_price,
                ROUND(SUM(buy) - SUM(sell), 4) AS net_volume
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
  <title>Broker Viewer</title>
  <style>
    :root {
      --bg-a: #f7f1e5;
      --bg-b: #d6e4f0;
      --panel: rgba(255, 255, 255, 0.88);
      --ink: #1e2a39;
      --muted: #5a6675;
      --accent: #0f766e;
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
      width: min(1200px, 100%);
      background: var(--panel);
      border: 1px solid #f2f4f7;
      border-radius: 18px;
      box-shadow: 0 18px 42px rgba(30, 42, 57, 0.12);
      overflow: hidden;
    }
    header {
      padding: 20px 24px 12px;
      border-bottom: 1px solid var(--line);
      background: linear-gradient(90deg, rgba(15,118,110,.08), rgba(245,158,11,.08));
    }
    h1 { margin: 0; font-size: 24px; }
    .sub { margin-top: 6px; color: var(--muted); font-size: 14px; }
    .tabs {
      display: flex;
      gap: 8px;
      padding: 12px 24px;
      border-bottom: 1px solid var(--line);
      background: #f9fcff;
    }
    .tab-btn {
      border: 1px solid var(--line);
      background: #fff;
      color: var(--ink);
      border-radius: 10px;
      padding: 8px 14px;
      cursor: pointer;
      font-size: 14px;
    }
    .tab-btn.active {
      background: var(--accent);
      color: #fff;
      border-color: var(--accent);
    }
    .controls {
      display: grid;
      grid-template-columns: repeat(5, minmax(0, 1fr));
      gap: 12px;
      padding: 16px 24px;
      border-bottom: 1px solid var(--line);
      align-items: end;
    }
    .field label { display: block; font-size: 12px; color: var(--muted); margin-bottom: 5px; }
    .field select, .field input {
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 10px;
      background: #fff;
      color: var(--ink);
      padding: 10px 11px;
      font-size: 14px;
      outline: none;
    }
    button {
      height: 42px;
      border: 0;
      border-radius: 11px;
      cursor: pointer;
      background: linear-gradient(90deg, var(--accent), #0d9488);
      color: #fff;
      font-weight: 600;
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
    .panel { display: none; }
    .panel.active { display: block; }
    .table-wrap { padding: 12px 24px 24px; overflow: auto; }
    table {
      width: 100%;
      border-collapse: collapse;
      min-width: 920px;
      background: #fff;
      border: 1px solid var(--line);
    }
    thead th {
      background: #eef4fa;
      text-align: left;
      font-size: 13px;
      color: #364153;
      padding: 10px 12px;
      border-bottom: 1px solid var(--line);
      user-select: none;
    }
    th.sortable { cursor: pointer; }
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
    @media (max-width: 1100px) {
      .controls { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .controls .field:last-child { grid-column: 1 / -1; }
    }
  </style>
</head>
<body>
  <main class="wrap">
    <header>
      <h1>Broker Viewer</h1>
      <div class="sub">預設為統計頁。每日資料頁保留查詢功能。</div>
    </header>

    <section class="tabs">
      <button id="tabSummary" class="tab-btn active">統計頁</button>
      <button id="tabDaily" class="tab-btn">每日資料頁</button>
    </section>

    <section class="meta">
      <div id="dbRange">資料範圍：-</div>
      <div id="rowInfo">筆數：-</div>
      <div id="sumInfo">合計：-</div>
      <div id="status" class="status">狀態：初始化中</div>
    </section>

    <section id="summaryPanel" class="panel active">
      <section class="controls">
        <div class="field">
          <label for="dbSelectSummary">Stock DB</label>
          <select id="dbSelectSummary"></select>
        </div>
        <div class="field">
          <label for="startDateSummary">Start Date</label>
          <input id="startDateSummary" type="date" />
        </div>
        <div class="field">
          <label for="endDateSummary">End Date</label>
          <input id="endDateSummary" type="date" />
        </div>
        <div class="field">
          <label for="brokerKeywordSummary">Broker 篩選</label>
          <input id="brokerKeywordSummary" type="text" placeholder="輸入分點代號或名稱 (可空白)" />
        </div>
        <div class="field">
          <label>&nbsp;</label>
          <button id="runSummaryBtn">重新載入統計</button>
        </div>
      </section>
      <section class="table-wrap">
        <table>
          <thead>
            <tr>
              <th class="sortable" data-sort-key="securities_trader_id">分點代號</th>
              <th class="sortable" data-sort-key="securities_trader">分點名稱</th>
              <th class="num sortable" data-sort-key="total_buy">買進量</th>
              <th class="num sortable" data-sort-key="total_sell">賣出量</th>
              <th class="num sortable" data-sort-key="avg_buy_price">買進均價</th>
              <th class="num sortable" data-sort-key="avg_sell_price">賣出均價</th>
              <th class="num sortable" data-sort-key="net_volume">淨買賣</th>
            </tr>
          </thead>
          <tbody id="summaryTbody">
            <tr><td colspan="7" class="status">尚未載入</td></tr>
          </tbody>
        </table>
      </section>
    </section>

    <section id="dailyPanel" class="panel">
      <section class="controls">
        <div class="field">
          <label for="dbSelectDaily">Stock DB</label>
          <select id="dbSelectDaily"></select>
        </div>
        <div class="field">
          <label for="brokerFilterDaily">Broker 篩選</label>
          <input id="brokerFilterDaily" type="text" placeholder="輸入分點代號或名稱" />
          <select id="brokerSelectDaily"></select>
        </div>
        <div class="field">
          <label for="startDateDaily">Start Date</label>
          <input id="startDateDaily" type="date" />
        </div>
        <div class="field">
          <label for="endDateDaily">End Date</label>
          <input id="endDateDaily" type="date" />
        </div>
        <div class="field">
          <label>&nbsp;</label>
          <button id="runDailyBtn">查詢每日資料</button>
        </div>
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
          <tbody id="dailyTbody">
            <tr><td colspan="6" class="status">尚未查詢</td></tr>
          </tbody>
        </table>
      </section>
    </section>
  </main>

  <script>
    const tabSummary = document.getElementById("tabSummary");
    const tabDaily = document.getElementById("tabDaily");
    const summaryPanel = document.getElementById("summaryPanel");
    const dailyPanel = document.getElementById("dailyPanel");
    const dbRange = document.getElementById("dbRange");
    const rowInfo = document.getElementById("rowInfo");
    const sumInfo = document.getElementById("sumInfo");
    const statusEl = document.getElementById("status");

    const dbSelectSummary = document.getElementById("dbSelectSummary");
    const startDateSummary = document.getElementById("startDateSummary");
    const endDateSummary = document.getElementById("endDateSummary");
    const brokerKeywordSummary = document.getElementById("brokerKeywordSummary");
    const runSummaryBtn = document.getElementById("runSummaryBtn");
    const summaryTbody = document.getElementById("summaryTbody");

    const dbSelectDaily = document.getElementById("dbSelectDaily");
    const brokerFilterDaily = document.getElementById("brokerFilterDaily");
    const brokerSelectDaily = document.getElementById("brokerSelectDaily");
    const startDateDaily = document.getElementById("startDateDaily");
    const endDateDaily = document.getElementById("endDateDaily");
    const runDailyBtn = document.getElementById("runDailyBtn");
    const dailyTbody = document.getElementById("dailyTbody");

    let summaryRows = [];
    let dailyBrokers = [];
    let selectedDailyTable = "";
    let summarySort = { key: "net_volume", dir: "desc" };

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
    function activateTab(mode) {
      const isSummary = mode === "summary";
      summaryPanel.classList.toggle("active", isSummary);
      dailyPanel.classList.toggle("active", !isSummary);
      tabSummary.classList.toggle("active", isSummary);
      tabDaily.classList.toggle("active", !isSummary);
    }
    function syncDbSelects(value) {
      dbSelectSummary.value = value;
      dbSelectDaily.value = value;
    }
    async function loadDbList() {
      const dbs = await getJson("/api/stocks");
      dbSelectSummary.innerHTML = "";
      dbSelectDaily.innerHTML = "";
      for (const item of dbs) {
        const text = `${item.stock_id} (${item.db_file})`;
        const opt1 = document.createElement("option");
        opt1.value = item.db_file;
        opt1.textContent = text;
        dbSelectSummary.appendChild(opt1);
        const opt2 = document.createElement("option");
        opt2.value = item.db_file;
        opt2.textContent = text;
        dbSelectDaily.appendChild(opt2);
      }
      if (!dbs.length) throw new Error("找不到任何 <stock_id>.sqlite");
      syncDbSelects(dbs[0].db_file);
    }
    async function loadDateRange(dbFile) {
      const range = await getJson(`/api/date_range?db_file=${encodeURIComponent(dbFile)}`);
      if (range.min_date) {
        startDateSummary.value = range.min_date;
        startDateSummary.min = range.min_date;
        startDateSummary.max = range.max_date;
        startDateDaily.value = range.min_date;
        startDateDaily.min = range.min_date;
        startDateDaily.max = range.max_date;
      }
      if (range.max_date) {
        endDateSummary.value = range.max_date;
        endDateSummary.min = range.min_date;
        endDateSummary.max = range.max_date;
        endDateDaily.value = range.max_date;
        endDateDaily.min = range.min_date;
        endDateDaily.max = range.max_date;
      }
      dbRange.textContent = `資料範圍：${range.min_date || "-"} ~ ${range.max_date || "-"}`;
    }
    function sortSummaryRows() {
      const key = summarySort.key;
      const dir = summarySort.dir === "asc" ? 1 : -1;
      const textKeys = new Set(["securities_trader_id", "securities_trader"]);
      summaryRows.sort((a, b) => {
        if (textKeys.has(key)) {
          return String(a[key]).localeCompare(String(b[key]), "zh-Hant") * dir;
        }
        return (Number(a[key]) - Number(b[key])) * dir;
      });
    }
    function renderSummaryRows() {
      if (!summaryRows.length) {
        summaryTbody.innerHTML = `<tr><td colspan="7" class="status">此條件查無資料</td></tr>`;
        rowInfo.textContent = "筆數：0";
        sumInfo.textContent = "合計：買進 0 / 賣出 0 / 淨買賣 0";
        return;
      }
      let sumBuy = 0;
      let sumSell = 0;
      const html = summaryRows.map((r) => {
        const b = Number(r.total_buy || 0);
        const s = Number(r.total_sell || 0);
        sumBuy += b;
        sumSell += s;
        return `<tr>
          <td>${r.securities_trader_id}</td>
          <td>${r.securities_trader}</td>
          <td class="num buy">${fmtNum(b)}</td>
          <td class="num sell">${fmtNum(s)}</td>
          <td class="num">${fmtNum(r.avg_buy_price)}</td>
          <td class="num">${fmtNum(r.avg_sell_price)}</td>
          <td class="num">${fmtNum(r.net_volume)}</td>
        </tr>`;
      }).join("");
      summaryTbody.innerHTML = html;
      rowInfo.textContent = `筆數：${summaryRows.length}`;
      sumInfo.textContent = `合計：買進 ${fmtNum(sumBuy)} / 賣出 ${fmtNum(sumSell)} / 淨買賣 ${fmtNum(sumBuy - sumSell)}`;
    }
    async function loadSummaryData() {
      const db = dbSelectSummary.value;
      const s = startDateSummary.value;
      const e = endDateSummary.value;
      if (!db || !s || !e) {
        setStatus("請完整選擇統計頁條件", true);
        return;
      }
      if (s > e) {
        setStatus("開始日期不能晚於結束日期", true);
        return;
      }
      setStatus("統計頁查詢中...");
      const data = await getJson(
        `/api/range_summary?db_file=${encodeURIComponent(db)}&start_date=${encodeURIComponent(s)}&end_date=${encodeURIComponent(e)}&broker_keyword=${encodeURIComponent(brokerKeywordSummary.value || "")}`
      );
      summaryRows = data;
      sortSummaryRows();
      renderSummaryRows();
      setStatus("完成");
    }
    function renderDailyBrokerOptions(items, keepTableName = "") {
      brokerSelectDaily.innerHTML = "";
      for (const b of items) {
        const opt = document.createElement("option");
        opt.value = b.table_name;
        opt.textContent = `${b.securities_trader_id} - ${b.securities_trader}`;
        brokerSelectDaily.appendChild(opt);
      }
      if (keepTableName && items.some((x) => x.table_name === keepTableName)) {
        brokerSelectDaily.value = keepTableName;
        selectedDailyTable = keepTableName;
        return;
      }
      selectedDailyTable = items.length ? items[0].table_name : "";
      if (items.length) brokerSelectDaily.value = items[0].table_name;
    }
    function filterDailyBrokers() {
      const keyword = (brokerFilterDaily.value || "").trim().toLowerCase();
      const current = selectedDailyTable;
      if (!keyword) {
        renderDailyBrokerOptions(dailyBrokers, current);
        return;
      }
      const filtered = dailyBrokers.filter((b) => {
        const id = String(b.securities_trader_id || "").toLowerCase();
        const name = String(b.securities_trader || "").toLowerCase();
        return id.includes(keyword) || name.includes(keyword);
      });
      renderDailyBrokerOptions(filtered, current);
    }
    async function loadDailyBrokers() {
      const db = dbSelectDaily.value;
      dailyBrokers = await getJson(`/api/brokers?db_file=${encodeURIComponent(db)}`);
      brokerFilterDaily.value = "";
      renderDailyBrokerOptions(dailyBrokers);
    }
    async function runDailyQuery() {
      const db = dbSelectDaily.value;
      const table = selectedDailyTable;
      const s = startDateDaily.value;
      const e = endDateDaily.value;
      if (!db || !table || !s || !e) {
        setStatus("請完整選擇每日頁條件", true);
        return;
      }
      if (s > e) {
        setStatus("開始日期不能晚於結束日期", true);
        return;
      }
      setStatus("每日頁查詢中...");
      const rows = await getJson(
        `/api/daily_volume?db_file=${encodeURIComponent(db)}&table_name=${encodeURIComponent(table)}&start_date=${encodeURIComponent(s)}&end_date=${encodeURIComponent(e)}`
      );
      if (!rows.length) {
        dailyTbody.innerHTML = `<tr><td colspan="6" class="status">此條件查無資料</td></tr>`;
        rowInfo.textContent = "筆數：0";
        sumInfo.textContent = "合計：買進 0 / 賣出 0 / 淨買賣 0";
        setStatus("完成");
        return;
      }
      let sumBuy = 0;
      let sumSell = 0;
      const html = rows.map((r) => {
        const b = Number(r.total_buy || 0);
        const s = Number(r.total_sell || 0);
        sumBuy += b;
        sumSell += s;
        return `<tr>
          <td>${r.date}</td>
          <td class="num buy">${fmtNum(b)}</td>
          <td class="num sell">${fmtNum(s)}</td>
          <td class="num">${fmtNum(r.avg_buy_price)}</td>
          <td class="num">${fmtNum(r.avg_sell_price)}</td>
          <td class="num">${fmtNum(r.net_volume)}</td>
        </tr>`;
      }).join("");
      dailyTbody.innerHTML = html;
      rowInfo.textContent = `筆數：${rows.length}`;
      sumInfo.textContent = `合計：買進 ${fmtNum(sumBuy)} / 賣出 ${fmtNum(sumSell)} / 淨買賣 ${fmtNum(sumBuy - sumSell)}`;
      setStatus("完成");
    }
    function bindSummaryHeaderSort() {
      document.querySelectorAll("th.sortable").forEach((th) => {
        th.addEventListener("click", () => {
          const key = th.dataset.sortKey;
          if (!key) return;
          if (summarySort.key === key) {
            summarySort.dir = summarySort.dir === "asc" ? "desc" : "asc";
          } else {
            summarySort.key = key;
            summarySort.dir = "desc";
          }
          sortSummaryRows();
          renderSummaryRows();
        });
      });
    }
    async function init() {
      try {
        await loadDbList();
        await loadDateRange(dbSelectSummary.value);
        await loadDailyBrokers();
        await loadSummaryData();
        bindSummaryHeaderSort();
      } catch (err) {
        setStatus(err.message, true);
      }
    }

    tabSummary.addEventListener("click", () => activateTab("summary"));
    tabDaily.addEventListener("click", () => activateTab("daily"));

    dbSelectSummary.addEventListener("change", async () => {
      try {
        syncDbSelects(dbSelectSummary.value);
        await loadDateRange(dbSelectSummary.value);
        await loadDailyBrokers();
        await loadSummaryData();
      } catch (err) {
        setStatus(err.message, true);
      }
    });
    dbSelectDaily.addEventListener("change", async () => {
      try {
        syncDbSelects(dbSelectDaily.value);
        await loadDateRange(dbSelectDaily.value);
        await loadDailyBrokers();
        await loadSummaryData();
      } catch (err) {
        setStatus(err.message, true);
      }
    });

    runSummaryBtn.addEventListener("click", async () => {
      try {
        await loadSummaryData();
      } catch (err) {
        setStatus(err.message, true);
      }
    });
    brokerFilterDaily.addEventListener("input", filterDailyBrokers);
    brokerSelectDaily.addEventListener("change", () => {
      selectedDailyTable = brokerSelectDaily.value || "";
    });
    runDailyBtn.addEventListener("click", async () => {
      try {
        await runDailyQuery();
      } catch (err) {
        setStatus(err.message, true);
      }
    });
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
            if path == "/api/range_summary":
                db_file = self._required(params, "db_file")
                start_date = self._required(params, "start_date")
                end_date = self._required(params, "end_date")
                broker_keyword = (params.get("broker_keyword") or [""])[0]
                if not valid_iso_date(start_date) or not valid_iso_date(end_date):
                    raise ValueError("Date format must be YYYY-MM-DD")
                if start_date > end_date:
                    raise ValueError("start_date must be <= end_date")
                db_path = resolve_db_path(db_file)
                data = query_range_summary(
                    db_path=db_path,
                    start_date=start_date,
                    end_date=end_date,
                    broker_keyword=broker_keyword,
                )
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
                data = query_daily_buy_sell(
                    db_path=db_path,
                    table_name=table_name,
                    start_date=start_date,
                    end_date=end_date,
                )
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
