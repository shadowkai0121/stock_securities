# 8271.sqlite 資料索引

最後更新: 2026-03-05
來源檔案: `8271.sqlite`

## 1. 資料庫總覽

- 主要用途: 儲存股票 `8271` 的分點明細、股價、還原股價與抓取歷程
- 物件數量:
  - table/view 總數: `944`
  - index 總數: `942`
- 主要模組:
  - 分點明細: `broker_<券商代號>_<hash>`（共 `940` 張）
  - 分點對照: `broker_tables`
  - 抓取歷程: `fetch_history`
  - 股價: `stock_price`
  - 還原股價: `stock_price_adj`

## 2. 主要資料表

## 2.1 `broker_tables`

用途: 券商代號與實際分表名對照。

欄位:
- `securities_trader_id` (TEXT, PK)
- `securities_trader` (TEXT)
- `table_name` (TEXT, UNIQUE)
- `stock_id` (TEXT)
- `updated_at` (TEXT, default `datetime('now')`)

筆數: `940`

## 2.2 `fetch_history`

用途: 每次執行抓取腳本的寫入紀錄。

欄位:
- `id` (INTEGER, PK)
- `stock_id` (TEXT)
- `start_date` (TEXT)
- `end_date` (TEXT)
- `fetched_rows` (INTEGER)
- `inserted_rows` (INTEGER)
- `fetched_at` (TEXT, default `datetime('now')`)
- `dataset` (TEXT, default `''`)

筆數: `7`

最新 4 筆資料集:
- `TaiwanStockTradingDailyReport`
- `TaiwanStockPriceAdj`
- `TaiwanStockPrice`
- `TaiwanStockPrice`

## 2.3 `stock_price`

用途: `TaiwanStockPrice` 日資料。

欄位:
- `id`, `date`, `stock_id`
- `open`, `max`, `min`, `close`
- `Trading_Volume`, `Trading_money`, `spread`, `Trading_turnover`
- `inserted_at`

統計:
- 筆數: `727`
- 日期範圍: `2023-03-06` ~ `2026-03-05`

## 2.4 `stock_price_adj`

用途: `TaiwanStockPriceAdj` 日資料。

欄位: 與 `stock_price` 相同。

統計:
- 筆數: `727`
- 日期範圍: `2023-03-06` ~ `2026-03-05`

## 2.5 `broker_<id>_<hash>`（分點明細分表）

用途: 每家券商分點獨立一張表，儲存該分點逐日成交價位買賣量。

命名規則:
- 由 `broker_tables.table_name` 提供實際表名
- 範例: `broker_1020_6d1270b059`

欄位:
- `id`, `date`, `stock_id`
- `securities_trader_id`, `securities_trader`
- `price`, `buy`, `sell`
- `inserted_at`

範例表統計 (`broker_1020_6d1270b059`):
- 筆數: `1662`
- 日期範圍: `2023-03-06` ~ `2026-03-04`

## 3. 索引策略

- 每張 `broker_*` 分表皆有:
  - `idx_<table>_date` (單欄位索引: `date`)
- `stock_price` / `stock_price_adj`:
  - `idx_<table>_date_stock` (`date, stock_id`)
- 唯一鍵:
  - `stock_price` / `stock_price_adj`: `(date, stock_id)`
  - 各 `broker_*`: `(date, stock_id, securities_trader_id, securities_trader, price, buy, sell)`

## 4. 常用查詢範例

```sql
-- 1) 列出所有分點對照
SELECT securities_trader_id, securities_trader, table_name
FROM broker_tables
ORDER BY securities_trader_id;
```

```sql
-- 2) 查指定分點逐日資料（先從 broker_tables 找 table_name）
SELECT date, price, buy, sell
FROM broker_1020_6d1270b059
ORDER BY date;
```

```sql
-- 3) 近期抓取紀錄
SELECT id, dataset, stock_id, start_date, end_date, fetched_rows, inserted_rows, fetched_at
FROM fetch_history
ORDER BY id DESC
LIMIT 20;
```

```sql
-- 4) 比較原始股價與還原股價
SELECT p.date, p.close AS close_raw, a.close AS close_adj
FROM stock_price p
LEFT JOIN stock_price_adj a
  ON p.date = a.date AND p.stock_id = a.stock_id
WHERE p.stock_id = '8271'
ORDER BY p.date;
```

## 5. 維護建議

- 新增資料時優先透過現有 CLI，避免直接手改分點分表。
- 若做跨分點彙總查詢，建議先用 `broker_tables` 動態組 SQL，或另建彙總表降低多表掃描成本。
- `fetch_history.dataset = ''` 代表舊版腳本紀錄，可視需要回填。
