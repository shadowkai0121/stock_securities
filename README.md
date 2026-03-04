# FinMind TaiwanStockTradingDailyReport CLI

## 環境準備

1. Python 3.10+（目前測試為 Python 3.12）
2. 在專案根目錄建立 `.env`，放入 FinMind Token：

```env
FINMIND_SPONSOR_API_KEY=你的_token
```

## 1) 下載區間資料（8271 範例）

> `TaiwanStockTradingDailyReport` 單次 API 只能查單日，本專案程式會自動逐日抓取後合併輸出。

```bash
python finmind_trading_report_cli.py --stock-id 8271 --start-date 2026-02-02 --end-date 2026-02-03 --output 8271_2026-02-02_2026-02-03.csv
```

不指定 `--output` 時，預設輸出檔名為：

```text
<stock_id>_<start_date>_<end_date>.csv
```

## 2) 合併同日同券商資料（股數加權平均）

```bash
python aggregate_broker_daily_weighted.py --input 8271_2026-02-02_2026-02-03.csv --output 8271_2026-02-02_2026-02-03_broker_daily_weighted.csv
```

若不指定 `--output`，預設輸出：

```text
<input_stem>_broker_daily_weighted.csv
```

## 3) 直接寫入 SQLite（依 stock id 建 DB、依分點 id 建分表）

```bash
python finmind_broker_to_sqlite.py --stock-id 8271 --start-date 2026-02-26 --end-date 2026-03-03
```

預設會建立：

```text
8271.sqlite
```

資料表規則：
- `broker_tables`：分點 id 與實際表名對照（例如 `broker_1160_4136f32e7d`）
- `fetch_history`：每次抓取寫入紀錄
- `broker_<securities_trader_id>_<hash10>`：該分點交易明細（避免表名衝突）

可選參數：

```bash
python finmind_broker_to_sqlite.py --stock-id 8271 --start-date 2026-02-26 --end-date 2026-03-03 --db-path data/8271.sqlite --replace
```

簡單查詢範例（先查分點表名）：

```sql
SELECT securities_trader_id, securities_trader, table_name
FROM broker_tables
ORDER BY securities_trader_id;
```

```sql
SELECT date, price, buy, sell
FROM broker_1160_4136f32e7d
ORDER BY date;
```

## 4) 啟動 Web 介面查詢區間買賣成交量與均價

```bash
python sqlite_broker_web.py --host 127.0.0.1 --port 8765
```

打開瀏覽器：

```text
http://127.0.0.1:8765
```

功能：
- 自動掃描目前資料夾下 `*.sqlite`（檔名需符合 `<stock_id>.sqlite`）
- 預設顯示「統計頁」
- 統計頁可選擇 stock DB 與日期區間，彙總各分點：
  - `買進量`
  - `賣出量`
  - `買進均價`（買進股數加權）
  - `賣出均價`（賣出股數加權）
  - `淨買賣`
- 統計頁可輸入關鍵字篩選分點（代號或名稱）
- 統計頁可直接點擊表頭進行排序（再次點擊切換升冪/降冪）
- 保留「每日資料頁」，只有每日資料頁需要按查詢按鈕才會送出查詢

## 加權計算方式

- 分組鍵：`date + stock_id + securities_trader_id + securities_trader`
- 權重股數：`buy + sell`
- 當日券商加權均價：

```text
weighted_avg_price = Σ(price * (buy + sell)) / Σ(buy + sell)
```

輸出檔也會包含：
- `total_buy`
- `total_sell`
- `total_volume`
- `weighted_avg_buy_price`
- `weighted_avg_sell_price`
- `row_count`
