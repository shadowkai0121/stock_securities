# `strategies/ma-cross` 全部落地版均線交叉回測方案

## 摘要
在不改動其他資料夾程式碼的前提下，將「策略相關程式、文件、測試」全部放在 `strategies/ma-cross`，提供可直接執行的長短均線交叉回測工具（做多/空手），支援 SQLite 讀取、可選自動補資料、Console/CSV/圖表輸出。

## 範圍與限制
- 只新增/修改 `strategies/ma-cross` 內檔案。
- 不新增 `src/`、`tests/`、`README.md`（root）等其他路徑的程式碼變更。
- 回測模式固定為你已選的「做多/空手」。
- 輸出固定包含 `Console + CSV + 圖`（保留 `--no-plot` 可關閉圖）。

## 檔案規劃（全部在 `strategies/ma-cross`）
1. `strategies/ma-cross/backtest.py`
   - 單檔主程式（避免 `ma-cross` 目錄名含 `-` 造成 Python package import 問題）。
   - 內含：參數解析、資料讀取/補抓、訊號計算、回測、績效統計、檔案輸出、畫圖。
2. `strategies/ma-cross/README.md`
   - 使用指令、參數、資料表需求、回測假設、輸出欄位說明。
3. `strategies/ma-cross/test_backtest.py`
   - 單元測試（`unittest`），以暫存 SQLite 驗證策略核心邏輯。

## 介面與公開行為
### CLI（`python strategies/ma-cross/backtest.py ...`）
- 必填：
  - `--stock-id`
  - `--start-date` (`YYYY-MM-DD`)
  - `--end-date` (`YYYY-MM-DD`)
- 參數：
  - `--short-window`（預設 `20`）
  - `--long-window`（預設 `60`，必須 `> short-window`）
  - `--table`（`price_adj_daily|price_daily`，預設 `price_adj_daily`）
  - `--db-path`（預設 `data/<stock_id>.sqlite`）
  - `--ensure-data`（可選先補抓資料）
  - `--replace-db`（搭配 `--ensure-data`）
  - `--token`（補抓時可傳，否則走 env/.env 規則）
  - `--fee-bps`（預設 `0.0`）
  - `--output-dir`（預設 `strategies/ma-cross/outputs`）
  - `--no-plot`
- 不變更現有 `finmind-dl` CLI public API。

## 實作細節（決策完成）
1. 啟動時在 `backtest.py` 內自動加入 `src` 到 `sys.path`（用 `Path(__file__).resolve().parents[2] / "src"`），確保未安裝 editable mode 也可匯入 `finmind_dl`。
2. `--ensure-data` 啟用時：
   - `price_adj_daily` 呼叫 `finmind_dl.datasets.price_adj.run`
   - `price_daily` 呼叫 `finmind_dl.datasets.price.run`
   - token 由 `finmind_dl.core.config.resolve_token` 解決。
3. SQL 讀取：
   - 從指定 table 取 `date, stock_id, open, close, is_placeholder`。
   - 條件：`stock_id` + 日期區間；排序 `ORDER BY date`。
   - 清洗：`is_placeholder=0`、`close` 非空、同日去重保留最後一筆。
4. 訊號與回測：
   - `short_ma`, `long_ma` 用 SMA。
   - `signal = (short_ma > long_ma).astype(int)`，MA 未成熟期間訊號強制 0。
   - `position = signal.shift(1).fillna(0)`（避免 lookahead）。
   - `ret = close.pct_change()`。
   - 成本：`trade_event = signal.diff().abs()`；`cost = trade_event.shift(1) * fee_bps/10000`。
   - `strategy_ret = position * ret - cost`，`equity` 與 `bh_equity` 累乘。
5. 交易明細：
   - 0→1 記進場，1→0 記出場。
   - 最後若持倉，最後一日強制平倉（`exit_reason=eod`）。
6. 輸出結構：
   - `output_dir/<stock>_<start>_<end>_s<short>_l<long>_<table>/`
   - `equity.csv`、`trades.csv`、`report.json`、`plot.png`（除非 `--no-plot`）。
   - 圖表用 `matplotlib` `Agg` backend。

## 失敗模式與處理
- 參數錯誤（日期格式、long<=short）直接 `ValueError` + 非 0 結束。
- DB/表不存在或查無資料：明確訊息指出 table/db-path/日期區間。
- `--ensure-data` 但 token 缺失：沿用 `resolve_token` 錯誤訊息。
- 資料筆數不足以形成長均線：仍輸出空交易報告，metrics 以 `0` 或 `NaN`（在 `report.json` 明確標註）。

## 測試案例（`strategies/ma-cross/test_backtest.py`）
1. `is_placeholder=1` 會被排除。
2. 合成價格序列可產生至少一次黃金/死亡交叉，驗證 `signal/position` 與交易筆數。
3. 驗證 `position` 必為 `signal` 的一日位移（無 lookahead）。
4. 啟用 `fee_bps` 時，`strategy_ret` 低於未計成本版本。
5. 最後持倉會自動在末日平倉並有 `exit_reason=eod`。
6. 不啟用 `--ensure-data` 的情境下，不觸發任何網路依賴。

## 假設與預設
- 年化交易日 `252`，`rf=0` 用於 Sharpe。
- 報酬計算採 close-to-close。
- 成本模型採固定 bps 簡化（非真實分點手續費+稅）。
- 你要求的限制生效：所有策略相關程式與測試檔都放在 `strategies/ma-cross`。
