# Figure/Table Specs: 產業別均線交叉適用性檢證

本文件定義論文中所有主要表格與圖形的「輸入資料、計算流程、輸出格式」規格，確保結果可重現與可審查。

## 0. 建議輸出路徑
建議將論文用中間結果與最終圖表放在：
- `strategies/ma-cross/outputs/thesis/`

並以固定資料截止日 `2025-12-31` 命名，避免每次重跑覆蓋造成不可重現。

## 1. 共用資料表（中間層）規格
以下為產出 Table/Figure 所需的「標準化中間資料」，即使目前尚未實作批次程式，也應以此作為資料交換格式。

### 1.1 股票清單與產業映射：`universe.csv`
- 每列代表一檔股票（研究納入樣本）
- 欄位：
  - `stock_id`：字串
  - `industry`：官方產業別名稱或代碼
  - `is_listed`：0/1（可選）
  - `is_otc`：0/1（可選）
  - `train_trading_days`：Train 期有效交易日數
  - `test_trading_days`：Test 期有效交易日數
  - `included`：0/1（是否通過門檻）
- 規則：
  - `included=1` 必須滿足 `train_trading_days>=504` 且 `test_trading_days>=252`

### 1.2 產業最佳化參數：`industry_best_params.csv`
- 每列代表一個產業
- 欄位：
  - `industry`
  - `best_short`
  - `best_long`
  - `train_score_sharpe_median`
  - `train_outperform_ratio`：Train 期 outperform 比例（tie-break 用）
  - `tie_break_rule`：字串（例如 `score`/`ratio`/`long`）

### 1.3 股票層級 OOS 結果：`stock_oos_results.csv`
- 每列代表一檔股票（Test 期）
- 欄位（最小集合）：
  - `stock_id`
  - `industry`
  - `best_short`
  - `best_long`
  - `fee_bps`
  - `table`：`price_adj_daily` 或 `price_daily`
  - `test_total_return_strategy`
  - `test_total_return_bh`
  - `test_outperform`：0/1（策略是否打敗 BH）
  - `test_sharpe_strategy`
  - `test_mdd_strategy`
  - `test_excess_return`：`test_total_return_strategy - test_total_return_bh`

## 2. Table Specs
### Table 1：樣本描述
- 目的：描述各產業樣本數與交易日覆蓋情況
- 輸入：`universe.csv`
- 計算：
  - 依 `industry` 分組，統計：
    - `n_stocks`：included=1 股票數
    - `train_days_median`、`test_days_median`
    - `train_days_p25/p75`、`test_days_p25/p75`（可選）
- 輸出：`strategies/ma-cross/outputs/thesis/table1_sample_description.csv`
- 欄位（建議）：
  - `industry, n_stocks, train_days_median, test_days_median`

### Table 2：產業最佳參數與訓練期分數
- 目的：呈現各產業最佳 (short,long) 與 Train 期產業分數
- 輸入：`industry_best_params.csv`
- 輸出：`strategies/ma-cross/outputs/thesis/table2_best_params.csv`
- 欄位：
  - `industry, best_short, best_long, train_score_sharpe_median, train_outperform_ratio`

### Table 3（主表）：產業 OOS 適用性比例 + CI + FDR
- 目的：呈現各產業在 OOS 的適用性（打敗 BH 比例）與統計顯著性
- 輸入：
  - `stock_oos_results.csv`
  - bootstrap 結果（可存成 `industry_bootstrap.csv`）
  - p-values（可存成 `industry_pvalues.csv`）
- 計算：
  - 依 `industry` 計 `Applicability = mean(test_outperform)`
  - bootstrap（B=5000，股票為單位）得到 `ci_low, ci_high`
  - H1：`Applicability > 0.5` 的 one-sided p-value
  - BH(FDR q=0.05) 校正顯著性
- 輸出：`strategies/ma-cross/outputs/thesis/table3_applicability_oos.csv`
- 欄位（建議）：
  - `industry, n_stocks, applicability, ci_low, ci_high, p_value, bh_significant`

## 3. Figure Specs
### Figure 1：產業適用性比例條狀圖（含 95% CI）
- 目的：視覺化比較各產業 OOS 適用性
- 輸入：Table 3 輸出 CSV
- 作圖：
  - x 軸：產業（依 applicability 由高到低排序）
  - y 軸：`applicability`
  - error bar：`[ci_low, ci_high]`
  - 顯著性：用顏色或符號標記 `bh_significant=1`
- 輸出：`strategies/ma-cross/outputs/thesis/figure1_applicability_bar.png`

### Figure 2：最佳化參數分布（short/long）
- 目的：觀察各產業最佳 (short,long) 是否呈現系統性
- 輸入：`industry_best_params.csv`
- 作圖（兩種皆可，論文固定採其中一種並在圖說寫死）：
  1. scatter：x=`best_long`, y=`best_short`, label=industry（或以顏色表示產業大類）
  2. heatmap：以 (short,long) 格點為格，填入「選到該格點的產業數」
- 輸出：
  - `strategies/ma-cross/outputs/thesis/figure2_best_params_scatter.png` 或
  - `strategies/ma-cross/outputs/thesis/figure2_best_params_heatmap.png`

## 4. Robustness Output Specs（固定三組）
### R1：成本敏感度（fee_bps ∈ {0,10,20}）
- 目的：檢查主結論是否依賴成本假設
- 輸入：`stock_oos_results.csv`（對不同 fee_bps 各一份）
- 輸出（建議）：
  - `strategies/ma-cross/outputs/thesis/robust_fee/table_applicability_by_fee.csv`
  - `strategies/ma-cross/outputs/thesis/robust_fee/figure_applicability_by_fee.png`

### R2：價格表替代（price_adj_daily vs price_daily）
- 目的：檢查調整/未調整價格差異對結論影響
- 輸入：同上，但 `table` 不同
- 輸出（建議）：
  - `strategies/ma-cross/outputs/thesis/robust_table/table_compare_adj_vs_raw.csv`

### R3：子樣本切分（2010-2014, 2015-2019, 2020-2025）
- 目的：檢查不同市場階段下的結論一致性
- 輸入：各子樣本各自重跑最佳化與 OOS（或固定參數重估）
- 輸出（建議）：
  - `strategies/ma-cross/outputs/thesis/robust_subsample/table_applicability_by_subsample.csv`

## 5. 與 `backtest.py` 的一致性檢查清單
在撰寫 Methods 與 Replication 時，需確認以下與程式一致：
- `position = signal.shift(1)`（訊號次日生效）
- `cost_t` 以 `|Δsignal_{t-1}|` 計並在下一期扣除（shift）
- 報酬為 close-to-close `pct_change()`

