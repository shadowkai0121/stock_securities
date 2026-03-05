# Appendix: 實驗設定、統計方法與可重現細節

本附錄固定化（lock down）所有會影響結論可重現性的實驗設定與統計計算細節。

## A.1 固定樣本期間與切分
- 全樣本期間：`2010-01-01` ~ `2025-12-31`
- Train：`2010-01-01` ~ `2018-12-31`
- Test：`2019-01-01` ~ `2025-12-31`

所有主結果以 Test 期為準。

## A.2 參數格點（Grid）
均線視窗：
- `short ∈ {5, 10, 20, 30, 40}`
- `long  ∈ {20, 60, 120, 200}` 且 `long > short`

每個產業只選出 1 組最佳參數 `(s*_j, l*_j)`，並在 Test 期套用至該產業所有納入股票。

## A.3 策略與成本模型（與 `backtest.py` 一致）
令 `signal_t = 1(SMA_short,t > SMA_long,t)`，且暖機期 `signal_t=0`。

避免前視偏誤：
- `position_t = signal_{t-1}`

close-to-close 報酬：
- `ret_t = close_t / close_{t-1} - 1`

成本（每次換倉事件固定 bps，成本在下一期扣除）：
- `cost_t = |signal_{t-1} - signal_{t-2}| * fee_bps / 10000`
- `strategy_ret_t = position_t * ret_t - cost_t`

### 成本敏感度（穩健性檢查）
固定檢查 `fee_bps ∈ {0, 10, 20}`。

## A.4 績效指標計算細節
- 年度交易日：252
- rf：0

Sharpe（Test 或 Train 各自計算）：
- `Sharpe = mean(strategy_ret) / std(strategy_ret) * sqrt(252)`
- 若 `std(strategy_ret)=0` 或樣本不足，定義為 NaN，不納入中位數計算（或視為極差值；本文採 NaN 剔除）。

Total return：
- `equity_T - 1`，其中 `equity_t = Π_{k<=t} (1 + strategy_ret_k)`

CAGR：
- 以交易日數換算年化；若樣本長度不足或 equity 非正，CAGR 設為 NaN。

MDD：
- `min_t(equity_t / max_{s<=t}(equity_s) - 1)`

## A.5 產業別最佳化分數（Score）細節
對產業 j 與參數 (s,l)，先對每檔股票 i 計算 Train 期 Sharpe：
- `Sharpe_{i,j}(s,l)`

產業分數：
- `Score_j(s,l) = median_i Sharpe_{i,j}(s,l)`

同分 tie-break：
1. Train 期 `TotalReturn_strategy_i > TotalReturn_bh_i` 的比例較高者
2. `long` 較大者

## A.6 適用性指標與勝率定義
Test 期每檔股票 i：
- `Outperform_i = 1` 若 `TotalReturn_strategy_i > TotalReturn_bh_i`
- 否則 `Outperform_i = 0`

產業適用性：
- `Applicability_j = mean_i(Outperform_i)`

全市場適用性（可作為參考）：
- 對所有納入股票計 `mean(Outperform_i)`。

## A.7 Bootstrap：信賴區間與 p-value（B=5,000）
### A.7.1 95% CI（分位數法）
對產業 j 有 N_j 檔股票：
1. 重複 b=1..B：
   - 放回抽樣 N_j 檔股票形成樣本集合
   - 計算 `Applicability_j^(b)`
2. 95% CI = `quantile({Applicability_j^(b)}, 0.025)` 與 `quantile(..., 0.975)`

### A.7.2 產業內假說檢定（one-sided）
檢定 H1：`Applicability_j > 0.5`。
以 bootstrap 分布估計 p-value：
- `p_j = mean_b( Applicability_j^(b) <= 0.5 )`
（若需要連續性修正可用 `(count + 1) / (B + 1)`；本文採後者。）

### A.7.3 產業間差異（補充分析）
對兩產業 j,k：
1. 分別對 j 與 k 做 bootstrap（各自放回抽樣）
2. 每次計算差異 `D^(b) = Applicability_j^(b) - Applicability_k^(b)`
3. 以 `D` 的分布建立 CI，並以雙尾檢定 `p = 2*min(P(D<=0), P(D>=0))`。

## A.8 Benjamini–Hochberg（BH）FDR 校正（q=0.05）
對所有產業得到 p-value `{p_1..p_m}`：
1. 依 p-value 由小到大排序：`p_(1) <= ... <= p_(m)`
2. 找最大 k 使得 `p_(k) <= (k/m) * q`
3. 宣告 `p_(1..k)` 對應產業為 FDR 控制下顯著

主表同時呈現：
- 未校正 p-value
- BH 校正後顯著性標記（或 adjusted q-value）

## A.9 產業分類快照（2019-01-01）處理原則
理想狀況：取得 `2019-01-01` 當下之官方產業對照表（`stock_id -> industry`）並固定於研究期間。

若只能取得「某個日期的最新分類」：
- 以該份分類作為分組（固定不變），並在限制章明確說明可能導致的偏誤（例如產業重分類會使部分股票被錯分）。

## A.10 可重現步驟（命令列）
### A.10.1 單一股票策略輸出
使用 `strategies/ma-cross/backtest.py`：

```bash
python strategies/ma-cross/backtest.py \
  --stock-id 2330 \
  --start-date 2010-01-01 \
  --end-date 2025-12-31 \
  --table price_adj_daily \
  --fee-bps 10
```

若需要補抓資料（需 token）：

```bash
python strategies/ma-cross/backtest.py \
  --stock-id 2330 \
  --start-date 2010-01-01 \
  --end-date 2025-12-31 \
  --table price_adj_daily \
  --ensure-data \
  --fee-bps 10
```

### A.10.2 測試

```bash
python -m unittest discover -s strategies/ma-cross -p "test_backtest.py"
```

## A.11 表圖輸出格式（對照規格）
所有 Table/Figure 的輸出欄位、計算方式、檔名規範請參考 `FIGURE_TABLE_SPECS.md`。

