# Appendix: 實驗設定、統計方法與可重現細節

本附錄固定化（lock down）所有會影響結論可重現性的實驗設定與統計計算細節；若後續更新程式或資料源，建議同時更新本附錄並保留版本記錄。

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
固定檢查 `fee_bps ∈ {0, 10, 20}`（輸出見 `outputs/thesis/robust_fee/`）。

## A.4 績效指標計算細節
- 年度交易日：252
- `rf`：0

Sharpe（Train/Test 各自計算）：
- `Sharpe = mean(strategy_ret) / std(strategy_ret) * sqrt(252)`
- 若 `std(strategy_ret)=0` 或樣本不足，定義為 NaN。

Total return：
- `equity_T - 1`，其中 `equity_t = Π_{k<=t} (1 + strategy_ret_k)`

MDD：
- `min_t(equity_t / max_{s<=t}(equity_s) - 1)`

## A.5 產業別最佳化分數（Score）細節
對產業 `j` 與參數 `(s,l)`，先對每檔股票 `i` 計算 Train 期 Sharpe：
- `Sharpe_{i,j}(s,l)`

產業分數：
- `Score_j(s,l) = median_i Sharpe_{i,j}(s,l)`

同分 tie-break（依序）：
1. Train 期 `TotalReturn_strategy_i > TotalReturn_bh_i` 的比例較高者
2. `long` 較大者

## A.6 適用性指標與勝率定義
Test 期每檔股票 `i`：
- `Outperform_i = 1` 若 `TotalReturn_strategy_i > TotalReturn_bh_i`，否則為 0

產業適用性：
- `Applicability_j = mean_i(Outperform_i)`

全市場適用性（參考）：
- 對所有納入股票計 `mean(Outperform_i)`。

## A.7 Bootstrap：信賴區間與 p-value（B=5,000）
### A.7.1 95% CI（分位數法）
對產業 `j` 有 `N_j` 檔股票：
1. 重複 `b=1..B`：
   - 放回抽樣 `N_j` 檔股票形成樣本集合
   - 計算 `Applicability_j^(b)`
2. 95% CI = `quantile({Applicability_j^(b)}, 0.025)` 與 `quantile(..., 0.975)`

### A.7.2 產業內假說檢定（one-sided）
檢定 `H1: Applicability_j > 0.5`，以 bootstrap 分布估計 p-value：
- `p_j = (count(Applicability_j^(b) <= 0.5) + 1) / (B + 1)`

## A.8 Benjamini–Hochberg（BH）FDR 校正（q=0.05）
對所有產業得到 p-value `{p_1..p_m}`：
1. 依 p-value 由小到大排序：`p_(1) <= ... <= p_(m)`
2. 找最大 `k` 使得 `p_(k) <= (k/m) * q`
3. 宣告 `p_(1..k)` 對應產業為 FDR 控制下顯著

## A.9 產業分類快照與資料源限制
理想設定：取得 `2019-01-01` 當下之官方產業對照表（`stock_id -> industry`）並固定於研究期間。

本專案使用的 `TaiwanStockInfo` 回傳含 `date` 欄位，但其行為並非提供「完整的歷史快照序列」。因此主流程採用：
- 以回傳資料中「覆蓋股票數最多」的日期作為參考快照（本次主分析為 `2026-03-05`，記錄於 `outputs/thesis/run_metadata.json`）
- 同一股票若同日出現多筆產業分類，偏好較具體（非總類／非「其他」等）的分類

上述做法可維持樣本內一致的分組，但可能引入產業重分類與前視性偏誤，需於限制章節揭露。

## A.10 可重現步驟（命令列）
### A.10.1 產生論文輸出（主分析）

```bash
python strategies/ma-cross/thesis_pipeline.py
```

輸出預設寫入：
- `strategies/ma-cross/outputs/thesis/`
- 個股 SQLite 寫入 `data/<stock_id>.sqlite`

### A.10.2 穩健性：成本敏感度

```bash
python strategies/ma-cross/thesis_pipeline.py --skip-download --fee-bps 0  --output-dir strategies/ma-cross/outputs/thesis/robust_fee/fee0  --universe-csv strategies/ma-cross/outputs/thesis/universe.csv
python strategies/ma-cross/thesis_pipeline.py --skip-download --fee-bps 10 --output-dir strategies/ma-cross/outputs/thesis/robust_fee/fee10 --universe-csv strategies/ma-cross/outputs/thesis/universe.csv
python strategies/ma-cross/thesis_pipeline.py --skip-download --fee-bps 20 --output-dir strategies/ma-cross/outputs/thesis/robust_fee/fee20 --universe-csv strategies/ma-cross/outputs/thesis/universe.csv
```

使用 `--universe-csv` 可避免產業分類快照隨 API 更新而漂移，維持與主分析相同之 stock_id 與 industry 映射。

### A.10.3 單一股票策略輸出（回測引擎）
使用 `strategies/ma-cross/backtest.py`：

```bash
python strategies/ma-cross/backtest.py \
  --stock-id 2330 \
  --start-date 2010-01-01 \
  --end-date 2025-12-31 \
  --table price_adj_daily \
  --fee-bps 10
```

### A.10.4 圖檔中文字型
`thesis_pipeline.py` 產生 PNG 圖檔時，Matplotlib 預設設定 `Microsoft JhengHei`/`Microsoft YaHei` 作為 `sans-serif` 字型，以避免中文缺字或亂碼。若在非 Windows 環境執行，請改用可用的 CJK 字型（例如 Noto Sans CJK）。

