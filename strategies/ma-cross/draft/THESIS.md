# 產業別視角下均線交叉策略適用性之樣本外檢證：以台灣股票市場為例

## 摘要（Draft）
本研究檢證均線交叉（moving average crossover, MA cross）策略在台灣股票市場不同產業類別中的樣本外（out-of-sample, OOS）適用性差異。研究設計以交易所官方產業別為分組基準，採用日資料與做多/空手（long/cash）策略，並以避免前視偏誤之訊號位移（`position = signal.shift(1)`）定義交易持倉。為降低資料探勘偏誤，本研究採「產業別參數最佳化 + 時間切分 OOS 檢證」流程：在訓練期對每個產業以 Sharpe ratio（rf=0）為目標於固定參數格點搜尋最佳 (short,long)，並將該組參數應用於測試期以衡量策略是否能穩健超越 buy-and-hold。主適用性指標定義為產業內股票在 OOS 期間「策略總報酬大於 buy-and-hold 總報酬」之比例，並以股票為單位 bootstrap 建立信賴區間與假說檢定，同時以 Benjamini–Hochberg（BH）法進行多重比較校正。研究結果將回應：不同產業是否存在顯著的策略適用性差異、最佳化參數是否呈現系統性分布，以及此差異在成本與樣本分割下的穩健性。

關鍵字：均線交叉、產業別、樣本外檢證、Sharpe ratio、多重比較、bootstrap、台灣股票市場

## 1. 研究動機與問題定義
技術分析策略的有效性常被質疑源自資料探勘與樣本內過度配適；另一方面，產業結構（景氣循環、波動結構、資訊擴散速度）可能導致同一策略在不同產業中表現差異。若均線交叉屬於趨勢追蹤型策略，其成效可能在具有較強趨勢性或較高動能延續性的產業中更為顯著。

本研究聚焦於下列研究問題（Research Questions, RQ）：
1. RQ1：均線交叉策略的 OOS 適用性是否因產業而異？
2. RQ2：產業最佳化參數 (short,long) 是否呈現系統性差異（例如偏短期或長期趨勢）？
3. RQ3：在控制成本與不同子樣本切分後，產業差異是否仍穩健存在？

## 2. 文獻回顧（段落模板）
本節撰寫時建議依三條主軸組織：
1. 趨勢追蹤與技術分析（含 MA cross）之經驗研究與爭論點（有效市場假說、交易成本、樣本外衰減）。
2. 產業異質性對策略績效的影響：不同產業在景氣循環敏感度、波動群聚、資訊不對稱等方面差異，可能影響趨勢策略的訊號品質。
3. 策略研究中的 data-snooping 與 multiple testing：參數掃描與大量比較下的偽陽性風險，以及常見處理（OOS、步進窗口、FDR 校正等）。

## 3. 資料與樣本
### 3.1 價格資料
- 資料來源：FinMind 日頻資料（優先使用調整後價格表 `price_adj_daily`）。
- 資料清理：排除 `is_placeholder = 1` 之占位資料列；排除 `close` 為空值之資料列。
- 報酬計算：採 close-to-close 日報酬 `ret_t = close_t / close_{t-1} - 1`。

### 3.2 產業分類
- 分類標準：交易所官方產業別。
- 分類時間點：固定採 `2019-01-01` 之產業分類快照作為分組基準，以避免後見之產業調整造成前視偏誤。
- 若無法取得歷史快照：以可取得之最早一致分類快照替代，並於限制章節明確說明可能偏誤方向。

### 3.3 納入條件（寫死）
對每檔股票，以有效交易日（排除 placeholder 與缺值）計：
- 訓練期有效交易日數 `>= 504`（約 2 年）
- 測試期有效交易日數 `>= 252`（約 1 年）
未達門檻者不納入該產業樣本。

## 4. 方法
### 4.1 策略定義（與 `strategies/ma-cross/backtest.py` 一致）
令 `close_t` 為 t 日收盤價，定義簡單移動平均（SMA）：
- `SMA_short,t = mean(close_{t-short+1} ... close_t)`
- `SMA_long,t  = mean(close_{t-long+1}  ... close_t)`

交易訊號（signal）定義為：
- `signal_t = 1` 若 `SMA_short,t > SMA_long,t`
- `signal_t = 0` 其餘情況
且於 MA 暖機期（任一 SMA 不可得）強制 `signal_t = 0`。

為避免前視偏誤，本研究採用持倉位移：
- `position_t = signal_{t-1}`
即 t 日的持倉由 t-1 日收盤後的訊號決定。

策略日報酬：
- `ret_t = close_t / close_{t-1} - 1`
- `cost_t = |signal_{t-1} - signal_{t-2}| * fee_bps / 10000`
- `strategy_ret_t = position_t * ret_t - cost_t`

基準 buy-and-hold（BH）：
- `bh_ret_t = ret_t`

### 4.2 績效指標
年度交易日數採 252，無風險利率 rf=0：
- Total return：`equity_T - 1`
- CAGR：以交易日序列換算年化成長率
- Max drawdown（MDD）：`min_t(equity_t / max_{s<=t}(equity_s) - 1)`
- 年化波動：`std(strategy_ret) * sqrt(252)`
- Sharpe：`mean(strategy_ret) / std(strategy_ret) * sqrt(252)`（std=0 時為 NaN）

### 4.3 時間切分與 OOS 設計（寫死）
- Train：`2010-01-01` ~ `2018-12-31`
- Test：`2019-01-01` ~ `2025-12-31`
研究中的所有主要結論皆以 Test 期結果為準。

### 4.4 產業別參數最佳化（核心設計）
參數格點（短長均線視窗）固定為：
- `short ∈ {5, 10, 20, 30, 40}`
- `long  ∈ {20, 60, 120, 200}` 且 `long > short`

對每個產業 j 與每一組參數 (s,l)：
1. 對產業 j 內每檔股票 i 計算 Train 期 `Sharpe_{i,j}(s,l)`。
2. 定義產業分數為該產業股票 Sharpe 的中位數：
   - `Score_j(s,l) = median_i Sharpe_{i,j}(s,l)`
3. 取 `Score_j(s,l)` 最大者作為該產業之最佳參數 `(s*_j, l*_j)`。
4. 同分 tie-break（依序）：
   1) Train 期「策略總報酬 > BH 總報酬」之股票比例較高者
   2) `long` 較大者（偏保守、降低換手與過擬合風險）

接著在 Test 期對產業 j 的所有股票統一使用 `(s*_j, l*_j)` 進行 OOS 評估。

### 4.5 適用性定義（主結果）
對 Test 期每檔股票 i，定義：
- `Outperform_i = 1` 若 `TotalReturn_strategy_i > TotalReturn_bh_i`
- `Outperform_i = 0` 其餘

產業適用性（主指標）：
- `Applicability_j = mean_i(Outperform_i)`（產業 j 打敗比例）

次要指標（輔助呈現）：
- `median_i(ExcessReturn_i)`，其中 `ExcessReturn_i = TotalReturn_strategy_i - TotalReturn_bh_i`
- `median_i(Sharpe_i)`（Test 期）
- `median_i(MDD_i)`（Test 期）

## 5. 統計檢定與多重比較
### 5.1 Bootstrap 信賴區間（股票為抽樣單位）
對每個產業 j：
- 以股票為單位、放回抽樣（bootstrap）B=5,000 次
- 每次重抽 N_j 檔股票，計算 `Applicability_j^(b)`
- 以分位數法取得 95% CI：`[2.5%, 97.5%]`

### 5.2 假說檢定（寫死採 bootstrap）
對每個產業 j 檢定：
- H1：`Applicability_j > 0.5`
以 bootstrap 分布計算 one-sided p-value（詳見 Appendix）。

產業間差異可作為補充分析：
- 兩兩產業差之 bootstrap 分布，或以 logistic regression（Outperform 為因變數、產業 dummy 為自變數）作為延伸。

### 5.3 多重比較校正
對所有產業之 p-value 以 Benjamini–Hochberg 法進行 FDR 控制（q=0.05）。主表中同時呈現未校正與校正後顯著性標記。

## 6. 實證結果（模板）
本節以表圖呈現主要發現。所有表圖規格與輸出欄位請參考 `FIGURE_TABLE_SPECS.md`。

### Table 1. 樣本描述（模板）
（此處放 Markdown 表或嵌入 CSV 連結）

### Table 2. 產業最佳化參數與訓練期分數（模板）

### Table 3. 產業 OOS 適用性比例（主表，模板）

### Figure 1. 產業適用性比例（含 95% CI，模板）

### Figure 2. 最佳化參數分布（short/long，模板）

## 7. 穩健性檢查（寫死至少三組）
1. 成本敏感度：`fee_bps ∈ {0, 10, 20}`
2. 價格資料替代：以 `price_daily` 重跑並比較主結果
3. 子樣本切分：
   - `2010-01-01` ~ `2014-12-31`
   - `2015-01-01` ~ `2019-12-31`
   - `2020-01-01` ~ `2025-12-31`

## 8. 討論
建議從下列角度討論產業差異可能機制：
- 產業景氣循環敏感度與趨勢延續性
- 波動群聚與假訊號
- 資訊擴散速度與追趨行為

## 9. 限制
必須明確揭露：
- 產業分類可能隨時間調整，若無歷史快照將引入偏誤
- 未納入退市股票可能造成存活者偏誤（survivorship bias）
- 成本模型簡化（固定 bps）與未納入稅費細節
- 策略限制為做多/空手，未考慮放空與融券限制

## 10. 結論
本研究將以 OOS 產業適用性比例作為主結論：指出哪些產業更可能在樣本外打敗 buy-and-hold，以及產業最佳化參數是否呈現一致結構；並討論在成本與子樣本切分下的穩健性。

## 可重現性（最低限度）
本研究的單檔股票回測定義以 `strategies/ma-cross/backtest.py` 為準。任何讀者可使用下列指令重現單一股票於指定期間的策略輸出（需具備對應 SQLite 與資料）：

```bash
python strategies/ma-cross/backtest.py \
  --stock-id 2330 \
  --start-date 2019-01-01 \
  --end-date 2025-12-31 \
  --table price_adj_daily \
  --fee-bps 10
```

若需自動補抓資料（需 FinMind token）：

```bash
python strategies/ma-cross/backtest.py \
  --stock-id 2330 \
  --start-date 2019-01-01 \
  --end-date 2025-12-31 \
  --table price_adj_daily \
  --ensure-data
```

## 參考文獻（待補）
- （在此列出引用格式與條目，投稿時補齊）

