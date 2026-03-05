# 分點籌碼量化研究與回測計畫（以 `8271.sqlite` 為例）

最後更新：2026-03-05（Asia/Taipei）

## 1) 目標

用你現有的資料結構（分點逐日成交明細 + 日股價/還原股價）做「可回測」的量化研究：

- 先用 `8271.sqlite` 跑通資料檢查、特徵工程與回測流程
- 以 **1–5 交易日**為主要持有期
- **Long-only**（單檔不重疊持倉）
- 同時做 **規則型 baseline** 與 **機器學習（walk-forward）** 對照
- 執行後輸出：Notebook（可互動）+ HTML 報告 + CSV/Parquet 結果檔

## 2) 你目前手上的資料（確認自 `DATA_INDEX_8271.md`）

`8271.sqlite` 目前包含：

- 分點明細分表：`broker_<securities_trader_id>_<hash>`（約 861 張）
- 分點對照：`broker_tables`
- 抓取歷程：`fetch_history`
- 日股價：`stock_price`
- 還原日股價：`stock_price_adj`

目前股價資料範圍（已在索引中統計）：`2026-01-02` ~ `2026-03-04`（36 筆）。

## 3) 研究主軸（這份資料可以做什麼）

**核心假說：分點資金流（淨買賣、集中度、持續性）對未來 1–5 日報酬具備解釋力/預測力。**

可落地的研究與策略方向：

1. **淨買賣放大（Flow Breakout）**
   - 以「當日淨買最大的前 K 個分點」的淨買佔比作訊號
   - 觀察是否對短期趨勢/突破有幫助
2. **買盤集中度（Concentration）**
   - 用 HHI（Herfindahl–Hirschman Index）衡量買進量集中度
   - 高集中度 + 淨買 + 價格突破的短期績效
3. **資料驅動（ML walk-forward）**
   - 以分點特徵 + 價格/量能特徵預測「扣成本後」的 1–5 日持有期報酬
   - 用 walk-forward 避免偷看（lookahead）

## 4) 回測假設（已選定）

### 4.1 交易成本（保守估計）

- 買入：手續費 0.1425% + 滑價 0.1% ⇒ `buy_cost = 0.002425`
- 賣出：手續費 0.1425% + 滑價 0.1% + 交易稅 0.3% ⇒ `sell_cost = 0.005425`

報酬計算：

- `entry = open_(t+1) * (1 + buy_cost)`
- `exit  = close_(t+hold_days) * (1 - sell_cost)`
- `return = exit / entry - 1`

### 4.2 訊號與成交時點

- 在交易日 `t` 收盤後才能得到分點資訊並計算訊號
- 若進場：下一交易日 `t+1` 以開盤價進場
- 持有 `hold_days ∈ {1,2,3,4,5}`，於 `t+hold_days` 收盤價出場
- 單檔 **不重疊持倉**

## 5) 特徵工程規格

### 5.1 broker_daily（分點-日級）

從每個分點表 `broker_*` 聚合成分點日資料（每分點每天一列）：

- `total_buy = SUM(buy)`
- `total_sell = SUM(sell)`
- `avg_buy_price = SUM(price*buy)/SUM(buy)`（`SUM(buy)=0` 則 NaN）
- `avg_sell_price = SUM(price*sell)/SUM(sell)`（`SUM(sell)=0` 則 NaN）
- `net_volume = total_buy - total_sell`

排除 `securities_trader_id="__NO_DATA__"` 的佔位分點。

### 5.2 day_features（股票-日級）

合併 `broker_daily` 與 `stock_price/stock_price_adj` 後，產生策略與 ML 輸入特徵（只用當日與過去資料）：

- 價格/量能（以 `stock_price_adj` 計算報酬為主）：
  - `ret_1, ret_3, ret_5, ret_20`（adj close 報酬）
  - `vol_5, vol_20`（`ret_1` rolling std）
  - `hl_range = (high-low)/close`
  - `oc_return = (close-open)/open`
  - `log_vol = log(Trading_Volume)`
  - `vol_z20`（20 日 z-score）
- 分點流（以 `Trading_Volume` 正規化）：
  - `top20_net_share`：依 `net_volume` 取前 20 名分點淨買總和 / `Trading_Volume`
  - `top20_net_share_3d`：3 日滾動和
  - `active_brokers`：當日有交易的分點數
  - `buy_hhi` / `sell_hhi`
  - `net_std_share = STD(net_volume_i) / Trading_Volume`

同時做資料一致性檢查，當日若不通過則標記為不可用（不進場）：

- `abs(sum(net_volume_i))/Trading_Volume` 應接近 0
- `sum(total_buy_i)` 與 `sum(total_sell_i)` 應接近
- 價格欄位缺失（停牌/資料缺）則視為不可交易日

## 6) 策略定義（規則型 + ML）

### 6.1 規則型策略 R1：Flow Breakout

- 指標：`top20_net_share`
- 用過去 120 日做 rolling percentile rank：`rank_120(top20_net_share)`
- 進場（在日 `t` 產生訊號）：
  - `rank_120 >= 0.8`
  - `ret_5 > 0`（動能濾網）
  - `Trading_Volume > rolling_median_20`（流動性濾網）
- 出場：固定 `hold_days`

### 6.2 規則型策略 R2：Concentration Breakout

- 指標：`buy_hhi`、`top20_net_share`
- 進場：
  - `rank_120(buy_hhi) >= 0.8`
  - `top20_net_share > 0`
  - `close_raw > max(close_raw_{t-20..t-1})`（20 日突破；用 raw close）
- 出場：固定 `hold_days`

### 6.3 ML 策略：Ridge 回歸 + walk-forward

- 標籤：`y_t = net_return(open_(t+1) → close_(t+hold_days))`（已扣成本）
- 模型：`StandardScaler + Ridge`
- walk-forward 訓練：
  - 每個交易日 `t` 用過去 504 日（約 2 年）訓練（不足則跳過直到至少 252 日）
  - 僅用 `<= t-1` 訓練資料 fit，對 `t` 預測 `y_hat_t`
- 交易規則：
  - 若空倉且 `y_hat_t > 0` ⇒ `t+1 open` 進場，持有 `hold_days`

## 7) 執行步驟（本次要做的：補齊 8271 近 3 年 + 產出 HTML）

### 7.1 安裝/補齊 notebook 執行與匯出依賴

（目前環境顯示缺 `nbformat/nbclient/nbconvert`）

```powershell
python -m pip install nbformat nbclient nbconvert
```

### 7.2 補齊 8271 近 3 年資料

建議區間（回推 3 年至今天）：

- `START_DATE = 2023-03-05`
- `END_DATE = 2026-03-05`（若當日尚未有收盤資料，會自動以 `null` 欄位寫入或略過）

股價/還原股價可一次抓完整段；分點資料為避免記憶體爆量，**按月分段抓取**。

### 7.3 產生並執行 notebook，輸出 HTML 報告與結果檔

將新增 notebook：`notebooks/broker_flow_backtest.ipynb`，並以 headless 方式執行與匯出：

```powershell
python -m jupyter nbconvert --to html --execute notebooks/broker_flow_backtest.ipynb --output outputs/broker_flow_backtest_8271.html
```

## 8) 驗收（完成後你應該看到）

- `docs/RESEARCH_PLAN.md` 存在且內容完整
- `notebooks/broker_flow_backtest.ipynb` 可執行
- `8271.sqlite` 的 `stock_price/stock_price_adj` 日期範圍擴張到近 3 年（以你實際抓到的最後交易日為準）
- `outputs/broker_flow_backtest_8271.html` 產生（包含策略績效表與圖）
- 同時輸出 features / trades / equity 的 CSV/Parquet 供後續比較

