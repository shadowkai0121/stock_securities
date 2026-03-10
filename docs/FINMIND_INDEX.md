# FinMind Dataset ID/Name Index

本文件整理 `https://finmind.github.io/` 文件中「資料集」相關內容，聚焦在:

- 哪裡取得可用的 `id <-> name` 對照表
- 哪些 dataset / endpoint 會用到這些 `id`（通常以 `data_id` 傳入）

產出時間: 2026-03-06

## 共通規則

- API Base: `https://api.finmindtrade.com/api/v4`
- 通用資料端點: `GET /data`
  - 常見 query: `dataset`, `data_id`(依資料集而定), `start_date`, `end_date`, `date`
  - 認證: `Authorization: Bearer <token>`
- 少數資料集不走 `/data`，改用專用 endpoint（見下方「特殊端點」）
- 部分資料集在文件中標註 `backer/sponsor` 限制，且有些資料集 `data_id` 可省略以一次抓「某日全部標的」；請以各資料集頁面說明為準

## 特殊端點（非 `/data`）

- `GET /taiwan_stock_tick_snapshot`
  - 台股即時資訊
  - `data_id` 可為單筆、陣列或空(全部)
- `GET /taiwan_futures_snapshot`
  - 台股期貨即時資訊
  - `data_id` 可為單筆、陣列或空(全部)
- `GET /taiwan_options_snapshot`
  - 台股選擇權即時資訊
  - `data_id` 可為單筆、陣列或空(全部)
- `GET /taiwan_stock_trading_daily_report`
  - 台股分點資料表(單日)
  - query 使用其一: `data_id`(股票代碼) 或 `securities_trader_id`(券商/分點代碼)，並搭配 `date`
- `GET /taiwan_stock_warrant_trading_daily_report`
  - 台股權證分點資料表(單日)
  - query 使用其一: `data_id`(權證代碼) 或 `securities_trader_id`(券商/分點代碼)，並搭配 `date`
- `GET /taiwan_stock_trading_daily_report_secid_agg`
  - 當日卷商分點統計表
  - query: `data_id`(股票代碼) + `securities_trader_id` + `start_date/end_date`

## 台灣市場: `stock_id`（台股/ETF/權證代碼）

文件: `tutor/TaiwanMarket/Technical`, `Chip`, `Fundamental`, `Others`, `RealTime`

取得 `stock_id <-> stock_name` 對照:

- `/data` `dataset=TaiwanStockInfo`
- `/data` `dataset=TaiwanStockInfoWithWarrant`（含權證）

權證標的對照（權證 `stock_id` <-> 標的 `stock_id`）:

- `/data` `dataset=TaiwanStockInfoWithWarrantSummary`

即時（以 `stock_id` 查）:

- `GET /taiwan_stock_tick_snapshot`

常見以 `stock_id` 作為 `data_id` 的 datasets（`GET /data`）:

- `TaiwanStock10Year`
- `TaiwanStockDayTrading`
- `TaiwanStockKBar`
- `TaiwanStockMonthPrice`
- `TaiwanStockPER`
- `TaiwanStockPrice`
- `TaiwanStockPriceAdj`
- `TaiwanStockPriceTick`
- `TaiwanStockWeekPrice`
- `TaiwanDailyShortSaleBalances`
- `TaiwanStockHoldingSharesPer`
- `TaiwanStockInstitutionalInvestorsBuySell`
- `TaiwanStockMarginPurchaseShortSale`
- `TaiwanStockMarginShortSaleSuspension`
- `TaiwanStockSecuritiesLending`
- `TaiwanStockShareholding`
- `TaiwanStockBalanceSheet`
- `TaiwanStockCapitalReductionReferencePrice`
- `TaiwanStockCashFlowsStatement`
- `TaiwanStockDividend`
- `TaiwanStockDividendResult`
- `TaiwanStockFinancialStatements`
- `TaiwanStockMarketValue`
- `TaiwanStockMarketValueWeight`
- `TaiwanStockMonthRevenue`
- `TaiwanStockParValueChange`
- `TaiwanStockSplitPrice`
- `TaiwanStockNews`

不需 `data_id`、但輸出包含 `stock_id`（適合做關聯 / join）的 datasets（`GET /data`）:

- `TaiwanStockDelisting`
- `TaiwanStockDispositionSecuritiesPeriod`
- `TaiwanStockIndustryChain`
- `TaiwanStockDayTradingSuspension`
- `TaiwanStockSuspended`
- `TaiwanStockStatisticsOfOrderBookAndTrade`

## 台灣市場: `index_id`（大盤/報酬指數代碼）

文件: `tutor/TaiwanMarket/Technical`

以指數代碼作為 `data_id` 的 datasets（`GET /data`）:

- `TaiwanStockTotalReturnIndex`（文件示例: `TAIEX`, `TPEx`）

不使用 `data_id` 的指數/統計 datasets（`GET /data`）:

- `TaiwanStockEvery5SecondsIndex`
- `TaiwanVariousIndicators5Seconds`

## 台灣市場: `securities_trader_id`（券商/分點代碼）

文件: `tutor/TaiwanMarket/Chip`

取得 `securities_trader_id <-> 名稱` 對照:

- `/data` `dataset=TaiwanSecuritiesTraderInfo`

以 `securities_trader_id` 查分點資料:

- `GET /taiwan_stock_trading_daily_report`（`securities_trader_id` + `date`）
- `GET /taiwan_stock_warrant_trading_daily_report`（`securities_trader_id` + `date`）
- `GET /taiwan_stock_trading_daily_report_secid_agg`（`data_id`(stock_id) + `securities_trader_id` + `start_date/end_date`）

## 台灣市場: `futures_id` / `option_id`（期貨/選擇權代碼）

文件: `tutor/TaiwanMarket/Derivative`, `RealTime`

取得期貨/選擇權代碼清單（`code <-> name`）:

- `/data` `dataset=TaiwanFutOptDailyInfo`（日成交資訊總覽）
- `/data` `dataset=TaiwanFutOptTickInfo`（即時報價總覽）

使用代碼作為 `data_id` 的 datasets（`GET /data`）:

- `TaiwanFuturesDaily`
- `TaiwanFuturesTick`
- `TaiwanFuturesInstitutionalInvestors`
- `TaiwanFuturesInstitutionalInvestorsAfterHours`
- `TaiwanFuturesDealerTradingVolumeDaily`
- `TaiwanFuturesOpenInterestLargeTraders`
- `TaiwanFuturesSpreadTrading`
- `TaiwanFuturesFinalSettlementPrice`
- `TaiwanOptionDaily`
- `TaiwanOptionTick`
- `TaiwanOptionInstitutionalInvestors`
- `TaiwanOptionInstitutionalInvestorsAfterHours`
- `TaiwanOptionDealerTradingVolumeDaily`
- `TaiwanOptionOpenInterestLargeTraders`
- `TaiwanOptionFinalSettlementPrice`

即時（專用 endpoint）:

- `GET /taiwan_futures_snapshot`（文件註記: `TXF`, `TMF`, `CDF`）
- `GET /taiwan_options_snapshot`（文件註記: `TXO`, `TX1`, `TX2`, `TX3`, `TX4`, `TX5`）

## 台灣市場: `cb_id`（可轉債代碼）

文件: `tutor/TaiwanMarket/ConvertibleBond`

取得 `cb_id <-> cb_name` 對照:

- `/data` `dataset=TaiwanStockConvertibleBondInfo`

使用 `cb_id` 作為 `data_id` 的 datasets（`GET /data`）:

- `TaiwanStockConvertibleBondDaily`
- `TaiwanStockConvertibleBondInstitutionalInvestors`
- `TaiwanStockConvertibleBondDailyOverview`

## 海外市場: `stock_id`（ticker/symbol）

文件: `tutor/UnitedStatesMarket/Technical`, `UnitedKingdomMarket/Technical`, `EuropeMarket/Technical`, `JapanMarket/Technical`

取得 `stock_id <-> stock_name` 對照:

- `/data` `dataset=USStockInfo`
- `/data` `dataset=UKStockInfo`
- `/data` `dataset=EuropeStockInfo`
- `/data` `dataset=JapanStockInfo`

使用 `stock_id` 作為 `data_id` 的 datasets（`GET /data`）:

- `USStockPrice`
- `USStockPriceMinute`
- `UKStockPrice`
- `EuropeStockPrice`
- `JapanStockPrice`

## 匯率: 幣別代碼（`currency`）

文件: `tutor/ExchangeRate`

- `/data` `dataset=TaiwanExchangeRate`
  - `data_id=<幣別代碼>`

支援的 `data_id`:

| data_id | 幣別 |
|---|---|
| `AUD` | 澳洲 |
| `CAD` | 加拿大 |
| `CHF` | 瑞士法郎 |
| `CNY` | 人民幣 |
| `EUR` | 歐元 |
| `GBP` | 英鎊 |
| `HKD` | 港幣 |
| `IDR` | 印尼幣 |
| `JPY` | 日圓 |
| `KRW` | 韓元 |
| `MYR` | 馬來幣 |
| `NZD` | 紐元 |
| `PHP` | 菲國比索 |
| `SEK` | 瑞典幣 |
| `SGD` | 新加坡幣 |
| `THB` | 泰幣 |
| `USD` | 美金 |
| `VND` | 越南盾 |
| `ZAR` | 南非幣 |

## 利率: 國家/央行代碼

文件: `tutor/InterestRate`

- `/data` `dataset=InterestRate`
  - `data_id=<國家/央行代碼>`

支援的 `data_id`:

| data_id | 國家/央行 |
|---|---|
| `BOE` | 英格蘭銀行 |
| `RBA` | 澳洲儲備銀行 |
| `FED` | 聯邦準備銀行 |
| `PBOC` | 中國人民銀行 |
| `BOC` | 中國銀行 |
| `ECB` | 歐洲中央銀行 |
| `RBNZ` | 紐西蘭儲備銀行 |
| `RBI` | 印度儲備銀行 |
| `CBR` | 俄羅斯中央銀行 |
| `BCB` | 馬來西亞商業銀行 |
| `BOJ` | 日本銀行 |
| `SNB` | 瑞士國家銀行 |

## 原物料: 品項代碼

文件: `tutor/Materials`

- `/data` `dataset=GoldPrice`（不使用 `data_id`）
- `/data` `dataset=CrudeOilPrices`
  - `data_id=<品項代碼>`

`CrudeOilPrices` 支援的 `data_id`:

| data_id | 名稱 |
|---|---|
| `Brent` | 布蘭特 |
| `WTI` | 西德州 |

## 美國國債殖利率: 債券代碼

文件: `tutor/GovernmentBondsYield`

- `/data` `dataset=GovernmentBondsYield`
  - `data_id=<債券代碼>`

支援的 `data_id`:

| data_id | 債券 |
|---|---|
| `United States 1-Month` | 1月期 |
| `United States 2-Month` | 2月期 |
| `United States 3-Month` | 3月期 |
| `United States 6-Month` | 6月期 |
| `United States 1-Year` | 1年期 |
| `United States 2-Year` | 2年期 |
| `United States 3-Year` | 3年期 |
| `United States 5-Year` | 5年期 |
| `United States 7-Year` | 7年期 |
| `United States 10-Year` | 10年期 |
| `United States 20-Year` | 20年期 |
| `United States 30-Year` | 30年期 |

## 其他（global）

文件: `tutor/Others`

- `/data` `dataset=CnnFearGreedIndex`（不使用 `data_id`）

## 文件命名差異備註

- `tutor/TaiwanMarket/Fundamental` 的 `TaiwanStockDividend` 段落中，R 範例曾出現 `dataset="TaiwanStockStockDividend"`；但總覽與 Python 範例使用 `dataset="TaiwanStockDividend"`。
- `tutor/TaiwanMarket/DataList` 列表中曾出現 `TaiwanOptionTIck`（大小寫）等差異；若遇到 dataset 名稱不通，建議以各資料集頁面中的 `requests.get(... /api/v4/data ...)` 範例為準（例如 `TaiwanOptionTick`）。
