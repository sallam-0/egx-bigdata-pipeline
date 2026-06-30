# Market Overview Page

> Provides a high-level snapshot of all 10 tracked EGX stocks for a selected trading day.

![Market Overview](../docs/images/market_overview.png)

## Data Source

| Hive View | Purpose |
|---|---|
| `v_daily_summary` | OHLCV prices, daily change %, and technical indicators |
| `v_top_movers` | Latest-day movers with percentage change |
| `v_rsi_signals` | RSI classification (overbought / oversold / neutral) |

## Filters

| Filter | Type | Behaviour |
|---|---|---|
| **Date Slicer** | Dropdown (top-right) | Filters the entire page to a single trading day. Defaults to the latest available date. |

## KPI Cards (Top Row)

Six summary cards span the top of the page, providing at-a-glance market metrics:

| Card | Measure / Field | Description |
|---|---|---|
| **Tracked Stock** | `DISTINCTCOUNT(v_daily_summary[symbol])` | Number of unique symbols with data on the selected day |
| **Total Volume** | `SUM(v_daily_summary[volume])` | Aggregate shares traded across all stocks |
| **Avg Change** | `AVERAGE(v_daily_summary[daily_change_pct])` | Mean daily percentage change across all stocks |
| **Overbought Count** | `COUNTROWS(FILTER(v_rsi_signals, v_rsi_signals[rsi_signal] = "overbought"))` | Stocks with RSI ≥ 70 |
| **Oversold Count** | `COUNTROWS(FILTER(v_rsi_signals, v_rsi_signals[rsi_signal] = "oversold"))` | Stocks with RSI ≤ 30 |
| **Neutral Count** | `COUNTROWS(FILTER(v_rsi_signals, v_rsi_signals[rsi_signal] = "neutral"))` | Stocks with 30 < RSI < 70 |

## Visuals

### Volume by Symbol (Bar Chart)
- **Type**: Clustered bar chart
- **Axis**: `symbol`
- **Value**: `SUM(volume)`
- **Sort**: Descending by volume
- **Purpose**: Quickly identify the most actively traded stocks. High volume often signals institutional interest or news-driven activity.

### RSI Signal Distribution (Pie Chart)
- **Type**: Pie chart
- **Legend**: `rsi_signal` (neutral / oversold / overbought)
- **Value**: Count of symbols per signal category
- **Purpose**: Shows the overall market sentiment balance. A market dominated by "overbought" signals suggests potential correction risk; heavy "oversold" presence may indicate buying opportunities.

### Daily Change % (Bar Chart)
- **Type**: Clustered bar chart
- **Axis**: `symbol`
- **Value**: `daily_change_pct`
- **Sort**: Ascending by change %
- **Conditional Formatting**: Positive bars are distinguished from negative bars
- **Purpose**: Visual ranking of gainers and losers for the day.

### Market Summary (Table)
- **Type**: Table visual
- **Columns**: `symbol`, `open`, `close`, `daily_change_pct`, `volume`
- **Sort**: Descending by `daily_change_pct`
- **Data bars**: Applied to `volume` column
- **Purpose**: Detailed tabular view of all stocks with key daily metrics. Serves as a reference grid complementing the chart visuals.
