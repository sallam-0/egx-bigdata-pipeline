# Live Ticks Page

> Real-time market monitoring — live market status, intraday price chart, and a streaming ticker table.

![Live Ticks](../docs/images/live_ticks.png)

## Data Source

| Hive View | Purpose |
|---|---|
| `v_latest_ticks` | Most recent tick per symbol (today's partition only) |
| `v_tick_history` | All intraday ticks for today — used for the price line chart |

## Filters

| Filter | Type | Behaviour |
|---|---|---|
| **Symbol Slicer** | Checkbox list (left panel) | Multi-select filter. Checking one or more symbols scopes all visuals to those stocks. Unchecking all shows the full market. |

## KPI Cards (Top Row)

Four cards provide real-time market metrics:

| Card | Field / Measure | Description |
|---|---|---|
| **Market Status** | `market_status` from `v_latest_ticks` | Displays **LIVE** (green) when the market is open or **CLOSED** (red) outside trading hours. Determined by UTC time window (07:00–12:30) + day-of-week + non-zero volume check. |
| **AVG Change %** | `AVERAGE(v_latest_ticks[change_pct])` | Average percentage change from previous close across selected symbols |
| **Price** | `SUM(v_latest_ticks[price])` | Aggregate price across selected symbols (or single stock price when filtered) |
| **Volume** | `SUM(v_latest_ticks[volume])` | Total traded volume across selected symbols |

## Visuals

### Price Tick Chart (Line Chart)
- **Type**: Line chart
- **X-Axis**: `tick_time` (timestamp)
- **Y-Axis**: `price`
- **Purpose**: Intraday price movement for the selected symbol(s). Each data point represents a 60-second tick from the real-time scraper. The chart updates when the Power BI page auto-refreshes (configurable interval). Useful for monitoring intraday trends and identifying price spikes during market hours.

### Live Ticker Data (Table)
- **Type**: Table visual
- **Columns**: `symbol`, `price`, `change_pct`, `volume`, `intraday_range_pct`
- **Sort**: Alphabetical by `symbol`
- **Totals Row**: Shows aggregated totals at the bottom
- **Purpose**: Tabular view of the latest tick for each stock. The `change_pct` column shows how much each stock has moved from its previous close. The `intraday_range_pct` column indicates the day's trading range as a percentage of the previous close — wider ranges suggest higher volatility.

## Auto-Refresh

This page is designed to be used with Power BI's **automatic page refresh** feature:

1. Select the page → **Format** → **Page refresh** → **Auto page refresh**
2. Set the interval (e.g., every 60 seconds to match the scraper cadence)
3. The page will re-query Hive on each refresh cycle

> **Note**: Auto page refresh requires DirectQuery mode and is not available in Import mode.
