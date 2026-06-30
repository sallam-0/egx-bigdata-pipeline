# Power BI Dashboard Documentation

This directory contains documentation for the EGX Big Data Pipeline Power BI report. The report connects to Apache Hive via DirectQuery (Simba ODBC) and visualises both historical OHLCV data and real-time tick prices for 10 tracked Egyptian Exchange stocks.

## Dashboard Pages

The report is organised into four navigable pages:

| Page | Purpose | Documentation |
|---|---|---|
| **Market Overview** | Broad market snapshot — KPIs, volume, daily movers, RSI distribution | [market_overview.md](market_overview.md) |
| **Symbol Detail** | Deep-dive into a single stock — price, Bollinger, SMA/EMA, MACD, RSI | [symbol_detail.md](symbol_detail.md) |
| **Signals & Risk** | Cross-market RSI screening, Bollinger width ranking, risk scatter | [signals_and_risk.md](signals_and_risk.md) |
| **Live Ticks** | Real-time tick feed — market status, intraday line chart, live table | [live_ticks.md](live_ticks.md) |

## Data Sources

All visuals query Hive views via DirectQuery. No data is imported or cached — every interaction runs a live SQL query.

| Hive View | Used By | Description |
|---|---|---|
| `v_daily_summary` | Market Overview, Symbol Detail | Daily OHLCV + indicators + change % |
| `v_top_movers` | Market Overview | Latest day movers with absolute change |
| `v_rsi_signals` | Market Overview, Signals & Risk | RSI values with overbought/oversold/neutral classification |
| `v_bollinger_squeeze` | Signals & Risk | Bollinger Band width per symbol per day |
| `v_weekly_rollup` | Symbol Detail | Weekly aggregated OHLCV |
| `v_latest_ticks` | Live Ticks | Most recent tick per symbol (today only) |
| `v_tick_history` | Live Ticks | All intraday ticks for today (line chart) |

## Measures

Power BI DAX measures are used for KPI cards and conditional formatting. See the [measures.md](measures.md) file for the full catalogue.

## Connection Setup

For ODBC connection instructions, see [../serving/powerbi_odbc.md](../serving/powerbi_odbc.md).
