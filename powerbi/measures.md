# DAX Measures Reference

> All custom DAX measures used in the Power BI report. Measures are computed client-side by Power BI on top of the DirectQuery data from Hive.

## Market Overview Measures

| Measure | DAX Expression | Used In |
|---|---|---|
| **Tracked Stock Count** | `DISTINCTCOUNT(v_daily_summary[symbol])` | KPI Card |
| **Total Volume** | `SUM(v_daily_summary[volume])` | KPI Card |
| **Avg Daily Change** | `AVERAGE(v_daily_summary[daily_change_pct])` | KPI Card |
| **Overbought Count** | `COUNTROWS(FILTER(v_rsi_signals, v_rsi_signals[rsi_signal] = "overbought"))` | KPI Card |
| **Oversold Count** | `COUNTROWS(FILTER(v_rsi_signals, v_rsi_signals[rsi_signal] = "oversold"))` | KPI Card |
| **Neutral Count** | `COUNTROWS(FILTER(v_rsi_signals, v_rsi_signals[rsi_signal] = "neutral"))` | KPI Card |

## Symbol Detail Measures

| Measure | DAX Expression | Used In |
|---|---|---|
| **Latest Close** | `CALCULATE(LASTNONBLANK(v_daily_summary[close], 1))` | KPI Card |
| **Latest Change %** | `CALCULATE(LASTNONBLANK(v_daily_summary[daily_change_pct], 1))` | KPI Card |
| **52W High** | `MAX(v_daily_summary[high])` | KPI Card |
| **52W Low** | `MIN(v_daily_summary[low])` | KPI Card |
| **Latest RSI** | `CALCULATE(LASTNONBLANK(v_daily_summary[rsi_14], 1))` | KPI Card |
| **RSI Signal** | `IF([Latest RSI] >= 70, "Overbought", IF([Latest RSI] <= 30, "Oversold", "Neutral"))` | KPI Card |
| **BB Position %** | `DIVIDE([Latest Close] - LASTNONBLANK(v_daily_summary[bb_lower], 1), LASTNONBLANK(v_daily_summary[bb_upper], 1) - LASTNONBLANK(v_daily_summary[bb_lower], 1)) * 100` | KPI Card |

## Live Ticks Measures

| Measure | DAX Expression | Used In |
|---|---|---|
| **Avg Change %** | `AVERAGE(v_latest_ticks[change_pct])` | KPI Card |
| **Total Price** | `SUM(v_latest_ticks[price])` | KPI Card |
| **Total Volume** | `SUM(v_latest_ticks[volume])` | KPI Card |
| **Market Status** | `IF(COUNTROWS(FILTER(v_latest_ticks, v_latest_ticks[market_status] = "LIVE")) > 0, "LIVE", "CLOSED")` | KPI Card (conditional formatting: green for LIVE, red for CLOSED) |

## Signals & Risk Measures

| Measure | DAX Expression | Used In |
|---|---|---|
| **Market Risk Score** | `SUMX(v_rsi_signals, ABS(v_rsi_signals[rsi_14] - 50))` | Gauge visual |

## Conditional Formatting Rules

| Visual | Rule | Format |
|---|---|---|
| Market Status card | `market_status = "LIVE"` | Background: green (#2E7D32), font: white |
| Market Status card | `market_status = "CLOSED"` | Background: red (#C62828), font: white |
| RSI Signal text | `rsi_signal = "overbought"` | Font colour: red |
| RSI Signal text | `rsi_signal = "oversold"` | Font colour: blue |
| RSI Signal text | `rsi_signal = "neutral"` | Font colour: default |
| Daily Change % bars | Value < 0 | Bar colour: red |
| Daily Change % bars | Value ≥ 0 | Bar colour: green |
