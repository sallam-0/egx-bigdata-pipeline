# Symbol Detail Page

> Deep-dive analysis of a single stock — technical indicators, price overlays, and volume trends over time.

![Symbol Detail](../docs/images/symbol_detail.png)

## Data Source

| Hive View | Purpose |
|---|---|
| `v_daily_summary` | Daily OHLCV, SMA-20/50, EMA-20, RSI-14, MACD, Bollinger Bands |

## Filters

| Filter | Type | Behaviour |
|---|---|---|
| **Symbol Slicer** | Dropdown (top-right) | Selects a single EGX ticker. All visuals on this page are scoped to the chosen symbol. |

## KPI Cards (Top Row)

Seven cards provide a snapshot of the selected stock's latest trading session and indicator values:

| Card | Field / Measure | Description |
|---|---|---|
| **Latest Close** | `close` (latest date) | Most recent closing price |
| **Latest Change %** | `daily_change_pct` (latest date) | Percentage change from open to close on the latest day |
| **High 52W** | `MAX(v_daily_summary[high])` | 52-week (or available history) high price |
| **52W Low** | `MIN(v_daily_summary[low])` | 52-week (or available history) low price |
| **Latest RSI** | `rsi_14` (latest date) | Current RSI-14 value (0–100 scale) |
| **RSI Signal** | Conditional text | Displays "Neutral", "Overbought", or "Oversold" based on RSI thresholds |
| **BB Position %** | Bollinger position measure | Where the current price sits within the Bollinger Band envelope (0% = at lower band, 100% = at upper band) |

## Visuals

### Bollinger Bands (Area Chart)
- **Type**: Area chart
- **X-Axis**: `trade_date`
- **Y-Axis**: `bb_upper`, `close`, `bb_lower`
- **Purpose**: Visualises price volatility. When bands contract (squeeze), a breakout is likely. Price touching the upper band suggests overbought conditions; touching the lower band suggests oversold.

### Price and Moving Averages (Line Chart)
- **Type**: Multi-line chart
- **X-Axis**: `trade_date`
- **Lines**: `close`, `sma_20`, `sma_50`
- **Purpose**: Trend identification. When the short-term SMA-20 crosses above the long-term SMA-50 (golden cross), it signals bullish momentum. The reverse (death cross) signals bearish momentum.

### Volume History (Column Chart)
- **Type**: Clustered column chart
- **X-Axis**: `trade_date`
- **Y-Axis**: `SUM(volume)`
- **Purpose**: Volume confirms price trends. Rising price with rising volume validates the move; rising price with declining volume suggests weakness.

### MACD Line (Line Chart)
- **Type**: Line chart
- **X-Axis**: `trade_date`
- **Y-Axis**: `macd_line`
- **Purpose**: Momentum oscillator. When the MACD line crosses above zero, it signals upward momentum. Divergence between MACD and price can indicate trend reversals.

### RSI (14) (Line Chart)
- **Type**: Line chart
- **X-Axis**: `trade_date`
- **Y-Axis**: `rsi_14`
- **Reference Lines**: 70 (overbought threshold), 30 (oversold threshold)
- **Purpose**: Momentum oscillator ranging 0–100. Values above 70 indicate overbought conditions (potential sell signal); below 30 indicates oversold (potential buy signal).
