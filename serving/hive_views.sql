USE egx_db;

-- ── Daily summary ─────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW v_daily_summary AS
SELECT
    symbol,
    `date`,
    open,
    high,
    low,
    close,
    volume,
    ROUND(((close - open) / open) * 100, 2) AS daily_change_pct,
    sma_20,
    sma_50,
    rsi_14,
    macd_line,
    bb_upper,
    bb_lower
FROM curated_ohlcv;


-- ── Weekly rollup ─────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW v_weekly_rollup AS
WITH ranked_data AS (
    SELECT
        symbol,
        TRUNC(`date`, 'WW') AS week_start,
        low,
        high,
        volume,
        FIRST_VALUE(open) OVER (PARTITION BY symbol, TRUNC(`date`, 'WW') ORDER BY `date`) AS week_open,
        LAST_VALUE(close) OVER (PARTITION BY symbol, TRUNC(`date`, 'WW') ORDER BY `date` ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS week_close
    FROM curated_ohlcv
)
SELECT
    symbol,
    week_start,
    MIN(low) AS week_low,
    MAX(high) AS week_high,
    SUM(volume) AS week_volume,
    MAX(week_open) AS week_open,
    MAX(week_close) AS week_close
FROM ranked_data
GROUP BY symbol, week_start;


-- ── Top movers (last available trading day) ───────────────────────────────
CREATE OR REPLACE VIEW v_top_movers AS
WITH max_date_cte AS (
    SELECT MAX(`date`) as max_d FROM curated_ohlcv
)
SELECT
    c.symbol,
    c.`date`,
    c.open,
    c.close,
    ROUND(((c.close - c.open) / c.open) * 100, 2) AS change_pct
FROM curated_ohlcv c
JOIN max_date_cte m ON c.`date` = m.max_d
ORDER BY ABS(change_pct) DESC
LIMIT 10;


-- ── RSI signals ───────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW v_rsi_signals AS
SELECT
    symbol,
    `date`,
    close,
    rsi_14,
    CASE
        WHEN rsi_14 >= 70 THEN 'overbought'
        WHEN rsi_14 <= 30 THEN 'oversold'
        ELSE 'neutral'
    END AS rsi_signal
FROM curated_ohlcv
WHERE rsi_14 IS NOT NULL;


-- ── Bollinger squeeze alert ───────────────────────────────────────────────
CREATE OR REPLACE VIEW v_bollinger_squeeze AS
SELECT
    symbol,
    `date`,
    close,
    bb_upper,
    bb_lower,
    ROUND(bb_upper - bb_lower, 4) AS band_width
FROM curated_ohlcv
WHERE bb_upper IS NOT NULL
ORDER BY band_width ASC;
