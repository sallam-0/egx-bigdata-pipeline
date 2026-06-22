USE egx_db;

-- ── Daily summary ─────────────────────────────────────────────────────────
-- NOTE: `date` is cast to STRING to avoid the Simba/Hardy ODBC driver NPE
--       that occurs when introspecting Hive 2.x native DATE columns.
--       Power BI will still auto-detect and parse it as a date.
CREATE OR REPLACE VIEW v_daily_summary AS
SELECT
    symbol,
    CAST(`date` AS STRING) AS trade_date,
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
-- Rewrote to use ROW_NUMBER + MAX instead of FIRST_VALUE/LAST_VALUE with
-- ROWS BETWEEN UNBOUNDED — that form forces a full shuffle and OOMs in Docker.
CREATE OR REPLACE VIEW v_weekly_rollup AS
WITH ranked AS (
    SELECT
        symbol,
        TRUNC(`date`, 'WW')                                                       AS week_dt,
        CAST(TRUNC(`date`, 'WW') AS STRING)                                       AS week_start,
        open,
        close,
        low,
        high,
        volume,
        ROW_NUMBER() OVER (PARTITION BY symbol, TRUNC(`date`, 'WW') ORDER BY `date` ASC)  AS rn_first,
        ROW_NUMBER() OVER (PARTITION BY symbol, TRUNC(`date`, 'WW') ORDER BY `date` DESC) AS rn_last
    FROM curated_ohlcv
),
base AS (
    SELECT
        symbol,
        week_start,
        low,
        high,
        volume,
        CASE WHEN rn_first = 1 THEN open  END AS week_open_val,
        CASE WHEN rn_last  = 1 THEN close END AS week_close_val
    FROM ranked
)
SELECT
    symbol,
    week_start,
    MIN(low)            AS week_low,
    MAX(high)           AS week_high,
    SUM(volume)         AS week_volume,
    MAX(week_open_val)  AS week_open,
    MAX(week_close_val) AS week_close
FROM base
GROUP BY symbol, week_start;


-- ── Top movers (last available trading day) ───────────────────────────────
-- Removed ORDER BY / LIMIT — those force MapReduce sort in Hive views.
-- Use Power BI Top-N visual filter to show top 5/10 movers instead.
CREATE OR REPLACE VIEW v_top_movers AS
WITH max_date_cte AS (
    SELECT MAX(`date`) AS max_d FROM curated_ohlcv
)
SELECT
    c.symbol,
    CAST(c.`date` AS STRING) AS trade_date,
    c.open,
    c.close,
    ROUND(((c.close - c.open) / c.open) * 100, 2) AS change_pct,
    ABS(ROUND(((c.close - c.open) / c.open) * 100, 2))  AS abs_change_pct
FROM curated_ohlcv c
JOIN max_date_cte m ON c.`date` = m.max_d;


-- ── RSI signals ───────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW v_rsi_signals AS
SELECT
    symbol,
    CAST(`date` AS STRING) AS trade_date,
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
-- Removed ORDER BY — forces MapReduce sort, OOMs in Docker containers.
-- Sort by band_width ASC in Power BI instead (visual-level sort).
CREATE OR REPLACE VIEW v_bollinger_squeeze AS
SELECT
    symbol,
    CAST(`date` AS STRING) AS trade_date,
    close,
    bb_upper,
    bb_lower,
    ROUND(bb_upper - bb_lower, 4) AS band_width
FROM curated_ohlcv
WHERE bb_upper IS NOT NULL;


-- ── Live: latest tick per symbol (Power BI auto-refresh page) ─────────────
-- Filters to today's partitions only for speed — no full table scan.
-- ROW_NUMBER picks the single most-recent tick per symbol.
-- All date/time columns are STRING to avoid the Hardy ODBC DATE/TIMESTAMP NPE.
CREATE OR REPLACE VIEW v_latest_ticks AS
WITH today_ranked AS (
    SELECT
        symbol,
        `timestamp`                                                      AS tick_time,
        price,
        COALESCE(volume, 0)                                              AS volume,
        -- open/high/low are NULL after market hours (yfinance fast_info limitation).
        -- COALESCE to price so downstream calcs don't break.
        COALESCE(open,  price)                                           AS open,
        COALESCE(high,  price)                                           AS high,
        COALESCE(low,   price)                                           AS low,
        prev_close,
        ROUND(((price - prev_close) / prev_close) * 100, 2)             AS change_pct,
        -- intraday_range_pct will be 0 when market is closed (high=low=price)
        ROUND(((COALESCE(high, price) - COALESCE(low, price)) / prev_close) * 100, 2) AS intraday_range_pct,
        -- Market status: EGX trades Sun–Thu 10:00–14:30 Cairo (UTC+3 in winter, UTC+2 in summer).
        -- We use a conservative UTC window of 07:00–12:30 to cover both offsets.
        -- Additionally require volume > 0 as a sanity check — zero-volume ticks
        -- are stale feed heartbeats sent by yfinance even when the market is closed.
        -- NOTE: open IS NULL alone is NOT reliable; yfinance returns the last known
        --       open price via fast_info even after market close.
        CASE
            WHEN (
                HOUR(FROM_UTC_TIMESTAMP(`timestamp`, 'UTC')) * 60 + MINUTE(FROM_UTC_TIMESTAMP(`timestamp`, 'UTC'))
                    BETWEEN 7 * 60 AND 12 * 60 + 30   -- 07:00–12:30 UTC ≈ 10:00–14:30 Cairo
                AND DAYOFWEEK(`timestamp`) BETWEEN 2 AND 6  -- Sun(2)–Thu(6) in Hive
                AND COALESCE(volume, 0) > 0
            ) THEN 'LIVE'
            ELSE 'CLOSED'
        END                                                              AS market_status,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY `timestamp` DESC) AS rn
    FROM raw_ticks
    WHERE date_partition = TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP()))
)
SELECT
    symbol,
    tick_time,
    price,
    volume,
    open,
    high,
    low,
    prev_close,
    change_pct,
    intraday_range_pct,
    market_status
FROM today_ranked
WHERE rn = 1;


-- ── Live: intraday tick history per symbol (sparkline / line chart) ────────
-- Returns ALL ticks for today, used for intraday price charts per symbol.
CREATE OR REPLACE VIEW v_tick_history AS
SELECT
    symbol,
    `timestamp`     AS tick_time,
    price,
    volume,
    ROUND(((price - prev_close) / prev_close) * 100, 2) AS change_pct
FROM raw_ticks
WHERE date_partition = TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP()))
ORDER BY symbol, tick_time;

