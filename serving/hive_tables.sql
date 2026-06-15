-- Run this once after your first ETL job to register the Hive external tables.
-- All tables are EXTERNAL — Hive reads data from HDFS, never copies it.

CREATE DATABASE IF NOT EXISTS egx_db;
USE egx_db;

-- ── Raw OHLCV ─────────────────────────────────────────────────────────────
CREATE EXTERNAL TABLE IF NOT EXISTS raw_ohlcv (
    `date`        DATE,
    symbol      STRING,
    open        DOUBLE,
    high        DOUBLE,
    low         DOUBLE,
    close       DOUBLE,
    volume      BIGINT
)
PARTITIONED BY (date_partition STRING)
STORED AS PARQUET
LOCATION 'hdfs://hadoop-namenode:8020/data/raw/egx/ohlcv';

-- Repair partitions (re-run after each ingestion)
MSCK REPAIR TABLE egx_db.raw_ohlcv;


-- ── Curated OHLCV with indicators ─────────────────────────────────────────
CREATE EXTERNAL TABLE IF NOT EXISTS curated_ohlcv (
    `date`        DATE COMMENT 'date',
    open        DOUBLE COMMENT 'open',
    high        DOUBLE COMMENT 'high',
    low         DOUBLE COMMENT 'low',
    close       DOUBLE COMMENT 'close',
    volume      BIGINT COMMENT 'volume',
    sma_20      DOUBLE COMMENT 'sma_20',
    sma_50      DOUBLE COMMENT 'sma_50',
    ema_20      DOUBLE COMMENT 'ema_20',
    rsi_14      DOUBLE COMMENT 'rsi_14',
    macd_line   DOUBLE COMMENT 'macd_line',
    macd_signal DOUBLE COMMENT 'macd_signal',
    macd_hist   DOUBLE COMMENT 'macd_hist',
    bb_upper    DOUBLE COMMENT 'bb_upper',
    bb_mid      DOUBLE COMMENT 'bb_mid',
    bb_lower    DOUBLE COMMENT 'bb_lower',
    ingested_at STRING COMMENT 'ingested_at'
)
COMMENT 'curated_ohlcv table'
PARTITIONED BY (symbol STRING)
STORED AS PARQUET
LOCATION 'hdfs://hadoop-namenode:8020/data/curated/egx/ohlcv';

MSCK REPAIR TABLE egx_db.curated_ohlcv;
