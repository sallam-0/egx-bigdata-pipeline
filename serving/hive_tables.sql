-- Run this once after your first ETL job to register the Hive external tables.
-- All tables are EXTERNAL — Hive reads data from HDFS, never copies it.

CREATE DATABASE IF NOT EXISTS egx_db;
USE egx_db;

-- ── Raw OHLCV ─────────────────────────────────────────────────────────────
CREATE EXTERNAL TABLE IF NOT EXISTS raw_ohlcv (
    date        DATE,
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
    date        DATE,
    open        DOUBLE,
    high        DOUBLE,
    low         DOUBLE,
    close       DOUBLE,
    volume      BIGINT,
    sma_20      DOUBLE,
    sma_50      DOUBLE,
    ema_20      DOUBLE,
    rsi_14      DOUBLE,
    macd_line   DOUBLE,
    macd_signal DOUBLE,
    macd_hist   DOUBLE,
    bb_upper    DOUBLE,
    bb_mid      DOUBLE,
    bb_lower    DOUBLE,
    ingested_at TIMESTAMP
)
PARTITIONED BY (symbol STRING)
STORED AS PARQUET
LOCATION 'hdfs://hadoop-namenode:8020/data/curated/egx/ohlcv';

MSCK REPAIR TABLE egx_db.curated_ohlcv;
