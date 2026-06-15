# EGX Big Data Pipeline 🇪🇬📈

A production-grade, end-to-end big data pipeline for the **Egyptian Exchange (EGX)** stock market. The pipeline ingests daily OHLCV data and real-time tick prices, processes them with Apache Spark, computes technical indicators, and serves the enriched data to Power BI dashboards via a Hive-on-HDFS data lake.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Technology Stack](#technology-stack)
3. [Repository Structure](#repository-structure)
4. [Module Deep-Dives](#module-deep-dives)
   - [Ingestion](#1-ingestion)
   - [Storage (HDFS)](#2-storage-hdfs)
   - [Processing (Spark)](#3-processing-spark)
   - [Orchestration (Airflow)](#4-orchestration-airflow)
   - [Serving (Hive + Power BI)](#5-serving-hive--power-bi)
5. [Infrastructure (Docker)](#infrastructure-docker)
6. [Data Flow End-to-End](#data-flow-end-to-end)
7. [HDFS Data Lake Layout](#hdfs-data-lake-layout)
8. [Airflow DAG Schedule](#airflow-dag-schedule)
9. [Hive Schema Reference](#hive-schema-reference)
10. [Technical Indicators Reference](#technical-indicators-reference)
11. [Getting Started](#getting-started)
12. [Power BI Connection](#power-bi-connection)

---

## Architecture Overview

The pipeline follows a **Lambda Architecture** pattern with both batch and streaming ingestion paths that converge in a unified HDFS data lake.

```
┌─────────────┐     ┌───────────────────┐     ┌──────────────────────────────────────────┐
│             │────▶│  Batch Ingest     │────▶│                                         │
│  yfinance   │     │  (daily OHLCV)    │     │           HDFS Data Lake                 │
│    API      │     └───────────────────┘     │                                          │
│             │                               │  Raw Zone  →  Staging Zone  →  Curated   │
│  (EGX ticks)│     ┌───────────────────┐     │                                          │
│             │────▶│ Real-time Scraper │─┐  └──────────────────────────────────────────┘
└─────────────┘     │  (every 60s)      │  │             │                   │
                    └───────────────────┘  │  ┌──────────▼──────┐   ┌────────▼──────────┐
                                           │  │   Spark Batch   │   │ Spark Streaming   │
                                           │  │   ETL + Indic.  │   │  Consumer         │
                                    ┌──────┤  └─────────────────┘   └───────────────────┘
                                    ▼      │
                             ┌────────────┐│   ┌─────────────────┐   ┌────────────────┐
                             │   Kafka    ││   │  Hive Metastore │   │    Power BI    │
                             │ egx.*      │└──▶│  + Views        │──▶│  DirectQuery  │
                             └────────────┘    └─────────────────┘   └────────────────┘
                                    │
                               Airflow orchestrates everything
```

---

## Technology Stack

| Component | Technology | Version | Purpose |
|---|---|---|---|
| Data Source | yfinance | latest | Fetch EGX OHLCV & tick data |
| Message Bus | Apache Kafka | 7.x (Confluent) | Real-time tick streaming |
| Distributed Storage | Apache Hadoop HDFS | 3.x | Persistent data lake |
| Batch Processing | Apache Spark (PySpark) | 3.4.1 | ETL + indicator computation |
| Stream Processing | Spark Structured Streaming | 3.4.1 | Real-time tick consumption |
| Table Metadata | Apache Hive | 2.x | Schema registry + SQL views |
| Orchestration | Apache Airflow | 2.x (Celery) | DAG scheduling |
| Containerization | Docker + Docker Compose | latest | All services in containers |
| BI / Reporting | Microsoft Power BI | Desktop | Dashboards via ODBC |
| ODBC Driver | Simba Hive ODBC | 2.6.26 | Power BI ↔ Hive bridge |

---

## Repository Structure

```
egx-bigdata-pipeline/
│
├── docker/                         # Container infrastructure
│   ├── docker-compose.yml          # Root compose: includes all sub-stacks
│   ├── .env                        # Airflow UID, Fernet key, secrets
│   ├── hadoop/                     # Hadoop NameNode, DataNode, YARN
│   ├── hive/                       # HiveServer2 + Metastore + PostgreSQL
│   ├── kafka/                      # Kafka broker + Zookeeper + Kafka UI
│   ├── spark/                      # Spark Master + Worker
│   └── airflow/                    # Airflow Webserver, Scheduler, Worker, Triggerer
│
├── ingestion/                      # Layer 1: Data ingestion
│   ├── config.py                   # Kafka broker, HDFS URL, scrape interval
│   ├── tickers.json                # List of EGX ticker symbols
│   ├── tickers.py                  # Helper to load tickers from JSON
│   ├── scraper.py                  # Real-time yfinance poller → Kafka producer
│   ├── producer.py                 # KafkaTickProducer class
│   └── batch_ingest.py             # Daily OHLCV batch fetch → HDFS raw zone
│
├── storage/                        # Layer 2: Storage layout
│   ├── hdfs_paths.py               # Central HDFS path constants (single source of truth)
│   └── init_hdfs.sh                # One-time HDFS directory initialisation script
│
├── processing/                     # Layer 3: Spark processing
│   ├── spark_config.py             # SparkSession factory (HDFS + Hive + Kafka config)
│   ├── batch_etl.py                # Raw → Staging → Curated ETL pipeline
│   ├── indicators.py               # PySpark technical indicator library
│   └── spark_streaming.py          # Structured Streaming: Kafka → HDFS raw/ticks
│
├── orchestration/                  # Layer 4: Airflow DAGs
│   ├── run_sql.py                  # CLI helper: runs HiveQL from DAGs
│   └── dags/
│       ├── ingestion_dag.py        # egx_ingestion: daily batch ingest
│       ├── etl_dag.py              # egx_etl: daily Spark ETL
│       ├── hive_refresh_dag.py     # egx_hive_refresh: refresh SQL views
│       └── streaming_dag.py        # egx_streaming: continuous tick scraper
│
├── serving/                        # Layer 5: BI layer
│   ├── hive_tables.sql             # DDL: raw_ohlcv + curated_ohlcv external tables
│   ├── hive_views.sql              # SQL views: daily summary, weekly rollup, RSI, etc.
│   └── powerbi_odbc.md             # Guide: connecting Power BI via Simba ODBC
│
└── config/
    └── hadoop/                     # Hadoop + Hive XML config files (core-site, hive-site, etc.)
```

---

## Module Deep-Dives

### 1. Ingestion

The ingestion layer has two independent paths that run concurrently:

#### 1a. Daily Batch Ingestion — `ingestion/batch_ingest.py`

Fetches historical OHLCV data for all tracked EGX tickers every trading day (Sunday–Thursday) after market close at 15:45 Cairo time.

**How it works:**
1. Loads the ticker list from `tickers.json`
2. Calls `yf.download()` for all tickers in a single vectorised request
3. Normalises column names (`Date` → `date`, `Close` → `close`, etc.)
4. Writes the data as a **Parquet** file to a date-partitioned HDFS path

**Output path:** `hdfs:///data/raw/egx/ohlcv/date_partition=YYYY-MM-DD/data.parquet`

**Tracked tickers:**

| Symbol | Company |
|---|---|
| `COMI.CA` | Commercial International Bank (CIB) |
| `FWRY.CA` | Fawry for Banking Technology |
| `TMGH.CA` | Talaat Mostafa Group |
| `SWDY.CA` | Elsewedy Electric |
| `EAST.CA` | Eastern Company |
| `HRHO.CA` | Hermes Financial Group |
| `ABUK.CA` | Abu Kir Fertilizers |
| `ETEL.CA` | Telecom Egypt |
| `AMOC.CA` | Alexandria Mineral Oils |
| `PHDC.CA` | Palm Hills Developments |

#### 1b. Real-Time Tick Scraper — `ingestion/scraper.py`

Polls yfinance every **60 seconds** and publishes live tick data to Apache Kafka. This provides the real-time path for near-live dashboards.

**How it works:**
1. Loads the ticker list
2. Creates a `KafkaTickProducer` instance
3. Schedules `scrape_all()` to run every `SCRAPE_INTERVAL_SECONDS` (60s) using APScheduler
4. For each ticker, calls `yf.Ticker(symbol).fast_info` to get the latest price, volume, open, high, low, and previous close
5. Publishes each tick as a JSON message to its dedicated Kafka topic

**Retry Logic:** Each ticker fetch retries up to 3 times with a 5-second delay between attempts, then logs an error and skips that tick for the current cycle.

#### 1c. Kafka Producer — `ingestion/producer.py`

The `KafkaTickProducer` class wraps the `kafka-python` producer with:
- **Delivery guarantee:** `acks="all"` (waits for all replicas)
- **Serialisation:** JSON-encoded UTF-8
- **Topic naming:** `egx.<symbol>` (dots in symbols replaced with underscores). For example, `COMI.CA` → topic `egx.comi_ca`
- **Retries:** 3 automatic retries on transient Kafka errors

---

### 2. Storage (HDFS)

#### Path Constants — `storage/hdfs_paths.py`

All HDFS paths are centralised in a single module to prevent hardcoded path duplication across job scripts.

```
/data/
├── raw/egx/
│   ├── ohlcv/           ← Daily OHLCV Parquet files (date-partitioned)
│   │   └── date_partition=2026-06-14/
│   │       └── data.parquet
│   └── ticks/           ← Real-time tick Parquet files (Spark Streaming output)
│       └── date_partition=.../
│           └── symbol=.../
│
├── staging/egx/
│   └── ohlcv/           ← Cleaned, typed, de-duplicated OHLCV
│       └── date_partition=.../
│
└── curated/egx/
    └── ohlcv/           ← OHLCV + all technical indicators (symbol-partitioned)
        ├── symbol=COMI.CA/
        │   └── part-00000-xxx.snappy.parquet
        ├── symbol=FWRY.CA/
        └── ...
```

#### Initialisation — `storage/init_hdfs.sh`

A one-time bootstrap script that creates the entire HDFS directory tree and sets permissions. Run once after Hadoop starts for the first time:

```bash
docker exec egx-hadoop-namenode bash /opt/egx-pipeline/storage/init_hdfs.sh
```

---

### 3. Processing (Spark)

#### 3a. SparkSession Factory — `processing/spark_config.py`

Centralised factory function `get_spark_session(app_name, include_kafka=False)` that configures Spark for this cluster:

- Connects to the Spark master at `spark://egx-spark-master:7077`
- Enables **Hive support** pointing at the Hive Metastore (`thrift://egx-hive-metastore:9083`)
- Sets the HDFS warehouse directory
- Configures **KryoSerializer** for performance
- Fixes IPv4 stack issues for Docker networking
- Optionally loads the **Kafka connector JAR** for streaming jobs

The factory also resolves Docker service hostnames to IP addresses via DNS to handle network edge cases inside containers.

#### 3b. Batch ETL — `processing/batch_etl.py`

The daily ETL job transforms raw OHLCV data through three HDFS zones:

```
Raw Zone → (Step 2) → Staging Zone → (Step 4) → Curated Zone
```

**Step-by-step:**

| Step | Description |
|---|---|
| 1. Read raw | Reads today's Parquet from `HDFS_URL/data/raw/egx/ohlcv/date_partition=<today>` |
| 2. Clean → Staging | Drops nulls in key columns, filters out invalid prices/volumes, casts `date` type, rounds OHLCV columns to 4dp, de-duplicates on `(symbol, date)`, adds `ingested_at` timestamp |
| 3. Read all staging | Reads the full historical staging dataset (all date partitions) for rolling-window indicator calculation |
| 4. Compute indicators | Calls `add_all_indicators()` from `indicators.py` — pure PySpark window functions |
| 5. Write curated | Casts `ingested_at` to STRING (Power BI ODBC compatibility), writes Parquet **partitioned by `symbol`** |

> **Why cast `ingested_at` to STRING before writing curated?**
> The Simba Hive ODBC driver (used by Power BI) throws a `NullPointerException` when the Hive Metastore reports a `TIMESTAMP` column without column comments. The column is stored as STRING in curated parquet and declared as STRING in the Hive DDL — Power BI reads it correctly as a sortable text date.

#### 3c. Technical Indicators — `processing/indicators.py`

A pure **PySpark window-function** library — no Pandas UDFs, no Python loops on executors. Every indicator runs distributed on the cluster.

| Function | Indicator | Parameters |
|---|---|---|
| `add_sma(df, period)` | Simple Moving Average | period=20, period=50 |
| `add_ema(df, period)` | Exponential Moving Average (approximation) | period=20 |
| `add_rsi(df, period)` | Relative Strength Index | period=14 |
| `add_macd(df, fast, slow, signal)` | MACD line, signal line, histogram | 12/26/9 |
| `add_bollinger(df, period, std_dev)` | Bollinger Bands (upper/mid/lower) | 20 periods, 2σ |
| `add_all_indicators(df)` | Runs all of the above | — |

All indicators use `Window.partitionBy("symbol").orderBy("date")` so each stock's history is computed independently and correctly.

#### 3d. Spark Structured Streaming — `processing/spark_streaming.py`

Consumes all EGX Kafka topics in real time and writes micro-batches to HDFS.

- **Topic pattern:** subscribes to `egx\..*` (all EGX ticker topics via regex)
- **Deserialization:** parses JSON bytes using the `TICK_SCHEMA` StructType
- **Output:** Parquet files in `HDFS/data/raw/egx/ticks/`, partitioned by `date_partition` and `symbol`
- **Checkpoint:** `HDFS/checkpoints/streaming/` for exactly-once semantics
- **Trigger:** micro-batch every **30 seconds**

---

### 4. Orchestration (Airflow)

All four DAGs run inside the Airflow container but execute their Python/Hive tasks by calling `docker exec egx-spark-master` — this approach gives Spark tasks full access to HDFS, Hive Metastore, and Kafka without adding complexity to the Airflow container image.

#### DAG 1: `egx_ingestion` — `orchestration/dags/ingestion_dag.py`

| | |
|---|---|
| **Schedule** | `45 13 * * 0-4` — 13:45 UTC (15:45 Cairo), Sunday–Thursday |
| **Purpose** | Fetch today's OHLCV data from yfinance and write to HDFS raw zone |

**Task graph:**
```
wait_for_market_close → run_batch_ingest → repair_hive_partitions
```

- `wait_for_market_close`: Log marker confirming market has closed
- `run_batch_ingest`: Runs `python -m ingestion.batch_ingest` to fetch and write Parquet
- `repair_hive_partitions`: Runs `MSCK REPAIR TABLE egx_db.raw_ohlcv` so Hive registers the new partition

---

#### DAG 2: `egx_etl` — `orchestration/dags/etl_dag.py`

| | |
|---|---|
| **Schedule** | `15 14 * * 0-4` — 14:15 UTC, 30 minutes after ingestion starts |
| **Purpose** | Run PySpark ETL to transform raw → staging → curated with indicators |

**Task graph:**
```
wait_for_ingestion → run_spark_etl → repair_curated_partitions
```

- `wait_for_ingestion`: `ExternalTaskSensor` waits for `egx_ingestion.repair_hive_partitions` to complete
- `run_spark_etl`: Runs `python3 -m processing.batch_etl` inside the Spark master container
- `repair_curated_partitions`: Runs `MSCK REPAIR TABLE egx_db.curated_ohlcv` to register new symbol partitions

---

#### DAG 3: `egx_hive_refresh` — `orchestration/dags/hive_refresh_dag.py`

| | |
|---|---|
| **Schedule** | `30 15 * * 0-4` — 15:30 UTC, after ETL is expected to finish |
| **Purpose** | Re-create all Hive SQL views so Power BI DirectQuery picks up the latest schema |

**Task graph:**
```
wait_for_etl → refresh_hive_views
```

- `wait_for_etl`: `ExternalTaskSensor` waits for `egx_etl.repair_curated_partitions`
- `refresh_hive_views`: Executes `serving/hive_views.sql` via `orchestration/run_sql.py`

---

#### DAG 4: `egx_streaming` — `orchestration/dags/streaming_dag.py`

| | |
|---|---|
| **Schedule** | `@once` — started once and runs continuously |
| **Purpose** | Launch the real-time tick scraper and Spark Streaming consumer |

**Task graph:**
```
run_consumer → run_scraper
```

- `run_consumer`: Starts `spark_streaming.py` as a background process inside `egx-spark-master`
- `run_scraper`: Starts `ingestion.scraper` which polls yfinance every 60 seconds

---

### 5. Serving (Hive + Power BI)

#### 5a. Hive External Tables — `serving/hive_tables.sql`

Registers two **external tables** (Hive reads the Parquet files but never moves or copies them):

**`raw_ohlcv`** — partitioned by `date_partition` (date string)
```sql
CREATE EXTERNAL TABLE IF NOT EXISTS raw_ohlcv (
    `date` DATE, symbol STRING, open DOUBLE,
    high DOUBLE, low DOUBLE, close DOUBLE, volume BIGINT
)
PARTITIONED BY (date_partition STRING)
STORED AS PARQUET
LOCATION 'hdfs://hadoop-namenode:8020/data/raw/egx/ohlcv';
```

**`curated_ohlcv`** — partitioned by `symbol` (one directory per stock)
```sql
CREATE EXTERNAL TABLE IF NOT EXISTS curated_ohlcv (
    `date` DATE COMMENT 'date',
    open DOUBLE COMMENT 'open', ...,
    ingested_at STRING COMMENT 'ingested_at'
)
COMMENT 'curated_ohlcv table'
PARTITIONED BY (symbol STRING)
STORED AS PARQUET
LOCATION 'hdfs://hadoop-namenode:8020/data/curated/egx/ohlcv';
```

> **Why `COMMENT` on every column?** The Simba Hive ODBC driver crashes with a `NullPointerException` when browsing tables in Power BI's Navigator if any column is missing a `COMMENT` field in the Metastore. Adding comments on all columns fixes this.

#### 5b. Hive Views — `serving/hive_views.sql`

Pre-built analytical SQL views on top of `curated_ohlcv`:

| View | Description | Key Columns |
|---|---|---|
| `v_daily_summary` | Full price + indicator snapshot per day per symbol | `symbol`, `date`, `open/high/low/close`, `daily_change_pct`, all indicators |
| `v_weekly_rollup` | Weekly OHLCV aggregated from daily data | `symbol`, `week_start`, `week_open/close`, `week_high/low`, `week_volume` |
| `v_top_movers` | Top 10 biggest movers on the latest trading day | `symbol`, `change_pct` |
| `v_rsi_signals` | RSI signal classification per symbol per day | `symbol`, `rsi_14`, `rsi_signal` (overbought/oversold/neutral) |
| `v_bollinger_squeeze` | Bollinger Band width — sorted to highlight squeeze | `symbol`, `band_width` |

#### 5c. Power BI Connection

Power BI connects via **DirectQuery** mode using the **Simba Hive ODBC Driver** (v2.6.26, 64-bit). DirectQuery means every report interaction runs a live SQL query against Hive — no stale import cache.

**DSN configuration (`ODBC Data Sources (64-bit)`):**
- Driver: `Simba Hive ODBC Driver`
- DSN Name: `EGX_Hive`
- Host: `localhost`
- Port: `10000`
- Database: `egx_db`
- Authentication: No Authentication

---

## Infrastructure (Docker)

All services are defined across five independent Docker Compose files, included by the root `docker/docker-compose.yml`. They all share the `egx-network` bridge network.

### Launch everything:
```bash
cd docker
docker compose -f docker-compose.yml up -d
```

### Services and ports:

| Service | Container | Port (Host→Container) | Purpose |
|---|---|---|---|
| HDFS NameNode | `egx-hadoop-namenode` | `9870:9870` | HDFS Web UI |
| HDFS DataNode | `egx-hadoop-datanode` | `9864:9864` | HDFS storage node |
| YARN ResourceManager | `egx-hadoop-resourcemanager` | `8088:8088` | YARN Web UI |
| Hive Metastore | `egx-hive-metastore` | `9083` (internal) | Thrift metastore |
| HiveServer2 | `egx-hive-server` | `10000:10000` | JDBC/ODBC endpoint |
| Kafka | `egx-kafka` | `9092:9092` | External Kafka |
| Zookeeper | `egx-zookeeper` | `2181:2181` | Kafka coordination |
| Kafka UI | `egx-kafka-ui` | `8080:8080` | Kafka topic browser |
| Spark Master | `egx-spark-master` | `8081:8081` | Spark Web UI |
| Spark Worker | `egx-spark-worker` | `8082:8081` | Spark worker |
| Airflow Webserver | `egx-airflow-webserver` | `8082:8080` | Airflow UI |
| Airflow Scheduler | `egx-airflow-scheduler` | — | DAG scheduler |
| Airflow Worker | `egx-airflow-worker` | — | Celery task executor |
| Airflow Triggerer | `egx-airflow-triggerer` | — | Deferred task trigger |

### Airflow configuration (`docker/.env`):
```env
AIRFLOW_UID=50000
AIRFLOW_EXECUTOR=CeleryExecutor
AIRFLOW_SECRET_KEY=egx-dev-secret-key
AIRFLOW_FERNET_KEY=<generate with: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())">
```

---

## Data Flow End-to-End

```
[Daily, 13:45 UTC]
yfinance API
    ↓
ingestion/batch_ingest.py
    ↓ writes Parquet
HDFS: /data/raw/egx/ohlcv/date_partition=YYYY-MM-DD/
    ↓ (MSCK REPAIR)
Hive Metastore: raw_ohlcv table updated
    ↓ (ExternalTaskSensor waits)
processing/batch_etl.py (runs on egx-spark-master)
    ├── Reads raw partition
    ├── Cleans → writes /data/staging/egx/ohlcv/
    ├── Reads all staging history
    ├── Computes SMA-20, SMA-50, EMA-20, RSI-14, MACD(12,26,9), Bollinger(20)
    └── Writes /data/curated/egx/ohlcv/symbol=<TICKER>/
    ↓ (MSCK REPAIR)
Hive Metastore: curated_ohlcv partitions updated
    ↓
serving/hive_views.sql refreshed
    ↓
Power BI DirectQuery via Simba ODBC on port 10000
    ↓
Live dashboard data

[Continuous, every 60s]
yfinance fast_info
    ↓
ingestion/scraper.py → ingestion/producer.py
    ↓ JSON messages
Kafka topics: egx.comi_ca, egx.fwry_ca, ...
    ↓
processing/spark_streaming.py (30s micro-batches)
    ↓ Parquet
HDFS: /data/raw/egx/ticks/date_partition=.../symbol=.../
```

---

## HDFS Data Lake Layout

The pipeline uses a **medallion architecture** with three zones:

| Zone | Path | Format | Partitioning | Content |
|---|---|---|---|---|
| **Raw** | `/data/raw/egx/ohlcv/` | Parquet | `date_partition` | As-is from yfinance, no transformation |
| **Raw Ticks** | `/data/raw/egx/ticks/` | Parquet | `date_partition`, `symbol` | Real-time tick snapshots |
| **Staging** | `/data/staging/egx/ohlcv/` | Parquet | `date_partition` | Cleaned, typed, de-duplicated OHLCV |
| **Curated** | `/data/curated/egx/ohlcv/` | Parquet (Snappy) | `symbol` | Full OHLCV + all technical indicators |

---

## Airflow DAG Schedule

| DAG ID | Cron | Trigger | Cairo Time |
|---|---|---|---|
| `egx_ingestion` | `45 13 * * 0-4` | Daily, Sun–Thu | 15:45 (market close + 15 min) |
| `egx_etl` | `15 14 * * 0-4` | Daily, Sun–Thu | 16:15 (30 min after ingestion) |
| `egx_hive_refresh` | `30 15 * * 0-4` | Daily, Sun–Thu | 17:30 (after ETL finishes) |
| `egx_streaming` | `@once` | Manual trigger | Runs continuously |

**Default credentials:** `admin` / `admin` at `http://localhost:8082`

---

## Hive Schema Reference

### `egx_db.curated_ohlcv`

| Column | Type | Description |
|---|---|---|
| `date` | DATE | Trading date |
| `open` | DOUBLE | Opening price |
| `high` | DOUBLE | Intraday high |
| `low` | DOUBLE | Intraday low |
| `close` | DOUBLE | Closing price |
| `volume` | BIGINT | Total shares traded |
| `sma_20` | DOUBLE | 20-day Simple Moving Average |
| `sma_50` | DOUBLE | 50-day Simple Moving Average |
| `ema_20` | DOUBLE | 20-day Exponential Moving Average |
| `rsi_14` | DOUBLE | 14-day RSI (0–100 scale) |
| `macd_line` | DOUBLE | MACD line (EMA12 − EMA26) |
| `macd_signal` | DOUBLE | 9-day EMA of MACD line |
| `macd_hist` | DOUBLE | MACD histogram (line − signal) |
| `bb_upper` | DOUBLE | Bollinger Upper Band (SMA20 + 2σ) |
| `bb_mid` | DOUBLE | Bollinger Middle Band (SMA20) |
| `bb_lower` | DOUBLE | Bollinger Lower Band (SMA20 − 2σ) |
| `ingested_at` | STRING | UTC timestamp when this row was processed |
| `symbol` *(partition)* | STRING | EGX ticker symbol (e.g. `COMI.CA`) |

---

## Technical Indicators Reference

| Indicator | Formula | Interpretation |
|---|---|---|
| **SMA-20** | Avg(close, last 20 days) | Short-term trend direction |
| **SMA-50** | Avg(close, last 50 days) | Medium-term trend direction |
| **EMA-20** | Weighted avg, recent prices weighted more | Faster-reacting trend signal |
| **RSI-14** | 100 − 100/(1 + avg_gain/avg_loss) | >70 = overbought, <30 = oversold |
| **MACD Line** | EMA(12) − EMA(26) | Momentum; cross above signal = bullish |
| **MACD Signal** | EMA(9) of MACD line | Smoothed momentum reference |
| **MACD Histogram** | MACD line − Signal | Visualises convergence/divergence |
| **Bollinger Upper** | SMA(20) + 2 × StdDev(20) | Price above = potentially overbought |
| **Bollinger Mid** | SMA(20) | Central value reference |
| **Bollinger Lower** | SMA(20) − 2 × StdDev(20) | Price below = potentially oversold |

---

## Getting Started

### Prerequisites

- Docker Desktop (Windows/Mac) or Docker Engine + Compose (Linux)
- At least **16 GB RAM** allocated to Docker (Hadoop + Spark + Kafka are memory-hungry)
- 20 GB free disk space
- Python 3.10+ (for local development only)

### Step 1: Clone and configure

```bash
git clone <repo-url>
cd egx-bigdata-pipeline
```

Generate a Fernet key for Airflow:
```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Update `docker/.env`:
```env
AIRFLOW_UID=50000
AIRFLOW_EXECUTOR=CeleryExecutor
AIRFLOW_SECRET_KEY=egx-dev-secret-key
AIRFLOW_FERNET_KEY=<paste your generated key here>
```

### Step 2: Start all services

```bash
cd docker
docker compose -f docker-compose.yml up -d
```

Wait ~2 minutes for all health checks to pass, then verify:
```bash
docker compose -f docker-compose.yml ps
```

### Step 3: Initialise HDFS directories

```bash
docker exec egx-hadoop-namenode bash /opt/egx-pipeline/storage/init_hdfs.sh
```

### Step 4: Register Hive tables

```bash
Get-Content serving/hive_tables.sql | docker exec -i egx-hive-server hive
# On Linux/Mac:
# cat serving/hive_tables.sql | docker exec -i egx-hive-server hive
```

### Step 5: Run initial ingestion manually

```bash
# Ingest today's OHLCV data
docker exec egx-airflow-worker bash -c "cd /opt/egx-pipeline && python -m ingestion.batch_ingest"

# Run the ETL to produce curated data
docker exec egx-spark-master bash -c "cd /opt/egx-pipeline && python3 -m processing.batch_etl"

# Repair Hive partitions
docker exec egx-hive-server hive -e "MSCK REPAIR TABLE egx_db.curated_ohlcv;"

# Create the analytical views
cat serving/hive_views.sql | docker exec -i egx-hive-server hive
```

### Step 6: Access the UIs

| UI | URL | Credentials |
|---|---|---|
| Airflow | http://localhost:8082 | admin / admin |
| Spark | http://localhost:8081 | — |
| Kafka UI | http://localhost:8080 | — |
| HDFS NameNode | http://localhost:9870 | — |
| YARN | http://localhost:8088 | — |

---

## Power BI Connection

1. Download and install [Simba Hive ODBC Driver v2.6.26 (64-bit)](https://www.cloudera.com/downloads/connectors/hive/odbc/2-6-26.html)
2. Open **ODBC Data Sources (64-bit)** → **Add** → **Simba Hive ODBC Driver**
   - DSN Name: `EGX_Hive`
   - Host: `localhost`
   - Port: `10000`
   - Database: `egx_db`
   - Authentication: No Authentication
3. Test the connection — it should say "Successfully connected"
4. Open Power BI Desktop → **Get Data** → **Other** → **ODBC**
5. Select `EGX_Hive` → **OK** → **DirectQuery** mode
6. Browse the Navigator and select the views you need

**Recommended views for dashboards:**

| View | Best For |
|---|---|
| `v_daily_summary` | Main candlestick + indicator dashboard |
| `v_weekly_rollup` | Weekly bar charts and performance |
| `v_top_movers` | Top gainers/losers card visual |
| `v_rsi_signals` | RSI overbought/oversold screening table |
| `v_bollinger_squeeze` | Volatility and squeeze alerts |






*Built for Egyptian Exchange market data analysis. The pipeline runs on Docker and is designed for local development and small-scale production use.*
