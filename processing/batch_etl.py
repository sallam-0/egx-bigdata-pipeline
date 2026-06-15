"""
Daily PySpark batch ETL.
Raw OHLCV → cleaned staging → enriched curated (with indicators).
Called via SparkSubmitOperator in etl_dag.py.
"""

import logging
from datetime import date
from typing import Optional

from pyspark.sql import functions as F

from processing.spark_config import get_spark_session
from processing.indicators import add_all_indicators
from storage.hdfs_paths import RAW_OHLCV, STAGING_OHLCV, CURATED_OHLCV, CURATED_INDICATORS
import os

HDFS_URL = os.getenv("HDFS_URL", "hdfs://hadoop-namenode:8020")
log = logging.getLogger(__name__)


def run(run_date: Optional[date] = None):
    run_date = run_date or date.today()
    partition = run_date.strftime("%Y-%m-%d")

    spark = get_spark_session("EGX-BatchETL")

    # ── Step 1: read raw ─────────────────────────────────────
    raw_path = f"{HDFS_URL}{RAW_OHLCV}/date_partition={partition}"
    log.info("Reading raw data from %s", raw_path)
    raw = spark.read.parquet(raw_path)

    # ── Step 2: clean → staging ──────────────────────────────
    staging = (
        raw
        .dropna(subset=["symbol", "date", "close", "volume"])
        .filter(F.col("close") > 0)
        .filter(F.col("volume") >= 0)
        .withColumn("date", F.to_date("date"))
        .withColumn("close",  F.round("close",  4))
        .withColumn("open",   F.round("open",   4))
        .withColumn("high",   F.round("high",   4))
        .withColumn("low",    F.round("low",    4))
        .withColumn("ingested_at", F.current_timestamp())
        .dropDuplicates(["symbol", "date"])
    )

    staging_path = f"{HDFS_URL}{STAGING_OHLCV}/date_partition={partition}"
    staging.write.mode("overwrite").parquet(staging_path)
    log.info("Staging written: %d rows", staging.count())

    # ── Step 3: read all history from staging for indicators ──
    all_staging = spark.read.parquet(f"{HDFS_URL}{STAGING_OHLCV}")
    all_staging = all_staging.orderBy("symbol", "date")

    # ── Step 4: compute indicators ───────────────────────────
    enriched = add_all_indicators(all_staging)

    # ── Step 5: write curated partitioned by symbol ──────────
    enriched = enriched.withColumn("ingested_at", F.col("ingested_at").cast("string"))
    curated_path = f"{HDFS_URL}{CURATED_OHLCV}"
    (
        enriched
        .write
        .partitionBy("symbol")
        .mode("overwrite")
        .parquet(curated_path)
    )
    log.info("Curated written to %s", curated_path)

    spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
