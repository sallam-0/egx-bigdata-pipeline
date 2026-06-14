"""
Spark Structured Streaming consumer.
Reads tick data from all EGX Kafka topics and writes micro-batches to HDFS raw/ticks.
"""

import os
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

from processing.spark_config import get_spark_session
from storage.hdfs_paths import RAW_TICKS

KAFKA_BROKER       = os.getenv("KAFKA_BROKER", "kafka:29092")
KAFKA_TOPIC_PREFIX = os.getenv("KAFKA_TOPIC_PREFIX", "egx")
HDFS_URL           = os.getenv("HDFS_URL", "hdfs://hadoop-namenode:8020")
CHECKPOINT_DIR     = f"{HDFS_URL}/checkpoints/streaming"

TICK_SCHEMA = StructType([
    StructField("symbol",    StringType(),  True),
    StructField("timestamp", StringType(),  True),
    StructField("price",     DoubleType(),  True),
    StructField("volume",    LongType(),    True),
    StructField("open",      DoubleType(),  True),
    StructField("high",      DoubleType(),  True),
    StructField("low",       DoubleType(),  True),
    StructField("prev_close", DoubleType(), True),
])


def run():
    spark = get_spark_session("EGX-Streaming", include_kafka=True)

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribePattern", f"{KAFKA_TOPIC_PREFIX}\\..*")
        .option("startingOffsets", "latest")
        .option("kafka.group.id", "egx-spark-consumer")
        .option("failOnDataLoss", "false")
        .load()
    )

    ticks = (
        raw_stream
        .select(F.from_json(F.col("value").cast("string"), TICK_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("event_time", F.to_timestamp("timestamp"))
        .withColumn("date_partition", F.to_date("event_time"))
    )

    query = (
        ticks.writeStream
        .format("parquet")
        .option("path", f"{HDFS_URL}{RAW_TICKS}")
        .option("checkpointLocation", CHECKPOINT_DIR)
        .partitionBy("date_partition", "symbol")
        .trigger(processingTime="30 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    run()
