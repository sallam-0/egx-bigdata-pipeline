"""
Airflow DAG: periodic Hive partition repair for raw_ticks.

Spark Structured Streaming writes new date_partition/symbol directories to HDFS
every 30 seconds, but Hive's metastore doesn't auto-discover them.
This DAG runs MSCK REPAIR TABLE every minute so v_latest_ticks always sees
the latest tick data written by Spark.

Without this DAG, v_latest_ticks returns 0 rows even when HDFS has data.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

SPARK_EXEC = "docker exec egx-spark-master"

default_args = {
    "owner":            "egx-pipeline",
    "depends_on_past":  False,
    "retries":          1,
    "retry_delay":      timedelta(seconds=30),
    "email_on_failure": False,
}

with DAG(
    dag_id="egx_ticks_partition_repair",
    default_args=default_args,
    description="Runs MSCK REPAIR TABLE raw_ticks every minute so Hive sees new Spark Streaming partitions",
    schedule_interval="* * * * *",   # every minute
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,               # prevent overlapping runs
    tags=["egx", "streaming", "hive"],
) as dag:

    repair_ticks_partitions = BashOperator(
        task_id="repair_ticks_partitions",
        bash_command=(
            f"{SPARK_EXEC} bash -c \""
            "beeline -u 'jdbc:hive2://hive-server:10000' "
            "-e 'MSCK REPAIR TABLE egx_db.raw_ticks;' "
            "--silent=true"
            "\""
        ),
    )
