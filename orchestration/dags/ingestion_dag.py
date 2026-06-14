"""
Airflow DAG: daily EGX batch ingestion.
Triggers after market close (15:45 Cairo time = 13:45 UTC).

All Spark/Hive tasks run inside egx-spark-master via `docker exec` because
that container has full network access to Hive metastore, HDFS, and Kafka.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# Project is mounted at /opt/egx-pipeline inside the Spark master container
SPARK_EXEC = "docker exec egx-spark-master"
PROJECT_DIR = "/opt/egx-pipeline"

default_args = {
    "owner":            "egx-pipeline",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="egx_ingestion",
    default_args=default_args,
    description="Daily EGX OHLCV batch ingestion from yfinance to HDFS",
    schedule_interval="45 13 * * 0-4",   # 13:45 UTC = 15:45 Cairo, Sun–Thu
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["egx", "ingestion"],
) as dag:

    wait_for_market_close = BashOperator(
        task_id="wait_for_market_close",
        bash_command="echo 'Market closed. Starting ingestion for {{ ds }}'",
    )

    run_batch_ingest = BashOperator(
        task_id="run_batch_ingest",
        bash_command=(
            f"cd {PROJECT_DIR} && python -m ingestion.batch_ingest"
        ),
    )

    repair_hive_partitions = BashOperator(
        task_id="repair_hive_partitions",
        bash_command=(
            f"{SPARK_EXEC} bash -c "
            f"'cd {PROJECT_DIR} && python3 -m orchestration.run_sql "
            f"-q \"MSCK REPAIR TABLE egx_db.raw_ohlcv\"'"
        ),
    )

    wait_for_market_close >> run_batch_ingest >> repair_hive_partitions
