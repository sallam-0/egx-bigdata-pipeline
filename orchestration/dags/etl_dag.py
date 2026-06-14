"""
Airflow DAG: daily Spark ETL.
Runs after ingestion_dag completes. Cleans raw data, computes indicators, writes curated zone.

All Spark/Hive tasks run inside egx-spark-master via `docker exec`.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

SPARK_EXEC  = "docker exec egx-spark-master"
PROJECT_DIR = "/opt/egx-pipeline"

default_args = {
    "owner":            "egx-pipeline",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=10),
    "email_on_failure": False,
}

with DAG(
    dag_id="egx_etl",
    default_args=default_args,
    description="Daily PySpark ETL: raw → staging → curated with technical indicators",
    schedule_interval="15 14 * * 0-4",   # 30 min after ingestion starts
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["egx", "spark", "etl"],
) as dag:

    wait_for_ingestion = ExternalTaskSensor(
        task_id="wait_for_ingestion",
        external_dag_id="egx_ingestion",
        external_task_id="repair_hive_partitions",
        execution_delta=timedelta(minutes=30),
        timeout=3600,
        poke_interval=60,
    )

    run_spark_etl = BashOperator(
        task_id="run_spark_etl",
        bash_command=(
            f"{SPARK_EXEC} bash -c "
            f"'cd {PROJECT_DIR} && python3 -m processing.batch_etl'"
        ),
    )

    repair_curated_partitions = BashOperator(
        task_id="repair_curated_partitions",
        bash_command=(
            f"{SPARK_EXEC} bash -c "
            f"'cd {PROJECT_DIR} && python3 -m orchestration.run_sql "
            f"-q \"MSCK REPAIR TABLE egx_db.curated_ohlcv\"'"
        ),
    )

    wait_for_ingestion >> run_spark_etl >> repair_curated_partitions
