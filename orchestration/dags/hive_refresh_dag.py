"""
Airflow DAG: refresh Hive views after ETL completes.
Re-creates views so Power BI DirectQuery picks up the latest schema.

All Hive tasks run inside egx-spark-master via `docker exec`.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

SPARK_EXEC  = "docker exec egx-spark-master"
PROJECT_DIR = "/opt/egx-pipeline"

default_args = {
    "owner":           "egx-pipeline",
    "depends_on_past": False,
    "retries":         1,
    "retry_delay":     timedelta(minutes=5),
}

with DAG(
    dag_id="egx_hive_refresh",
    default_args=default_args,
    description="Refresh Hive views for Power BI after curated data is ready",
    schedule_interval="30 15 * * 0-4",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["egx", "hive"],
) as dag:

    wait_for_etl = ExternalTaskSensor(
        task_id="wait_for_etl",
        external_dag_id="egx_etl",
        external_task_id="repair_curated_partitions",
        timeout=7200,
        poke_interval=60,
    )

    refresh_views = BashOperator(
        task_id="refresh_hive_views",
        bash_command=(
            f"{SPARK_EXEC} bash -c "
            f"'cd {PROJECT_DIR} && python3 -m orchestration.run_sql "
            f"-f serving/hive_views.sql'"
        ),
    )

    wait_for_etl >> refresh_views
