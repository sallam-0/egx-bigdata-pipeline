"""
Airflow DAG: real-time EGX streaming.
Triggers the scraper script to continuously push ticks to Kafka.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# Project is mounted at /opt/egx-pipeline
PROJECT_DIR = "/opt/egx-pipeline"

default_args = {
    "owner":            "egx-pipeline",
    "depends_on_past":  False,
    "retries":          5,
    "retry_delay":      timedelta(minutes=1),
    "email_on_failure": False,
}

with DAG(
    dag_id="egx_streaming",
    default_args=default_args,
    description="Real-time EGX tick scraper pushing to Kafka",
    schedule_interval="@once",   # Run once and let it run continuously
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["egx", "streaming"],
) as dag:

    run_consumer = BashOperator(
        task_id="run_consumer",
        bash_command=f"docker exec -d egx-spark-master bash -c 'cd {PROJECT_DIR} && python3 -m processing.spark_streaming'",
    )

    run_scraper = BashOperator(
        task_id="run_scraper",
        bash_command=f"cd {PROJECT_DIR} && pip install apscheduler && python -m ingestion.scraper",
    )

    run_consumer >> run_scraper
