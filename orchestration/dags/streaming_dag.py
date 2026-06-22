"""
Airflow DAG: real-time EGX streaming.
- prepare_kafka_jars:  downloads Kafka connector JARs via wget (bypasses broken Ivy/--packages)
- kill_stale_consumer: kills any previous streaming process to avoid duplicate consumers
- run_consumer:        starts Spark Structured Streaming with --jars (no Ivy dependency)
- verify_consumer:     waits 20s and checks the log for successful startup
- run_scraper:         starts the yfinance scraper inside the Airflow worker
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_DIR  = "/opt/egx-pipeline"
SPARK_MASTER = "spark://spark-master:7077"
LOG_FILE     = "/tmp/egx_spark_streaming.log"
JAR_DIR      = "/tmp/spark_jars"

# Maven Central URLs for Spark 3.4.1 Kafka Structured Streaming dependencies
MAVEN = "https://repo1.maven.org/maven2"
KAFKA_JARS = {
    "spark-sql-kafka-0-10_2.12-3.4.1.jar":
        f"{MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.12/3.4.1/spark-sql-kafka-0-10_2.12-3.4.1.jar",
    "spark-token-provider-kafka-0-10_2.12-3.4.1.jar":
        f"{MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.4.1/spark-token-provider-kafka-0-10_2.12-3.4.1.jar",
    "kafka-clients-3.3.2.jar":
        f"{MAVEN}/org/apache/kafka/kafka-clients/3.3.2/kafka-clients-3.3.2.jar",
    "commons-pool2-2.11.1.jar":
        f"{MAVEN}/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar",
}

# Comma-separated jar paths for spark-submit --jars
JARS_LIST = ",".join(f"{JAR_DIR}/{jar}" for jar in KAFKA_JARS)

# wget commands — skip download if file already cached
WGET_CMDS = " && ".join(
    f"[ -f {JAR_DIR}/{jar} ] || wget -q -P {JAR_DIR} {url}"
    for jar, url in KAFKA_JARS.items()
)

default_args = {
    "owner":            "egx-pipeline",
    "depends_on_past":  False,
    "retries":          3,
    "retry_delay":      timedelta(minutes=1),
    "email_on_failure": False,
}

with DAG(
    dag_id="egx_streaming",
    default_args=default_args,
    description="Real-time EGX tick scraper → Kafka → Spark Streaming → HDFS",
    schedule_interval="@once",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["egx", "streaming"],
) as dag:

    # ── Download Kafka JARs directly (no Ivy) ────────────────────────────────
    # --packages uses Apache Ivy which fails with a FileOutputStream permission
    # error in this container. wget bypasses Ivy completely.
    # JARs are cached in JAR_DIR — skipped on subsequent DAG runs.
    prepare_kafka_jars = BashOperator(
        task_id="prepare_kafka_jars",
        bash_command=(
            f"docker exec egx-spark-master bash -c '"
            f"mkdir -p {JAR_DIR} && "
            f"{WGET_CMDS} && "
            f"echo \"JARs ready: $(ls {JAR_DIR})\""
            f"'"
        ),
    )

    # ── Kill any stale streaming process ─────────────────────────────────────
    # Uses pgrep + kill (not pkill -f) to avoid self-kill of the bash wrapper.
    kill_stale_consumer = BashOperator(
        task_id="kill_stale_consumer",
        bash_command=(
            "docker exec egx-spark-master bash -c "
            "'kill $(pgrep -f processing.spark_streaming) 2>/dev/null; exit 0' "
            "|| true"
        ),
    )

    # ── Start Spark Structured Streaming ─────────────────────────────────────
    # --jars uses local files (downloaded above) — no Maven/Ivy resolution.
    # Runs detached; all output goes to LOG_FILE for verify_consumer to check.
    # Tail logs: docker exec egx-spark-master tail -f /tmp/egx_spark_streaming.log
    run_consumer = BashOperator(
        task_id="run_consumer",
        bash_command=(
            "docker exec -d egx-spark-master bash -c '"
            f"cd {PROJECT_DIR} && "
            f"KAFKA_BROKER=kafka:29092 "
            f"PYTHONPATH={PROJECT_DIR} "
            f"spark-submit "
            f"  --master {SPARK_MASTER} "
            f"  --jars {JARS_LIST} "
            "  --conf spark.sql.catalogImplementation=hive "
            "  --conf spark.hive.metastore.uris=thrift://hive-metastore:9083 "
            "  --conf spark.sql.warehouse.dir=hdfs://hadoop-namenode:8020/user/hive/warehouse "
            "  --conf spark.driver.extraJavaOptions=-Djava.net.preferIPv4Stack=true "
            "  --conf spark.executor.extraJavaOptions=-Djava.net.preferIPv4Stack=true "
            f"  {PROJECT_DIR}/processing/spark_streaming.py "
            f"  > {LOG_FILE} 2>&1"
            "'"
        ),
    )

    # ── Verify streaming job started successfully ─────────────────────────────
    # Spark Structured Streaming does NOT log "Streaming query started".
    # It emits a JSON progress report containing "FileSink[hdfs://...]".
    # That JSON is what verify checks for.
    verify_consumer = BashOperator(
        task_id="verify_consumer",
        bash_command=(
            "sleep 20 && "
            "docker exec egx-spark-master bash -c "
            f"'grep -q \"FileSink\" {LOG_FILE} && "
            f"echo \"✅ Spark Streaming running — writing to HDFS\" || "
            f"(echo \"❌ Consumer failed — last 30 lines:\"; tail -30 {LOG_FILE}; exit 1)'"
        ),
    )

    # ── Start yfinance scraper (runs in Airflow worker) ───────────────────────
    # APScheduler blocks indefinitely — this task runs forever (by design).
    run_scraper = BashOperator(
        task_id="run_scraper",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "pip install -q apscheduler yfinance kafka-python python-dotenv && "
            "python -m ingestion.scraper"
        ),
    )

    prepare_kafka_jars >> kill_stale_consumer >> run_consumer >> verify_consumer >> run_scraper
