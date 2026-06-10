import os
import sys
from pyspark.sql import SparkSession

# Environment variables with sensible defaults
SPARK_MASTER = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
HIVE_METASTORE_URIS = os.getenv("HIVE_METASTORE_URI", "thrift://hive-metastore:9083")
HDFS_NAMENODE = os.getenv("HDFS_NAMENODE", "hdfs://hadoop-namenode:8020")
WAREHOUSE_DIR = f"{HDFS_NAMENODE}/user/hive/warehouse"
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

# Kafka Connector Package for Spark 3.4.1
KAFKA_JARS_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1"

def get_spark_session(app_name="EGX-Pipeline-Job"):
    """
    Initializes and returns a SparkSession configured for the EGX Big Data Pipeline.
    Integrates with HDFS, Hive Metastore, and Kafka.
    """
    print(f"Initializing SparkSession '{app_name}'...")
    print(f"Spark Master: {SPARK_MASTER}")
    print(f"Hive Metastore URIs: {HIVE_METASTORE_URIS}")
    print(f"HDFS Warehouse: {WAREHOUSE_DIR}")
    print(f"Kafka Bootstrap Servers: {KAFKA_BOOTSTRAP_SERVERS}")

    try:
        spark = SparkSession.builder \
            .appName(app_name) \
            .master(SPARK_MASTER) \
            .config("spark.sql.warehouse.dir", WAREHOUSE_DIR) \
            .config("spark.hive.metastore.uris", HIVE_METASTORE_URIS) \
            .config("spark.sql.catalogImplementation", "hive") \
            .config("spark.sql.hive.metastore.version", "2.3.2") \
            .config("spark.sql.hive.metastore.jars", "builtin") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.session.timeZone", "UTC") \
            .config("spark.jars.packages", KAFKA_JARS_PACKAGE) \
            .enableHiveSupport() \
            .getOrCreate()
            
        print("SparkSession initialized successfully.")
        return spark
    except Exception as e:
        print(f"Error initializing SparkSession: {e}", file=sys.stderr)
        raise

if __name__ == "__main__":
    # Test script: verifies that the SparkSession can be instantiated
    print("Testing Spark configuration...")
    # For testing outside container, fallback to local master if spark-master is unreachable
    if "SPARK_MASTER_URL" not in os.environ:
        print("SPARK_MASTER_URL not set in environment. Defaulting to local[*] for test run.")
        SPARK_MASTER = "local[*]"
        
    try:
        spark = get_spark_session("Spark-Config-Test")
        print("\nSpark Configuration settings:")
        for k, v in spark.sparkContext.getConf().getAll():
            print(f"  {k} = {v}")
        spark.stop()
        print("\nTest completed successfully!")
    except Exception as e:
        print(f"\nTest failed: {e}", file=sys.stderr)
        sys.exit(1)
