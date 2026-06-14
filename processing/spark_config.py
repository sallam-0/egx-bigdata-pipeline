import os
import sys
from pyspark.sql import SparkSession

import socket

def resolve_uri(uri):
    try:
        if "://" in uri:
            protocol, address = uri.split("://", 1)
            if ":" in address:
                host, port = address.split(":", 1)
                ip = socket.gethostbyname(host)
                return f"{protocol}://{ip}:{port}"
            else:
                ip = socket.gethostbyname(address)
                return f"{protocol}://{ip}"
        elif ":" in uri:
            host, port = uri.split(":", 1)
            ip = socket.gethostbyname(host)
            return f"{ip}:{port}"
        else:
            return socket.gethostbyname(uri)
    except Exception:
        return uri

# Environment variables with sensible defaults
# These match the Docker service hostnames, which resolve correctly inside the Spark container
SPARK_MASTER = resolve_uri(os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077"))
HIVE_METASTORE_URIS = resolve_uri(os.getenv("HIVE_METASTORE_URI", "thrift://hive-metastore:9083"))
HDFS_NAMENODE = resolve_uri(os.getenv("HDFS_NAMENODE", "hdfs://hadoop-namenode:8020"))
WAREHOUSE_DIR = f"{HDFS_NAMENODE}/user/hive/warehouse"
KAFKA_BOOTSTRAP_SERVERS = resolve_uri(os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"))

# Tell Spark where its conf directory is so hive-site.xml is auto-discovered by the JVM
os.environ.setdefault("SPARK_CONF_DIR", "/opt/spark/conf")

# Kafka Connector Package for Spark 3.4.1
KAFKA_JARS_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1"

def get_spark_session(app_name="EGX-Pipeline-Job", include_kafka=False):
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
        builder = SparkSession.builder \
            .appName(app_name) \
            .master(SPARK_MASTER) \
            .config("spark.sql.warehouse.dir", WAREHOUSE_DIR) \
            .config("spark.hive.metastore.uris", HIVE_METASTORE_URIS) \
            .config("spark.sql.catalogImplementation", "hive") \
            .config("spark.sql.hive.metastore.version", "2.3.9") \
            .config("spark.sql.hive.metastore.jars", "builtin") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.session.timeZone", "UTC") \
            .config("spark.driver.extraJavaOptions", "-Djava.net.preferIPv4Stack=true") \
            .config("spark.executor.extraJavaOptions", "-Djava.net.preferIPv4Stack=true") \
            .enableHiveSupport()
            
        if include_kafka:
            builder = builder.config("spark.jars.packages", KAFKA_JARS_PACKAGE)
            
        spark = builder.getOrCreate()
            
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
