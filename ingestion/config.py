import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER     = os.getenv("KAFKA_BROKER", "kafka:29092")
KAFKA_TOPIC_PREFIX = os.getenv("KAFKA_TOPIC_PREFIX", "egx")

HDFS_URL         = os.getenv("HDFS_URL", "hdfs://hadoop-namenode:8020")
HDFS_USER        = os.getenv("HDFS_USER", "hadoop")

SCRAPE_INTERVAL_SECONDS = 60          # yfinance poll frequency (real-time)
BATCH_PERIOD            = "1d"        # daily OHLCV period
YFINANCE_RETRY_LIMIT    = 3
YFINANCE_RETRY_DELAY    = 5           # seconds between retries

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
