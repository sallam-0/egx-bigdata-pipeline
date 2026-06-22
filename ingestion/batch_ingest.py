"""
Daily batch OHLCV ingestion.
Fetches one day of data for all tickers and writes Parquet to HDFS raw zone.
Called by Airflow ingestion_dag.py via SparkSubmitOperator or BashOperator.
"""

import logging
from datetime import date

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yfinance as yf

from ingestion.config import HDFS_URL
from ingestion.tickers import load_tickers
from storage.hdfs_paths import RAW_ZONE

log = logging.getLogger(__name__)


def fetch_ohlcv(symbols: list[str], period: str = "1mo") -> pd.DataFrame:
    data = yf.download(symbols, period=period, group_by="ticker", auto_adjust=True)
    frames = []
    for sym in symbols:
        try:
            df = data[sym].copy()
            df["symbol"] = sym
            df.reset_index(inplace=True)
            df.rename(columns={"Date": "date", "Open": "open", "High": "high",
                                "Low": "low", "Close": "close", "Volume": "volume"}, inplace=True)
            df["date"] = pd.to_datetime(df["date"]).dt.date
            frames.append(df)
        except KeyError:
            log.warning("No OHLCV data returned for %s", sym)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


import os
import subprocess

def write_to_hdfs(df: pd.DataFrame, run_date: date) -> None:
    partition_str = run_date.strftime("%Y-%m-%d")
    hdfs_dir = f"/data/raw/egx/ohlcv/date_partition={partition_str}"
    
    # Write locally to mounted volume
    os.makedirs("/opt/egx-pipeline/data_tmp", exist_ok=True)
    local_path = f"/opt/egx-pipeline/data_tmp/raw_{partition_str}.parquet"
    
    table = pa.Table.from_pandas(df)
    pq.write_table(table, local_path)
    
    # Create HDFS directory via docker exec
    subprocess.run(["docker", "exec", "egx-hadoop-namenode", "hdfs", "dfs", "-mkdir", "-p", hdfs_dir], check=True)
    
    # Upload to HDFS (Avoid docker exec stdin streaming which causes lease exceptions)
    subprocess.run(["docker", "cp", local_path, "egx-hadoop-namenode:/tmp/data_tmp.parquet"], check=True)
    subprocess.run(["docker", "exec", "egx-hadoop-namenode", "hdfs", "dfs", "-put", "-f", "/tmp/data_tmp.parquet", f"{hdfs_dir}/data.parquet"], check=True)
    subprocess.run(["docker", "exec", "egx-hadoop-namenode", "rm", "/tmp/data_tmp.parquet"], check=True)
        
    log.info("Wrote %d rows to HDFS %s/data.parquet", len(df), hdfs_dir)


def run(run_date: date | None = None) -> None:
    run_date = run_date or date.today()
    tickers = load_tickers()
    log.info("Batch ingest for %s — %d tickers", run_date, len(tickers))
    df = fetch_ohlcv(tickers)
    if df.empty:
        log.error("No data fetched. Aborting.")
        import sys
        sys.exit(1)
    write_to_hdfs(df, run_date)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
