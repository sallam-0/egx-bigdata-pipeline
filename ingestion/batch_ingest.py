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


def fetch_ohlcv(symbols: list[str], period: str = "1d") -> pd.DataFrame:
    data = yf.download(symbols, period=period, group_by="ticker", auto_adjust=True)
    frames = []
    for sym in symbols:
        try:
            df = data[sym].copy()
            df["symbol"] = sym
            df.reset_index(inplace=True)
            df.rename(columns={"Date": "date", "Open": "open", "High": "high",
                                "Low": "low", "Close": "close", "Volume": "volume"}, inplace=True)
            frames.append(df)
        except KeyError:
            log.warning("No OHLCV data returned for %s", sym)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def write_to_hdfs(df: pd.DataFrame, run_date: date) -> None:
    partition = run_date.strftime("%Y/%m/%d")
    path = f"{HDFS_URL}{RAW_ZONE}/ohlcv/date={partition}/data.parquet"
    table = pa.Table.from_pandas(df)
    pq.write_table(table, path)
    log.info("Wrote %d rows to %s", len(df), path)


def run(run_date: date | None = None) -> None:
    run_date = run_date or date.today()
    tickers = load_tickers()
    log.info("Batch ingest for %s — %d tickers", run_date, len(tickers))
    df = fetch_ohlcv(tickers)
    if df.empty:
        log.error("No data fetched. Aborting.")
        return
    write_to_hdfs(df, run_date)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
