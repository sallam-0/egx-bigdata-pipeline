"""
Real-time EGX tick scraper.
Polls yfinance every SCRAPE_INTERVAL_SECONDS and pushes to Kafka.
Run directly for development; in production Airflow starts this via BashOperator.
"""

import json
import logging
import time
from datetime import datetime, timezone

import yfinance as yf
from apscheduler.schedulers.blocking import BlockingScheduler

from ingestion.config import SCRAPE_INTERVAL_SECONDS, YFINANCE_RETRY_LIMIT, YFINANCE_RETRY_DELAY
from ingestion.producer import KafkaTickProducer
from ingestion.tickers import load_tickers

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def fetch_tick(symbol: str) -> dict | None:
    """Fetch the latest tick for a single ticker. Returns None on failure."""
    for attempt in range(1, YFINANCE_RETRY_LIMIT + 1):
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.fast_info
            return {
                "symbol":    symbol,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "price":     info.last_price,
                "volume":    info.last_volume,
                "open":      info.open,
                "high":      info.day_high,
                "low":       info.day_low,
                "prev_close": info.previous_close,
            }
        except Exception as exc:
            log.warning("Attempt %d/%d failed for %s: %s", attempt, YFINANCE_RETRY_LIMIT, symbol, exc)
            if attempt < YFINANCE_RETRY_LIMIT:
                time.sleep(YFINANCE_RETRY_DELAY)
    return None


def scrape_all(producer: KafkaTickProducer, tickers: list[str]) -> None:
    log.info("Scraping %d tickers...", len(tickers))
    for symbol in tickers:
        tick = fetch_tick(symbol)
        if tick:
            producer.send(symbol, tick)
            log.debug("Sent tick for %s: price=%.2f", symbol, tick["price"])
        else:
            log.error("No data for %s — skipping this cycle.", symbol)


def main() -> None:
    tickers = load_tickers()
    producer = KafkaTickProducer()

    scheduler = BlockingScheduler()
    scheduler.add_job(
        scrape_all,
        trigger="interval",
        seconds=SCRAPE_INTERVAL_SECONDS,
        args=[producer, tickers],
        next_run_time=datetime.now(),
    )

    log.info("Scraper started. Polling every %ds for %d tickers.", SCRAPE_INTERVAL_SECONDS, len(tickers))
    try:
        scheduler.start()
    except KeyboardInterrupt:
        log.info("Scraper stopped.")
        producer.close()


if __name__ == "__main__":
    main()
