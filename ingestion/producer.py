"""
Kafka producer for EGX tick data.
One topic per ticker: egx.<SYMBOL> (dots replaced with underscores).
"""

import json
import logging

from kafka import KafkaProducer
from kafka.errors import KafkaError

from ingestion.config import KAFKA_BROKER, KAFKA_TOPIC_PREFIX

log = logging.getLogger(__name__)


def _topic_name(symbol: str) -> str:
    safe = symbol.replace(".", "_").lower()
    return f"{KAFKA_TOPIC_PREFIX}.{safe}"


class KafkaTickProducer:
    def __init__(self) -> None:
        self._producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",
            retries=3,
        )
        log.info("KafkaTickProducer connected to %s", KAFKA_BROKER)

    def send(self, symbol: str, tick: dict) -> None:
        topic = _topic_name(symbol)
        future = self._producer.send(topic, key=symbol, value=tick)
        try:
            future.get(timeout=10)
        except KafkaError as exc:
            log.error("Failed to send tick for %s: %s", symbol, exc)

    def close(self) -> None:
        self._producer.flush()
        self._producer.close()
        log.info("KafkaTickProducer closed.")
