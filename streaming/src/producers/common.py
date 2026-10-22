"""Shared Kafka producer setup for the FX and transaction producers.

Both producers write JSON values keyed by a business id, wait for every
replica to acknowledge (acks=all), and flush on SIGTERM so `docker compose
stop` does not drop buffered events.
"""

import json
import logging
import os
import signal
from datetime import datetime, timezone

from confluent_kafka import Producer

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)


def env(name, default):
    """Read an env var, treating an empty value as unset."""
    value = os.getenv(name)
    return default if value in (None, "") else value


def utc_now_iso():
    """Current UTC time, ISO 8601 with milliseconds, used as event_time."""
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


class StopFlag:
    """Flips to True on SIGTERM or SIGINT so the main loop can exit cleanly."""

    def __init__(self):
        self.stopped = False
        signal.signal(signal.SIGTERM, self._stop)
        signal.signal(signal.SIGINT, self._stop)

    def _stop(self, *_):
        self.stopped = True


class JsonProducer:
    def __init__(self, client_id):
        self.log = logging.getLogger(client_id)
        self.failed = 0
        self.producer = Producer(
            {
                "bootstrap.servers": env("KAFKA_BOOTSTRAP", "redpanda:9092"),
                "client.id": client_id,
                "acks": "all",
                "enable.idempotence": True,
                "linger.ms": 50,
            }
        )

    def _on_delivery(self, err, msg):
        if err is not None:
            self.failed += 1
            self.log.error("delivery failed topic=%s err=%s", msg.topic(), err)

    def send(self, topic, key, value):
        self.producer.produce(
            topic,
            key=key.encode("utf-8"),
            value=json.dumps(value, ensure_ascii=False).encode("utf-8"),
            on_delivery=self._on_delivery,
        )
        self.producer.poll(0)

    def close(self):
        remaining = self.producer.flush(10)
        if remaining:
            self.log.warning("%d events were not delivered before exit", remaining)
