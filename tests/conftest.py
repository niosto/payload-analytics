"""Shared setup: import paths, stubs for broker/network clients, Spark session."""

import importlib.util
import os
import sys
import types
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURES = REPO_ROOT / "tests" / "fixtures"
PRODUCERS_DIR = REPO_ROOT / "streaming" / "src" / "producers"
SPARK_DIR = REPO_ROOT / "backend" / "src" / "spark"
MIGRATIONS_DIR = REPO_ROOT / "backend" / "database" / "rds" / "migrations"

for path in (PRODUCERS_DIR, SPARK_DIR):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

# The producers import their Kafka and WebSocket clients at module level. The
# tests only exercise pure functions, so stub them when they are not installed:
# no broker and no network are needed.
if importlib.util.find_spec("confluent_kafka") is None:
    stub = types.ModuleType("confluent_kafka")

    class Producer:  # pragma: no cover - never instantiated in tests
        def __init__(self, *args, **kwargs):
            raise RuntimeError("confluent_kafka is stubbed in tests")

    stub.Producer = Producer
    sys.modules["confluent_kafka"] = stub

if importlib.util.find_spec("websockets") is None:
    stub = types.ModuleType("websockets")

    class WebSocketException(Exception):
        pass

    def connect(*args, **kwargs):  # pragma: no cover
        raise RuntimeError("websockets is stubbed in tests")

    stub.WebSocketException = WebSocketException
    stub.connect = connect
    sys.modules["websockets"] = stub

# Module-level settings must use the code defaults, not a developer's env.
for name in ("FX_MARKET_SYMBOLS", "FX_MARKET_HEARTBEAT_SECONDS", "FX_MARKET_PUBLISH_MS"):
    os.environ.pop(name, None)


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("payload-analytics-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
