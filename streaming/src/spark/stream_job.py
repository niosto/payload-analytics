"""Spark Structured Streaming: Redpanda topics into silver.stream_* on RDS.

Two queries share one Spark session:

  fx_sink  fx.rates -> silver.stream_fx_rates, one row per tick.
  tx_sink  fintech.live_transactions LEFT JOIN fx.rates -> silver.stream_transactions.

The join is by currency and event-time range: a transaction picks up the FX
ticks published in the FX_JOIN_SECONDS before its own event_time, and the
latest of them is kept. It is a left outer join, so a transaction with no tick
in that window is still written, with NULL FX columns, once the watermark
passes; dbt falls back to seed_fx_rates for those rows.

Rows are written with psycopg2 upserts inside foreachBatch, so replaying a
micro-batch after a restart does not duplicate anything. Offsets and join
state live in CHECKPOINT_DIR. A transient database error (dropped SSL
connection, server restart) is retried a few times on a fresh connection
before the batch fails.

processed_at is taken on the driver, once per micro-batch, right before the
upsert, and written explicitly. The producers set event_time with the same
Docker VM clock, so both ends of the latency (processed_at - event_time) come
from one clock. The table default NOW() runs on the RDS clock instead; the VM
clock drifts ahead of it, and that produced negative latencies.

Database credentials come from the RDS master secret in Secrets Manager (the
same secret Glue and dbt use). For a local test database, set DB_HOST and the
other DB_* variables instead and the secret is not read.
"""

import json
import logging
import os
import time
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, from_json, upper, trim
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

logging.basicConfig(level="INFO", format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("stream_job")
# py4j logs every callback from the JVM at INFO; one per micro-batch per query.
logging.getLogger("py4j").setLevel(logging.WARNING)


def env(name, default):
    value = os.getenv(name)
    return default if value in (None, "") else value


KAFKA_BOOTSTRAP = env("KAFKA_BOOTSTRAP", "redpanda:9092")
STARTING_OFFSETS = env("STARTING_OFFSETS", "latest")
CHECKPOINT_DIR = env("CHECKPOINT_DIR", "/checkpoints")
TRIGGER_SECONDS = int(env("TRIGGER_SECONDS", "1"))
WATERMARK = env("WATERMARK", "2 minutes")
FX_JOIN_SECONDS = int(env("FX_JOIN_SECONDS", "60"))
SECRET_NAME = env("DB_SECRET_NAME", "/payload-analytics-rds/MasterCredentials")
DB_RETRY_DELAYS = (1, 2, 4)  # seconds before each retry of a failed upsert

FX_TOPIC = "fx.rates"
TX_TOPIC = "fintech.live_transactions"

FX_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("event_time", StringType()),
        StructField("currency", StringType()),
        StructField("base", StringType()),
        StructField("units_per_usd", DoubleType()),
        StructField("usd_per_unit", DoubleType()),
        StructField("provider", StringType()),
        StructField("provider_updated_at", StringType()),
    ]
)

TX_BUSINESS_FIELDS = [
    "customer_id", "account_id", "date", "amount", "currency", "type",
    "category", "merchant", "channel", "status", "description",
]

# Every field is StringType, as in backend/src/spark/schemas/spark_schema.py:
# casting dirty values belongs in dbt.
TX_SCHEMA = StructType(
    [StructField(name, StringType()) for name in
     ["event_id", "event_time", "source", "transaction_id", *TX_BUSINESS_FIELDS]]
)

FX_COLUMNS = [
    "event_id", "currency", "base", "units_per_usd", "usd_per_unit", "provider",
    "provider_updated_at", "event_time", "kafka_ts",
]

TX_COLUMNS = [
    "transaction_id", *TX_BUSINESS_FIELDS, "event_id", "source", "event_time",
    "kafka_ts", "fx_usd_per_unit", "fx_event_time", "fx_provider_updated_at",
]

# processed_at is not selected from Spark: the driver adds it per micro-batch.
FX_INSERT_COLUMNS = [*FX_COLUMNS, "processed_at"]
TX_INSERT_COLUMNS = [*TX_COLUMNS, "processed_at"]

FX_UPSERT = f"""
    INSERT INTO silver.stream_fx_rates ({", ".join(FX_INSERT_COLUMNS)})
    VALUES %s
    ON CONFLICT (event_id) DO NOTHING
"""

# On replay, keep the first processed_at (it is the latency measurement, so it
# is not in the SET list) and only replace the FX match with a later tick.
TX_UPSERT = f"""
    INSERT INTO silver.stream_transactions AS t ({", ".join(TX_INSERT_COLUMNS)})
    VALUES %s
    ON CONFLICT (transaction_id) DO UPDATE SET
        fx_usd_per_unit        = EXCLUDED.fx_usd_per_unit,
        fx_event_time          = EXCLUDED.fx_event_time,
        fx_provider_updated_at = EXCLUDED.fx_provider_updated_at
    WHERE EXCLUDED.fx_event_time IS NOT NULL
      AND EXCLUDED.fx_event_time > COALESCE(t.fx_event_time, '-infinity'::timestamptz)
"""


def db_settings():
    if os.getenv("DB_HOST"):
        return {
            "host": os.environ["DB_HOST"],
            "port": int(env("DB_PORT", "5432")),
            "dbname": env("DB_NAME", "payload_analytics"),
            "user": os.environ["DB_USER"],
            "password": os.environ["DB_PASSWORD"],
            "sslmode": env("DB_SSLMODE", "prefer"),
        }

    import boto3

    secret = json.loads(
        boto3.client("secretsmanager").get_secret_value(SecretId=SECRET_NAME)["SecretString"]
    )
    missing = [k for k in ("host", "port", "dbname", "username", "password") if not secret.get(k)]
    if missing:
        raise KeyError(f"Secret {SECRET_NAME} is missing keys: {', '.join(missing)}")
    return {
        "host": secret["host"],
        "port": int(secret["port"]),
        "dbname": secret["dbname"],
        "user": secret["username"],
        "password": secret["password"],
        "sslmode": env("DB_SSLMODE", "require"),
    }


class Sink:
    """One reused connection, reopened after a failure. Runs on the driver."""

    def __init__(self, settings):
        self.settings = settings
        self.conn = None

    def _connection(self):
        if self.conn is None or self.conn.closed:
            # collect() returns naive datetimes in the container's zone (UTC);
            # pin the session zone so timestamptz columns read them as UTC.
            self.conn = psycopg2.connect(
                connect_timeout=10, options="-c timezone=UTC", **self.settings
            )
        return self.conn

    def _close(self):
        if self.conn is not None:
            try:
                self.conn.close()
            except Exception:
                pass

    def upsert(self, sql, columns, rows, processed_at):
        """Write rows plus processed_at (aware UTC, from the driver clock).

        The statements are idempotent upserts, so a batch that failed halfway
        is retried whole on a new connection. Only connection-level errors are
        retried; after the last attempt the error is raised and the container
        restarts from the checkpoint.
        """
        if not rows:
            return
        values = [(*(row[c] for c in columns), processed_at) for row in rows]
        for attempt in range(len(DB_RETRY_DELAYS) + 1):
            try:
                conn = self._connection()
                with conn.cursor() as cur:
                    execute_values(cur, sql, values, page_size=500)
                conn.commit()
                return
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as exc:
                self._close()
                if attempt == len(DB_RETRY_DELAYS):
                    raise
                delay = DB_RETRY_DELAYS[attempt]
                log.warning(
                    "database error, retry %d/%d in %ds: %s",
                    attempt + 1, len(DB_RETRY_DELAYS), delay, str(exc).strip(),
                )
                time.sleep(delay)
            except Exception:
                self._close()
                raise


def read_topic(spark, topic, schema):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        .load()
        .select(
            from_json(col("value").cast("string"), schema).alias("e"),
            col("timestamp").alias("kafka_ts"),
        )
        .select("e.*", "kafka_ts")
        .withColumn("event_time", col("event_time").cast("timestamp"))
        .filter(col("event_time").isNotNull())
    )


def main():
    sink = Sink(db_settings())

    spark = (
        SparkSession.builder.appName("payload_analytics_streaming")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "3")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    fx = read_topic(spark, FX_TOPIC, FX_SCHEMA).withColumn(
        "provider_updated_at", col("provider_updated_at").cast("timestamp")
    )
    tx = read_topic(spark, TX_TOPIC, TX_SCHEMA)

    def write_fx(batch_df, batch_id):
        started = time.perf_counter()
        rows = batch_df.select(*FX_COLUMNS).collect()
        collected = time.perf_counter()
        sink.upsert(FX_UPSERT, FX_COLUMNS, rows, datetime.now(timezone.utc))
        log.info(
            "fx batch=%s rows=%d spark_ms=%d db_ms=%d", batch_id, len(rows),
            (collected - started) * 1000, (time.perf_counter() - collected) * 1000,
        )

    fx_query = (
        fx.writeStream.queryName("fx_sink")
        .foreachBatch(write_fx)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/fx_sink")
        .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
        .start()
    )

    fx_side = fx.select(
        upper(trim(col("currency"))).alias("fx_currency"),
        col("usd_per_unit").alias("fx_usd_per_unit"),
        col("event_time").alias("fx_event_time"),
        col("provider_updated_at").alias("fx_provider_updated_at"),
    ).withWatermark("fx_event_time", WATERMARK)

    joined = tx.withWatermark("event_time", WATERMARK).join(
        fx_side,
        expr(
            f"""
            fx_currency = upper(trim(currency))
            AND fx_event_time BETWEEN event_time - INTERVAL {FX_JOIN_SECONDS} SECONDS
                                  AND event_time
            """
        ),
        "leftOuter",
    )

    def write_tx(batch_df, batch_id):
        started = time.perf_counter()
        # Several ticks match each transaction; keep the latest. Batches hold a
        # few seconds of events, so this runs on the driver: a Spark window here
        # costs an extra shuffle per micro-batch, about a second each.
        latest = {}
        for row in batch_df.select(*TX_COLUMNS).collect():
            kept = latest.get(row["transaction_id"])
            if kept is None or (
                row["fx_event_time"] is not None
                and (kept["fx_event_time"] is None or row["fx_event_time"] > kept["fx_event_time"])
            ):
                latest[row["transaction_id"]] = row
        rows = list(latest.values())
        collected = time.perf_counter()
        sink.upsert(TX_UPSERT, TX_COLUMNS, rows, datetime.now(timezone.utc))
        unmatched = sum(1 for r in rows if r["fx_usd_per_unit"] is None)
        log.info(
            "tx batch=%s rows=%d without_fx=%d spark_ms=%d db_ms=%d",
            batch_id, len(rows), unmatched,
            (collected - started) * 1000, (time.perf_counter() - collected) * 1000,
        )

    tx_query = (
        joined.writeStream.queryName("tx_sink")
        .foreachBatch(write_tx)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/tx_sink")
        .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
        .start()
    )

    log.info(
        "started fx_sink=%s tx_sink=%s offsets=%s trigger=%ss watermark=%s join=%ss",
        fx_query.id, tx_query.id, STARTING_OFFSETS, TRIGGER_SECONDS, WATERMARK, FX_JOIN_SECONDS,
    )
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
