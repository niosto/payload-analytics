"""Loads the CI fixture into a throwaway Postgres, the way production does.

1. Batch: the JSON sample goes through the real build_silver_frames with local
   Spark and the same JDBC write as the Glue job (overwrite + truncate into the
   tables the migration created).
2. Streaming: one fresh FX tick per seed currency in silver.stream_fx_rates and
   a few live transactions on fixture accounts in silver.stream_transactions,
   shaped like the Spark streaming job's rows (processed_at > event_time, the
   joined tick inside the 60 s window before event_time).

Connection settings come from the same DBT_* variables as profiles.yml.
"""

import csv
import json
import os
import sys
import uuid
from pathlib import Path

import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

REPO_ROOT = Path(__file__).resolve().parents[2]
SPARK_SRC = REPO_ROOT / "backend" / "src" / "spark"
sys.path.insert(0, str(SPARK_SRC))

from schemas.spark_schema import SCHEMA  # noqa: E402
from transformations import build_silver_frames  # noqa: E402

FIXTURE = Path(os.getenv("CI_FIXTURE", REPO_ROOT / "tests" / "fixtures" / "fintech_banking_sample.json"))
JDBC_JAR = SPARK_SRC / "jars" / "postgresql-42.7.3.jar"
SEED_FX = REPO_ROOT / "backend" / "src" / "dbt" / "seeds" / "seed_fx_rates.csv"

DB = {
    "host": os.getenv("DBT_HOST", "localhost"),
    "port": int(os.getenv("DBT_PORT", "5432")),
    "dbname": os.getenv("DBT_DBNAME", "payload_analytics"),
    "user": os.getenv("DBT_USER", "payload_analytics_admin"),
    "password": os.getenv("DBT_PASSWORD", ""),
}


def load_batch():
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("ci-load-fixture")
        .config("spark.jars", str(JDBC_JAR))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    jdbc_url = f"jdbc:postgresql://{DB['host']}:{DB['port']}/{DB['dbname']}"
    parsed_df = (
        spark.read.schema(SCHEMA)
        .option("multiLine", True)
        .json(str(FIXTURE))
        .withColumn("load_timestamp", current_timestamp())
    )
    for table_name, df in build_silver_frames(parsed_df).items():
        (
            df.write.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", table_name)
            .option("user", DB["user"])
            .option("password", DB["password"])
            .option("driver", "org.postgresql.Driver")
            .option("truncate", "true")
            .mode("overwrite")
            .save()
        )
        print(f"[ci] replaced {table_name}")
    spark.stop()


def seed_rates():
    with open(SEED_FX, encoding="utf-8") as f:
        return {row["currency"].strip().upper(): float(row["usd_per_unit"]) for row in csv.DictReader(f)}


def pick_accounts(rates, limit=6):
    """Fixture accounts whose currency has a seed rate, one per currency first."""
    with open(FIXTURE, encoding="utf-8") as f:
        customers = json.load(f)
    picked, seen = [], set()
    for customer in customers:
        for account in customer.get("accounts") or []:
            currency = (account.get("currency") or "").strip().upper()
            if currency in rates and currency not in seen:
                picked.append((customer["customer_id"], account["account_id"], account["currency"]))
                seen.add(currency)
    return picked[:limit]


def load_stream():
    rates = seed_rates()
    accounts = pick_accounts(rates)
    if len(accounts) < 3:
        raise SystemExit("fixture has too few accounts with a seed currency")

    # (amount, type, status, date format) templates in the source's dirty style.
    templates = [
        ("$120.50", "Fee", "completed", "YYYY-MM-DD"),
        ("75.25", "purchase", "pending", "DD/MM/YYYY"),
        ("1500,75", "Transfer", "completed", "YYYYMMDD"),
        ("$9.99", "fee", "failed", "MM-DD-YYYY"),
        ("300", "Deposit", "completed", "YYYY-MM-DD"),
        ("42.10", "Withdrawal", "pending", "YYYY-MM-DD"),
    ]

    with psycopg2.connect(**DB) as conn, conn.cursor() as cur:
        cur.execute("SET TIME ZONE 'UTC'")
        cur.execute("TRUNCATE silver.stream_fx_rates, silver.stream_transactions")

        # One fresh tick per seed currency, close to the seed so the
        # sane-vs-seed test holds; the provider time is a bit older, as on a
        # heartbeat.
        for currency, usd_per_unit in rates.items():
            live = usd_per_unit * 1.02
            cur.execute(
                """
                INSERT INTO silver.stream_fx_rates
                    (event_id, currency, base, units_per_usd, usd_per_unit, provider,
                     provider_updated_at, event_time, kafka_ts, processed_at)
                VALUES (%s, %s, 'USD', %s, %s, 'ci',
                        NOW() - INTERVAL '45 seconds', NOW() - INTERVAL '30 seconds',
                        NOW() - INTERVAL '30 seconds', NOW() - INTERVAL '29 seconds')
                """,
                (str(uuid.uuid4()), currency, 1.0 / live, live),
            )

        # Live transactions. The last one has no joined tick, so dbt falls back
        # to fx_rates_current, like a transaction during an FX outage.
        for i, ((customer_id, account_id, currency), template) in enumerate(zip(accounts, templates)):
            amount, tx_type, status, date_mask = template
            joined = i < len(accounts) - 1
            live = rates[currency.strip().upper()] * 1.02
            cur.execute(
                f"""
                INSERT INTO silver.stream_transactions
                    (transaction_id, customer_id, account_id, date, amount, currency, type,
                     category, merchant, channel, status, description, event_id, source,
                     event_time, kafka_ts, fx_usd_per_unit, fx_event_time,
                     fx_provider_updated_at, processed_at)
                VALUES (%s, %s, %s, TO_CHAR(NOW(), '{date_mask}'), %s, %s, %s,
                        'shopping', 'N/A', ' POS ', %s, 'ci live transaction', %s, 'ci',
                        NOW() - INTERVAL '20 seconds', NOW() - INTERVAL '19 seconds',
                        %s,
                        CASE WHEN %s THEN NOW() - INTERVAL '30 seconds' END,
                        CASE WHEN %s THEN NOW() - INTERVAL '45 seconds' END,
                        NOW())
                """,
                (
                    f"LTX-{uuid.uuid4().hex[:12].upper()}", customer_id, account_id, amount,
                    currency, tx_type, status, str(uuid.uuid4()),
                    live if joined else None, joined, joined,
                ),
            )
        print(f"[ci] inserted {len(rates)} FX ticks and {len(accounts)} live transactions")


if __name__ == "__main__":
    load_batch()
    load_stream()
