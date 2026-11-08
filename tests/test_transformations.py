import json
import re
from datetime import datetime

import pytest
from pyspark.sql.functions import current_timestamp

from conftest import FIXTURES, MIGRATIONS_DIR
from schemas.spark_schema import SCHEMA
from transformations import build_silver_frames, deduplicate

TABLES = ("silver.stg_customers", "silver.stg_accounts", "silver.stg_transactions", "silver.stg_loans")


def migration_columns():
    """Column names per silver.stg_* table, in order, from the yoyo migration."""
    sql = (MIGRATIONS_DIR / "20260930000000_silver_staging.sql").read_text(encoding="utf-8")
    columns = {}
    for table, body in re.findall(r"CREATE TABLE (silver\.\w+) \((.*?)\);", sql, re.S):
        columns[table] = [line.split()[0] for line in body.strip().splitlines() if line.strip()]
    return columns


@pytest.fixture(scope="module")
def frames(spark):
    parsed = (
        spark.read.schema(SCHEMA)
        .option("multiLine", True)
        .json(str(FIXTURES / "tiny_customers.json"))
        .withColumn("load_timestamp", current_timestamp())
    )
    return build_silver_frames(parsed)


def test_returns_the_four_staging_tables(frames):
    assert tuple(frames) == TABLES


def test_columns_match_the_migration(frames):
    # The Glue job writes with overwrite + truncate into tables created by the
    # migration, so every frame must carry exactly the table's columns.
    expected = migration_columns()
    for table in TABLES:
        assert sorted(frames[table].columns) == sorted(expected[table]), table


def test_every_business_column_stays_a_string(frames):
    for table, df in frames.items():
        for field in df.schema.fields:
            if field.name != "load_timestamp":
                assert field.dataType.simpleString() == "string", (table, field.name)


def test_row_counts_after_explode_and_dedupe(frames):
    counts = {table: frames[table].count() for table in TABLES}
    assert counts == {
        "silver.stg_customers": 2,  # CUST-0000001 appears twice in the file
        "silver.stg_accounts": 3,  # ACC-000000000001 appears twice
        "silver.stg_transactions": 3,
        "silver.stg_loans": 1,
    }


def test_nested_structs_become_json_text(frames):
    rows = {r["customer_id"]: r for r in frames["silver.stg_customers"].collect()}
    lucas = rows["CUST-0000001"]
    assert lucas["first_name"] == "Lucas"
    # Numbers in the source become text under the StringType schema.
    assert lucas["risk_score"] == "68.06"
    # One of the two duplicates is kept; both are valid JSON or NULL.
    if lucas["credit_info"] is not None:
        assert json.loads(lucas["credit_info"])["credit_score"] == "712"
    ana = rows["CUST-0000002"]
    assert ana["credit_info"] is None
    assert json.loads(ana["digital_engagement"])["preferred_channel"] == "branch"


def test_child_rows_keep_customer_id_and_dirty_values(frames):
    tx = {r["transaction_id"]: r for r in frames["silver.stg_transactions"].collect()}
    assert tx["TXN-000000000002"]["customer_id"] == "CUST-0000001"
    assert tx["TXN-000000000002"]["amount"] == "$33593.14"
    assert tx["TXN-000000000002"]["category"] == "N/A"
    assert tx["TXN-000000000003"]["description"] is None

    accounts = {r["account_id"]: r for r in frames["silver.stg_accounts"].collect()}
    assert accounts["ACC-000000000002"]["currency"] == " clp "
    assert accounts["ACC-000000000003"]["customer_id"] == "CUST-0000002"


def test_deduplicate_keeps_newest_load(spark):
    df = spark.createDataFrame(
        [
            ("A", "old", datetime(2026, 1, 1)),
            ("A", "new", datetime(2026, 1, 2)),
            ("B", "only", datetime(2026, 1, 1)),
        ],
        "id string, value string, load_timestamp timestamp",
    )
    result = {r["id"]: r["value"] for r in deduplicate(df, "id").collect()}
    assert result == {"A": "new", "B": "only"}
    assert "row_num" not in deduplicate(df, "id").columns
