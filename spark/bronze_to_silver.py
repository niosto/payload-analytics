"""
bronze_to_silver.py
PySpark job: Bronze to Silver transformation using JDBC.

Reads raw JSONB records from bronze.bronze_fintech_raw,
flattens/casts them into typed columns, then writes the result
to silver.silver_fintech as a clean, structured table.

Environment variables (set by docker-compose / Airflow):
    POSTGRES_USER     – default: qversity-admin
    POSTGRES_PASSWORD – default: qversity-admin
    POSTGRES_DB       – default: qversity
    POSTGRES_HOST     – default: postgres   (service name inside Docker network)
    POSTGRES_PORT     – default: 5432
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, from_json, explode, row_number, to_json

from schemas.spark_schema import SCHEMA
#  Connection config
PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_DB   = os.getenv("POSTGRES_DB",   "payload_analytics")
PG_USER = os.getenv("POSTGRES_USER", "payload_analytics-admin")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "payload_analytics-admin")

JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"

JDBC_PROPS = {
    "user":     PG_USER,
    "password": PG_PASS,
    "driver":   "org.postgresql.Driver",
}

def deduplicate(df, pk_col):
    """
    Deduplication criteria:
    - Two rows are considered duplicates if they share the same primary key.
    - When duplicates exist, keep the most recent version based on load_timestamp.
    """
    window_spec = Window.partitionBy(pk_col).orderBy(col("load_timestamp").desc())
    return df.withColumn("row_num", row_number().over(window_spec)) \
             .filter(col("row_num") == 1) \
             .drop("row_num")

def main():
    spark = (
        SparkSession.builder
        .appName("payload_bronze_to_silver")
        # PostgreSQL JDBC driver — present in the Spark jars directory
        .config(
            "spark.jars",
            "/opt/airflow/spark/jars/postgresql-42.7.3.jar",
        )
        .getOrCreate()
    )

    #  1. Read Bronze via JDBC 
    bronze_df = (
        spark.read
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "bronze.bronze_fintech_raw")
        .option("user", PG_USER)
        .option("password", PG_PASS)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    #  3. Parse JSONB column
    parsed_df = bronze_df.select(
        from_json(col("raw_data"), SCHEMA).alias("data"),
        col("load_timestamp"),
    ).select("data.*", "load_timestamp")

    # 1. CUSTOMERS
    # Drop array columns (handled separately via explode).
    # credit_info and digital_engagement are StructType — serialize to JSON
    # strings so Spark's JDBC writer can store them in PostgreSQL text columns.
    # dbt will later extract fields using the ::jsonb operator.
    customers_df = parsed_df \
        .drop("accounts", "transactions", "loans") \
        .withColumn("credit_info", to_json(col("credit_info"))) \
        .withColumn("digital_engagement", to_json(col("digital_engagement")))
    customers_df = deduplicate(customers_df, "customer_id")

    # 2. ACCOUNTS
    accounts_df = parsed_df.select(
        col("customer_id"),
        explode(col("accounts")).alias("account"),
        col("load_timestamp"),
    ).select(
        col("customer_id"),
        col("account.*"),
        col("load_timestamp"),
    )
    accounts_df = deduplicate(accounts_df, "account_id")

    # 3. TRANSACTIONS
    transactions_df = parsed_df.select(
        col("customer_id"),
        explode(col("transactions")).alias("transaction"),
        col("load_timestamp"),
    ).select(
        col("customer_id"),
        col("transaction.*"),
        col("load_timestamp"),
    )
    transactions_df = deduplicate(transactions_df, "transaction_id")

    # 4. LOANS
    loans_df = parsed_df.select(
        col("customer_id"),
        explode(col("loans")).alias("loan"),
        col("load_timestamp"),
    ).select(
        col("customer_id"),
        col("loan.*"),
        col("load_timestamp"),
    )
    loans_df = deduplicate(loans_df, "loan_id")

    # 5. credit_info and digital_engagement are now kept as JSON strings
    #    in stg_customers and extracted by dbt using JSONB operators.

    # 6. Write Silver tables via JDBC
    silver_tables = {
        "silver.stg_customers":     customers_df,
        "silver.stg_accounts":      accounts_df,
        "silver.stg_transactions":  transactions_df,
        "silver.stg_loans":         loans_df,
    }

    for table_name, df in silver_tables.items():
        (
        df.write
        .format("jdbc") 
        .option("url", JDBC_URL)
        .option("dbtable", table_name)
        .option("user", PG_USER)
        .option("password", PG_PASS)
        .option("driver", "org.postgresql.Driver")
        .mode("append") 
        .save()
        )
        print(f"[silver] Written {df.count()} records to {table_name}")

    spark.stop()

if __name__ == "__main__":
    main()
