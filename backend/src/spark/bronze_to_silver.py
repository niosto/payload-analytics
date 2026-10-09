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
from pyspark.sql.functions import col, from_json

from schemas.spark_schema import SCHEMA
from transformations import build_silver_frames
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

    silver_tables = build_silver_frames(parsed_df)

    # 6. Write Silver tables via JDBC
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
