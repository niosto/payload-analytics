"""Shared Bronze-shaped frames to silver.stg_* tables.

Both the local Airflow job and the Glue job call build_silver_frames.
The input is one row per customer document, with nested arrays still intact
and a load_timestamp column already present.
"""

from pyspark.sql.functions import col, explode, row_number, to_json
from pyspark.sql.window import Window


def deduplicate(df, pk_col):
    """Keep the newest row for each primary key, by load_timestamp."""
    window_spec = Window.partitionBy(pk_col).orderBy(col("load_timestamp").desc())
    return (
        df.withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )


def build_silver_frames(parsed_df):
    """Explode and dedupe a parsed customer dataframe into the four staging tables.

    credit_info and digital_engagement stay JSON strings on stg_customers so the
    JDBC writer can store them in text columns. dbt extracts the fields later.
    """
    customers_df = (
        parsed_df.drop("accounts", "transactions", "loans")
        .withColumn("credit_info", to_json(col("credit_info")))
        .withColumn("digital_engagement", to_json(col("digital_engagement")))
    )
    customers_df = deduplicate(customers_df, "customer_id")

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

    return {
        "silver.stg_customers": customers_df,
        "silver.stg_accounts": accounts_df,
        "silver.stg_transactions": transactions_df,
        "silver.stg_loans": loans_df,
    }
