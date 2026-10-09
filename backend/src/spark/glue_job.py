"""Glue entrypoint: one raw JSON file on S3 into silver.stg_*.

S3 is the Bronze layer. Tables must already exist (yoyo migration). Writes use
overwrite + truncate so the definitions stay and the rows are replaced.
"""

import json
import sys

import boto3
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

from schemas.spark_schema import SCHEMA
from transformations import build_silver_frames

SECRET_KEYS = ("host", "port", "dbname", "username", "password")


def load_db_secret(secret_name):
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(response["SecretString"])
    missing = [key for key in SECRET_KEYS if secret.get(key) in (None, "")]
    if missing:
        raise KeyError(f"Secret {secret_name} is missing keys: {', '.join(missing)}")
    return secret


def main():
    args = getResolvedOptions(sys.argv, ["source_path", "secret_name"])
    secret = load_db_secret(args["secret_name"])
    jdbc_url = (
        f"jdbc:postgresql://{secret['host']}:{secret['port']}/{secret['dbname']}"
    )

    spark = SparkSession.builder.getOrCreate()
    parsed_df = (
        spark.read.schema(SCHEMA)
        .option("multiLine", True)
        .json(args["source_path"])
        .withColumn("load_timestamp", current_timestamp())
    )

    for table_name, df in build_silver_frames(parsed_df).items():
        (
            df.write.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", table_name)
            .option("user", secret["username"])
            .option("password", secret["password"])
            .option("driver", "org.postgresql.Driver")
            .option("truncate", "true")
            .mode("overwrite")
            .save()
        )
        print(f"[silver] Replaced {table_name} from {args['source_path']}")

    spark.stop()


if __name__ == "__main__":
    main()
