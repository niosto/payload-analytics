import json
import requests
import os

from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.schema_postgre import SCHEMA_POSTGRES

# Task 1 helper: S3 download is temporarily disabled.
# def download_s3(params: dict):
#     """Download a JSON file from a public S3 URL and persist it locally."""
#     url = params["url"]
#     data = requests.get(url).text
#     path = params["path"]
#     os.makedirs(os.path.dirname(path), exist_ok=True)
#     with open(path, "w") as f:
#         f.write(data)

# Task 2 helper: Download raw JSON from S3
def create_schemas(params: dict):
    """
    Create bronze and silver schemas in Postgres using PostgresHook.
    """
    hook = PostgresHook(postgres_conn_id=params["postgres_conn_id"])

    hook.run(SCHEMA_POSTGRES)

# Task 2 helper: Load JSON records into Bronze
def load_to_postgres(params: dict):
    """
    Read the downloaded JSON file and insert every
    record into bronze using PostgresHook.

    Expected params:
        path (str): Filesystem path to the downloaded JSON file.
    """

    path = params.get("path") or os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        "data",
        "raw",
        "fintech_banking_dataset.json",
    )
    path = os.path.abspath(path)

    if not os.path.exists(path):
        raise FileNotFoundError(f"Raw JSON file not found at: {path}")

    with open(path, "r") as f:
        data = json.load(f)

    rows = [(json.dumps(rec),) for rec in data]

    hook = PostgresHook(postgres_conn_id=params["postgres_conn_id"])
    hook.insert_rows(
        table="bronze.bronze_fintech_raw",
        rows=rows,
        target_fields=["raw_data"],
        commit_every=500,
    )