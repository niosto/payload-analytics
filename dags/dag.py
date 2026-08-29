import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from utils.functions import download_s3, create_schemas, load_to_postgres
from utils.spark_functions import run_spark_bronze_to_silver

default_args = {
    "owner": "payload_analytics",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "payload_analytics_fintech_pipeline",
    default_args=default_args,
    description="payload_analytics Fintech/Banking ELT Pipeline",
    schedule_interval=None,
    catchup=False,
    tags=["payload_analytics", "fintech"],
)

# Task 1: Download JSON from S3 and load to Bronze
download_raw_data = PythonOperator(
    task_id="download_raw_data",
    python_callable=download_s3,
    dag=dag,
    params={
        "url": "https://qversity-raw-public-data.s3.amazonaws.com/fintech_banking_dataset.json",
        "path": "/opt/airflow/data/raw/fintech_banking_dataset.json",
    }
)

# Task 2: Create schemas
create_schemas = PythonOperator(
    task_id="create_schemas",
    python_callable=create_schemas,
    dag=dag,
    params={
        "postgres_conn_id": "postgres_default"
    },
)

# Task 3: Load raw JSON records into bronze_fintech_raw via PostgresHook
load_bronze = PythonOperator(
    task_id="load_to_postgres",
    python_callable=load_to_postgres,
    dag=dag,
    params={
        "path": "/opt/airflow/data/raw/fintech_banking_dataset.json",
        "postgres_conn_id": "postgres_default"
    },
)

# Task 4: PySpark - Bronze to Silver via JDBC
spark_bronze_to_silver = PythonOperator(
    task_id="spark_bronze_to_silver",
    python_callable=run_spark_bronze_to_silver,
    params={
        "spark_script_dir": "/opt/airflow/spark/"
    },
    dag=dag,
)

# Task dependencies:
download_raw_data >> create_schemas >> load_bronze >> spark_bronze_to_silver