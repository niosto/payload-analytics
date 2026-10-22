# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.1.0] - 15/10/2026

### Added

- Local streaming stack under `streaming/`: Redpanda, a Spark Structured Streaming job, and producers for live transactions and FX ticks
- FX producer that reads real market rates from Binance
- Spark job that joins each transaction to the latest FX tick in a short window and upserts `silver.stream_transactions` and `silver.stream_fx_rates`
- dbt models on that stream: `fx_rates_current`, `live_transactions`, and incremental `fct_live_transactions`, with tests for the FX window, the USD conversion, and the revenue rule
- `to_usd` prefers the live rate and falls back to `seed_fx_rates`
- Batch timing logs and notes on RDS latency for the streaming writes

### Fixed

- Streaming containers shut down on SIGTERM instead of being killed mid-batch
- Serverless plugin manifests included so the backend and infrastructure deploys resolve their plugins

## [1.0.0] - 08/10/2026

### Added

- `infrastructure` subproject, deployed from its own devcontainer:
  - S3 data lake (`raw/`, `processed/`, `failed/`, EventBridge on) and an artifacts bucket (`storage`)
  - Public PostgreSQL RDS in the default VPC, master user `payload_analytics_admin`, database `payload_analytics` (`rds`)
  - ECR repository for the dbt image (`registry`)
  - SNS email topic and a monthly cost budget (`alerts`)
- `backend` subproject, deployed from its own devcontainer:
  - yoyo migration that creates `silver` and the four `stg_*` tables
  - Glue Spark job that flattens one JSON file from S3 into those tables
  - Fargate task that runs `dbt seed`, `dbt run`, and `dbt test` from the ECR image
  - Step Functions state machine: Glue, then dbt, then archive the object to `processed/` or `failed/` with an alert email
- Runbook for uploading the sample file, watching the state machine, and connecting to `payload_analytics` (not the empty `postgres` database)

### Changed

- The local Airflow stack is no longer how the pipeline runs. A file dropped in `raw/` starts the AWS pipeline on its own
- dbt and Spark live under `backend/src/`. Spark only explodes and deduplicates; cleaning stays in dbt

### Removed

- Airflow DAGs, the root Docker Compose file, and the local Spark entrypoint as the way to execute the pipeline

## [0.1.0] - 31/08/2026

Work from 24/08 through 31/08: a local ELT on Docker, Airflow, Spark, and dbt, plus the first dashboard.

### Added

- Project skeleton: Docker image, Compose services, and the first Airflow DAG
- Bronze ingestion of the fintech JSON, then a Spark job that flattens it into Silver staging
- dbt project: cleaning macros, seeds, Silver models, Gold star schema, generic tests, and singular tests
- Airflow task that runs dbt after Spark
- Power BI dashboard on Gold, layer diagrams, and a short write-up of the data findings
- Sample dataset checked into the repo so the pipeline can run without downloading from S3

### Fixed

- dbt is invoked from Airflow, the dbt container can be reached when the DAG starts the task, and `is_delinquent` uses one definition on Silver and Gold
- Gold tables are indexed for the dashboard queries
- The sample file is read from disk while S3 access is unavailable
