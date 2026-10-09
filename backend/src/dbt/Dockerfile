FROM python:3.11-slim

WORKDIR /dbt

RUN pip install --no-cache-dir dbt-core==1.12.3 dbt-postgres==1.11.0

COPY . .
RUN chmod +x run.sh

ENV DBT_PROFILES_DIR=/dbt

ENTRYPOINT ["./run.sh"]
