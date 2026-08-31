FROM apache/airflow:2.7.3-python3.11

USER root

# Install OpenJDK for PySpark
RUN apt-get update && apt-get install -y default-jre-headless && apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow

# dbt lives in its own virtualenv: its pinned dependencies (jinja2, click,
# protobuf...) would otherwise collide with Airflow's constraint-managed ones.
# The DAG invokes it by absolute path, so neither the Docker CLI nor a
# bind-mounted docker socket is required to run dbt.
# Versions are pinned to the same pair the dbt service uses, so the pipeline
# behaves identically whether dbt runs from Airflow or by hand.
#
# The Airflow image ships with PIP_USER=true so that pip installs into
# ~/.local. That flag is incompatible with installing into a virtualenv
# ("Can not perform a '--user' install"), so it is turned off for this step
# and restored afterwards, since the runtime _PIP_ADDITIONAL_REQUIREMENTS
# install still expects the image default.
ENV PIP_USER=false

RUN python -m venv /opt/airflow/dbt_venv \
    && /opt/airflow/dbt_venv/bin/pip install --no-cache-dir --upgrade pip \
    && /opt/airflow/dbt_venv/bin/pip install --no-cache-dir \
        dbt-core==1.12.3 \
        dbt-postgres==1.11.0

ENV PIP_USER=true
