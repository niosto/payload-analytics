FROM apache/airflow:2.7.3-python3.11

USER root

# Install OpenJDK for PySpark
RUN apt-get update && apt-get install -y default-jre-headless && apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow