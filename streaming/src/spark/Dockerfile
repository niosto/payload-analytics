FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root

# Kafka source for Structured Streaming and its runtime dependencies, baked
# into the image so the job does not resolve packages from Maven on every start.
ARG MAVEN=https://repo1.maven.org/maven2
RUN cd /opt/spark/jars \
    && curl -fsSLO $MAVEN/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar \
    && curl -fsSLO $MAVEN/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar \
    && curl -fsSLO $MAVEN/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
    && curl -fsSLO $MAVEN/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

RUN mkdir -p /checkpoints && chown spark:spark /checkpoints

COPY stream_job.py /app/stream_job.py

USER spark
ENV TZ=UTC
WORKDIR /app

CMD ["/opt/spark/bin/spark-submit", "--master", "local[2]", "--driver-memory", "1g", "/app/stream_job.py"]
