# syntax=docker/dockerfile:1
# PyFlink image used for the JobManager, TaskManagers and the job submitter.
# Build context is the repository root so the shared aqi_common package can be copied in.
ARG FLINK_VERSION=1.18.1
FROM flink:${FLINK_VERSION}-scala_2.12-java11

ARG FLINK_VERSION=1.18.1
ARG KAFKA_CONNECTOR_VERSION=3.2.0-1.18

RUN apt-get update -y \
 && apt-get install -y --no-install-recommends python3 python3-pip python3-dev curl \
 && rm -rf /var/lib/apt/lists/* \
 && ln -sf /usr/bin/python3 /usr/bin/python \
 && pip3 install --no-cache-dir "apache-flink==${FLINK_VERSION}"

# Kafka connector (SQL fat jar works for both Table API and Python DataStream API).
ADD --chown=flink:flink --chmod=644 \
    https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar \
    /opt/flink/lib/

# Prometheus metrics reporter + S3 filesystem (for checkpoints and landing-zone sink).
RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop \
 && cp /opt/flink/opt/flink-s3-fs-hadoop-*.jar /opt/flink/plugins/s3-fs-hadoop/ \
 && ls /opt/flink/plugins/metrics-prometheus/

COPY --chown=flink:flink flink/jobs/ /opt/flink/jobs/
COPY --chown=flink:flink aqi_common/ /opt/flink/jobs/aqi_common/
# PyFlink ships directories to Python workers by contents, not as a package, so
# also ship aqi_common as a zip that lands on the worker's PYTHONPATH.
RUN python3 -c "import shutil; shutil.make_archive('/opt/flink/jobs/aqi_common', 'zip', '/opt/flink/jobs', 'aqi_common')"
COPY --chown=flink:flink --chmod=755 flink/submit-jobs.sh /opt/flink/bin/submit-jobs.sh
