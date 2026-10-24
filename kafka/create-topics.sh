#!/bin/bash
# Idempotently creates the AQI topics. Partition counts are sized for horizontal
# scaling of consumers (Flink source parallelism <= partitions).
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:9092}"
RF="${KAFKA_REPLICATION_FACTOR:-1}"
RAW_PARTITIONS="${AQI_RAW_PARTITIONS:-12}"
KT=/opt/kafka/bin/kafka-topics.sh

echo "Waiting for Kafka at ${BOOTSTRAP}..."
until $KT --bootstrap-server "$BOOTSTRAP" --list >/dev/null 2>&1; do sleep 2; done

create() {
  local topic=$1 partitions=$2; shift 2
  $KT --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
      --topic "$topic" --partitions "$partitions" --replication-factor "$RF" "$@"
}

# Raw sensor readings, keyed by sensor_id. 7 days retention; lz4 compressed by producers.
create aqi.raw "$RAW_PARTITIONS" \
  --config retention.ms=604800000 --config compression.type=producer --config min.insync.replicas=1
# Readings enriched with AQI / category by Flink.
create aqi.enriched "$RAW_PARTITIONS" --config retention.ms=259200000
# Threshold alerts and anomalies (low volume, longer retention for audit).
create aqi.alerts 3 --config retention.ms=2592000000
create aqi.anomalies 3 --config retention.ms=2592000000
# Dead-letter queue for unparseable messages.
create aqi.dlq 3 --config retention.ms=1209600000

$KT --bootstrap-server "$BOOTSTRAP" --describe --topics-with-overrides
echo "Topics ready."
