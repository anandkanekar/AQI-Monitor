"""Real-time AQI threshold detection and anomaly alerting (PyFlink DataStream API).

Topology
  aqi.raw (Kafka)
    -> ParseAndEnrich         : JSON validation, AQI computation     (malformed -> aqi.dlq)
    -> key_by(sensor_id)
    -> DetectAlertsAndAnomalies: keyed state per sensor
         * ThresholdAlerter  : AQI / CO2 severity escalation, cooldown, resolution -> aqi.alerts
         * AnomalyDetector   : EWMA z-score, out-of-range, stuck sensor           -> aqi.anomalies
    -> aqi.enriched (every reading with AQI attached)

State is checkpointed to MinIO (s3://flink/checkpoints) so the job resumes exactly where
it stopped; Kafka offsets are committed on checkpoint (visible as consumer lag in Prometheus).
"""
from __future__ import annotations

import json
import logging
import os
import sys

from pyflink.common import Duration, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import (
    CheckpointingMode,
    KeyedProcessFunction,
    OutputTag,
    ProcessFunction,
    RuntimeContext,
    StreamExecutionEnvironment,
)
from pyflink.datastream.connectors.base import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetResetStrategy,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.state import StateTtlConfig, ValueStateDescriptor
from pyflink.common.time import Time

JOBS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, JOBS_DIR)

from aqi_common.detection import (  # noqa: E402
    AnomalyDetector,
    MalformedReading,
    ThresholdAlerter,
    enrich,
    parse_reading,
)

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_RAW = os.getenv("TOPIC_RAW", "aqi.raw")
TOPIC_ENRICHED = os.getenv("TOPIC_ENRICHED", "aqi.enriched")
TOPIC_ALERTS = os.getenv("TOPIC_ALERTS", "aqi.alerts")
TOPIC_ANOMALIES = os.getenv("TOPIC_ANOMALIES", "aqi.anomalies")
TOPIC_DLQ = os.getenv("TOPIC_DLQ", "aqi.dlq")
ALERT_COOLDOWN_MS = int(os.getenv("ALERT_COOLDOWN_MINUTES", "15")) * 60 * 1000
ANOMALY_Z = float(os.getenv("ANOMALY_Z_THRESHOLD", "4.0"))
PARALLELISM = int(os.getenv("DETECTION_PARALLELISM", "2"))

DLQ_TAG = OutputTag("dlq", Types.STRING())
ALERT_TAG = OutputTag("alerts", Types.STRING())
ANOMALY_TAG = OutputTag("anomalies", Types.STRING())


def _dumps(obj) -> str:
    return json.dumps(obj, separators=(",", ":"), default=str)


class ParseAndEnrich(ProcessFunction):
    def open(self, runtime_context: RuntimeContext):
        group = runtime_context.get_metrics_group().add_group("aqi")
        self.malformed = group.counter("malformed_readings")

    def process_element(self, value, ctx):
        try:
            reading = enrich(parse_reading(value))
        except MalformedReading as exc:
            self.malformed.inc()
            yield DLQ_TAG, _dumps({"error": str(exc), "payload": value[:4096]})
            return
        yield reading


class DetectAlertsAndAnomalies(KeyedProcessFunction):
    def open(self, runtime_context: RuntimeContext):
        # Expire state for sensors that went silent for a week (bounded state size).
        ttl = StateTtlConfig.new_builder(Time.days(7)) \
            .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite) \
            .cleanup_full_snapshot() \
            .build()
        desc = ValueStateDescriptor("sensor_state", Types.STRING())
        desc.enable_time_to_live(ttl)
        self.state = runtime_context.get_state(desc)
        self.alerter = ThresholdAlerter(cooldown_ms=ALERT_COOLDOWN_MS)
        self.detector = AnomalyDetector(z_threshold=ANOMALY_Z)

        group = runtime_context.get_metrics_group().add_group("aqi")
        self.readings = group.counter("readings_processed")
        self.alerts = group.counter("alerts_emitted")
        self.anomalies = group.counter("anomalies_detected")

    def process_element(self, reading, ctx):
        raw_state = self.state.value()
        state = json.loads(raw_state) if raw_state else {}

        alert_state, alerts = self.alerter.evaluate(state.get("alerts"), reading)
        anomaly_state, anomalies = self.detector.update(state.get("anomaly"), reading)
        self.state.update(_dumps({"alerts": alert_state, "anomaly": anomaly_state}))

        self.readings.inc()
        for alert in alerts:
            self.alerts.inc()
            alert["aqi"] = reading.get("aqi")
            yield ALERT_TAG, _dumps(alert)
        for anomaly in anomalies:
            self.anomalies.inc()
            yield ANOMALY_TAG, _dumps(anomaly)

        out = {k: v for k, v in reading.items() if k != "event_time_ms"}
        out["anomaly_count"] = len(anomalies)
        yield _dumps(out)


def kafka_sink(topic: str) -> KafkaSink:
    return (
        KafkaSink.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .set_property("compression.type", "lz4")
        .build()
    )


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(PARALLELISM)
    env.enable_checkpointing(30_000, CheckpointingMode.AT_LEAST_ONCE)
    env.get_checkpoint_config().set_min_pause_between_checkpoints(10_000)
    env.get_checkpoint_config().set_checkpoint_timeout(120_000)
    env.add_python_file(os.path.join(JOBS_DIR, "aqi_common.zip"))

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_topics(TOPIC_RAW)
        .set_group_id("aqi-detection")
        .set_starting_offsets(KafkaOffsetsInitializer.committed_offsets(KafkaOffsetResetStrategy.EARLIEST))
        .set_value_only_deserializer(SimpleStringSchema())
        .set_property("partition.discovery.interval.ms", "60000")
        .set_property("commit.offsets.on.checkpoint", "true")
        .build()
    )

    raw = env.from_source(
        source,
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(30)),
        "kafka-aqi-raw",
    ).uid("kafka-aqi-raw")

    parsed = raw.process(ParseAndEnrich()).name("parse-and-enrich").uid("parse-and-enrich")
    parsed.get_side_output(DLQ_TAG).sink_to(kafka_sink(TOPIC_DLQ)).name("dlq-sink").uid("dlq-sink")

    detected = (
        parsed.key_by(lambda r: r["sensor_id"], key_type=Types.STRING())
        .process(DetectAlertsAndAnomalies(), output_type=Types.STRING())
        .name("detect-alerts-anomalies")
        .uid("detect-alerts-anomalies")
    )
    detected.sink_to(kafka_sink(TOPIC_ENRICHED)).name("enriched-sink").uid("enriched-sink")
    detected.get_side_output(ALERT_TAG).sink_to(kafka_sink(TOPIC_ALERTS)).name("alerts-sink").uid("alerts-sink")
    detected.get_side_output(ANOMALY_TAG).sink_to(kafka_sink(TOPIC_ANOMALIES)).name("anomalies-sink").uid("anomalies-sink")

    env.execute("aqi-detection")


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
    main()
