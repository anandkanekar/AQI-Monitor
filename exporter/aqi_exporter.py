#!/usr/bin/env python3
"""Bridges the Flink output topics to Prometheus.

Consumes aqi.enriched / aqi.alerts / aqi.anomalies and exposes:
  aqi_index{sensor_id,city}                             latest AQI per sensor
  aqi_pollutant_concentration{sensor_id,city,pollutant} latest concentration
  aqi_sensor_last_seen_timestamp_seconds{sensor_id,city}
  aqi_readings_total{city,category}
  aqi_alerts_total{alert,severity,status,city}
  aqi_anomalies_total{type,pollutant,city}
  aqi_active_alert{sensor_id,city,metric,severity}      1 while a Flink alert is FIRING

Prometheus alert rules on these series drive Alertmanager notifications.
"""
from __future__ import annotations

import json
import logging
import os
import signal
import sys
import time
from datetime import datetime

from confluent_kafka import Consumer, KafkaError
from prometheus_client import Counter, Gauge, start_http_server

log = logging.getLogger("aqi-exporter")

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_ENRICHED = os.getenv("TOPIC_ENRICHED", "aqi.enriched")
TOPIC_ALERTS = os.getenv("TOPIC_ALERTS", "aqi.alerts")
TOPIC_ANOMALIES = os.getenv("TOPIC_ANOMALIES", "aqi.anomalies")
PORT = int(os.getenv("EXPORTER_PORT", "8000"))
# Readings older than this (e.g. replayed backfill) update counters but not "latest" gauges.
MAX_STALENESS_S = float(os.getenv("MAX_STALENESS_SECONDS", "900"))
POLLUTANTS = ("pm25", "pm10", "no2", "o3", "co2")

AQI = Gauge("aqi_index", "Latest US EPA AQI per sensor", ["sensor_id", "city"])
CONC = Gauge("aqi_pollutant_concentration", "Latest pollutant concentration (ug/m3 for PM, ppb for NO2/O3, ppm for CO2)",
             ["sensor_id", "city", "pollutant"])
LAST_SEEN = Gauge("aqi_sensor_last_seen_timestamp_seconds", "Event time of the latest reading", ["sensor_id", "city"])
READINGS = Counter("aqi_readings_total", "Readings processed", ["city", "category"])
ALERTS = Counter("aqi_alerts_total", "Threshold alerts emitted by Flink", ["alert", "severity", "status", "city"])
ANOMALIES = Counter("aqi_anomalies_total", "Anomalies detected by Flink", ["type", "pollutant", "city"])
ACTIVE = Gauge("aqi_active_alert", "1 while a streaming threshold alert is firing", ["sensor_id", "city", "metric", "severity"])
DECODE_ERRORS = Counter("aqi_exporter_decode_errors_total", "Messages that could not be decoded", ["topic"])
CONSUMED = Counter("aqi_exporter_messages_total", "Messages consumed", ["topic"])


def _event_ts(msg: dict) -> float:
    t = str(msg.get("event_time", ""))
    try:
        return datetime.fromisoformat(t.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return time.time()


_last_seen: dict = {}


def expire_stale_series(now: float) -> None:
    """Drop gauges of silent sensors so stale values don't keep alerts firing.
    The SensorSilent Prometheus rule still fires via aqi_sensor_last_seen_timestamp_seconds."""
    for (sensor, city), ts in list(_last_seen.items()):
        if now - ts <= MAX_STALENESS_S:
            continue
        del _last_seen[(sensor, city)]
        for gauge, extra in [(AQI, [()]), (CONC, [(p,) for p in POLLUTANTS])]:
            for labels in extra:
                try:
                    gauge.remove(sensor, city, *labels)
                except KeyError:
                    pass


def handle_enriched(m: dict) -> None:
    sensor, city = m.get("sensor_id", "unknown"), m.get("city", "unknown")
    READINGS.labels(city, m.get("aqi_category") or "unknown").inc()
    ts = _event_ts(m)
    if time.time() - ts > MAX_STALENESS_S:
        return
    _last_seen[(sensor, city)] = ts
    LAST_SEEN.labels(sensor, city).set(ts)
    if m.get("aqi") is not None:
        AQI.labels(sensor, city).set(m["aqi"])
    for p in POLLUTANTS:
        v = m.get(p)
        if v is not None:
            CONC.labels(sensor, city, p).set(v)


def handle_alert(m: dict) -> None:
    city, sensor, metric = m.get("city", "unknown"), m.get("sensor_id", "unknown"), m.get("metric", "aqi")
    severity, status = m.get("severity", "unknown"), m.get("status", "FIRING")
    ALERTS.labels(m.get("alert", "unknown"), severity, status, city).inc()
    for sev in ("warning", "critical", "emergency"):
        try:
            ACTIVE.remove(sensor, city, metric, sev)
        except KeyError:
            pass
    if status == "FIRING":
        ACTIVE.labels(sensor, city, metric, severity).set(1)


def handle_anomaly(m: dict) -> None:
    ANOMALIES.labels(m.get("type", "unknown"), m.get("pollutant", "unknown"), m.get("city", "unknown")).inc()


def main() -> int:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(name)s %(message)s")
    handlers = {TOPIC_ENRICHED: handle_enriched, TOPIC_ALERTS: handle_alert, TOPIC_ANOMALIES: handle_anomaly}
    start_http_server(PORT)
    log.info("metrics on :%d, consuming %s from %s", PORT, list(handlers), BOOTSTRAP)

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": os.getenv("CONSUMER_GROUP", "aqi-exporter"),
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
        "client.id": "aqi-exporter",
    })
    consumer.subscribe(list(handlers))

    running = True

    def stop(*_):
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    next_gc = time.time() + 60
    try:
        while running:
            if time.time() >= next_gc:
                expire_stale_series(time.time())
                next_gc = time.time() + 60
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.warning("kafka error: %s", msg.error())
                continue
            topic = msg.topic()
            CONSUMED.labels(topic).inc()
            try:
                handlers[topic](json.loads(msg.value()))
            except (ValueError, TypeError, AttributeError) as exc:
                DECODE_ERRORS.labels(topic).inc()
                log.debug("bad message on %s: %s", topic, exc)
    finally:
        consumer.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
