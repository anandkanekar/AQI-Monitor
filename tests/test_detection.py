import json

import pytest

from aqi_common.detection import (
    AnomalyDetector,
    MalformedReading,
    ThresholdAlerter,
    enrich,
    parse_event_time_ms,
    parse_reading,
)


def reading(**overrides):
    base = {
        "sensor_id": "sensor-001",
        "city": "Delhi",
        "pm25": 20.0,
        "pm10": 40.0,
        "no2": 20.0,
        "o3": 30.0,
        "co2": 450.0,
        "event_time": "2026-09-23T10:00:00.000Z",
    }
    base.update(overrides)
    return base


def test_parse_event_time_handles_z_suffix():
    assert parse_event_time_ms("1970-01-01T00:00:01.500Z") == 1500
    assert parse_event_time_ms("1970-01-01T01:00:00+01:00") == 0


def test_parse_reading_valid():
    parsed = parse_reading(json.dumps(reading(pm25="12.5")))
    assert parsed["pm25"] == 12.5
    assert parsed["event_time_ms"] > 0


@pytest.mark.parametrize(
    "payload", ["not json", "[1,2]", json.dumps({"city": "x"}), json.dumps(reading(event_time="yesterday"))]
)
def test_parse_reading_malformed(payload):
    with pytest.raises(MalformedReading):
        parse_reading(payload)


def test_enrich_ignores_implausible_values():
    out = enrich(parse_reading(json.dumps(reading(pm25=5000.0))))
    assert out["sub_indices"]["pm25"] is None
    assert out["dominant_pollutant"] != "pm25"


def test_anomaly_detector_flags_spike_after_warmup():
    det = AnomalyDetector(warmup=10, z_threshold=4.0, stuck_threshold=1000)
    state = None
    for i in range(30):
        state, anomalies = det.update(state, reading(pm25=20.0 + (i % 3)))
        assert anomalies == []
    state, anomalies = det.update(state, reading(pm25=250.0))
    assert [a["type"] for a in anomalies] == ["spike"]
    assert anomalies[0]["pollutant"] == "pm25"


def test_anomaly_detector_no_alert_during_warmup():
    det = AnomalyDetector(warmup=10)
    state, anomalies = det.update(None, reading(pm25=20.0))
    state, anomalies = det.update(state, reading(pm25=400.0))
    assert anomalies == []


def test_anomaly_detector_out_of_range_does_not_update_baseline():
    det = AnomalyDetector()
    state, _ = det.update(None, reading())
    new_state, anomalies = det.update(state, reading(pm25=-5.0))
    assert anomalies[0]["type"] == "out_of_range"
    assert new_state["pm25"] == state["pm25"]


def test_anomaly_detector_stuck_sensor():
    det = AnomalyDetector(stuck_threshold=5)
    state, found = None, []
    for _ in range(7):
        state, anomalies = det.update(state, reading(pm25=33.3, pm10=None, no2=None, o3=None, co2=None))
        found += anomalies
    assert [a["type"] for a in found] == ["stuck_sensor"]


def test_state_is_json_serialisable():
    state, _ = AnomalyDetector().update(None, reading())
    json.dumps(state)
    state, _ = ThresholdAlerter().evaluate(None, enrich(parse_reading(json.dumps(reading()))))
    json.dumps(state)


def _enriched(minute, **kw):
    return enrich(parse_reading(json.dumps(reading(event_time=f"2026-09-23T10:{minute:02d}:00Z", **kw))))


def test_threshold_alerter_escalation_cooldown_and_resolution():
    alerter = ThresholdAlerter(cooldown_ms=10 * 60 * 1000)
    state, alerts = alerter.evaluate(None, _enriched(0))
    assert alerts == []

    state, alerts = alerter.evaluate(state, _enriched(1, pm25=80.0))  # AQI ~163 -> warning
    assert [(a["status"], a["severity"]) for a in alerts] == [("FIRING", "warning")]

    state, alerts = alerter.evaluate(state, _enriched(2, pm25=85.0))  # same level, within cooldown
    assert alerts == []

    state, alerts = alerter.evaluate(state, _enriched(3, pm25=200.0))  # escalate -> critical
    assert [(a["status"], a["severity"]) for a in alerts] == [("FIRING", "critical")]

    state, alerts = alerter.evaluate(state, _enriched(14, pm25=210.0))  # cooldown elapsed -> re-notify
    assert [a["status"] for a in alerts] == ["FIRING"]

    state, alerts = alerter.evaluate(state, _enriched(15, pm25=10.0))
    assert [(a["status"], a["severity"]) for a in alerts] == [("RESOLVED", "critical")]


def test_threshold_alerter_co2():
    state, alerts = ThresholdAlerter().evaluate(None, _enriched(0, co2=2500.0))
    assert [(a["alert"], a["severity"]) for a in alerts] == [("CO2ThresholdExceeded", "critical")]
