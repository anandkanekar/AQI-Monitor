"""Stateless-by-design detection logic for the streaming job.

All state is passed in and returned as plain JSON-serialisable dicts so it can be
kept in Flink keyed ValueState (or any other state store) and unit-tested without
a Flink runtime. Must remain compatible with Python 3.10.
"""
from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from aqi_common.aqi import ALL_POLLUTANTS, PLAUSIBLE_RANGES, compute_aqi

REQUIRED_FIELDS = ("sensor_id", "event_time")

# Severity levels ordered by escalation.
SEVERITY_NONE = 0
SEVERITY_WARNING = 1
SEVERITY_CRITICAL = 2
SEVERITY_EMERGENCY = 3
SEVERITY_NAMES = {
    SEVERITY_NONE: "none",
    SEVERITY_WARNING: "warning",
    SEVERITY_CRITICAL: "critical",
    SEVERITY_EMERGENCY: "emergency",
}

# AQI >= value -> severity
AQI_THRESHOLDS: List[Tuple[int, int]] = [
    (301, SEVERITY_EMERGENCY),
    (201, SEVERITY_CRITICAL),
    (151, SEVERITY_WARNING),
]
# CO2 ppm >= value -> severity
CO2_THRESHOLDS: List[Tuple[float, int]] = [
    (5000.0, SEVERITY_EMERGENCY),
    (2000.0, SEVERITY_CRITICAL),
    (1500.0, SEVERITY_WARNING),
]


class MalformedReading(ValueError):
    pass


def parse_event_time_ms(value: str) -> int:
    """Parse ISO-8601 timestamps (with 'Z' or offset) into epoch milliseconds."""
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def parse_reading(raw: str) -> Dict[str, Any]:
    """Parse and validate a raw sensor JSON message. Raises MalformedReading."""
    try:
        data = json.loads(raw)
    except (TypeError, ValueError) as exc:
        raise MalformedReading(f"invalid json: {exc}") from exc
    if not isinstance(data, dict):
        raise MalformedReading("payload is not a JSON object")
    for field in REQUIRED_FIELDS:
        if not data.get(field):
            raise MalformedReading(f"missing field: {field}")
    try:
        data["event_time_ms"] = parse_event_time_ms(str(data["event_time"]))
    except ValueError as exc:
        raise MalformedReading(f"invalid event_time: {data['event_time']}") from exc
    for p in ALL_POLLUTANTS:
        v = data.get(p)
        if v is None:
            continue
        try:
            data[p] = float(v)
        except (TypeError, ValueError):
            data[p] = None
    return data


def enrich(reading: Dict[str, Any]) -> Dict[str, Any]:
    """Attach AQI, category and dominant pollutant to a parsed reading."""
    clean = {
        p: reading.get(p) if _plausible(p, reading.get(p)) else None for p in ALL_POLLUTANTS
    }
    result = compute_aqi(clean)
    out = dict(reading)
    out.update(
        aqi=result["aqi"],
        aqi_category=result["aqi_category"],
        dominant_pollutant=result["dominant_pollutant"],
        co2_level=result["co2_level"],
        sub_indices=result["sub_indices"],
    )
    return out


def _plausible(pollutant: str, value: Optional[float]) -> bool:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return False
    lo, hi = PLAUSIBLE_RANGES[pollutant]
    return lo <= value <= hi


# ---------------------------------------------------------------------------
# Anomaly detection: per-sensor, per-pollutant EWMA z-score + range checks
# ---------------------------------------------------------------------------

class AnomalyDetector:
    """Exponentially weighted mean/variance z-score detector.

    State layout: {pollutant: {"mean": float, "var": float, "n": int, "last": float, "stuck": int}}
    """

    def __init__(
        self,
        alpha: float = 0.1,
        z_threshold: float = 4.0,
        warmup: int = 20,
        min_std: Optional[Dict[str, float]] = None,
        stuck_threshold: int = 30,
    ):
        self.alpha = alpha
        self.z_threshold = z_threshold
        self.warmup = warmup
        self.stuck_threshold = stuck_threshold
        # Floors on std-dev so tiny natural variance doesn't produce false positives.
        self.min_std = min_std or {"pm25": 2.0, "pm10": 4.0, "no2": 3.0, "o3": 3.0, "co2": 25.0}

    def update(
        self, state: Optional[Dict[str, Any]], reading: Dict[str, Any]
    ) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        state = dict(state or {})
        anomalies: List[Dict[str, Any]] = []
        for p in ALL_POLLUTANTS:
            value = reading.get(p)
            if value is None:
                continue
            base = self._base(reading, p, value)
            if not _plausible(p, value):
                anomalies.append(dict(base, type="out_of_range", expected_range=list(PLAUSIBLE_RANGES[p])))
                continue  # never let faulty values pollute the baseline

            s = dict(state.get(p) or {"mean": value, "var": 0.0, "n": 0, "last": None, "stuck": 0})
            std = max(math.sqrt(s["var"]), self.min_std.get(p, 1.0))
            z = (value - s["mean"]) / std
            if s["n"] >= self.warmup and abs(z) >= self.z_threshold:
                anomalies.append(
                    dict(base, type="spike" if z > 0 else "drop", zscore=round(z, 2),
                         baseline_mean=round(s["mean"], 3), baseline_std=round(std, 3))
                )

            s["stuck"] = s["stuck"] + 1 if s["last"] is not None and value == s["last"] else 0
            if s["stuck"] == self.stuck_threshold:
                anomalies.append(dict(base, type="stuck_sensor", repeated_readings=s["stuck"]))

            diff = value - s["mean"]
            incr = self.alpha * diff
            s["mean"] = s["mean"] + incr
            s["var"] = (1 - self.alpha) * (s["var"] + diff * incr)
            s["n"] = s["n"] + 1
            s["last"] = value
            state[p] = s
        return state, anomalies

    @staticmethod
    def _base(reading: Dict[str, Any], pollutant: str, value: float) -> Dict[str, Any]:
        return {
            "sensor_id": reading.get("sensor_id"),
            "city": reading.get("city"),
            "pollutant": pollutant,
            "value": value,
            "event_time": reading.get("event_time"),
        }


# ---------------------------------------------------------------------------
# Threshold alerts with escalation + cooldown de-duplication
# ---------------------------------------------------------------------------

def _severity_for(value: Optional[float], thresholds) -> int:
    if value is None:
        return SEVERITY_NONE
    for limit, sev in thresholds:
        if value >= limit:
            return sev
    return SEVERITY_NONE


class ThresholdAlerter:
    """Emits FIRING alerts on escalation or after a cooldown, RESOLVED on recovery.

    Uses event time so replays / backfills do not flood downstream channels.
    State layout: {"aqi": {"level": int, "last_emit_ms": int}, "co2": {...}}
    """

    def __init__(self, cooldown_ms: int = 15 * 60 * 1000):
        self.cooldown_ms = cooldown_ms

    def evaluate(
        self, state: Optional[Dict[str, Any]], reading: Dict[str, Any]
    ) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        state = dict(state or {})
        now = int(reading["event_time_ms"])
        alerts: List[Dict[str, Any]] = []
        checks = (
            ("aqi", reading.get("aqi"), AQI_THRESHOLDS),
            ("co2", reading.get("co2") if _plausible("co2", reading.get("co2")) else None, CO2_THRESHOLDS),
        )
        for metric, value, thresholds in checks:
            prev = state.get(metric) or {"level": SEVERITY_NONE, "last_emit_ms": 0}
            level = _severity_for(value, thresholds)
            status = None
            if level > prev["level"]:
                status = "FIRING"
            elif level == prev["level"] and level > SEVERITY_NONE and now - prev["last_emit_ms"] >= self.cooldown_ms:
                status = "FIRING"
            elif level == SEVERITY_NONE and prev["level"] > SEVERITY_NONE:
                status = "RESOLVED"

            if status:
                alerts.append({
                    "alert": "AQIThresholdExceeded" if metric == "aqi" else "CO2ThresholdExceeded",
                    "status": status,
                    "severity": SEVERITY_NAMES[level if status == "FIRING" else prev["level"]],
                    "metric": metric,
                    "value": value,
                    "sensor_id": reading.get("sensor_id"),
                    "city": reading.get("city"),
                    "aqi_category": reading.get("aqi_category"),
                    "dominant_pollutant": reading.get("dominant_pollutant"),
                    "event_time": reading.get("event_time"),
                })
                state[metric] = {"level": level, "last_emit_ms": now}
            elif level < prev["level"]:
                # De-escalation (but not to NONE): track the new level without notifying.
                state[metric] = {"level": level, "last_emit_ms": prev["last_emit_ms"]}
        return state, alerts
