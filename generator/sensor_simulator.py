#!/usr/bin/env python3
"""Synthetic AQI sensor fleet that streams readings into Kafka.

Features
  * N sensors spread over several cities, each with a city-specific pollution baseline
  * diurnal traffic pattern (morning / evening rush hour peaks)
  * city-wide pollution episodes (multi-minute AQI surges -> threshold alerts)
  * per-sensor faults: single-reading spikes, negative values, stuck sensors (-> anomalies)
  * optional malformed messages (-> dead-letter queue)
  * backfill mode to generate historical data with past event timestamps

Examples
  python sensor_simulator.py --bootstrap localhost:9094 --sensors 50 --interval 5
  python sensor_simulator.py --bootstrap localhost:9094 --backfill-hours 48 --backfill-step 300 --exit-after-backfill
  python sensor_simulator.py --dry-run --sensors 3 --max-messages 5
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import random
import signal
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterator, List, Optional

log = logging.getLogger("sensor-simulator")

# name: (lat, lon, pm25, pm10, no2 ppb, o3 ppb, co2 ppm) baselines
CITIES: Dict[str, tuple] = {
    "Delhi": (28.6139, 77.2090, 85.0, 160.0, 45.0, 35.0, 520.0),
    "Mumbai": (19.0760, 72.8777, 45.0, 95.0, 30.0, 30.0, 480.0),
    "Bengaluru": (12.9716, 77.5946, 30.0, 60.0, 25.0, 28.0, 460.0),
    "Beijing": (39.9042, 116.4074, 55.0, 100.0, 35.0, 40.0, 490.0),
    "London": (51.5072, -0.1276, 10.0, 18.0, 20.0, 30.0, 430.0),
    "New York": (40.7128, -74.0060, 8.0, 16.0, 18.0, 32.0, 425.0),
    "Los Angeles": (34.0522, -118.2437, 12.0, 28.0, 22.0, 48.0, 440.0),
    "Sydney": (-33.8688, 151.2093, 6.0, 15.0, 10.0, 25.0, 415.0),
}
POLLUTANTS = ("pm25", "pm10", "no2", "o3", "co2")


def utc_iso(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def diurnal_factor(ts: datetime, pollutant: str, longitude: float = 0.0) -> float:
    """Traffic-driven daily cycle in local solar time; ozone peaks in the afternoon instead."""
    h = (ts.hour + ts.minute / 60.0 + longitude / 15.0) % 24
    if pollutant == "o3":
        return 0.6 + 0.8 * math.exp(-((h - 15.0) ** 2) / 8.0)
    rush = math.exp(-((h - 8.5) ** 2) / 3.0) + math.exp(-((h - 18.5) ** 2) / 4.0)
    return 0.75 + 0.6 * rush


@dataclass
class Episode:
    city: str
    start: datetime
    end: datetime
    multiplier: float

    def factor(self, ts: datetime) -> float:
        if not (self.start <= ts <= self.end):
            return 1.0
        total = (self.end - self.start).total_seconds()
        progress = (ts - self.start).total_seconds() / total
        return 1.0 + (self.multiplier - 1.0) * math.sin(math.pi * progress)  # smooth rise and fall


@dataclass
class Sensor:
    sensor_id: str
    city: str
    latitude: float
    longitude: float
    baseline: Dict[str, float]
    firmware: str
    bias: float = 1.0
    stuck_until: Optional[datetime] = None
    stuck_values: Dict[str, float] = field(default_factory=dict)


class Fleet:
    def __init__(self, n_sensors: int, anomaly_rate: float, episode_rate: float, seed: Optional[int]):
        self.rng = random.Random(seed)
        self.anomaly_rate = anomaly_rate
        self.episode_rate = episode_rate
        self.episodes: List[Episode] = []
        cities = list(CITIES)
        self.sensors = []
        for i in range(n_sensors):
            city = cities[i % len(cities)]
            lat, lon, *base = CITIES[city]
            self.sensors.append(Sensor(
                sensor_id=f"{city.lower().replace(' ', '-')}-{i:04d}",
                city=city,
                latitude=round(lat + self.rng.uniform(-0.15, 0.15), 5),
                longitude=round(lon + self.rng.uniform(-0.15, 0.15), 5),
                baseline=dict(zip(POLLUTANTS, base)),
                firmware=self.rng.choice(["2.3.1", "2.4.0", "2.4.2"]),
                bias=self.rng.uniform(0.85, 1.15),
            ))

    def _maybe_start_episode(self, ts: datetime, tick_seconds: float) -> None:
        self.episodes = [e for e in self.episodes if e.end >= ts]
        # episode_rate = expected episodes per city per hour
        p = self.episode_rate * tick_seconds / 3600.0
        for city in CITIES:
            if any(e.city == city for e in self.episodes):
                continue
            if self.rng.random() < p:
                duration = timedelta(minutes=self.rng.uniform(10, 45))
                ep = Episode(city, ts, ts + duration, self.rng.uniform(2.5, 5.0))
                self.episodes.append(ep)
                log.info("pollution episode in %s x%.1f for %s", city, ep.multiplier, duration)

    def _episode_factor(self, city: str, ts: datetime) -> float:
        f = 1.0
        for e in self.episodes:
            if e.city == city:
                f = max(f, e.factor(ts))
        return f

    def tick(self, ts: datetime, tick_seconds: float) -> Iterator[dict]:
        self._maybe_start_episode(ts, tick_seconds)
        for s in self.sensors:
            ep = self._episode_factor(s.city, ts)
            values = {}
            for p in POLLUTANTS:
                base = s.baseline[p] * s.bias
                if p == "co2":
                    # CO2 is mostly ambient (~420ppm) plus a traffic-driven component
                    v = 400 + (base - 400) * diurnal_factor(ts, p, s.longitude) * (1 + 0.3 * (ep - 1))
                    v += self.rng.gauss(0, 8)
                else:
                    v = base * diurnal_factor(ts, p, s.longitude) * (ep if p != "o3" else 1 + 0.2 * (ep - 1))
                    v *= max(0.05, self.rng.gauss(1.0, 0.07))
                values[p] = round(max(v, 0.0), 2)

            values = self._inject_faults(s, ts, values)
            yield {
                "schema_version": 1,
                "sensor_id": s.sensor_id,
                "city": s.city,
                "latitude": s.latitude,
                "longitude": s.longitude,
                **values,
                "firmware": s.firmware,
                "event_time": utc_iso(ts),
            }

    def _inject_faults(self, s: Sensor, ts: datetime, values: Dict[str, float]) -> Dict[str, float]:
        if s.stuck_until and ts <= s.stuck_until:
            return dict(s.stuck_values)
        s.stuck_until = None
        if self.rng.random() >= self.anomaly_rate:
            return values
        fault = self.rng.choices(["spike", "negative", "stuck", "dropout"], weights=[6, 2, 1, 1])[0]
        p = self.rng.choice(POLLUTANTS)
        if fault == "spike":
            values[p] = round(values[p] * self.rng.uniform(6, 12), 2)
        elif fault == "negative":
            values[p] = -round(self.rng.uniform(1, 50), 2)
        elif fault == "dropout":
            values[p] = None
        else:
            s.stuck_until = ts + timedelta(minutes=self.rng.uniform(5, 20))
            s.stuck_values = dict(values)
        log.debug("injected %s on %s/%s", fault, s.sensor_id, p)
        return values


class Sink:
    def send(self, key: str, value: bytes) -> None: ...
    def poll(self) -> None: ...
    def flush(self) -> None: ...


class StdoutSink(Sink):
    def send(self, key: str, value: bytes) -> None:
        sys.stdout.write(value.decode() + "\n")

    def poll(self) -> None:
        pass

    def flush(self) -> None:
        sys.stdout.flush()


class KafkaSink(Sink):
    def __init__(self, bootstrap: str, topic: str):
        from confluent_kafka import Producer  # imported lazily so --dry-run works without the lib

        self.topic = topic
        self.errors = 0
        self.producer = Producer({
            "bootstrap.servers": bootstrap,
            "client.id": "aqi-sensor-simulator",
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": "lz4",
            "linger.ms": 50,
            "batch.num.messages": 10000,
            "message.timeout.ms": 120000,
        })

    def _on_delivery(self, err, msg) -> None:
        if err is not None:
            self.errors += 1
            log.error("delivery failed: %s", err)

    def send(self, key: str, value: bytes) -> None:
        while True:
            try:
                # key = sensor_id -> per-sensor ordering within a partition
                self.producer.produce(self.topic, key=key.encode(), value=value, on_delivery=self._on_delivery)
                return
            except BufferError:
                self.producer.poll(0.5)

    def poll(self) -> None:
        self.producer.poll(0)

    def flush(self) -> None:
        self.producer.flush(30)


def parse_args(argv=None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bootstrap", default="localhost:9094", help="Kafka bootstrap servers")
    ap.add_argument("--topic", default="aqi.raw")
    ap.add_argument("--sensors", type=int, default=40)
    ap.add_argument("--interval", type=float, default=5.0, help="seconds between readings per sensor (live)")
    ap.add_argument("--anomaly-rate", type=float, default=0.002, help="probability of a fault per reading")
    ap.add_argument("--episode-rate", type=float, default=0.5, help="pollution episodes per city per hour")
    ap.add_argument("--malformed-rate", type=float, default=0.0, help="probability of emitting a corrupt message")
    ap.add_argument("--backfill-hours", type=float, default=0.0, help="generate this many hours of history first")
    ap.add_argument("--backfill-step", type=float, default=300.0, help="seconds between readings in backfill")
    ap.add_argument("--exit-after-backfill", action="store_true")
    ap.add_argument("--max-messages", type=int, default=0, help="stop after N messages (0 = unlimited)")
    ap.add_argument("--seed", type=int, default=None)
    ap.add_argument("--dry-run", action="store_true", help="print JSON to stdout instead of Kafka")
    ap.add_argument("--log-level", default="INFO")
    return ap.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=args.log_level, format="%(asctime)s %(levelname)s %(name)s %(message)s",
                        stream=sys.stderr)
    fleet = Fleet(args.sensors, args.anomaly_rate, args.episode_rate, args.seed)
    sink: Sink = StdoutSink() if args.dry_run else KafkaSink(args.bootstrap, args.topic)

    running = True

    def stop(*_):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    sent = 0

    def emit(ts: datetime, tick: float) -> bool:
        nonlocal sent
        for msg in fleet.tick(ts, tick):
            payload = json.dumps(msg, separators=(",", ":")).encode()
            if args.malformed_rate and fleet.rng.random() < args.malformed_rate:
                payload = payload[: len(payload) // 2]
            sink.send(msg["sensor_id"], payload)
            sent += 1
            if args.max_messages and sent >= args.max_messages:
                return False
        sink.poll()
        return True

    now = datetime.now(timezone.utc)
    if args.backfill_hours > 0:
        ts = now - timedelta(hours=args.backfill_hours)
        log.info("backfilling %.1fh of history from %s (step %.0fs)", args.backfill_hours, utc_iso(ts), args.backfill_step)
        while running and ts < now:
            if not emit(ts, args.backfill_step):
                running = False
            ts += timedelta(seconds=args.backfill_step)
        sink.flush()
        log.info("backfill complete: %d messages", sent)
        if args.exit_after_backfill:
            return 0

    log.info("streaming %d sensors every %.1fs to %s", args.sensors, args.interval,
             "stdout" if args.dry_run else f"{args.bootstrap}/{args.topic}")
    next_tick = time.monotonic()
    last_report = time.monotonic()
    while running:
        if not emit(datetime.now(timezone.utc), args.interval):
            break
        if time.monotonic() - last_report > 60:
            log.info("sent %d messages", sent)
            last_report = time.monotonic()
        next_tick += args.interval
        time.sleep(max(0.0, next_tick - time.monotonic()))
    sink.flush()
    log.info("stopped after %d messages", sent)
    return 0


if __name__ == "__main__":
    sys.exit(main())
