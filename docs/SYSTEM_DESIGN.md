# AQI Tracking System — System Design Document

*A case-study-style system design document, written the way you'd whiteboard and defend this system in a
system-design interview or architecture review. Every design decision below is backed by what's actually
implemented in this repository — nothing here is hypothetical.*

---

## Table of Contents

1. [Problem Statement](#1-problem-statement)
2. [Requirements](#2-requirements)
3. [Capacity Estimation (Back-of-the-Envelope)](#3-capacity-estimation-back-of-the-envelope)
4. [High-Level Design](#4-high-level-design)
5. [API / Interface Design](#5-api--interface-design)
6. [Data Model](#6-data-model)
7. [Deep Dive: Component Design](#7-deep-dive-component-design)
8. [Scalability](#8-scalability)
9. [Reliability & Fault Tolerance](#9-reliability--fault-tolerance)
10. [Consistency Model](#10-consistency-model)
11. [Trade-offs & Alternatives Considered](#11-trade-offs--alternatives-considered)
12. [Bottlenecks & Mitigations](#12-bottlenecks--mitigations)
13. [Security Considerations](#13-security-considerations)
14. [Observability: SLIs, SLOs, and On-Call](#14-observability-slis-slos-and-on-call)
15. [Interview Q&A Bank](#15-interview-qa-bank)
16. [Follow-Up Design Challenges (Practice Prompts)](#16-follow-up-design-challenges-practice-prompts)
17. [Glossary](#17-glossary)

---

## 1. Problem Statement

> "Design a system that ingests continuous sensor telemetry (air quality readings) from a large fleet of
> distributed devices, computes a derived health index in real time, alerts on dangerous conditions with
> low latency, and also retains the full history for compliance reporting and trend analysis — such that
> the real-time view and the historical view are always numerically consistent with each other."

This is a **hybrid streaming + batch (Lambda-style) analytics platform**. It is structurally identical to
a large class of real interview questions: "design Uber's surge pricing pipeline," "design a stock ticker
alerting system," "design an IoT fleet-monitoring platform," "design a fraud-detection pipeline." The
AQI domain is just the concrete instance we built and ran end-to-end.

**Why this is a good interview case study:** it forces you to reason about the same trade-offs that show
up in almost every senior data-engineering / distributed-systems interview:
- streaming vs. batch (and why you often need *both*)
- exactly-once vs. at-least-once processing
- partitioning and ordering guarantees
- schema management and backward compatibility
- how to keep two independently-computed views (fast/approximate vs. slow/authoritative) from disagreeing
- alerting semantics (dedup, escalation, cooldown, resolution)
- horizontal scalability of every layer independently

---

## 2. Requirements

### 2.1 Functional Requirements

| # | Requirement |
|---|---|
| F1 | Ingest PM2.5, PM10, NO2, O3, CO2 readings from many independent sensors, each publishing on its own cadence. |
| F2 | Compute a standardized Air Quality Index (AQI) per reading using the US EPA breakpoint formula. |
| F3 | Detect and alert when AQI/CO2 crosses configurable severity thresholds, with escalation and automatic resolution. |
| F4 | Detect anomalous sensor behavior (spikes, drift, stuck values, physically impossible readings) independent of AQI thresholds. |
| F5 | Persist every raw reading, immutably, for replay/audit — even ones that later turn out to be corrupt. |
| F6 | Provide ad-hoc SQL access to historical data (daily/weekly rollups) for BI and compliance reporting. |
| F7 | Route alerts to multiple channels (email, Slack, SMS) based on severity and audience. |
| F8 | Provide a live dashboard of current conditions and a separate dashboard of pipeline health. |

### 2.2 Non-Functional Requirements

| # | Requirement | Target in this design |
|---|---|---|
| N1 | **Ingestion durability** — no reading should be silently dropped. | Kafka with replication; malformed messages routed to a dead-letter topic rather than discarded. |
| N2 | **Alert latency** — unhealthy air must be surfaced in *minutes*, not next-day. | End-to-end p99 well under 60s in the streaming path (Kafka consume → detection → alert publish → exporter scrape → Alertmanager route). |
| N3 | **Historical query latency** — ad-hoc SQL over months of data must return in seconds. | Partitioned/columnar Parquet + Trino's distributed query engine. |
| N4 | **Consistency between real-time and historical AQI** — the same reading must produce the same AQI number whether computed by the streaming job or the nightly batch job. | Single source of truth for the AQI formula, executed as Python in Flink and as generated Trino SQL in Airflow, with a test suite that proves the two are numerically identical. |
| N5 | **Horizontal scalability** — must scale from tens to (architecturally) hundreds of thousands of sensors without a redesign. | Every layer (Kafka partitions, Flink parallelism, Trino workers, MinIO nodes) scales independently and horizontally. |
| N6 | **Idempotent reprocessing** — re-running a day's batch job must be safe. | Every batch step overwrites its own partition; no append-only accumulation bugs. |
| N7 | **Fault isolation** — one job crashing must not take down unrelated jobs. | Detection and landing-zone archival are two independently deployed/restartable Flink jobs sharing only the source topic. |
| N8 | **Observability** — the operator must be able to tell *why* something is wrong, not just *that* it's wrong. | Two-tier alerting: pipeline health (Kafka lag, Flink job state, checkpoint failures) vs. business alerts (AQI thresholds), so a "no alerts firing" state can be distinguished from "the pipeline is dead and can't fire alerts." |

**Explicitly out of scope (v1):** multi-region active-active deployment, per-tenant isolation/multi-tenancy,
a public write API for third-party sensor vendors, mobile push notifications.

---

## 3. Capacity Estimation (Back-of-the-Envelope)

This is the part interviewers weight heavily — showing you can reason quantitatively, not just name
technologies.

**Assumptions for a mid-size city-scale deployment** (numbers used are realistic for a metro-area AQI
network; the reference implementation runs a smaller version of the same shape):

- 10,000 sensors
- 1 reading every 30 seconds per sensor → **~333 readings/sec** sustained
- Each JSON reading ≈ 250 bytes on the wire

**Ingestion throughput**

```
333 msgs/sec × 250 B  ≈ 83 KB/s  ≈ 7.2 GB/day raw ingest
```

Trivial for a single Kafka broker; the number that actually matters is **partition count**, driven by
*consumer* parallelism, not byte throughput: with 12 partitions (as configured), each Flink subtask
handles ~28 msgs/sec — comfortably real-time even with per-message AQI computation and state lookups.

**Storage growth (raw landing zone, JSON)**

```
7.2 GB/day × 90-day retention (LANDING_RETENTION_DAYS default) ≈ 650 GB
```

**Storage growth (curated Parquet, columnar + compressed)**

Parquet + ZSTD typically achieves 5–10x compression over raw JSON for this kind of repetitive numeric
data:

```
7.2 GB/day raw → ~1 GB/day curated
365 days retained ≈ 365 GB/year for the curated zone (kept indefinitely — it's the source of truth for history)
```

**Kafka topic retention (state/replay buffer, not permanent storage)**

```
aqi.raw:  7 days × 7.2 GB/day ≈ 50 GB  (replay buffer for backfill/reprocessing)
aqi.enriched/alerts/anomalies: much smaller (alerts/anomalies are rare events, not per-reading)
```

**Flink state size**

Per-sensor keyed state (EWMA mean/variance per pollutant + alert level) is a few hundred bytes per
sensor. At 10,000 sensors that's **a few MB of state total** — this workload is throughput-bound, not
state-size-bound, which is an important distinction from, e.g., a session-store use case with millions of
keys and large per-key state.

**Query engine sizing**

A single day of curated data (~1 GB compressed) scanned by Trino for a dashboard query is sub-second on
a handful of workers. The aggregation tables (`daily_city_aqi`, etc.) are tiny (rows = cities × days) —
dashboards should *always* query the pre-aggregated tables, never re-scan raw curated data for a chart
that only needs city-level daily averages. This is the classic **"materialize your rollups"** lesson.

**Takeaway for the interview:** the numbers tell you where to spend engineering effort. Here, ingest
volume is small enough that a single Kafka broker/Flink TaskManager could handle 10x this load; the
actual scaling driver is **breadth of downstream fan-out** (alerts, dashboards, BI users), which is why
the design decouples storage (MinIO/Trino) from the hot path (Kafka/Flink) rather than serving dashboards
directly off the stream.

---

## 4. High-Level Design

```
                    ┌─────────────┐
  Sensors  ───────► │   Kafka     │  aqi.raw (12 partitions, key=sensor_id)
                    └──────┬──────┘
                           │  consumed by TWO independent Flink jobs
              ┌────────────┴────────────┐
              ▼                         ▼
     ┌──────────────────┐      ┌──────────────────────┐
     │  aqi-detection    │      │  aqi-landing-zone     │
     │  (per-sensor      │      │  (exactly-once file   │
     │   keyed state)    │      │   sink, no state)     │
     └─────────┬─────────┘      └──────────┬────────────┘
               │ enriched/alerts/anomalies  │ raw JSON
               ▼                            ▼
     ┌──────────────────┐         ┌──────────────────────┐
     │ Prometheus/Alert- │         │  MinIO (S3) landing   │
     │ manager/Grafana   │         │  zone                 │
     │ (fast, minutes)   │         └──────────┬────────────┘
     └──────────────────┘                     │ nightly Airflow ETL
                                               ▼
                                     ┌──────────────────────┐
                                     │  Trino + Hive         │
                                     │  Metastore: curated + │
                                     │  aggregated Parquet   │
                                     │  (accurate, ~1 day    │
                                     │   lag, queryable BI)  │
                                     └──────────────────────┘
```

This is a textbook **Lambda architecture**: a *speed layer* (Flink → Prometheus/Grafana) optimized for
low latency and approximate/real-time correctness, and a *batch layer* (Airflow → Trino) optimized for
completeness and auditability, both reading from the same immutable source of truth (Kafka → landing
zone) and both computing AQI from the same formula so they never disagree on the number, only on
*freshness*.

---

## 5. API / Interface Design

There is deliberately no bespoke REST API in v1 — every "API" here is a well-understood open protocol,
which is itself a design decision worth defending in an interview ("why build a custom API when the
consumers are internal dashboards and BI tools that already speak SQL and PromQL?"):

| Interface | Protocol | Consumer |
|---|---|---|
| Sensor ingestion | Kafka producer API (JSON payloads) | Any sensor / gateway that can speak Kafka, or an HTTP-to-Kafka bridge if sensors can't |
| Real-time metrics | Prometheus exposition format (`/metrics`) | Prometheus scraper |
| Alert delivery | Alertmanager webhook JSON | Email/Slack/SMS relays (pluggable — any endpoint that accepts the standard webhook payload) |
| Historical query | Trino wire protocol (HTTP+JSON), SQL | Grafana, BI tools, analysts, any JDBC/ODBC client |
| Dashboards | Grafana HTTP API | End users' browsers |

**If asked to design a public API on top of this** (a natural interview follow-up): put a thin read-only
service (e.g., FastAPI) in front of Trino for `GET /aqi/current?city=`, `GET /aqi/history?city=&from=&to=`,
backed by the pre-aggregated tables — never let external API traffic hit Trino directly (no query
cost control, no schema stability guarantee for external consumers).

**Sensor ingestion payload contract:**

```json
{
  "schema_version": 1,
  "sensor_id": "delhi-0000",
  "city": "Delhi",
  "latitude": 28.6139, "longitude": 77.2090,
  "pm25": 64.17, "pm10": 112.75, "no2": 33.27, "o3": 18.96, "co2": 463.1,
  "firmware": "2.3.1",
  "event_time": "2026-09-23T03:27:47.948Z"
}
```

`schema_version` is present from day one specifically so schema evolution has a hook — see §11 for the
schema-registry discussion.

---

## 6. Data Model

### 6.1 Streaming: Kafka topics (schema-on-read JSON)

| Topic | Partitions | Key | Retention | Purpose |
|---|---|---|---|---|
| `aqi.raw` | 12 | `sensor_id` | 7 days | Immutable ingest buffer; replay source for reprocessing |
| `aqi.enriched` | 12 | `sensor_id` | 3 days | Every reading + computed AQI; feeds live dashboards |
| `aqi.alerts` | 3 | `sensor_id` | 30 days | Threshold-crossing events (audit trail) |
| `aqi.anomalies` | 3 | `sensor_id` | 30 days | Sensor-fault events (audit trail) |
| `aqi.dlq` | 3 | `sensor_id` | 14 days | Unparseable/malformed messages |

**Key design decision — partition key = `sensor_id`:** guarantees per-sensor ordering (critical for
stateful anomaly detection, which depends on seeing a sensor's readings in order) while still allowing
horizontal fan-out across partitions for unrelated sensors. The alternative, keying by `city` or no key
(round-robin), would either create hot partitions (a handful of huge cities) or break per-sensor ordering
guarantees.

### 6.2 Batch: Hive-cataloged tables (schema-on-write Parquet)

```
hive.landing.raw_readings      -- external table over the Flink JSON archive, partitioned by (dt, hour)
hive.curated.readings          -- deduplicated, validated, AQI-enriched, partitioned by dt
hive.analytics.daily_sensor_aqi
hive.analytics.daily_city_aqi
hive.analytics.weekly_city_aqi -- partitioned by week_start
```

`curated.readings` schema (the canonical fact table):

```
sensor_id, city, latitude, longitude, event_ts,
pm25, pm10, co2, no2, o3,
pm25_index, pm10_index, no2_index, o3_index,   -- per-pollutant EPA sub-indices
aqi, aqi_category, dominant_pollutant, co2_level,
firmware, dt   (partition column)
```

**Why keep the sub-indices as separate columns instead of only the final AQI?** So "why is the air bad
today" (dominant pollutant analysis) is a column read, not a recomputation — a classic
normalize-for-write-once / denormalize-for-read-many trade-off.

---

## 7. Deep Dive: Component Design

### 7.1 Ingestion — Kafka

- **KRaft mode** (no ZooKeeper) — fewer moving parts, one less distributed system to operate.
- Producers use `acks=all` + idempotent producer to prevent duplicate/lost writes on retry.
- Consumers commit offsets **on checkpoint**, not on receipt — so a consumer crash replays only the
  unacknowledged window, never silently skips data (see §9).

### 7.2 Stream Processing — Flink

Two independently deployed jobs consuming the same topic — a deliberate **single-responsibility split**:

- **`aqi-detection`**: stateful, CPU-light, latency-sensitive. Per-sensor `KeyedProcessFunction` holding:
  - an EWMA mean/variance per pollutant (anomaly detection) — O(1) space per sensor, no sliding-window buffer needed
  - an alert-level state machine (threshold escalation, cooldown, resolution)
  - State is checkpointed to MinIO every 30s with a TTL so silent sensors don't grow state forever.
- **`aqi-landing-zone`**: stateless (well, checkpoint-coordinated for exactly-once file commits), I/O-heavy,
  latency-insensitive. A crash/restart here never affects alerting, and vice versa.

**Why split instead of one job doing both?** Different scaling and failure profiles. If the file sink
backpressures on slow object storage, you don't want that backpressure propagating into the
latency-critical alerting path (Flink backpressure is a shared-fate mechanism *within* a job, not across
jobs).

**Exactly-once semantics, precisely:** Flink's checkpointing gives *exactly-once state updates* and
*exactly-once file commits* (two-phase commit on the file sink). It does **not**, by itself, give
exactly-once delivery to `aqi.alerts`/`aqi.anomalies` — those Kafka sinks are configured
at-least-once (`DeliveryGuarantee.AT_LEAST_ONCE`) because true exactly-once Kafka sinks require
transactional producers with real cost/complexity trade-offs, and duplicate alert delivery is a much
cheaper failure mode than *losing* an alert (a classic "which side do you want your bug on" decision —
see §9).

### 7.3 Storage — MinIO (S3-compatible object store)

Medallion/multi-zone layout:
- **Landing (bronze)**: raw, untransformed, append-only, cheap, short retention.
- **Processed (silver/gold)**: validated, deduplicated, columnar, long retention, versioned bucket.

Object storage (not a database) is the right primitive here because writes are append-only/immutable and
reads are large sequential scans by an OLAP engine — exactly the access pattern S3-style storage is
optimized for, and it decouples storage scaling from compute scaling entirely (Trino workers can scale
up/down without touching the data).

### 7.4 Catalog & Query — Hive Metastore + Trino

The Metastore is a **separate, shared** piece of metadata infrastructure — not baked into Trino — so
multiple engines (Trino today; Spark, Presto, or a future engine tomorrow) can share one source of truth
for table/partition definitions. Partition pruning (via `dt=`/`hour=` Hive-style partitioning) is what
makes "scan 90 days out of a year" not turn into "scan the whole year."

### 7.5 Orchestration — Airflow

Each DAG task is a **pure, idempotent SQL statement executed by Trino** — Airflow does zero data
processing itself, only scheduling/dependency/retry logic. This means:
- Backfilling N days is just running the DAG N times with different `target_date` params — no special
  backfill code path to maintain.
- A failed run never leaves partial/duplicate data, because every INSERT targets a specific partition
  with overwrite semantics.

### 7.6 Monitoring & Alerting

Two independent rule sets on the same Prometheus instance, intentionally separated by **audience and
failure mode**:
- *Pipeline health* rules answer "is the system working?" (Kafka lag, Flink job state, checkpoint
  failures) — routed to the data-platform on-call.
- *AQI business* rules answer "is the air dangerous?" — routed to public-health/field-ops.

This separation matters because **the business alerts depend on the pipeline being healthy** — if you
only had AQI-threshold alerts and Kafka went down, you'd get silence, which is indistinguishable from
"the air is fine." The pipeline-health alerts exist specifically to catch that failure mode
(`NoSensorDataIngested`, `FlinkJobsNotRunning`).

### 7.7 Visualization — Grafana

Two data sources, two dashboards, matched to the Lambda architecture's two layers: Prometheus for the
"now" view, Trino for the "history" view. Grafana never computes AQI itself — it only renders numbers
already computed upstream, keeping "what is AQI" defined in exactly one place per layer (Python for
streaming, generated SQL for batch).

---

## 8. Scalability

| Layer | Scaling knob | How |
|---|---|---|
| Kafka | More partitions | Increase `aqi.raw` partitions; consumer parallelism can scale up to the partition count |
| Flink | More TaskManagers / higher parallelism | `docker compose up -d --scale taskmanager=N`, raise `DETECTION_PARALLELISM` (bounded by partition count — parallelism beyond partition count is wasted) |
| MinIO | More nodes, erasure coding | Distributed mode; storage capacity and read/write throughput scale with node count |
| Trino | More workers | Stateless query execution nodes; add workers behind the coordinator |
| Hive Metastore | Vertical / HA Postgres | Metadata volume (tables × partitions) is small relative to data volume; rarely the bottleneck |
| Airflow | CeleryExecutor/KubernetesExecutor | Task-level parallelism across workers, not needed until DAG count/complexity grows a lot |

**The single most important scaling insight in this design:** ingestion (Kafka) and hot-path compute
(Flink) scale with *sensor count × reading frequency* (a write-side, streaming concern), while query
compute (Trino) and storage (MinIO) scale with *retention × BI/dashboard query volume* (a read-side,
batch concern) — these are **orthogonal scaling dimensions**, and this architecture is specifically
structured so they can be scaled independently rather than being coupled through one monolithic system.

---

## 9. Reliability & Fault Tolerance

### 9.1 Failure-mode table (a favorite interview drill: "what happens if X dies?")

| Component fails | Immediate effect | Recovery |
|---|---|---|
| A sensor | That sensor's data stops; `SensorSilent` alert fires after 5 min | No action needed centrally; sensor state expires via TTL after 7 days |
| A Kafka broker (single-broker demo; 3+ in prod) | In prod: no data loss, leader election, brief latency blip | KRaft controller re-elects; producers/consumers retry transparently |
| `aqi-detection` job crashes | Alerting pauses | Flink restart strategy (exponential backoff) resumes from last checkpoint; at most a few seconds of alert-state replay, no data loss |
| `aqi-landing-zone` job crashes | Raw archival pauses; alerting **unaffected** (independent job) | Same restart-from-checkpoint mechanism; in-flight uncommitted files are rolled back (two-phase commit) |
| A TaskManager (worker) dies | Its tasks are rescheduled on remaining TaskManagers | JobManager reassigns from the last checkpoint |
| MinIO unavailable | Flink checkpoints fail → `FlinkCheckpointsFailing` alert; landing writes stall | Job keeps retrying checkpoints; once MinIO recovers, backlog drains from Kafka (which buffered it) |
| Trino unavailable | Historical dashboards and the nightly DAG fail | Airflow retries (exponential backoff, 2 retries); Grafana Trino panels show "no data," Prometheus panels unaffected |
| Airflow scheduler down | No new DAG runs trigger | Once restored, missed daily runs can be backfilled (idempotent partitions) |
| Prometheus down | No new alerts fire, no metrics history | Business risk during the outage window — mitigated by keeping Prometheus itself simple/stateless-ish and monitored externally in production |

### 9.2 Idempotency patterns used throughout

1. **Partition overwrite**, not append, for every batch write (`INSERT INTO ... WHERE dt = '...'`
   preceded by dropping/overwriting that partition) — reruns never double-count.
2. **Deduplication** in the curation step by `(sensor_id, event_time)` with `ROW_NUMBER()`, keeping the
   latest ingest — handles at-least-once delivery from the landing-zone Flink sink.
3. **Event-time-based alert cooldown** rather than wall-clock — replaying old data during a backfill
   doesn't spam every historical alert as if it just happened.

### 9.3 What is *not* handled today (be ready to say this explicitly in an interview)

- No cross-region disaster recovery — this is a single-node/single-region reference deployment.
- No producer-side buffering/retry beyond Kafka's client defaults if the sensor's local network drops.
- No automatic Kafka topic partition rebalancing strategy for a very skewed sensor distribution (e.g.,
  one city with 10x the sensors of everywhere else) — you'd need a smarter key or a custom partitioner.

---

## 10. Consistency Model

This system is a great vehicle for discussing consistency because it deliberately makes **different
consistency trade-offs at different layers**, on purpose:

- **Kafka**: tunable consistency via `acks` and `min.insync.replicas`. Configured for durability
  (`acks=all`) over raw throughput.
- **Flink state**: exactly-once *internal* consistency (checkpointed state is always consistent with the
  input offset it reflects) — this is what lets you say "if we replay from checkpoint N, we get the exact
  same alert history," a strong guarantee.
- **Flink → Kafka sinks (alerts/anomalies)**: at-least-once. A consumer of `aqi.alerts` must be prepared
  for occasional duplicate alert messages (Alertmanager's own dedup/grouping absorbs this in practice).
- **Batch curated tables**: this is the *strongly consistent, authoritative* view — deduplicated,
  reprocessable, the number you'd put in a compliance report. It lags real-time by up to a day.
- **The relationship between the two views is intentionally eventual-consistency-like**: the streaming
  AQI for a given reading and the batch AQI for that same reading, once curated, are numerically
  *identical* (proven by test — see §11), but the streaming number is available first and the batch
  number is the one that's authoritative for history. This is precisely the trade-off the **CAP
  theorem** and the **Lambda architecture** pattern are about: you get low latency now at the cost of it
  being a "preliminary" view, and correctness/completeness later.

**A good interview framing:** "We don't have one global consistency model — we have a *fast, approximate*
path and a *slow, authoritative* path, and the interesting engineering problem is making sure they never
silently disagree, not picking one universal consistency level."

---

## 11. Trade-offs & Alternatives Considered

This is the section to actually rehearse out loud — interviewers care much more about *why not X* than
*what did you pick*.

### Kafka vs. Pulsar
Both are valid. Kafka was chosen for ecosystem maturity (Flink/Trino/kafka-exporter connectors are more
battle-tested) and because KRaft mode removes the ZooKeeper operational burden that used to be Kafka's
biggest ops complaint. Pulsar's tiered storage and multi-tenancy are genuinely nice, but this workload
doesn't need multi-tenant topic isolation.

### Flink vs. Spark Structured Streaming
Flink was chosen because this workload is **fundamentally a per-key, low-latency, unbounded state**
problem (per-sensor EWMA + alert state that must persist indefinitely across micro-batches). Flink's
native `KeyedProcessFunction` + `ValueState` is a more direct fit than Spark's micro-batch model, and
Flink checkpointing/watermarking is more mature for this event-time-driven use case. Spark Structured
Streaming would be the better call if the team already standardized on Spark for batch and wanted one
engine for both (operational simplicity often beats theoretical fit).

### Lambda vs. Kappa architecture
This system is Lambda (separate speed + batch layers, same source data). The alternative, **Kappa**
architecture, would run *everything* through the streaming engine, including reprocessing history by
replaying Kafka. We didn't choose Kappa because (a) Kafka's practical retention window is much shorter
than the multi-year retention needed for compliance data — you'd need infinite log retention or a
tiered-storage Kafka, and (b) some batch aggregations (weekly rollups, data-quality checks with
cross-partition joins) are far more natural to express and optimize in Trino SQL than as streaming
operators. Lambda's classic downside — maintaining two codepaths — is directly mitigated here by
generating both codepaths (Python and SQL) from one shared breakpoint table (`aqi_common/aqi.py`).

### Hive Metastore + Trino vs. a cloud-native lakehouse (Glue + Athena, or Databricks/Snowflake)
Hive Metastore + Trino is the **open-source-portable** choice — it runs identically on a laptop, on-prem,
or any cloud, which matters for a reference architecture. A cloud-native stack (Glue/Athena on AWS) would
have less operational overhead in production but locks the design to one cloud. Snowflake/Databricks
trade licensing cost for a much smoother managed experience — a legitimate choice if the org already pays
for one of those platforms.

### MinIO vs. cloud object storage (S3/GCS/Azure Blob)
MinIO gives an identical S3 API for local development and air-gapped/on-prem deployments. In cloud
production, you'd typically point the same Trino/Flink S3 configuration directly at the cloud provider's
object store instead of self-hosting MinIO — the connectors don't care which S3-compatible backend is
behind them, which is the whole point of standardizing on the S3 API early.

### Push (Kafka + Alertmanager) vs. pull (poll a database) alerting
Alerts are generated by the stream processor and pushed as events, then Prometheus *pulls* metrics
(standard Prometheus model) while Alertmanager *pushes* notifications outward. This hybrid is intentional:
pull-based metrics scraping is what makes Prometheus resilient (a scrape target being briefly unreachable
just means one missed data point, not a lost push that needs a retry queue), while alert delivery
genuinely needs to be pushed to be timely.

---

## 12. Bottlenecks & Mitigations

| Bottleneck | Symptom | Mitigation |
|---|---|---|
| Too few Kafka partitions for sensor fleet size | Consumer lag grows even though CPU is idle | Increase partitions (note: never decrease — repartitioning breaks key-to-partition ordering guarantees) |
| Flink parallelism capped by partition count | Adding more TaskManagers doesn't reduce lag | Must raise `aqi.raw` partitions *and* `DETECTION_PARALLELISM` together |
| Unbounded per-sensor state growth | Checkpoint size/time creeps up over months | State TTL (7 days) already bounds this; monitor `flink_jobmanager_job_lastCheckpointDuration` |
| Dashboards querying raw curated data instead of rollups | Slow Grafana panels, high Trino load | Always query `hive.analytics.*` pre-aggregated tables for anything above sensor-level granularity |
| Hot partition from one mega-city | Uneven load across Flink subtasks | Consider a composite key (`sensor_id` is fine at current scale; would need a smarter partitioner at extreme skew) |
| Alertmanager notification storms during a city-wide pollution episode | Every sensor in the city fires independently | Alert grouping (`group_by: [alertname, city]`) and inhibition rules already collapse these; the city-median-based alert rules (`quantile(0.5, ...)`) further reduce single-sensor noise |

---

## 13. Security Considerations

*(what's implemented vs. what's a known gap — be explicit about both in an interview)*

**Implemented:** Bucket-per-zone separation in MinIO (so a compromised ingestion path can't overwrite
curated history), secrets externalized to `.env` (not committed), non-root container users for the
custom images, malformed-input isolation via the DLQ (defense against poison-pill messages crashing a
consumer).

**Known gaps for production** (always name these proactively):
- No TLS/SASL on Kafka, no auth on Trino/MinIO/Prometheus in the reference deployment — fine for local
  dev, not acceptable for production. Production hardening path: mTLS everywhere, Trino with
  OAuth2/LDAP + Ranger-based row/column access control, MinIO IAM policies scoped per service account.
- No input validation beyond type/range checks — a malicious sensor could still publish plausible-looking
  garbage AQI values; a real deployment would want per-device signing/authentication upstream of Kafka.
- Secrets currently live in environment variables; production should use Vault or a cloud secrets
  manager (see roadmap).

---

## 14. Observability: SLIs, SLOs, and On-Call

A well-designed system defines **what "working" means numerically**, not just "no red alerts."

| SLI | Example SLO | Measured via |
|---|---|---|
| Ingestion freshness | 99% of readings visible in `aqi.enriched` within 30s of `event_time` | `aqi_sensor_last_seen_timestamp_seconds` vs. wall clock |
| Alert latency | 99% of threshold crossings notified within 2 minutes | time between reading ingestion and Alertmanager `FIRING` |
| Pipeline availability | Both Flink jobs `RUNNING` 99.9% of the time | `flink_jobmanager_numRunningJobs` |
| Consumer lag | `aqi-*` consumer groups lag < 10,000 messages | `kafka_consumergroup_lag` (this is literally an existing alert rule) |
| Batch completeness | Nightly DAG succeeds by 02:00 UTC 99% of days | Airflow DAG run history |
| Historical query latency | p95 Grafana Trino panel load < 3s | Grafana query inspector / Trino query history |

**On-call split** (mirrors the two alert categories in §7.6): a *data-platform* on-call owns pipeline
SLOs (lag, job health, checkpoint failures); a *public-health/field-ops* audience owns the business SLOs
(AQI thresholds, sensor silence). This is a realistic reflection of how large orgs split "keep the system
running" from "act on what the system tells you."

---

## 15. Interview Q&A Bank

Use these as self-test flashcards. Each answer below is intentionally short — practice *expanding* each
one out loud for 60–90 seconds, the way you would in a real interview.

**Q: How would you design a system that ingests IoT sensor data and alerts on anomalies in real time?**
A: Kafka for durable, ordered (per-device) ingestion → a stateful stream processor (Flink) keyed by
device ID holding a lightweight statistical baseline (EWMA) per device → threshold/anomaly detection
emits events to dedicated output topics → a metrics bridge converts business events into
Prometheus/Grafana-visible metrics → Alertmanager handles routing, dedup, and escalation. Separately,
archive raw+enriched data to object storage for historical analysis, decoupled from the alerting path.

**Q: How do you guarantee ordering per device but still scale horizontally?**
A: Partition the topic by device ID. Kafka guarantees ordering *within* a partition only, which is
exactly the granularity you need (you don't need global ordering across unrelated devices), and it lets
you add partitions/consumers to scale throughput without breaking any single device's ordering.

**Q: What happens if you need to reprocess a partition with a bug fix?**
A: Because every batch write targets a specific partition with overwrite (not append) semantics, and the
raw landing zone retains the immutable original data independent of any downstream bug, you fix the bug
and re-run the DAG for the affected date range — the SQL naturally overwrites the previously-wrong output.
This is why idempotent, partition-scoped writes are worth the upfront design effort.

**Q: How do you prevent duplicate/flapping alerts when a metric hovers right at a threshold?**
A: Two mechanisms working together: (1) severity-level state machine — only escalating *changes* or a
periodic cooldown re-notify fire, not every single reading above threshold; (2) Alertmanager's own
grouping/`for:` duration requires the condition to hold for a sustained window before firing at all,
absorbing single-sample noise.

**Q: Real-time and batch pipelines computed different numbers for the same day — how do you debug that,
and how would you prevent it in the first place?**
A: Prevention: never implement the same business logic twice by hand in two languages — generate one from
the other, or share a library, and write a test that executes both and diffs the result (this repo
literally does this: the Trino SQL is generated from the same Python module used by Flink, and a
DuckDB-based test proves numeric equivalence across thousands of sample values). Debugging without that
safety net: check for (a) different rounding/truncation rules, (b) different null/invalid-value handling,
(c) different dedup logic, (d) late-arriving data included in one path but not the other.

**Q: How would you scale this from thousands to a million sensors?**
A: Identify which resource actually saturates first (almost never CPU on the stream processor — usually
partition count / consumer parallelism, or downstream fan-out like alert volume). Concretely: raise Kafka
partition count and Flink parallelism together (bounded together — extra parallelism beyond partition
count is wasted); consider composite partition keys if a few "hot" sources dominate; move from a single
Kafka broker to a multi-broker cluster with rack awareness; scale Trino workers and MinIO nodes
independently since they scale with *query/storage* volume, not sensor count.

**Q: Why not just write straight to a database instead of Kafka?**
A: A database write path couples the sensor fleet's write rate directly to the database's write capacity
and doesn't give you replay, multiple independent consumers (detection AND archival from the same event,
without double-ingesting), or natural back-pressure buffering during downstream slowdowns. Kafka
decouples producers from consumers in both rate and time.

**Q: What's your disaster-recovery story if the object store is unavailable for an hour?**
A: The stream processing layer keeps running — Kafka is the durable buffer, so no sensor data is lost;
Flink's checkpoint attempts fail and retry with backoff, and once the object store recovers, the backlog
of writes drains from Kafka's retained log. The business impact is a delayed (not lost) landing-zone
archive and a stalled nightly batch job, not an alerting outage.

**Q: How do you test a distributed streaming pipeline without standing up the whole cluster?**
A: Push as much logic as possible into pure, dependency-free functions (parsing, AQI math, state-machine
transitions) that take-and-return plain data structures — then unit test those directly with pytest, no
Flink runtime needed. Reserve full-stack verification (`docker compose up`, watch metrics, trigger a
backfill, confirm dashboard data) for a smaller number of true end-to-end checks. This project's test
suite is built exactly this way: `aqi_common/` has zero Flink/Airflow imports and is 100% unit-testable.

**Q: Your Prometheus alert rule fires on "median sensor AQI > 150" instead of "any sensor > 150" — why?**
A: A single faulty sensor spiking to an absurd value shouldn't page anyone for a city-wide health alert —
that's what the *anomaly detector* and a separate *per-sensor* (not city-median) alert are for. Using the
median for the city-wide, public-facing signal makes it robust to individual sensor noise, which is a
basic but important statistical-robustness decision.

**Q: How would you add a new pollutant (e.g., SO2) to this system?**
A: Add its breakpoint table to the single shared `aqi_common/aqi.py` module (both the Python sub-index
function and the SQL-generation function read from the same `BREAKPOINTS` dict), bump `schema_version` in
the sensor payload contract, add the column to the Kafka message schema and the curated table DDL, and
because both the streaming and batch AQI calculations are derived from that one module, both paths pick
up the new pollutant automatically and stay consistent — this is the payoff of the "single source of
truth" design decision made in §7/§11.

**Q: This system doesn't have a schema registry — is that a problem?**
A: For a reference implementation with one producer type, no. At real scale with many independent sensor
vendors, yes — you'd want Avro/Protobuf with a registry enforcing backward compatibility, so a firmware
update on one vendor's device can't silently break every downstream consumer. It's explicitly called out
as a production roadmap item rather than something accidentally missed.

---

## 16. Follow-Up Design Challenges (Practice Prompts)

Try sketching an answer to each of these using this system as your starting point — this is exactly how
senior/staff interviews escalate the question mid-conversation:

1. **"Now add a requirement that alerts must be delivered exactly once, even across a consumer crash."**
   (Hint: transactional Kafka producers + consumer-side dedup table keyed by an alert idempotency key.)
2. **"A single city now has 100x more sensors than any other city. How does your partitioning strategy
   change?"**
3. **"Product wants sub-5-second alert latency, ten times faster than today. What in this design has to
   change?"**
4. **"You need to support 50 independent customers (multi-tenancy), each with their own sensor fleet and
   dashboards, without cross-tenant data leakage. Redesign the storage and query layers."**
5. **"Sensors are now mobile (moving vehicles), not fixed. How does this change your data model and your
   AQI-by-city aggregation logic?"**
6. **"You must support 'time travel' queries — show me what the dashboard would have looked like as of
   any past timestamp, including data that was later corrected/deleted."** (Hint: this repo's own README
   roadmap flags exactly this — migrating the curated zone to Apache Iceberg.)
7. **"Leadership wants a cost breakdown per layer before approving 10x scale. What do you measure, and
   where do you expect the cost curve to bend first?"**

---

## 17. Glossary

| Term | Meaning in this system |
|---|---|
| **AQI** | Air Quality Index — a single 0–500 number derived from the worst pollutant sub-index |
| **EWMA** | Exponentially Weighted Moving Average — the running baseline used for anomaly z-scores |
| **Lambda architecture** | Separate speed (streaming) and batch layers computing the same logic over the same source data |
| **Watermark** | Flink's mechanism for reasoning about event-time progress despite out-of-order arrivals |
| **Checkpoint** | A consistent, recoverable snapshot of a Flink job's state + input offsets |
| **Exactly-once** | Each input event's effect is applied exactly once to state/output, even across failures/retries |
| **Idempotent write** | A write that can be safely repeated without changing the result beyond the first application |
| **Partition (Kafka)** | An ordered, appendable log shard; the unit of both ordering guarantee and parallelism |
| **Medallion architecture** | Landing (bronze) → curated (silver) → aggregated (gold) data zoning |
| **SLI / SLO** | Service Level Indicator (a measurement) / Objective (the target for that measurement) |
| **DLQ** | Dead-letter queue — where messages that fail processing are routed instead of being dropped |

---

*Companion documents: `README.md` (setup/reference), `docs/PROJECT_OVERVIEW.md` (stakeholder overview),
`docs/AQI_Project_Walkthrough.docx` (guided walkthrough), `docs/architecture.svg` (diagram).*
