#!/usr/bin/env python3
"""Generates docs/architecture.svg - the AQI platform architecture diagram.

Clean, grid-aligned, factually accurate to the actual docker-compose.yml / codebase
(no icon packs, no placeholder/garbled labels). Regenerate after any architectural change:

    python docs/architecture.py
"""
from pathlib import Path

OUT = Path(__file__).resolve().with_name("architecture.svg")
W, H = 1650, 1060

svg = []


def esc(t):
    return t.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def zone(x, y, w, h, title, fill, stroke="#90a4ae", caption=None, dashed=False):
    dash = ' stroke-dasharray="8,5"' if dashed else ""
    svg.append(
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="14" fill="{fill}" '
        f'stroke="{stroke}" stroke-width="1.6"{dash}/>'
        f'<text x="{x + 16}" y="{y + 27}" class="zone-title">{esc(title)}</text>'
    )
    if caption:
        svg.append(f'<text x="{x + 16}" y="{y + 45}" class="zone-caption">{esc(caption)}</text>')


def box(x, y, w, h, title, sub=None, fill="#ffffff", stroke="#546e7a", dashed=False):
    dash = ' stroke-dasharray="6,4"' if dashed else ""
    svg.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="8" fill="{fill}" stroke="{stroke}" stroke-width="1.4"{dash}/>')
    lines = [title] + (list(sub) if sub else [])
    if len(lines) == 1:
        svg.append(f'<text x="{x + w/2}" y="{y + h/2 + 5}" class="box-title" text-anchor="middle">{esc(title)}</text>')
        return
    ty = y + 21
    svg.append(f'<text x="{x + w/2}" y="{ty}" class="box-title" text-anchor="middle">{esc(title)}</text>')
    for s in lines[1:]:
        ty += 15.5
        svg.append(f'<text x="{x + w/2}" y="{ty}" class="box-sub" text-anchor="middle">{esc(s)}</text>')


def pill(x, y, w, h, text, fill="#ffffff", stroke="#8d6e63"):
    svg.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{h/2}" fill="{fill}" stroke="{stroke}" stroke-width="1.2"/>')
    svg.append(f'<text x="{x + w/2}" y="{y + h/2 + 4}" class="pill" text-anchor="middle">{esc(text)}</text>')


def arrow(x1, y1, x2, y2, label=None, dashed=False, color="#455a64", lx=None, ly=None):
    dash = ' stroke-dasharray="6,4"' if dashed else ""
    svg.append(f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{color}" stroke-width="2"{dash} marker-end="url(#ah)"/>')
    if label:
        svg.append(f'<text x="{lx if lx is not None else (x1+x2)/2}" y="{ly if ly is not None else (y1+y2)/2 - 6}" class="arrow-label" text-anchor="middle">{esc(label)}</text>')


def polyline(points, label=None, dashed=False, color="#455a64", lx=None, ly=None):
    dash = ' stroke-dasharray="6,4"' if dashed else ""
    pts = " ".join(f"{x},{y}" for x, y in points)
    svg.append(f'<polyline points="{pts}" fill="none" stroke="{color}" stroke-width="2"{dash} marker-end="url(#ah)"/>')
    if label and lx is not None:
        svg.append(f'<text x="{lx}" y="{ly}" class="arrow-label" text-anchor="middle">{esc(label)}</text>')


# ===================================================================== layout grid
# 5 equal-width columns across the canvas; two content rows plus a roadmap strip.
COLS = [20, 346, 672, 998, 1324]
COL_W = 306
ROW1_Y, ROW1_H = 95, 335       # ingestion -> stream processing -> monitoring -> notify
GAP1 = 445                      # free corridor between row 1 and row 2 (430-465)
ROW2_Y, ROW2_H = 465, 300       # access -> storage lakehouse -> orchestration -> dashboards
GAP2 = 782                      # free corridor between row 2 and roadmap strip (765-800)
ROW3_Y, ROW3_H = 800, 170       # production roadmap (not yet implemented)

FILL_SRC, STROKE_SRC = "#eceff1", "#90a4ae"
FILL_KAFKA, STROKE_KAFKA = "#fff8e1", "#f9a825"
FILL_FLINK, STROKE_FLINK = "#e3f2fd", "#1e88e5"
FILL_MON, STROKE_MON = "#fce4ec", "#d81b60"
FILL_ALERT, STROKE_ALERT = "#ffebee", "#e53935"
FILL_STORAGE, STROKE_STORAGE = "#e8f5e9", "#43a047"
FILL_ORCH, STROKE_ORCH = "#e0f7fa", "#00acc1"
FILL_DASH, STROKE_DASH = "#f3e5f5", "#8e24aa"
FILL_ROAD, STROKE_ROAD = "#f5f5f5", "#9e9e9e"

# --------------------------------------------------------------------- ROW 1
zone(COLS[0], ROW1_Y, COL_W, ROW1_H, "Sources", FILL_SRC, STROKE_SRC)
box(COLS[0] + 20, 140, COL_W - 40, 56, "Sensor fleet", ["generator/sensor_simulator.py"])
box(COLS[0] + 20, 206, COL_W - 40, 56, "JSON reading", ["PM2.5 · PM10 · NO2 · O3 · CO2"])
box(COLS[0] + 20, 272, COL_W - 40, 80, "8 simulated cities", ["diurnal traffic pattern +", "fault injection (spikes, dropouts,", "stuck sensors)"])

zone(COLS[1], ROW1_Y, COL_W, ROW1_H, "Kafka 3.8 (KRaft)", FILL_KAFKA, STROKE_KAFKA)
box(COLS[1] + 20, 140, COL_W - 40, 46, "aqi.raw", ["12 partitions · key=sensor_id"], fill="#fffde7")
box(COLS[1] + 20, 196, COL_W - 40, 36, "aqi.enriched", fill="#fffde7")
box(COLS[1] + 20, 242, COL_W - 40, 36, "aqi.alerts", fill="#fffde7")
box(COLS[1] + 20, 288, COL_W - 40, 36, "aqi.anomalies", fill="#fffde7")
box(COLS[1] + 20, 334, COL_W - 40, 46, "aqi.dlq", ["malformed messages"], fill="#fffde7")

zone(COLS[2], ROW1_Y, COL_W, ROW1_H, "Flink 1.18 Cluster", FILL_FLINK, STROKE_FLINK)
box(COLS[2] + 20, 145, COL_W - 40, 115, "aqi-detection", [
    "AQI calc (EPA breakpoints)",
    "threshold alerts (warn/critical/emergency)",
    "EWMA anomaly detection",
    "checkpointed per-sensor state",
], fill="#e1f5fe")
box(COLS[2] + 20, 270, COL_W - 40, 100, "aqi-landing-zone", [
    "Flink SQL file sink",
    "exactly-once JSON archive",
    "committed every checkpoint (~60s)",
], fill="#e1f5fe")

zone(COLS[3], ROW1_Y, COL_W, ROW1_H, "Monitoring & Alerting", FILL_MON, STROKE_MON)
box(COLS[3] + 20, 140, COL_W - 40, 64, "aqi-exporter · kafka-exporter", [
    "business metrics + consumer lag",
    "\u2192 Prometheus /metrics",
], fill="#fdf2f6")
box(COLS[3] + 20, 214, COL_W - 40, 64, "Prometheus", [
    "pipeline-health + AQI threshold rules",
    "monitoring/prometheus/rules/*.yml",
], fill="#fdf2f6")
box(COLS[3] + 20, 288, COL_W - 40, 64, "Alertmanager", [
    "severity routing + cooldown",
    "inhibition rules",
], fill="#fdf2f6")

zone(COLS[4], ROW1_Y, COL_W, ROW1_H, "Notification Channels", FILL_ALERT, STROKE_ALERT)
box(COLS[4] + 20, 145, COL_W - 40, 60, "Email", ["Mailpit (dev) / SMTP relay"], fill="#fff0f0")
box(COLS[4] + 20, 215, COL_W - 40, 60, "Slack", ["incoming webhook"], fill="#fff0f0")
box(COLS[4] + 20, 285, COL_W - 40, 60, "SMS", ["webhook \u2192 Twilio / SNS relay"], fill="#fff0f0")

# --------------------------------------------------------------------- ROW 2
zone(COLS[0], ROW2_Y, COL_W, ROW2_H, "Users & Access", FILL_SRC, STROKE_SRC)
box(COLS[0] + 20, 510, COL_W - 40, 80, "Data analysts & BI tools", [
    "Trino JDBC/ODBC, DuckDB,",
    "Superset, Metabase, ...",
])
box(COLS[0] + 20, 605, COL_W - 40, 70, "Public API", ["planned \u2014 see Roadmap"], dashed=True, stroke="#9e9e9e")

STORAGE_W = COLS[2] + COL_W - COLS[1]  # spans Kafka + Flink columns
zone(COLS[1], ROW2_Y, STORAGE_W, ROW2_H, "Storage Lakehouse", FILL_STORAGE, STROKE_STORAGE,
     caption="MinIO (S3) buckets")
pill(COLS[1] + 20, 505, STORAGE_W - 40, 28, "landing/aqi/raw  (JSON, dt=/hour=)", fill="#f1f8e9")
pill(COLS[1] + 20, 538, STORAGE_W - 40, 28, "processed/  (curated + analytics Parquet)", fill="#f1f8e9")
pill(COLS[1] + 20, 571, STORAGE_W - 40, 28, "flink/checkpoints", fill="#f1f8e9")
pill(COLS[1] + 20, 604, STORAGE_W - 40, 28, "warehouse/  (Hive metastore dir)", fill="#f1f8e9")
svg.append(f'<text x="{COLS[1] + 20}" y="{650}" class="zone-caption">Catalog &amp; query engine</text>')
cat_w = (STORAGE_W - 40 - 2 * 12) / 3
box(COLS[1] + 20, 660, cat_w, 64, "Postgres", ["metastore + airflow DB"], fill="#f8f0fa")
box(COLS[1] + 20 + cat_w + 12, 660, cat_w, 64, "Hive Metastore 4.0", ["thrift catalog (:9083)"], fill="#f8f0fa")
box(COLS[1] + 20 + 2 * (cat_w + 12), 660, cat_w, 64, "Trino 455", ["SQL query engine (:8080)"], fill="#f8f0fa")

zone(COLS[3], ROW2_Y, COL_W, ROW2_H, "Orchestration \u2014 Airflow 2.10", FILL_ORCH, STROKE_ORCH,
     caption="aqi_daily_aggregation (00:30 UTC daily)")
box(COLS[3] + 20, 522, COL_W - 40, 58, "1. Sync & curate", [
    "register partitions, dedupe,",
    "AQI calc \u2192 curated Parquet",
], fill="#e0f2f1")
box(COLS[3] + 20, 588, COL_W - 40, 58, "2. Daily aggregates", [
    "per-sensor & per-city rollups",
    "+ data-quality checks",
], fill="#e0f2f1")
box(COLS[3] + 20, 654, COL_W - 40, 58, "3. Weekly reports", [
    "weekly city trend + league table",
    "ANALYZE (query stats)",
], fill="#e0f2f1")

zone(COLS[4], ROW2_Y, COL_W, ROW2_H, "Dashboards \u2014 Grafana 11", FILL_DASH, STROKE_DASH)
box(COLS[4] + 20, 505, COL_W - 40, 95, "AQI Overview", [
    "current worst/median AQI",
    "AQI-by-city bar & trend",
    "pollutant explorer, top sensors",
    "firing alerts, category timeline",
], fill="#fdf2f6")
box(COLS[4] + 20, 610, COL_W - 40, 95, "Pipeline Health", [
    "Flink job / TaskManager status",
    "Kafka consumer lag per partition",
    "checkpoints, restarts, backpressure",
    "MinIO capacity & requests",
], fill="#fdf2f6")

# --------------------------------------------------------------------- ROW 3 (roadmap)
zone(20, ROW3_Y, W - 40, ROW3_H, "Production Roadmap \u2014 not yet implemented (see README \u00a710)",
     FILL_ROAD, STROKE_ROAD, dashed=True)
roadmap_items = [
    ("Flink on Kubernetes", "Flink Operator, JobManager HA,", "RocksDB + savepoints"),
    ("HA Kafka Cluster", "3+ brokers, TLS/SASL,", "min.insync.replicas=2"),
    ("Erasure-coded MinIO", "distributed mode,", "per-zone bucket policies"),
    ("Apache Iceberg Tables", "ACID MERGE, schema evolution,", "time travel"),
    ("CI/CD Pipeline", "pytest + compose config +", "image smoke tests on every PR"),
    ("Secrets Management", "Vault / cloud secrets manager,", "no plaintext env vars"),
]
n = len(roadmap_items)
gap = 14
chip_w = (W - 40 - 40 - (n - 1) * gap) / n
for i, (title, s1, s2) in enumerate(roadmap_items):
    cx = 40 + i * (chip_w + gap)
    box(cx, 855, chip_w, 90, title, [s1, s2], fill="#fafafa", stroke="#9e9e9e", dashed=True)

# ===================================================================== arrows
# Row 1: sensors -> kafka -> flink, with flink feeding topics back + monitoring.
# All same-row cross-zone arrows sit in the shared empty band y=395-418
# (every row-1 zone's boxes end by y=380, and the zone border is at y=430).
# Labels are placed ABOVE their (short, 20px-wide) connector line, centered on the
# true midpoint of the gap between the two zones - not on an arbitrary zone offset.
gap_s_k = (COLS[0] + COL_W + COLS[1]) / 2       # Sources | Kafka
gap_k_f = (COLS[1] + COL_W + COLS[2]) / 2       # Kafka | Flink
gap_f_m = (COLS[2] + COL_W + COLS[3]) / 2       # Flink | Monitoring
gap_m_a = (COLS[3] + COL_W + COLS[4]) / 2       # Monitoring | Alerts

arrow(COLS[0] + COL_W, 392, COLS[1], 392, "produce (key=sensor_id)", lx=gap_s_k, ly=382)
arrow(COLS[1] + COL_W, 392, COLS[2], 392, "consume aqi.raw", lx=gap_k_f, ly=382)
arrow(COLS[2], 412, COLS[1] + COL_W, 412, "produce: enriched / alerts / anomalies / dlq", lx=gap_k_f, ly=424)
arrow(COLS[2] + COL_W, 400, COLS[3], 400, "metrics :9249", dashed=True, lx=gap_f_m, ly=388)
arrow(COLS[3] + COL_W, 400, COLS[4], 400, "notify", lx=gap_m_a, ly=388)

polyline([(COLS[1] + COL_W // 2, ROW1_Y + ROW1_H), (COLS[1] + COL_W // 2, GAP1),
          (COLS[3] + COL_W // 2, GAP1), (COLS[3] + COL_W // 2, ROW1_Y + ROW1_H)],
         dashed=True, lx=910, ly=GAP1 - 8, label="consumer lag + business metrics")

# Row 1 -> Row 2 (vertical connectors through the free corridor, y 430-465)
mid_flink_x = COLS[2] + COL_W / 2 + 40
arrow(mid_flink_x, ROW1_Y + ROW1_H, mid_flink_x, ROW2_Y, "JSON files", lx=mid_flink_x - 95, ly=GAP1 + 5)
mon_mid_x = COLS[3] + COL_W / 2
dash_mid_x = COLS[4] + COL_W / 2
polyline([(mon_mid_x, ROW1_Y + ROW1_H), (mon_mid_x, GAP1 + 12), (dash_mid_x, GAP1 + 12), (dash_mid_x, ROW2_Y)],
         dashed=True, lx=(mon_mid_x + dash_mid_x) / 2, ly=GAP1 + 26, label="PromQL")

# Row 2 internal, all routed through the shared bottom margin y 690-745
# (Users&Access boxes end 675; Storage catalog boxes end 724; Orchestration boxes end 712;
#  Dashboards boxes end 705; zone border is at y=765).
gap_o_s = (COLS[3] + COLS[1] + STORAGE_W) / 2   # Orchestration | Storage
gap_u_s = (COLS[0] + COL_W + COLS[1]) / 2       # Users | Storage
arrow(COLS[3], 733, COLS[1] + STORAGE_W, 733, "Trino SQL (ETL)", lx=gap_o_s, ly=721)
arrow(COLS[0] + COL_W, 733, COLS[1], 733, "SQL / API access", lx=gap_u_s, ly=721)
svg.append(f'<text x="{gap_u_s}" y="{749}" class="arrow-label-sm" text-anchor="middle">JDBC, ODBC, Trino</text>')
storage_mid_x = COLS[1] + STORAGE_W - 90
dash2_mid_x = COLS[4] + COL_W / 2
polyline([(storage_mid_x, ROW2_Y + ROW2_H), (storage_mid_x, GAP2), (dash2_mid_x, GAP2), (dash2_mid_x, ROW2_Y + ROW2_H)],
         lx=(storage_mid_x + dash2_mid_x) / 2, ly=GAP2 + 15, label="Trino SQL (historical panels)")

# legend
LEG_Y = 1005
svg.append(f'<line x1="30" y1="{LEG_Y}" x2="70" y2="{LEG_Y}" stroke="#455a64" stroke-width="2" marker-end="url(#ah)"/>')
svg.append(f'<text x="78" y="{LEG_Y + 4}" class="arrow-label">data flow</text>')
svg.append(f'<line x1="170" y1="{LEG_Y}" x2="210" y2="{LEG_Y}" stroke="#455a64" stroke-width="2" stroke-dasharray="6,4" marker-end="url(#ah)"/>')
svg.append(f'<text x="218" y="{LEG_Y + 4}" class="arrow-label">metrics / state</text>')
svg.append(f'<rect x="345" y="{LEG_Y - 10}" width="26" height="18" rx="4" fill="#fafafa" stroke="#9e9e9e" stroke-width="1.4" stroke-dasharray="6,4"/>')
svg.append(f'<text x="378" y="{LEG_Y + 4}" class="arrow-label">planned / roadmap (not yet implemented)</text>')

CSS = """
text { font-family: 'Segoe UI', 'Helvetica Neue', Arial, sans-serif; fill: #263238; }
.zone-title { font-size: 15px; font-weight: 700; fill: #37474f; }
.zone-caption { font-size: 11px; font-style: italic; fill: #78909c; }
.box-title { font-size: 12.5px; font-weight: 600; }
.box-sub { font-size: 10.5px; fill: #546e7a; }
.arrow-label { font-size: 11px; fill: #37474f; font-style: italic; }
.arrow-label-sm { font-size: 9.5px; fill: #78909c; font-style: italic; }
.pill { font-size: 11px; fill: #33691e; }
.title { font-size: 23px; font-weight: 700; }
.subtitle { font-size: 12.5px; fill: #607d8b; }
"""

header = f'''<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}">
<defs>
  <marker id="ah" markerWidth="9" markerHeight="9" refX="7" refY="4.5" orient="auto">
    <path d="M0,0 L9,4.5 L0,9 z" fill="#455a64"/>
  </marker>
  <style>{CSS}</style>
</defs>
<rect width="{W}" height="{H}" fill="#ffffff"/>
<text x="30" y="40" class="title">AQI Tracking System \u2014 Architecture</text>
<text x="30" y="62" class="subtitle">Streaming ingestion &amp; detection (top) \u00b7 lakehouse storage, ETL &amp; BI (middle) \u00b7 production roadmap (bottom) \u2014 Docker Compose reference deployment</text>
'''

body = "".join(svg)
OUT.write_text(header + body + "</svg>", encoding="utf-8")
print(f"wrote {OUT}")
