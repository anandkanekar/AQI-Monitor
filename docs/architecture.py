"""Generate docs/screenshots/architecture.svg (stdlib only).

    python docs/architecture.py
"""
from pathlib import Path
from xml.sax.saxutils import escape

OUT = Path(__file__).resolve().parent / "screenshots" / "architecture.svg"

W, H = 1730, 1140
COL_W, GUTTER, LEFT = 250, 100, 40
PAD = 20
ROW1 = (130, 440)
ROW2 = (530, 850)

INK = "#263238"
MUTED = "#607d8b"
EDGE = "#455a64"
BOX_STROKE = "#b0bec5"

THEMES = {
    "source": ("#546e7a", "#eceff1", "#f7f9fa"),
    "kafka": ("#ef6c00", "#fff3e0", "#fffaf3"),
    "flink": ("#1565c0", "#e3f2fd", "#f5faff"),
    "monitor": ("#c62828", "#fdecea", "#fff8f7"),
    "notify": ("#6a1b9a", "#f3e5f5", "#fcf7fd"),
    "users": ("#546e7a", "#eceff1", "#f7f9fa"),
    "lake": ("#2e7d32", "#e8f5e9", "#f6fbf6"),
    "airflow": ("#00838f", "#e0f7fa", "#f4fdfe"),
    "grafana": ("#ad1457", "#fce4ec", "#fff7fa"),
    "roadmap": ("#757575", "#f0f0f0", "#fafafa"),
}

parts: list[str] = []


def col_x(i: int) -> int:
    return LEFT + i * (COL_W + GUTTER)


def text(x, y, s, cls, anchor="middle", extra=""):
    parts.append(f'<text x="{x}" y="{y}" class="{cls}" text-anchor="{anchor}"{extra}>{escape(s)}</text>')


def zone(x, y, w, h, title, theme, caption=None, dashed=False):
    stroke, band, _ = THEMES[theme]
    dash = ' stroke-dasharray="7,5"' if dashed else ""
    parts.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="10" fill="#ffffff"/>')
    parts.append(f'<path d="M{x},{y + 40} V{y + 10} a10,10 0 0 1 10,-10 H{x + w - 10} '
                 f'a10,10 0 0 1 10,10 V{y + 40} Z" fill="{band}"/>')
    parts.append(f'<line x1="{x}" y1="{y + 40}" x2="{x + w}" y2="{y + 40}" stroke="{stroke}" '
                 f'stroke-opacity="0.35"/>')
    parts.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="10" fill="none" '
                 f'stroke="{stroke}" stroke-width="1.5"{dash}/>')
    parts.append(f'<circle cx="{x + 18}" cy="{y + 20}" r="4.5" fill="{stroke}"/>')
    text(x + 30, y + 25, title, "zone-title", "start")
    if caption:
        text(x + w - 14, y + 25, caption, "zone-caption", "end")


def box(x, y, w, h, title, subs=(), theme="source", dashed=False, mono=False):
    _, _, fill = THEMES[theme]
    dash = ' stroke-dasharray="5,4"' if dashed else ""
    parts.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="6" fill="{fill}" '
                 f'stroke="{BOX_STROKE}" stroke-width="1.2"{dash}/>')
    n = 1 + len(subs)
    block = 15 + (n - 1) * 15
    ty = y + (h - block) / 2 + 11
    text(x + w / 2, ty, title, "box-title-mono" if mono else "box-title")
    for i, s in enumerate(subs):
        text(x + w / 2, ty + 16 + i * 15, s, "box-sub")


def pill(x, y, w, h, label):
    parts.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{h / 2}" fill="#ffffff" '
                 f'stroke="#81c784" stroke-width="1.1"/>')
    text(x + w / 2, y + h / 2 + 4, label, "pill")


def path(points, dashed=False, head=True):
    d = " ".join(("M" if i == 0 else "L") + f"{px},{py}" for i, (px, py) in enumerate(points))
    dash = ' stroke-dasharray="6,4"' if dashed else ""
    marker = ' marker-end="url(#ah)"' if head else ""
    parts.append(f'<path d="{d}" fill="none" stroke="{EDGE}" stroke-width="1.6" '
                 f'stroke-linejoin="round"{dash}{marker}/>')


def label(x, y, lines, anchor="middle"):
    for i, s in enumerate(lines):
        text(x, y + i * 13, s, "edge-label", anchor)


def build() -> str:
    r1y, r1b = ROW1
    r2y, r2b = ROW2
    r1h, r2h = r1b - r1y, r2b - r2y
    bw = COL_W - 2 * PAD

    # Header
    text(LEFT, 46, "AQI Monitor — System Architecture", "title", "start")
    text(LEFT, 70, "Real-time detection on Kafka + Flink, lakehouse analytics on MinIO + Trino, "
         "observability with Prometheus + Grafana · Docker Compose reference deployment",
         "subtitle", "start")

    # ---------------- Row 1: real-time path ----------------
    c = [col_x(i) for i in range(5)]
    three = [(192, 64), (276, 64), (360, 64)]

    zone(c[0], r1y, COL_W, r1h, "Data Sources", "source")
    for (y, h), (t, s) in zip(three, [
        ("Sensor fleet", ["8 cities · diurnal traffic pattern"]),
        ("JSON reading", ["PM2.5 · PM10 · NO2 · O3 · CO2"]),
        ("Kafka producer", ["sensor simulator · JSON over Kafka"]),
    ]):
        box(c[0] + PAD, y, bw, h, t, s, "source")

    zone(c[1], r1y, COL_W, r1h, "Apache Kafka", "kafka", "KRaft")
    topics = [("aqi.enriched", "reading + computed AQI"), ("aqi.alerts", "threshold alerts"),
              ("aqi.anomalies", "spikes · drops · stuck"), ("aqi.dlq", "malformed messages"),
              ("aqi.raw", "12 partitions · key = sensor_id")]
    topic_y = [192 + i * 48 for i in range(5)]
    for y, (t, s) in zip(topic_y, topics):
        box(c[1] + PAD, y, bw, 40, t, [s], "kafka", mono=True)

    zone(c[2], r1y, COL_W, r1h, "Apache Flink", "flink", "PyFlink")
    box(c[2] + PAD, 192, bw, 126, "aqi-detection",
        ["US EPA AQI calculation", "threshold alerts + cooldown", "EWMA anomaly detection",
         "checkpointed per-sensor state"], "flink", mono=True)
    box(c[2] + PAD, 330, bw, 94, "aqi-landing-zone",
        ["Flink SQL filesystem sink", "exactly-once JSON archive", "commit per checkpoint (~60 s)"],
        "flink", mono=True)

    zone(c[3], r1y, COL_W, r1h, "Monitoring & Alerting", "monitor")
    for (y, h), (t, s) in zip(three, [
        ("aqi-exporter · kafka-exporter", ["AQI business metrics · consumer lag"]),
        ("Prometheus", ["pipeline-health + AQI threshold rules"]),
        ("Alertmanager", ["severity routing · inhibition"]),
    ]):
        box(c[3] + PAD, y, bw, h, t, s, "monitor")

    zone(c[4], r1y, COL_W, r1h, "Notification Channels", "notify")
    for (y, h), (t, s) in zip(three, [
        ("Email", ["SMTP relay (Mailpit in dev)"]),
        ("Slack", ["incoming webhook"]),
        ("SMS", ["webhook → Twilio / SNS relay"]),
    ]):
        box(c[4] + PAD, y, bw, h, t, s, "notify")

    # Row 1 edges
    k_l, k_r = c[1] + PAD, c[1] + PAD + bw
    f_l, f_r = c[2] + PAD, c[2] + PAD + bw
    raw_cy = topic_y[4] + 20
    path([(c[0] + PAD + bw, raw_cy), (k_l, raw_cy)])
    label((c[0] + PAD + bw + k_l) / 2, raw_cy - 22, ["produce", "key = sensor_id"])

    path([(k_r, raw_cy), (f_l, raw_cy)])
    path([(f_l - 30, raw_cy), (f_l - 30, 290), (f_l, 290)])
    label(k_r + 50, raw_cy - 10, ["consume aqi.raw"])

    bus_x = k_r + 20
    path([(f_l, 212), (bus_x, 212), (bus_x, topic_y[3] + 20)], head=False)
    for y in topic_y[:4]:
        path([(bus_x, y + 20), (k_r, y + 20)])
    label((bus_x + f_l) / 2, 204, ["produce"])

    m_l, m_r = c[3] + PAD, c[3] + PAD + bw
    path([(f_r, 308), (m_l, 308)], dashed=True)
    label((f_r + m_l) / 2, 300, ["metrics :9249"])

    path([(m_l + bw / 2, 256), (m_l + bw / 2, 276)], dashed=True)
    path([(m_l + bw / 2, 340), (m_l + bw / 2, 360)])

    n_l = c[4] + PAD
    nbus = m_r + 70
    path([(m_r, 392), (nbus, 392), (nbus, 224)], head=False)
    for y, _ in three:
        path([(nbus, y + 32), (n_l, y + 32)])
    label((m_r + nbus) / 2, 384, ["route"])

    lane_y = 108
    path([(c[1] + COL_W / 2, r1y), (c[1] + COL_W / 2, lane_y), (m_l + bw / 2, lane_y),
          (m_l + bw / 2, 192)], dashed=True)
    label((c[1] + COL_W / 2 + m_l + bw / 2) / 2, lane_y - 6,
          ["topic consumption · consumer-group lag"])

    # ---------------- Row 2: batch & serving ----------------
    zone(c[0], r2y, COL_W, r2h, "Users & Access", "users")
    users = [(594, "Public API", ["planned — see roadmap"], True),
             (689, "On-call engineers", ["Grafana · Slack · Email · SMS"], False),
             (784, "Analysts & BI tools", ["Superset · Metabase · DBeaver"], False)]
    for y, t, s, d in users:
        box(c[0] + PAD, y, bw, 50, t, s, "users", dashed=d)

    lake_x, lake_w = c[1], 2 * COL_W + GUTTER
    zone(lake_x, r2y, lake_w, r2h, "Lakehouse — Storage, Catalog & Query", "lake")
    il, ir = lake_x + PAD, lake_x + lake_w - PAD
    parts.append(f'<rect x="{il}" y="584" width="{ir - il}" height="102" rx="6" fill="#f6fbf6" '
                 f'stroke="{BOX_STROKE}" stroke-width="1.2"/>')
    text(il + 14, 603, "Object storage — MinIO (S3 API)", "sub-title", "start")
    pw = (ir - il - 40) / 2
    px = [il + 15, il + 25 + pw]
    for i, s in enumerate(["landing/ — raw JSON (dt, hour)", "processed/ — curated Parquet",
                           "flink/ — checkpoints", "warehouse/ — Hive tables"]):
        pill(px[i % 2], 614 + (i // 2) * 36, pw, 26, s)

    box(il, 704, 220, 58, "Hive Metastore", ["Thrift catalog · :9083"], "lake")
    box(il + 250, 704, 220, 58, "PostgreSQL", ["metastore + Airflow DB"], "lake")
    path([(il + 220, 733), (il + 250, 733)])
    trino_y, trino_h = 784, 50
    box(il, trino_y, ir - il, trino_h, "Trino", ["distributed SQL engine · :8080"], "lake")
    path([(il + 110, trino_y), (il + 110, 762)])
    s3x = ir - 45
    path([(s3x, trino_y), (s3x, 686)])
    text(s3x + 8, 738, "S3", "edge-label", "start")

    zone(c[3], r2y, COL_W, r2h, "Apache Airflow", "airflow", "daily 00:30 UTC")
    steps = [(598, "1 · Sync & curate", "dedupe · AQI · curated Parquet"),
             (660, "2 · Quality & daily rollups", "per-sensor + per-city"),
             (722, "3 · Weekly rollups", "city league table · ANALYZE"),
             (784, "Landing partition sync", "every 15 minutes")]
    for y, t, s in steps:
        box(c[3] + PAD, y, bw, 50, t, [s], "airflow")

    zone(c[4], r2y, COL_W, r2h, "Grafana", "grafana")
    box(c[4] + PAD, 590, bw, 115, "AQI Overview",
        ["current worst / median AQI", "AQI by city · pollutant explorer",
         "firing alerts · category timeline", "historical trends (Trino)"], "grafana")
    box(c[4] + PAD, 719, bw, 115, "Pipeline Health",
        ["Flink jobs · TaskManagers", "Kafka lag per partition", "checkpoints · backpressure",
         "MinIO capacity & requests"], "grafana")

    # Row 2 / cross-row edges
    tcy = trino_y + trino_h / 2
    path([(c[0] + PAD + bw, tcy), (il, tcy)])
    label((c[0] + PAD + bw + c[1]) / 2, tcy - 9, ["SQL · JDBC/ODBC"])
    path([(c[3] + PAD, tcy), (ir, tcy)])
    label((ir + c[3] + PAD) / 2, tcy - 9, ["Trino SQL (ETL)"])

    fx = f_l + bw / 2
    path([(fx, 424), (fx, 584)])
    label(fx + 10, 482, ["raw JSON files", "+ checkpoints"], "start")

    g_cx = c[4] + COL_W / 2
    path([(m_l + bw - 20, r1b), (m_l + bw - 20, 485), (g_cx, 485), (g_cx, 590)], dashed=True)
    label((m_l + bw - 20 + g_cx) / 2, 477, ["PromQL (live metrics)"])

    lane2 = 880
    path([(il + 300, trino_y + trino_h), (il + 300, lane2), (g_cx, lane2), (g_cx, 834)])
    label((il + 300 + g_cx) / 2, lane2 - 6, ["Trino SQL (historical panels)"])

    # ---------------- Roadmap ----------------
    ry, rh = 910, 150
    zone(LEFT, ry, W - 2 * LEFT, rh, "Production Roadmap", "roadmap", "not yet implemented",
         dashed=True)
    items = [("Flink on Kubernetes", ["Flink Operator · JobManager HA", "RocksDB + savepoints"]),
             ("HA Kafka cluster", ["3+ brokers · TLS/SASL", "min.insync.replicas = 2"]),
             ("Distributed MinIO", ["erasure coding", "per-zone bucket policies"]),
             ("Apache Iceberg tables", ["ACID MERGE · schema evolution", "time travel"]),
             ("CI/CD pipeline", ["pytest · compose config", "image smoke tests"]),
             ("Secrets management", ["Vault / cloud secrets manager", "no plaintext env vars"])]
    gap = 16
    iw = (W - 2 * LEFT - 2 * PAD - gap * (len(items) - 1)) / len(items)
    for i, (t, s) in enumerate(items):
        box(LEFT + PAD + i * (iw + gap), ry + 58, iw, 72, t, s, "roadmap", dashed=True)

    # ---------------- Legend ----------------
    ly = 1100
    path([(LEFT, ly), (LEFT + 40, ly)])
    text(LEFT + 50, ly + 4, "data flow", "legend", "start")
    path([(LEFT + 150, ly), (LEFT + 190, ly)], dashed=True)
    text(LEFT + 200, ly + 4, "metrics / monitoring", "legend", "start")
    parts.append(f'<rect x="{LEFT + 350}" y="{ly - 9}" width="28" height="18" rx="4" fill="#fafafa" '
                 f'stroke="{BOX_STROKE}" stroke-width="1.2" stroke-dasharray="5,4"/>')
    text(LEFT + 388, ly + 4, "planned (not yet implemented)", "legend", "start")

    style = f"""
text {{ font-family: 'Inter', 'Segoe UI', 'Helvetica Neue', Arial, sans-serif; fill: {INK}; }}
.title {{ font-size: 24px; font-weight: 700; }}
.subtitle {{ font-size: 13px; fill: {MUTED}; }}
.zone-title {{ font-size: 14px; font-weight: 700; }}
.zone-caption {{ font-size: 11px; fill: {MUTED}; }}
.sub-title {{ font-size: 11.5px; font-weight: 600; fill: #37474f; }}
.box-title {{ font-size: 12.5px; font-weight: 600; }}
.box-title-mono {{ font-size: 12.5px; font-weight: 600;
  font-family: 'JetBrains Mono', 'SFMono-Regular', Consolas, 'Liberation Mono', monospace; }}
.box-sub {{ font-size: 10.5px; fill: {MUTED}; }}
.pill {{ font-size: 11px; fill: #2e7d32; }}
.edge-label {{ font-size: 10.5px; fill: #37474f; }}
.legend {{ font-size: 11.5px; fill: #37474f; }}
"""
    head = (f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" '
            f'viewBox="0 0 {W} {H}">\n<defs>\n'
            f'  <marker id="ah" viewBox="0 0 10 10" refX="10" refY="5" markerWidth="9" '
            f'markerHeight="9" markerUnits="userSpaceOnUse" orient="auto">'
            f'<path d="M0,0 L10,5 L0,10 z" fill="{EDGE}"/></marker>\n'
            f'  <style>{style}</style>\n</defs>\n'
            f'<rect width="{W}" height="{H}" fill="#ffffff"/>\n')
    return head + "\n".join(parts) + "\n</svg>\n"


if __name__ == "__main__":
    OUT.write_text(build(), encoding="utf-8")
    print(f"wrote {OUT}")
