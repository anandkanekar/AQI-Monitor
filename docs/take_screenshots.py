"""Capture running-stack UI screenshots into docs/screenshots/.

Requires the stack to be up (`docker compose up -d`) and a local Chrome install:

    pip install playwright
    python docs/take_screenshots.py
"""
import time
from playwright.sync_api import sync_playwright

OUT = "docs/screenshots"

with sync_playwright() as p:
    browser = p.chromium.launch(channel="chrome", headless=True)
    page = browser.new_page(viewport={"width": 1680, "height": 1050})

    # ---- Grafana ----
    page.goto("http://localhost:3000/login")
    page.fill('input[name="user"]', "admin")
    page.fill('input[name="password"]', "admin")
    page.click('button[type="submit"]')
    time.sleep(4)
    # Grafana offers a "Skip" link on the default-password warning screen
    if page.locator('text=Skip').count():
        page.click('text=Skip')
        page.wait_for_load_state("networkidle")

    page.goto("http://localhost:3000/d/aqi-overview?kiosk&from=now-30d&to=now",
              wait_until="networkidle")
    time.sleep(15)  # let panel queries + canvases render
    page.screenshot(path=f"{OUT}/grafana-aqi-overview.png", full_page=True)

    page.goto("http://localhost:3000/d/aqi-pipeline?kiosk&from=now-3h&to=now",
              wait_until="networkidle")
    time.sleep(15)
    page.screenshot(path=f"{OUT}/grafana-aqi-pipeline.png", full_page=True)

    # ---- Airflow ----
    page.goto("http://localhost:8088/login/")
    page.fill('input[name="username"]', "admin")
    page.fill('input[name="password"]', "admin")
    page.click('input[type="submit"], button[type="submit"]')
    page.wait_for_load_state("networkidle")
    page.goto("http://localhost:8088/dags/aqi_daily_aggregation/grid")
    page.wait_for_load_state("networkidle")
    time.sleep(4)
    page.screenshot(path=f"{OUT}/airflow-dag.png", full_page=True)

    # ---- Flink UI ----
    page.goto("http://localhost:8081/", wait_until="networkidle")
    time.sleep(4)
    page.screenshot(path=f"{OUT}/flink-overview.png", full_page=True)

    # ---- Trino UI ----
    page.goto("http://localhost:8080/ui/login.html")
    page.fill('input[name="username"]', "admin")
    page.click('button[type="submit"], input[type="submit"]')
    page.wait_for_load_state("networkidle")
    time.sleep(4)
    page.screenshot(path=f"{OUT}/trino-ui.png", full_page=True)

    browser.close()
    print("done")
