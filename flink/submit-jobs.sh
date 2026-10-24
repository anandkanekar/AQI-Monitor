#!/bin/bash
# Submits the PyFlink jobs to the session cluster. Idempotent: jobs that are already
# RUNNING (matched by name) are skipped, so this can be re-run safely.
set -euo pipefail

JM="${FLINK_JOBMANAGER:-jobmanager:8081}"

echo "Waiting for Flink JobManager at ${JM}..."
until curl -sf "http://${JM}/overview" >/dev/null; do sleep 3; done
echo "Waiting for a TaskManager to register..."
until [ "$(curl -sf "http://${JM}/overview" | python3 -c 'import json,sys; print(json.load(sys.stdin)["slots-available"])')" -gt 0 ]; do sleep 3; done

is_running() {
  curl -sf "http://${JM}/jobs/overview" | python3 -c '
import json, sys
name = sys.argv[1]
jobs = json.load(sys.stdin)["jobs"]
sys.exit(0 if any(j["name"] == name and j["state"] in ("RUNNING", "CREATED", "INITIALIZING", "RESTARTING") for j in jobs) else 1)
' "$1"
}

submit() {
  local name=$1 script=$2
  if is_running "$name"; then
    echo "Job '${name}' already running - skipping."
    return
  fi
  echo "Submitting '${name}' (${script})..."
  /opt/flink/bin/flink run -m "${JM}" -d -py "/opt/flink/jobs/${script}"
}

submit "aqi-landing-zone" landing_zone_job.py
submit "aqi-detection" aqi_detection_job.py

curl -sf "http://${JM}/jobs/overview" | python3 -m json.tool
