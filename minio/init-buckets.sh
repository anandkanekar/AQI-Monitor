#!/bin/sh
# Creates the storage zones in MinIO and applies lifecycle rules.
set -eu

until mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null 2>&1; do
  echo "Waiting for MinIO..."; sleep 2
done

for bucket in landing processed warehouse flink; do
  mc mb --ignore-existing "local/${bucket}"
done

# Processed zone is the system of record for curated data: keep object versions.
mc version enable local/processed

# Landing zone keeps raw data for LANDING_RETENTION_DAYS (curated copies live in processed/).
if ! mc ilm rule ls local/landing 2>/dev/null | grep -q Expir; then
  mc ilm rule add --expire-days "${LANDING_RETENTION_DAYS:-90}" local/landing
fi
# Flink checkpoints older than 7 days are garbage.
if ! mc ilm rule ls local/flink 2>/dev/null | grep -q Expir; then
  mc ilm rule add --expire-days 7 local/flink
fi

mc ls local
echo "Buckets ready."
