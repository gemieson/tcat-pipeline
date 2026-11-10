#!/usr/bin/env bash
# Health check on the VM.
cd "$(dirname "$0")/.."
BUCKET="${GCS_BUCKET:-tcat-pipeline-tcat-raw}"
echo "=== services ==="
systemctl is-active tcat-producer tcat-consumer tcat-collector tcat-sync.timer \
  | paste <(printf "producer\nconsumer\ncollector\nsync-timer\n") -
echo; echo "=== memory ==="; free -h | head -2
echo; echo "=== disk (space, then inodes) ==="; df -h / | tail -1; df -i / | tail -1
echo; echo "=== last producer lines ==="; journalctl -u tcat-producer -n 3 --no-pager -o cat
echo; echo "=== last consumer lines ==="; journalctl -u tcat-consumer -n 3 --no-pager -o cat
echo; echo "=== consumer lag ==="
sudo docker compose -f deploy/docker-compose.vm.yml exec -T redpanda rpk group describe tcat-gcs-sink 2>/dev/null | tail -7
echo; echo "=== quarantined messages (should be empty) ==="
gcloud storage ls "gs://$BUCKET/quarantine/**" 2>/dev/null | tail -3
echo; echo "=== newest GCS file per stream ==="
for s in vehicles departures; do
  echo -n "$s: "; gcloud storage ls "gs://$BUCKET/raw/$s/**" 2>/dev/null | sort | tail -1
done
