#!/usr/bin/env bash
# One-time setup on the VM. Run from the project folder: bash deploy/setup.sh
# Safe to re-run.
set -euo pipefail
cd "$(dirname "$0")/.."
APP="$(pwd)"; ME="$(whoami)"
BUCKET="${GCS_BUCKET:-tcat-pipeline-tcat-raw}"
COMPOSE="sudo docker compose -f deploy/docker-compose.vm.yml"

echo "== 1/5 swap (1 GB of RAM is tight; swap stops out-of-memory kills)"
if ! swapon --show | grep -q /swapfile; then
  sudo fallocate -l 2G /swapfile
  sudo chmod 600 /swapfile && sudo mkswap /swapfile && sudo swapon /swapfile
  echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab >/dev/null
fi

echo "== 2/5 packages + docker"
sudo apt-get update -y
sudo apt-get install -y python3-venv python3-pip curl gzip
command -v docker >/dev/null || curl -fsSL https://get.docker.com | sudo sh

echo "== 3/5 python venv"
python3 -m venv .venv
.venv/bin/pip install -q --upgrade pip
.venv/bin/pip install -q requests confluent-kafka google-cloud-storage

echo "== 4/5 redpanda + topics"
$COMPOSE up -d
until $COMPOSE exec -T redpanda rpk cluster health 2>/dev/null | grep -q 'Healthy:.*true'; do
  echo "   waiting for redpanda..."; sleep 5
done
for t in tcat.vehicle_positions.v1 tcat.stop_departures.v1; do
  $COMPOSE exec -T redpanda rpk topic create "$t" -p 3 >/dev/null 2>&1 || true
done
$COMPOSE exec -T redpanda rpk topic list

echo "== 5/5 systemd services (auto-start on boot, auto-restart on crash)"
for f in deploy/systemd/*; do
  sed -e "s#__APP__#$APP#g" -e "s#__USER__#$ME#g" -e "s#__BUCKET__#$BUCKET#g" "$f" \
    | sudo tee "/etc/systemd/system/$(basename "$f")" >/dev/null
done
sudo systemctl daemon-reload
sudo systemctl enable --now tcat-producer tcat-consumer tcat-collector tcat-sync.timer

echo
echo "Done. Check with:  bash deploy/status.sh"
