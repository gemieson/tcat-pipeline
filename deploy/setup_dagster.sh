#!/usr/bin/env bash
# Hand hourly dbt over from the systemd timer to Dagster.
# Run on the server from the project folder: bash deploy/setup_dagster.sh
# Safe to re-run -- re-run it after any dbt model change (it re-parses dbt).
set -euo pipefail
cd "$(dirname "$0")/.."
APP="$(pwd)"; ME="$(whoami)"
VENV="$APP/.venv-dbt/bin"

echo "== 1/5 install Dagster into the dbt virtualenv"
"$VENV/pip" install -q dagster dagster-webserver dagster-dbt

echo "== 2/5 Dagster home (run history, one run at a time)"
mkdir -p .dagster_home
cat > .dagster_home/dagster.yaml <<EOF
telemetry:
  enabled: false
# one run at a time: two dbt builds at once would fight over memory and tables
run_coordinator:
  module: dagster.core.run_coordinator
  class: QueuedRunCoordinator
  config:
    max_concurrent_runs: 1
EOF

echo "== 3/5 parse dbt (Dagster reads target/manifest.json)"
( cd dbt/tcat && "$VENV/dbt" deps && "$VENV/dbt" parse )

echo "== 4/5 retire the old systemd dbt timer (Dagster schedules replace it)"
sudo systemctl disable --now tcat-dbt.timer 2>/dev/null || true

echo "== 5/5 start Dagster services"
for f in deploy/systemd/tcat-dagster-webserver.service deploy/systemd/tcat-dagster-daemon.service; do
  sed -e "s#__APP__#$APP#g" -e "s#__USER__#$ME#g" "$f" \
    | sudo tee "/etc/systemd/system/$(basename "$f")" >/dev/null
done
sudo systemctl daemon-reload
sudo systemctl enable tcat-dagster-webserver tcat-dagster-daemon
sudo systemctl restart tcat-dagster-webserver tcat-dagster-daemon
sleep 15
systemctl is-active tcat-dagster-webserver tcat-dagster-daemon \
  | paste <(printf "webserver\ndaemon\n") -
echo; free -h | head -2
echo
echo "Open the UI from your laptop:"
echo "  gcloud compute ssh tcat-pipeline --zone=us-east1-b -- -N -L 3000:localhost:3000"
echo "  then browse to http://localhost:3000"
