#!/usr/bin/env bash
# One-time: install dbt on the VM and schedule an hourly `dbt build`.
# Run from the project folder: bash deploy/setup_dbt.sh   (safe to re-run)
set -euo pipefail
cd "$(dirname "$0")/.."
APP="$(pwd)"; ME="$(whoami)"

echo "== 1/4 dbt virtualenv"
python3 -m venv .venv-dbt
.venv-dbt/bin/pip install -q --upgrade pip
.venv-dbt/bin/pip install -q dbt-bigquery

echo "== 2/4 profile (VM service account, no key file)"
mkdir -p ~/.dbt
cat > ~/.dbt/profiles.yml <<EOF
tcat:
  target: prod
  outputs:
    prod:
      type: bigquery
      method: oauth
      project: tcat-pipeline
      dataset: tcat_staging
      location: us-east1
      threads: 2
EOF

echo "== 3/4 dbt packages + connection check"
( cd dbt/tcat && "$APP/.venv-dbt/bin/dbt" deps && "$APP/.venv-dbt/bin/dbt" debug )

echo "== 4/4 systemd timer"
for f in deploy/systemd/tcat-dbt.service deploy/systemd/tcat-dbt.timer; do
  sed -e "s#__APP__#$APP#g" -e "s#__USER__#$ME#g" "$f" \
    | sudo tee "/etc/systemd/system/$(basename "$f")" >/dev/null
done
sudo systemctl daemon-reload
sudo systemctl enable --now tcat-dbt.timer

echo; echo "== first run now =="
sudo systemctl start tcat-dbt.service || true
journalctl -u tcat-dbt -n 25 --no-pager -o cat
echo; systemctl list-timers tcat-dbt.timer --no-pager
