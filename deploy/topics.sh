#!/usr/bin/env bash
# Redpanda topics and cluster settings, as code. Safe to re-run: creates what's
# missing and re-applies the config below to topics that already exist.
# Run on the VM from the project folder: bash deploy/topics.sh
set -euo pipefail
cd "$(dirname "$0")/.."
RPK="${RPK:-sudo docker compose -f deploy/docker-compose.vm.yml exec -T redpanda rpk}"

PARTITIONS=3
# Redpanda is a buffer in front of GCS: retention only has to outlast a
# consumer outage nobody noticed (alerts fire within ~6h). 7 days by time; the
# per-partition size cap is a backstop so a backlog can never fill the VM disk
# (2 topics x 3 partitions x 512 MiB = 3 GiB at most). docs/DECISIONS.md, D10.
TOPIC_CONFIG=(retention.ms=604800000 retention.bytes=536870912)
TOPICS=(tcat.vehicle_positions.v1 tcat.stop_departures.v1)

existing="$($RPK topic list | awk 'NR > 1 {print $1}')"
for t in "${TOPICS[@]}"; do
  if grep -qx "$t" <<<"$existing"; then
    echo "   $t exists: re-applying config"
  else
    echo "   creating $t"
    $RPK topic create "$t" -p "$PARTITIONS" $(printf -- '-c %s ' "${TOPIC_CONFIG[@]}")
  fi
  $RPK topic alter-config "$t" $(printf -- '--set %s ' "${TOPIC_CONFIG[@]}")
done

# Continuous partition and core balancing are Enterprise features with nothing
# to balance on one broker. Switch them off where this version has them.
for kv in partition_autobalancing_mode=node_add core_balancing_continuous=false; do
  key="${kv%%=*}"
  if $RPK cluster config get "$key" >/dev/null 2>&1; then
    $RPK cluster config set "$key" "${kv#*=}"
  else
    echo "   this Redpanda version has no setting $key: skipped"
  fi
done

$RPK topic list
