#!/usr/bin/env bash
# TCAT raw collector -- Phase 0.
# Lands RAW bytes only. No parsing, no reshaping: parse downstream so you can
# reprocess when (not if) you find a bug.
#
# Usage:  ./scripts/collect.sh
# Stop:   Ctrl-C
#
# Layout (Hive-style so BigQuery external tables can partition-prune):
#   data/raw/vehicles/dt=YYYY-MM-DD/vehicles_<ts>.json.gz
#   data/raw/departures/dt=YYYY-MM-DD/stop=<id>/departures_<ts>.json.gz
#   data/raw/messages/dt=YYYY-MM-DD/messages_<ts>.json.gz
#   data/raw/_meta/dt=YYYY-MM-DD/meta.ndjson        <- every poll, incl. failures
#   data/dims/dt=YYYY-MM-DD/{stops,route_<id>}.json.gz

set -uo pipefail   # deliberately NOT -e: one bad poll must not kill the loop

BASE="https://realtimetcatbus.availtec.com/InfoPoint/rest"
ROOT="${ROOT:-data}"

# --- what to poll ------------------------------------------------------------
# Timepoint stops to watch for scheduled-vs-actual. Replace with your own picks
# from GetAllStops where IsTimePoint == true. Keep it to ~15-25.
DEPARTURE_STOPS=(100 165 172 1343 1344 1513 1541 1715 1534 339 349 374 541 549 665 3588 3579 1774 4783 2631 996)

VEHICLE_EVERY=30     # seconds
DEPARTURE_EVERY=60
MESSAGE_EVERY=300
DIMS_EVERY=86400
# -----------------------------------------------------------------------------

now_iso()  { date -u +%Y-%m-%dT%H:%M:%SZ; }
now_file() { date -u +%Y%m%dT%H%M%SZ; }
today()    { date -u +%Y-%m-%d; }

# fetch <url> <output-path-without-.gz> <source-label> [extra-meta-json]
# Always writes a metadata line. Only writes a payload on HTTP 200.
fetch() {
  local url="$1" out="$2" source="$3" extra="${4:-}"
  local dt; dt="$(today)"
  local meta_dir="$ROOT/raw/_meta/dt=$dt"
  mkdir -p "$meta_dir" "$(dirname "$out")"

  local ingest_ts; ingest_ts="$(now_iso)"
  local tmp; tmp="$(mktemp)"
  local line code dur bytes

  # -w gives us status, wall time and size without a second request.
  line="$(curl -sS --max-time 20 --compressed \
            -w '%{http_code} %{time_total} %{size_download}' \
            -o "$tmp" "$url" 2>/dev/null)" || line="000 0 0"
  read -r code dur bytes <<<"$line"

  local ok=false
  if [[ "$code" == "200" && "$bytes" -gt 0 ]]; then
    gzip -c "$tmp" > "${out}.gz" && ok=true
  fi
  rm -f "$tmp"

  printf '{"ingest_ts":"%s","source":"%s","url":"%s","http_code":%s,"duration_s":%s,"bytes":%s,"stored":%s%s}\n' \
    "$ingest_ts" "$source" "$url" "${code:-0}" "${dur:-0}" "${bytes:-0}" "$ok" \
    "${extra:+,$extra}" >> "$meta_dir/meta.ndjson"

  [[ "$ok" == true ]]
}

poll_vehicles() {
  local dt ts
  dt="$(today)"; ts="$(now_file)"
  fetch "$BASE/Routes/GetVisibleRoutes" \
        "$ROOT/raw/vehicles/dt=$dt/vehicles_$ts.json" \
        "vehicles"
}

poll_departures() {
  local dt ts stop
  dt="$(today)"; ts="$(now_file)"
  for stop in "${DEPARTURE_STOPS[@]}"; do
    fetch "$BASE/StopDepartures/Get/$stop" \
          "$ROOT/raw/departures/dt=$dt/stop=$stop/departures_$ts.json" \
          "departures" "\"stop_id\":$stop"
  done
}

poll_messages() {
  local dt ts
  dt="$(today)"; ts="$(now_file)"
  fetch "$BASE/PublicMessages/GetCurrentMessages" \
        "$ROOT/raw/messages/dt=$dt/messages_$ts.json" \
        "messages"
}

# Daily dimension snapshot: this is what makes SCD2 possible. Without
# yesterday's copy you cannot detect that RouteRecordId changed.
snapshot_dims() {
  local dt rid
  dt="$(today)"
  local dir="$ROOT/dims/dt=$dt"
  [[ -f "$dir/.done" ]] && return 0
  mkdir -p "$dir"

  fetch "$BASE/Stops/GetAllStops" "$dir/stops.json" "dim_stops"

  # Route ids straight from the live routes payload -- no hardcoded list to
  # drift out of date.
  local latest
  latest="$(ls -1t "$ROOT"/raw/vehicles/dt=*/vehicles_*.json.gz 2>/dev/null | head -1)"
  if [[ -n "$latest" ]]; then
    for rid in $(gzip -dc "$latest" | grep -o '"RouteId":[0-9]*' | cut -d: -f2 | sort -un); do
      fetch "$BASE/RouteDetails/Get/$rid" "$dir/route_$rid.json" \
            "dim_route" "\"route_id\":$rid"
      sleep 0.3
    done
  fi
  touch "$dir/.done"
}

echo "collecting into $ROOT/ -- Ctrl-C to stop"
trap 'echo; echo "stopped at $(now_iso)"; exit 0' INT TERM

start=$SECONDS
last_dep=0; last_msg=0; last_dim=0

while true; do
  elapsed=$(( SECONDS - start ))

  poll_vehicles

  if (( elapsed - last_dep >= DEPARTURE_EVERY )); then
    poll_departures & last_dep=$elapsed
  fi
  if (( elapsed - last_msg >= MESSAGE_EVERY )); then
    poll_messages; last_msg=$elapsed
  fi
  snapshot_dims

  # Heartbeat so you can tell it is alive without tailing files.
  printf '\r%s  polls: %s ' "$(now_iso)" \
    "$(ls -1 "$ROOT"/raw/vehicles/dt=*/ 2>/dev/null | wc -l | tr -d ' ')"

  sleep "$VEHICLE_EVERY"
done