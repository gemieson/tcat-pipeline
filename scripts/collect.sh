#!/usr/bin/env bash
# TCAT side collector: the two things the Kafka pipeline doesn't collect.
#   * service alerts (PublicMessages) every 5 minutes
#   * a daily snapshot of all stops and each route's details, which is what
#     makes change tracking (SCD2) possible later
#
# Vehicles and departures are NOT polled here any more. producer/produce.py
# collects them; keeping a second copy here wrote ~33,000 files a day to the
# VM disk, never deleted, and uploaded driver names (docs/DECISIONS.md, D3).
#
# Lands RAW bytes only. No parsing, no reshaping: parse downstream so you can
# reprocess when (not if) you find a bug.
#
# Layout (Hive-style so BigQuery external tables can partition-prune):
#   data/raw/messages/dt=YYYY-MM-DD/messages_<ts>.json.gz
#   data/raw/_meta/dt=YYYY-MM-DD/meta.ndjson        <- every request, incl. failures
#     (the producer writes producer.ndjson beside it: one request log, D11)
#   data/dims/dt=YYYY-MM-DD/{stops,route_<id>}.json.gz  + .done once complete

set -uo pipefail   # deliberately NOT -e: one bad request must not kill the loop

BASE="${TCAT_BASE:-https://realtimetcatbus.availtec.com/InfoPoint/rest}"
ROOT="${ROOT:-data}"
MESSAGE_EVERY="${MESSAGE_EVERY:-300}"       # seconds
DIM_RETRY_EVERY="${DIM_RETRY_EVERY:-3600}"  # after a failed snapshot, wait this long

now_iso()  { date -u +%Y-%m-%dT%H:%M:%SZ; }
now_file() { date -u +%Y%m%dT%H%M%SZ; }
today()    { date -u +%Y-%m-%d; }

# fetch <url> <output-path-without-.gz> <source-label> [extra-meta-json]
# Always writes a metadata line. Only writes a payload on HTTP 200, and writes
# it atomically: gzip to <out>.gz.part, then rename. A kill mid-write leaves a
# .part file that the sync never mistakes for a complete one.
fetch() {
  local url="$1" out="$2" source="$3" extra="${4:-}"
  local meta_dir="$ROOT/raw/_meta/dt=$(today)"
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
  if [[ "$code" == "200" && "${bytes:-0}" -gt 0 ]]; then
    if gzip -c "$tmp" > "${out}.gz.part" && mv -f "${out}.gz.part" "${out}.gz"; then
      ok=true
    else
      rm -f "${out}.gz.part"
    fi
  fi
  rm -f "$tmp"

  printf '{"ingest_ts":"%s","writer":"collector","source":"%s","url":"%s","http_code":%s,"duration_s":%s,"bytes":%s,"stored":%s%s}\n' \
    "$ingest_ts" "$source" "$url" "${code:-0}" "${dur:-0}" "${bytes:-0}" "$ok" \
    "${extra:+,$extra}" >> "$meta_dir/meta.ndjson"

  [[ "$ok" == true ]]
}

poll_messages() {
  fetch "$BASE/PublicMessages/GetCurrentMessages" \
        "$ROOT/raw/messages/dt=$(today)/messages_$(now_file).json" \
        "messages"
}

# Route ids of the routes running right now. The response includes vehicles
# (and driver names), so it is read and discarded, never stored.
visible_route_ids() {
  local tmp; tmp="$(mktemp)"
  if curl -sS --max-time 20 --compressed -f -o "$tmp" "$BASE/Routes/GetVisibleRoutes" 2>/dev/null; then
    grep -oE '"RouteId": *[0-9]+' "$tmp" | grep -oE '[0-9]+$' | sort -un
  fi
  rm -f "$tmp"
}

# Daily dimension snapshot. The day is marked .done only if every request
# succeeded; otherwise it is retried after DIM_RETRY_EVERY seconds.
snapshot_dims() {
  local dir="$ROOT/dims/dt=$(today)"
  [[ -f "$dir/.done" ]] && return 0
  mkdir -p "$dir"

  local ok=true rid rids
  fetch "$BASE/Stops/GetAllStops" "$dir/stops.json" "dim_stops" || ok=false

  rids="$(visible_route_ids)"
  if [[ -z "$rids" ]]; then
    ok=false   # no buses visible (or the call failed): try again later
  fi
  for rid in $rids; do
    fetch "$BASE/RouteDetails/Get/$rid" "$dir/route_$rid.json" \
          "dim_route" "\"route_id\":$rid" || ok=false
    sleep 0.3
  done

  if [[ "$ok" == true ]]; then
    touch "$dir/.done"
    echo "$(now_iso) dims snapshot complete: stops + $(wc -w <<<"$rids" | tr -d ' ') routes"
  else
    echo "$(now_iso) dims snapshot incomplete, retrying in ${DIM_RETRY_EVERY}s" >&2
    return 1
  fi
}

echo "collecting alerts + daily dims into $ROOT/ -- Ctrl-C to stop"
trap 'kill "${sleeper:-}" 2>/dev/null; echo "stopped at $(now_iso)"; exit 0' INT TERM

last_dim_try=-$DIM_RETRY_EVERY
while true; do
  if poll_messages; then
    echo "$(now_iso) messages ok"
  else
    echo "$(now_iso) messages failed" >&2
  fi

  if (( SECONDS - last_dim_try >= DIM_RETRY_EVERY )); then
    snapshot_dims || last_dim_try=$SECONDS
  fi

  sleep "$MESSAGE_EVERY" &
  sleeper=$!
  wait "$sleeper"   # so SIGTERM is handled at once, not after the sleep
done
