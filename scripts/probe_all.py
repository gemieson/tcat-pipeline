"""Probe the TCAT Avail InfoPoint API: what exists, and what shape it returns."""
import json
import os
import requests

BASE = "https://realtimetcatbus.availtec.com/InfoPoint/rest"
OUT = "data/probe"
os.makedirs(OUT, exist_ok=True)

# Endpoints to test. Route-scoped ones use route 10 (DTown-Cornell Loop);
# stop-scoped ones use stop 100 (Ithaca Commons - Seneca St), both confirmed real.
ENDPOINTS = [
    "Routes/GetVisibleRoutes",
    "Routes/GetAllRoutes",
    "RouteDetails/Get/10",
    "RouteDetails/GetDirectionsByRouteId/10",
    "RouteDetails/GetStopsByRouteId/10",
    "Stops/GetAllStops",
    "Stops/GetAllStopsForRoute?routeId=10",
    "Stops/GetStopDepartures?stopIds=100",
    "Stops/GetStopDeparturesForRoutes?stopIds=100&routeIds=10",
    "Vehicles/GetAllVehiclesForRoute?routeID=10",
    "Vehicles/GetAllVehiclesForRoutes?routeIDs=10",
    "PublicMessages/GetCurrentMessages",
]


def describe(value, depth=0, max_depth=2):
    """Summarize a JSON structure without printing the whole thing."""
    pad = "  " * (depth + 1)

    if isinstance(value, list):
        if not value:
            return "empty list"
        inner = describe(value[0], depth, max_depth)
        return f"list of {len(value)} — first item: {inner}"

    if isinstance(value, dict):
        if depth >= max_depth:
            return f"object with {len(value)} fields"
        lines = [f"object, {len(value)} fields:"]
        for k, v in value.items():
            if isinstance(v, (list, dict)):
                lines.append(f"{pad}{k}: {describe(v, depth + 1, max_depth)}")
            else:
                shown = repr(v)
                if len(shown) > 60:
                    shown = shown[:57] + "..."
                lines.append(f"{pad}{k}: {shown}")
        return "\n".join(lines)

    return repr(value)


def probe(path):
    slug = path.replace("/", "_").replace("?", "_").replace("=", "-").replace("&", "_")

    try:
        r = requests.get(f"{BASE}/{path}", timeout=20)
    except requests.RequestException as e:
        print(f"\n### {path}\n    REQUEST FAILED: {e}")
        return

    print(f"\n### {path}")
    print(f"    {r.status_code} · {len(r.content):,} bytes")

    if r.status_code != 200:
        print(f"    {r.text[:200]}")
        return

    try:
        data = r.json()
    except ValueError:
        # A 200 that isn't JSON usually means an HTML error page.
        print(f"    not JSON: {r.text[:200]}")
        return

    # Keep the raw response — cheap now, useful when you're writing models later.
    with open(f"{OUT}/{slug}.json", "w") as f:
        json.dump(data, f, indent=2)

    print("    " + describe(data).replace("\n", "\n    "))


if __name__ == "__main__":
    for path in ENDPOINTS:
        probe(path)
    print(f"\nRaw responses saved to {OUT}/")