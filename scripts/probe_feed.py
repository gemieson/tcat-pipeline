"""Probe the TCAT Avail InfoPoint API and print what's actually there."""
import json
import requests

BASE = "https://realtimetcatbus.availtec.com/InfoPoint/rest"


def get(path):
    r = requests.get(f"{BASE}/{path}", timeout=15)
    print(f"\n=== {path} -> {r.status_code} ({len(r.content)} bytes)")
    if r.status_code != 200:
        print(r.text[:300])
        return None
    try:
        return r.json()
    except Exception as e:
        print(f"not JSON: {e}")
        print(r.text[:300])
        return None


routes = get("Routes/GetVisibleRoutes")
if routes:
    print(f"{len(routes)} routes")
    for route in routes:
        print(route["RouteId"], route["LongName"])
    route_id = routes[0].get("RouteId")
    vehicles = get(f"Vehicles/GetAllVehiclesForRoute?routeID={route_id}")
    if vehicles:
        print(f"{len(vehicles)} vehicles on route {route_id}")
        if vehicles:
            print(json.dumps(vehicles[0], indent=2))
        else:
            print("no vehicles running right now — try during service hours")

stops = get("Stops/GetAllStops")
if stops:
    print(f"{len(stops)} stops")
    print(json.dumps(stops[0], indent=2))