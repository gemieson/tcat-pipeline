"""Fetch the stop sequence for every route."""
import json
import time

import requests

BASE = "https://realtimetcatbus.availtec.com/InfoPoint/rest"

routes = json.load(open("data/probe/Routes_GetVisibleRoutes.json"))
out = []

for r in routes:
    rid = r["RouteId"]
    resp = requests.get(f"{BASE}/RouteDetails/Get/{rid}", timeout=20)
    out.append(resp.json())
    print(f"route {rid}: {len(resp.json().get('RouteStops') or [])} stops")
    time.sleep(0.3)   # be polite to an API you don't own

json.dump(out, open("data/reference/route_details.json", "w"), indent=2)
print(f"\n{len(out)} routes saved")