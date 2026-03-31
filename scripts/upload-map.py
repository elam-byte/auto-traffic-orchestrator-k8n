"""
Upload a map JSON file to ats-env via viz-gateway.
----------------------------------------------------
This is the command-line equivalent of loading a map in the viewer.
ats-env does NOT start until it receives a map on sim.map.

Usage:
  python scripts/upload-map.py                         # uses map/ats-map-v1.json
  python scripts/upload-map.py path/to/my-map.json
  VIZ_URL=http://localhost:8090 python scripts/upload-map.py

What happens:
  1. This script POSTs the map JSON to http://localhost:8090/map
     with the filename in the X-Map-Filename header
  2. viz-gateway saves the file to /maps/{filename} (shared volume)
     and publishes { mapRef: "/maps/{filename}" } to NATS sim.map
  3. ats-env reads the file from /maps/{filename}, builds the road graph,
     publishes sim.vehicles, and starts the 20 Hz tick loop
  4. vehicle-orchestrator receives sim.vehicles and creates one
     vehicle-{id} container per vehicle in the map
"""

import json
import sys
import urllib.request
import urllib.error
from pathlib import Path

VIZ_URL = __import__("os").environ.get("VIZ_URL", "http://localhost:8090")
MAP_PATH = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).parent.parent / "map" / "ats-map-v1.json"

print(f"Reading map from: {MAP_PATH}")
try:
    raw = MAP_PATH.read_text()
    data = json.loads(raw)
except FileNotFoundError:
    print(f"ERROR: File not found: {MAP_PATH}")
    sys.exit(1)
except json.JSONDecodeError as e:
    print(f"ERROR: Invalid JSON: {e}")
    sys.exit(1)

print(f"Map: {len(data.get('roads', []))} roads, {len(data.get('vehicles', []))} vehicles")
print(f"Uploading to {VIZ_URL}/map ...")

try:
    req = urllib.request.Request(
        f"{VIZ_URL}/map",
        data=raw.encode(),
        headers={
            "Content-Type": "application/json",
            "X-Map-Filename": MAP_PATH.name,   # viz-gateway uses this as the saved filename
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        result = json.loads(resp.read())
        if result.get("ok"):
            print(f"Map saved to {result.get('mapRef', '/maps/')} on viz-gateway.")
            print("ats-env will read the file, publish sim.vehicles, and start the tick loop.")
            print("vehicle-orchestrator will create one container per vehicle.")
        else:
            print(f"Upload failed: {result}")
except urllib.error.URLError as e:
    print(f"ERROR: Could not reach viz-gateway at {VIZ_URL}: {e.reason}")
    print("Make sure viz-gateway is running:  docker compose up viz-gateway")
    sys.exit(1)
