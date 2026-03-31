"""
Test script: ATS Environment exploration
-----------------------------------------
What this does:
  1. Uploads the map to viz-gateway (POST /map with X-Map-Filename header)
     → viz-gateway saves to /maps/, publishes { mapRef } to sim.map
     → ats-env reads the file, publishes sim.vehicles, starts tick loop
  2. Connects to NATS
  3. Subscribes to sim.obs.> to see VehicleObservations (with lane_id/road_id/junction)
  4. Subscribes to sim.snapshots to see WorldSnapshots
  5. Publishes mock VehicleCommands back so ats-env can integrate them
  6. Captures 5 ticks and shows how vehicle position changes tick by tick

This lets you see ats-env in action WITHOUT needing vehicle-agent or the orchestrator.
You become the vehicle-agent manually.

Run:
  pip install nats-py
  docker compose up nats ats-env viz-gateway
  python scripts/test-ats-env.py
"""

import asyncio
import json
import os
import sys
import urllib.request
import urllib.error
from pathlib import Path

import nats

NATS_URL   = os.environ.get("NATS_URL",  "nats://localhost:4222")
VIZ_URL    = os.environ.get("VIZ_URL",   "http://localhost:8090")
MAP_PATH   = Path(__file__).parent.parent / "map" / "ats-map-v1.json"
OUTPUT_DIR = Path(__file__).parent.parent / "output"
TICKS_TO_CAPTURE = 5


def upload_map() -> bool:
    """POST map JSON to viz-gateway so ats-env can start."""
    try:
        raw = MAP_PATH.read_text()
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
                print(f"[upload] Map saved to {result.get('mapRef', '/maps/')} → sim.map → ats-env")
                return True
    except urllib.error.URLError as e:
        print(f"[upload] WARNING: Could not reach viz-gateway ({e.reason}) — ats-env may already have a map.")
    except Exception as e:
        print(f"[upload] WARNING: {e}")
    return False


async def main():
    print("Step 1: Upload map so ats-env can initialise...")
    upload_map()
    print()
    print("Step 2: Connect to NATS to watch the tick loop...")
    try:
        nc = await nats.connect(NATS_URL, connect_timeout=5)
    except Exception as e:
        print(f"ERROR: {e}")
        print("Make sure NATS is running:  docker compose up nats")
        sys.exit(1)
    print(f"Connected to {NATS_URL}\n")

    observations = []
    snapshots = []
    commands_sent = []

    # ── Observation handler ─────────────────────────────────────────────────
    async def on_observation(msg):
        vehicle_id = msg.subject.split(".", 2)[-1]
        obs = json.loads(msg.data.decode())
        observations.append(obs)

        corridor = obs.get("lane_corridor", [])
        first_lane = corridor[0].get("lane_id", "?") if corridor else "?"
        junction_info = ""
        if obs.get("junction"):
            j = obs["junction"]
            junction_info = f"  JUNCTION at={j['at_edge']} choices={j['choices']}"

        print(f"[obs]  vehicle={vehicle_id:8s}  t={obs['t']}  "
              f"x={obs['x']:.2f}  y={obs['y']:.2f}  "
              f"heading={obs['heading']:.3f}  speed={obs['speed']:.2f} m/s  "
              f"lane={first_lane}  corridor_pts={len(corridor)}{junction_info}")

        # Reply with a simple command: accelerate to cruising speed, drive straight
        # If the observation includes a junction, echo back the current_choice
        cmd = {
            "t": obs["t"],
            "id": obs["id"],
            "desired_accel": 0.5 if obs["speed"] < 8.0 else 0.0,
            "desired_steer": 0.0,
        }
        if obs.get("junction"):
            j = obs["junction"]
            cmd["junction_choice"] = {"at_edge": j["at_edge"], "choice": j["current_choice"]}

        await nc.publish(f"sim.cmd.{obs['id']}", json.dumps(cmd).encode())
        commands_sent.append(cmd)
        print(f"[cmd]  vehicle={vehicle_id:8s}  accel={cmd['desired_accel']:.2f}  steer={cmd['desired_steer']:.2f}")

    # ── Snapshot handler ────────────────────────────────────────────────────
    async def on_snapshot(msg):
        snap = json.loads(msg.data.decode())
        snapshots.append(snap)
        n = len(snap.get("vehicles", []))
        print(f"[snap] t={snap['t']}  vehicles_in_world={n}")

    sub_obs  = await nc.subscribe("sim.obs.>", cb=on_observation)
    sub_snap = await nc.subscribe("sim.snapshots", cb=on_snapshot)

    print("Subscribed. Waiting for ats-env to publish observations...")
    print("(Is ats-env running?  docker compose up ats-env)\n")

    # Wait until we've seen at least TICKS_TO_CAPTURE observation cycles
    start = asyncio.get_event_loop().time()
    while len(observations) < TICKS_TO_CAPTURE:
        await asyncio.sleep(0.1)
        if asyncio.get_event_loop().time() - start > 10:
            print("TIMEOUT — ats-env did not publish observations within 10 s.")
            break

    await sub_obs.unsubscribe()
    await sub_snap.unsubscribe()
    await nc.close()

    # ── Summary ─────────────────────────────────────────────────────────────
    print(f"\n--- Captured {len(observations)} observations, {len(snapshots)} snapshots ---")

    if observations:
        ids = list({o["id"] for o in observations})
        for vid in ids:
            vobs = [o for o in observations if o["id"] == vid]
            print(f"\nVehicle {vid} position trace:")
            for o in vobs:
                print(f"  t={o['t']}  x={o['x']:.3f}  y={o['y']:.3f}  "
                      f"speed={o['speed']:.3f}  heading={o['heading']:.4f}")

    # ── Save outputs ─────────────────────────────────────────────────────────
    OUTPUT_DIR.mkdir(exist_ok=True)

    if observations:
        p = OUTPUT_DIR / "ats-env-observations-captured.json"
        p.write_text(json.dumps(observations[:10], indent=2))
        print(f"\nObservations saved to {p}")

    if snapshots:
        p = OUTPUT_DIR / "ats-env-snapshots-captured.json"
        p.write_text(json.dumps(snapshots[:5], indent=2))
        print(f"Snapshots saved to {p}")

    if commands_sent:
        p = OUTPUT_DIR / "ats-env-commands-sent.json"
        p.write_text(json.dumps(commands_sent[:10], indent=2))
        print(f"Commands sent saved to {p}")


if __name__ == "__main__":
    asyncio.run(main())
