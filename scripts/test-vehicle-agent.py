"""
Test script: Vehicle Agent standalone exploration
--------------------------------------------------
What this does:
  1. Connects to NATS (must be running: docker compose up nats)
  2. Publishes fake VehicleObservations to sim.obs.{id}
  3. Waits for the vehicle-agent to reply on sim.cmd.{id}
  4. Prints the command and saves results to output/

Three scenarios:
  A. Straight road, below target speed  — expects accel > 0, steer ≈ 0
  B. Off-heading, needs steering         — expects non-zero steer
  C. Approaching a junction             — expects junction_choice in the command

Run:
  pip install nats-py
  # Start a vehicle-agent first (fleet-shared mode, no VEHICLE_ID):
  docker compose up nats
  docker run --rm --network ats_default -e NATS_URL=nats://nats:4222 vehicle-agent:latest
  # Then:
  python scripts/test-vehicle-agent.py
"""

import asyncio
import json
import os
import sys
from pathlib import Path

import nats

NATS_URL = os.environ.get("NATS_URL", "nats://localhost:4222")
OUTPUT_DIR = Path(__file__).parent.parent / "output"

# ── Scenario A: straight road, below target speed ─────────────────────────────
SCENARIO_A = {
    "t": 1711900000000,
    "id": "v-1",
    "x": 250.5, "y": 196.8,
    "heading": 0.0,
    "speed": 5.0,
    "lane_corridor": [
        {"x": 251.5, "y": 196.8, "heading": 0.0, "width": 3.5, "speed_limit": 14.0, "lane_id": "r-1:right", "road_id": "r-1"},
        {"x": 260.0, "y": 196.8, "heading": 0.0, "width": 3.5, "speed_limit": 14.0, "lane_id": "r-1:right", "road_id": "r-1"},
        {"x": 270.0, "y": 196.8, "heading": 0.0, "width": 3.5, "speed_limit": 14.0, "lane_id": "r-1:right", "road_id": "r-1"},
        {"x": 280.0, "y": 196.8, "heading": 0.0, "width": 3.5, "speed_limit": 14.0, "lane_id": "r-1:right", "road_id": "r-1"},
        {"x": 290.0, "y": 196.8, "heading": 0.0, "width": 3.5, "speed_limit": 14.0, "lane_id": "r-1:right", "road_id": "r-1"},
        {"x": 300.0, "y": 196.8, "heading": 0.0, "width": 3.5, "speed_limit": 14.0, "lane_id": "r-1:right", "road_id": "r-1"},
    ],
}

# ── Scenario B: off-heading, needs to steer ───────────────────────────────────
SCENARIO_B = {
    "t": 1711900001000,
    "id": "v-1",
    "x": 250.5, "y": 196.8,
    "heading": 0.3,   # drifted CCW — corridor curves back
    "speed": 8.0,
    "lane_corridor": [
        {"x": 251.0, "y": 196.5, "heading": -0.05, "width": 3.5, "speed_limit": 14.0, "lane_id": "r-1:right", "road_id": "r-1"},
        {"x": 255.0, "y": 196.2, "heading": -0.03, "width": 3.5, "speed_limit": 14.0, "lane_id": "r-1:right", "road_id": "r-1"},
        {"x": 265.0, "y": 196.0, "heading":  0.00, "width": 3.5, "speed_limit": 14.0, "lane_id": "r-1:right", "road_id": "r-1"},
        {"x": 280.0, "y": 196.0, "heading":  0.00, "width": 3.5, "speed_limit": 14.0, "lane_id": "r-1:right", "road_id": "r-1"},
    ],
}

# ── Scenario C: approaching a junction ────────────────────────────────────────
# junction field tells the agent: "at edge r-1:right, you can go to r-6:right or r-5:left"
SCENARIO_C = {
    "t": 1711900002000,
    "id": "v-1",
    "x": 315.0, "y": 196.8,
    "heading": 0.0,
    "speed": 8.0,
    "lane_corridor": [
        {"x": 316.0, "y": 196.8, "heading": 0.0,   "width": 3.5, "speed_limit": 14.0, "lane_id": "r-1:right", "road_id": "r-1"},
        {"x": 320.0, "y": 196.8, "heading": 0.0,   "width": 3.5, "speed_limit": 14.0, "lane_id": "r-1:right", "road_id": "r-1"},
        {"x": 325.0, "y": 193.0, "heading": -0.4,  "width": 3.5, "speed_limit": 14.0, "lane_id": "r-6:right", "road_id": "r-6"},
        {"x": 328.0, "y": 188.0, "heading": -0.8,  "width": 3.5, "speed_limit": 14.0, "lane_id": "r-6:right", "road_id": "r-6"},
    ],
    "junction": {
        "at_edge": "r-1:right",
        "choices": ["r-6:right", "r-5:left"],
        "current_choice": "r-6:right"
    },
}


async def main():
    print(f"Connecting to NATS at {NATS_URL}...")
    try:
        nc = await nats.connect(NATS_URL, connect_timeout=5)
    except Exception as e:
        print(f"ERROR: Cannot connect to NATS — {e}")
        print("Make sure NATS is running:  docker compose up nats")
        sys.exit(1)
    print("Connected.\n")

    results = []

    scenarios = [
        ("A: straight road, below target speed — expect accel>0, steer≈0", SCENARIO_A),
        ("B: off-heading, needs steering correction — expect non-zero steer", SCENARIO_B),
        ("C: approaching junction — expect junction_choice in command",       SCENARIO_C),
    ]

    for label, obs in scenarios:
        print(f"--- Scenario {label} ---")
        print("INPUT  (VehicleObservation):")
        print(json.dumps(obs, indent=2))

        future: asyncio.Future = asyncio.get_event_loop().create_future()

        async def handle(msg, f=future):
            if not f.done():
                f.set_result(msg.data.decode())

        sub = await nc.subscribe(f"sim.cmd.{obs['id']}", cb=handle)
        await nc.publish(f"sim.obs.{obs['id']}", json.dumps(obs).encode())
        print(f"\nPublished to:  sim.obs.{obs['id']}")
        print(f"Waiting for reply on: sim.cmd.{obs['id']} ...")

        try:
            reply_raw = await asyncio.wait_for(future, timeout=5.0)
            reply = json.loads(reply_raw)
            print("\nOUTPUT (VehicleCommand):")
            print(json.dumps(reply, indent=2))
            if "junction_choice" in reply:
                print(f"  → junction_choice: {reply['junction_choice']}")
            results.append({"scenario": label, "observation": obs, "command": reply})
        except asyncio.TimeoutError:
            print("TIMEOUT — vehicle-agent did not reply within 5 s.")
            print("Make sure a vehicle-agent is running (fleet-shared mode):")
            print("  docker run --rm --network ats_default -e NATS_URL=nats://nats:4222 vehicle-agent:latest")
        finally:
            await sub.unsubscribe()

        print()

    await nc.close()

    out_path = OUTPUT_DIR / "vehicle-agent-test-results.json"
    OUTPUT_DIR.mkdir(exist_ok=True)
    out_path.write_text(json.dumps(results, indent=2))
    print(f"Results saved to {out_path}")


if __name__ == "__main__":
    asyncio.run(main())
