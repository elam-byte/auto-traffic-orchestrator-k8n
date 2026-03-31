"""
Test script: Viz-Gateway WebSocket exploration
----------------------------------------------
What this does:
  1. Connects to the viz-gateway WebSocket at ws://localhost:8090
  2. Receives WorldSnapshot messages (same as the viewer gets)
  3. Prints them and saves a few to output/

Run:
  pip install websockets
  python scripts/test-viz-gateway.py [--count N]   (default: 10 frames)
"""

import asyncio
import json
import os
import sys
import time
from pathlib import Path

import websockets

WS_URL = os.environ.get("VIZ_WS_URL", "ws://localhost:8090")
OUTPUT_DIR = Path(__file__).parent.parent / "output"

# How many frames to capture (default 10, override with --count N)
COUNT = 10
for i, arg in enumerate(sys.argv):
    if arg == "--count" and i + 1 < len(sys.argv):
        COUNT = int(sys.argv[i + 1])


async def main():
    print(f"Connecting to viz-gateway WebSocket at {WS_URL} ...")
    try:
        ws = await websockets.connect(WS_URL, open_timeout=5)
    except Exception as e:
        print(f"ERROR: {e}")
        print("Make sure viz-gateway is running:  docker compose up viz-gateway")
        sys.exit(1)
    print(f"Connected. Capturing {COUNT} WorldSnapshot frames...\n")

    frames = []
    try:
        for i in range(COUNT):
            raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
            snapshot = json.loads(raw)
            frames.append(snapshot)

            n_vehicles = len(snapshot.get("vehicles", []))
            t = snapshot.get("t", 0)
            print(f"Frame {i+1:3d}  t={t}  vehicles={n_vehicles}")
            for v in snapshot.get("vehicles", []):
                print(f"         id={v['id']:8s}  x={v['x']:.2f}  y={v['y']:.2f}  "
                      f"heading={v['heading']:.3f} rad  "
                      f"len={v['length']} w={v['width']}")
            print()

    except asyncio.TimeoutError:
        print("TIMEOUT — no message received for 5 s. Is ats-env running and publishing?")
    finally:
        await ws.close()

    # Save captured frames
    OUTPUT_DIR.mkdir(exist_ok=True)
    out_path = OUTPUT_DIR / "viz-gateway-capture.json"
    out_path.write_text(json.dumps(frames, indent=2))
    print(f"Saved {len(frames)} frames to {out_path}")

    # Also save just the first frame as a clean example
    if frames:
        example_path = OUTPUT_DIR / "world-snapshot-live.json"
        example_path.write_text(json.dumps(frames[0], indent=2))
        print(f"First frame saved to {example_path}")


if __name__ == "__main__":
    asyncio.run(main())
