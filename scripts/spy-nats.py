"""
NATS spy — subscribe to every ATS subject and print all messages live.
----------------------------------------------------------------------
Subjects watched:
  sim.map          { mapRef }                  viz-gateway → ats-env  (on map upload)
  sim.vehicles     { vehicles: [...] }         ats-env → orchestrator (on map load)
  sim.obs.>        VehicleObservation          ats-env → agent        (20 Hz per vehicle)
  sim.cmd.>        VehicleCommand              agent → ats-env        (in response)
  sim.snapshots    WorldSnapshot               ats-env → gateway      (20 Hz)

Run:
  pip install nats-py
  python scripts/spy-nats.py [--save]

Options:
  --save   Write a capture file to output/nats-capture.jsonl (one JSON per line)
"""

import asyncio
import json
import os
import sys
import time
from pathlib import Path

import nats

NATS_URL = os.environ.get("NATS_URL", "nats://localhost:4222")
SAVE = "--save" in sys.argv
OUTPUT_FILE = Path(__file__).parent.parent / "output" / "nats-capture.jsonl"

# Track message counts per subject for the summary line
counts: dict[str, int] = {}
save_fp = None


def pretty(subject: str, data: bytes) -> str:
    try:
        obj = json.loads(data.decode())
        short = json.dumps(obj, separators=(",", ":"))
        if len(short) > 160:
            short = short[:157] + "..."
        return short
    except Exception:
        return data[:80].decode(errors="replace")


async def main():
    global save_fp

    print(f"Connecting to NATS at {NATS_URL} ...")
    try:
        nc = await nats.connect(NATS_URL, connect_timeout=5)
    except Exception as e:
        print(f"ERROR: {e}")
        print("Make sure NATS is running:  docker compose up nats")
        sys.exit(1)
    print("Connected. Listening... (Ctrl-C to stop)\n")

    if SAVE:
        OUTPUT_FILE.parent.mkdir(exist_ok=True)
        save_fp = open(OUTPUT_FILE, "w")
        print(f"Saving capture to {OUTPUT_FILE}\n")

    async def handler(msg):
        subj = msg.subject
        counts[subj] = counts.get(subj, 0) + 1
        ts = time.strftime("%H:%M:%S")
        line = f"[{ts}] {subj:30s}  {pretty(subj, msg.data)}"
        print(line)
        if save_fp:
            try:
                record = {
                    "ts": time.time(),
                    "subject": subj,
                    "data": json.loads(msg.data.decode()),
                }
            except Exception:
                record = {"ts": time.time(), "subject": subj, "data": msg.data.decode(errors="replace")}
            save_fp.write(json.dumps(record) + "\n")
            save_fp.flush()

    # Subscribe to all ATS subjects
    subs = []
    subs.append(await nc.subscribe("sim.map",       cb=handler))
    subs.append(await nc.subscribe("sim.vehicles",  cb=handler))
    subs.append(await nc.subscribe("sim.obs.>",     cb=handler))
    subs.append(await nc.subscribe("sim.cmd.>",     cb=handler))
    subs.append(await nc.subscribe("sim.snapshots", cb=handler))

    print("Subscribed to:  sim.map  |  sim.vehicles  |  sim.obs.>  |  sim.cmd.>  |  sim.snapshots\n")

    try:
        while True:
            await asyncio.sleep(5)
            if counts:
                summary = "  ".join(f"{k}={v}" for k, v in sorted(counts.items()))
                print(f"[stats] {summary}")
    except (asyncio.CancelledError, KeyboardInterrupt):
        pass
    finally:
        for sub in subs:
            await sub.unsubscribe()
        await nc.close()
        if save_fp:
            save_fp.close()
            print(f"\nCapture saved to {OUTPUT_FILE}")
        print(f"\nFinal counts: {counts}")


if __name__ == "__main__":
    asyncio.run(main())
