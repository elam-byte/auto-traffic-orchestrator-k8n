import asyncio
import os

import nats

from .controller import compute_command
from .types import VehicleObservation

NATS_URL = os.environ.get("NATS_URL", "nats://localhost:4222")
# If set, subscribe only to this vehicle's observations (per-vehicle container mode).
# If unset, subscribe to all vehicles via wildcard (fleet-shared mode).
VEHICLE_ID = os.environ.get("VEHICLE_ID")


async def run() -> None:
    print(f"[vehicle-agent] Connecting to NATS: {NATS_URL}")
    if VEHICLE_ID:
        print(f"[vehicle-agent] Per-vehicle mode: VEHICLE_ID={VEHICLE_ID}")
    else:
        print("[vehicle-agent] Fleet-shared mode: subscribing to all vehicles (sim.obs.>)")

    nc = await nats.connect(
        NATS_URL,
        max_reconnect_attempts=-1,
        reconnect_time_wait=1,
    )
    print("[vehicle-agent] NATS connected")

    async def message_handler(msg) -> None:
        # Subject: sim.obs.{vehicle_id}
        vehicle_id = msg.subject.split(".", 2)[-1]
        try:
            obs = VehicleObservation.model_validate_json(msg.data)
        except Exception as e:
            print(f"[vehicle-agent] Parse error for {vehicle_id}: {e}")
            return

        cmd = compute_command(obs)
        payload = cmd.model_dump_json(exclude_none=True).encode()
        await nc.publish(f"sim.cmd.{obs.id}", payload)

    subject = f"sim.obs.{VEHICLE_ID}" if VEHICLE_ID else "sim.obs.>"
    await nc.subscribe(subject, cb=message_handler)
    print(f"[vehicle-agent] Subscribed to {subject}")

    # Keep running until interrupted
    try:
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        pass
    finally:
        print("[vehicle-agent] Draining...")
        await nc.drain()


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
