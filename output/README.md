# output/ — JSON reference files

These are hand-written reference examples showing the exact shape of every message type in the system. Open them to understand what each NATS subject carries.

---

## NATS subjects and their messages

| Subject | Direction | Sample file |
|---------|-----------|-------------|
| `sim.map` | viz-gateway → ats-env | `sim-map-message.json` |
| `sim.vehicles` | ats-env → vehicle-orchestrator | `sim-vehicles-message.json` |
| `sim.obs.{id}` | ats-env → vehicle-agent | `vehicle-observation.json` |
| `sim.cmd.{id}` | vehicle-agent → ats-env | `vehicle-command.json` |
| `sim.snapshots` | ats-env → viz-gateway → WebSocket | `world-snapshot.json` |

---

## File descriptions

| File | What it shows |
|------|--------------|
| `sim-map-message.json` | The path-only map reference published to `sim.map` after a viewer upload |
| `sim-vehicles-message.json` | The vehicle list published to `sim.vehicles` so the orchestrator can spawn containers |
| `vehicle-observation.json` | Full `VehicleObservation` with `lane_id`/`road_id` on corridor points and a `junction` example |
| `vehicle-command.json` | Full `VehicleCommand` with a `junction_choice` example |
| `world-snapshot.json` | `WorldSnapshot` as received by the viewer over WebSocket |

---

## Files written by scripts

| File | Written by |
|------|-----------|
| `vehicle-agent-test-results.json` | `scripts/test-vehicle-agent.py` |
| `nats-capture.jsonl` | `scripts/spy-nats.py --save` |
| `ats-env-observations-captured.json` | `scripts/test-ats-env.py` |
| `ats-env-snapshots-captured.json` | `scripts/test-ats-env.py` |
| `ats-env-commands-sent.json` | `scripts/test-ats-env.py` |
| `viz-gateway-capture.json` | `scripts/test-viz-gateway.py` |

---

See `EXPLORE.md` in the repo root for the full system walkthrough.
