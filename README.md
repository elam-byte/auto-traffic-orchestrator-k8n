# Autonomous Traffic Simulator (ATS)

> A case study in distributed, real-time multi-agent orchestration — the same class of systems that powers **autonomous vehicle fleets**, **live financial engines**, **cloud resource allocation systems**, **news feed ranking systems**, **dynamic pricing and recommendation engines**, **robotic warehouses**, and **drone swarms**. These systems operate under partial observability and strict latency constraints, optimizing global objectives through decentralized, real-time decision-making.

---

## What This Is

A full-stack, containerised runtime that places multiple autonomous vehicle agents on a road network and simulates their movement at 20 Hz. Each vehicle is an independent Docker container running its own control algorithm. A central environment process owns world state, distributes personalised observations, integrates physics, and broadcasts live snapshots to a browser visualiser — all over a NATS message bus.

A PySpark analytics pipeline runs alongside the simulation, capturing live telemetry, computing historical aggregates, building ML-ready datasets, and running scenario tests — turning raw simulation output into structured insight.

---

## Why This Architecture Matters

| Problem | Solution |
|---------|----------|
| Agents must act independently | One container per vehicle; NATS pub/sub decouples everything |
| Environment must be consistent regardless of agent latency | Central 20 Hz tick; stale commands trigger safe fallback |
| Observations must be personalised per agent | Per-vehicle 100 m lane corridor published individually |
| Visualisation must not block simulation | Viz-Gateway is separate with latest-only, drop-on-slow semantics |
| Road network can be arbitrarily complex | Lane graph built from declarative map JSON; topology is data |
| Raw telemetry must be analysable at scale | PySpark pipeline ingests NATS snapshots into Parquet datasets |
| Real-time metrics must be visible without stopping the sim | Streaming Spark job feeds a live dashboard over WebSocket |

This pattern — **environment + message bus + stateless agents + analytics pipeline** — scales from a laptop to a Kubernetes cluster with minimal change.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Docker Compose / K8s                       │
│                                                                   │
│  ┌──────────────┐     NATS JetStream      ┌────────────────────┐  │
│  │  ATS-Env     │──► sim.obs.{id} ───────►│  Vehicle Agent(s)  │  │
│  │  (TypeScript)│◄── sim.cmd.{id} ────────│  (Python, Docker)  │  │
│  │              │                         └────────────────────┘  │
│  │  World State │──► sim.snapshots ──────►┌────────────────────┐  │
│  │  Physics     │          │              │  Viz-Gateway       │  │
│  │  Lane Graph  │          │              │  WebSocket :8090   │  │
│  └──────────────┘          │              └────────┬───────────┘  │
│         ▲                  │                       │              │
│         │ map JSON         ▼                       ▼              │
│  ┌──────┴──────┐   ┌───────────────┐      ┌───────────────────┐   │
│  │  Map File   │   │  Analytics    │      │   viewer.html     │   │
│  │  (JSON)     │   │  (PySpark)    │      │   (Browser)       │   │
│  └─────────────┘   └───────────────┘      └───────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

### Services

**ATS Environment (`ats-env`)** — TypeScript
Authoritative world process. Runs a fixed 20 Hz tick: sends `VehicleObservation` per vehicle, waits up to 50 ms for `VehicleCommand` responses, integrates physics, publishes `WorldSnapshot`. Agents missing the deadline receive a safe-stop automatically.

**Vehicle Agent (`vehicle-agent`)** — Python, Docker
Stateless service. Receives one observation per tick, returns one command. Implements a Stanley lane-centering controller with curve pre-braking and lateral deviation correction.

**Viz-Gateway (`viz-gateway`)** — Node.js
NATS → WebSocket bridge. Holds the latest snapshot and pushes to all clients at 20 Hz. Slow clients are dropped — the simulation never waits for the visualiser.

**NATS**
Message bus. All inter-service communication flows through NATS subjects. New consumers (loggers, analytics, recorders) attach without touching existing components.

**Analytics Pipeline (`analytics/`)** — PySpark
Four extensions that consume simulation output and produce structured datasets and dashboards:

| Extension | What it does |
|-----------|-------------|
| `ext1_realtime` | Streams NATS snapshots through Spark Structured Streaming into live metrics |
| `ext2_historical` | Records snapshots to Parquet, runs batch aggregations (speed, spacing, incidents) |
| `ext3_ml_pipeline` | Engineers features from telemetry, splits train/val/test datasets |
| `ext4_scenario_testing` | Runs predefined scenarios, captures outcomes, validates controller behaviour |

A live dashboard (`analytics/dashboard/`) serves metrics over HTTP and WebSocket on port 8050.

---

## Design Principles

**Hard real-time tick** — World advances every 50 ms regardless of agent behaviour.
**Stateless agents** — All context is in the observation; agents are trivially replaceable.
**Data-driven topology** — Lane graph, speed limits, and junctions are map config, not code.
**Separation of concerns** — Environment, agents, viz, and analytics are fully independent processes.
**Backpressure by design** — Viz-Gateway and analytics never block the simulation.
**Cyclic lane convention** — Roads authored anticlockwise; right lane forward, left lane reverse.

---

## Coordinate System

- **Origin** — bottom-left; **+x** right, **+y** up
- **Heading** — radians, 0 = east, counter-clockwise positive

---

## Performance Targets

| Metric | Target |
|--------|--------|
| Environment tick (p95) | < 40 ms |
| Lane corridor construction per vehicle | < 5 ms |
| Viz-Gateway WebSocket broadcast (p95) | < 2 ms |
| Vehicle agent response (p95) | < 30 ms |

---

## Prerequisites

- **Docker** and **Docker Compose v2**
- **Node.js 20+** and **pnpm**
- **Python 3.12+** with PySpark for analytics
- A map file in `map/` — three are included

---

## Running the Full Stack

```bash
docker compose up
```

Open `viz-gateway/viewer.html` and connect to `ws://localhost:8090`.

To load a map, POST any map JSON to `http://localhost:8090/map` with the `X-Map-Filename` header. No restart required.

To run the analytics pipeline:

```bash
cd analytics
docker compose -f docker-compose.analytics.yml up
```

Dashboard available at `http://localhost:8050`.

---

## Repository Layout

```
AutonomousTrafficSimulator/
├── ats-env/                   TypeScript environment process
│   └── src/
│       ├── roadGraph.ts       Lane graph construction and connectivity
│       ├── corridorBuilder.ts Per-vehicle 100 m corridor builder
│       ├── physics.ts         Kinematic bicycle model
│       ├── tickLoop.ts        20 Hz tick and NATS orchestration
│       ├── mapLoader.ts       Map JSON parsing (Zod)
│       └── vehicleState.ts    In-memory vehicle store
├── vehicle-agent/             Python Stanley controller
├── viz-gateway/               Node.js NATS→WebSocket bridge + viewer
├── analytics/                 PySpark data pipeline
│   ├── ext1_realtime/         Structured Streaming live metrics
│   ├── ext2_historical/       Batch Parquet recorder and aggregations
│   ├── ext3_ml_pipeline/      Feature engineering and dataset prep
│   ├── ext4_scenario_testing/ Scenario runner and outcome capture
│   ├── dashboard/             Live metrics dashboard (port 8050)
│   └── shared/                Shared config and types
├── shared/                    Shared TypeScript map type definitions
├── map/                       ATS Map JSON v1 files
├── scripts/                   Integration test and diagnostic utilities
└── docker-compose.yml
```
