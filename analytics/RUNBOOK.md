# ATS Analytics Pipeline — Runbook

Step-by-step guide to start and test each extension independently.

---

## Prerequisites

### Java (required for PySpark)
```bash
export JAVA_HOME=/home/elam/.antigravity/extensions/redhat.java-1.53.0-linux-x64/jre/21.0.10-linux-x86_64
export PATH=$JAVA_HOME/bin:$PATH

# Verify
java -version
# Expected: openjdk version "21.0.10" ...
```

> Add these two lines to `~/.bashrc` or `~/.zshrc` to make permanent.

### Python dependencies
```bash
pip install --break-system-packages -r analytics/requirements.txt

# Verify
python3 -c "import pyspark, nats, fastapi, kafka; print('OK')"
```

---

## Step 1 — Main ATS stack

Start NATS, ats-env, viz-gateway, and vehicle-orchestrator.

```bash
# From repo root
docker compose up -d

# Verify all 4 services are Up
docker compose ps
```

Expected output:
```
NAME                        STATUS
ats-nats-1                  Up
ats-ats-env-1               Up
ats-viz-gateway-1           Up
ats-vehicle-orchestrator-1  Up
```

### Upload the map (spawns vehicles)
```bash
python3 scripts/upload-map.py
# Expected: "Map saved ... 4 vehicles"
```

Wait ~5 seconds, then verify vehicles are driving:
```bash
docker ps --format "{{.Names}}" | grep "^vehicle-"
# Should list: vehicle-v-1, vehicle-v-2, vehicle-v-3, vehicle-v-4
```

### Test: NATS messages flowing
```bash
python3 scripts/spy-nats.py
# Should print sim.snapshots and sim.obs.* at 20 Hz
# Ctrl-C to stop
```

---

## Step 2 — Analytics infrastructure (Kafka + Spark)

```bash
docker compose -f analytics/docker-compose.analytics.yml up -d

# Verify
docker compose -f analytics/docker-compose.analytics.yml ps
```

Expected output:
```
NAME               STATUS
ats-kafka          Up (healthy)
ats-spark-master   Up
ats-spark-worker   Up
```

Kafka web UI is not exposed. Spark master UI: http://localhost:8085

### Test: Kafka is reachable
```bash
docker exec ats-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
# Should return empty (no topics yet) or existing topics
```

---

## Step 3 — Extension 1: Real-Time Analytics

Runs in the background. Needs Step 1 + Step 2 running.

### 3a. Start the NATS → Kafka bridge
```bash
# Terminal A
python3 analytics/ext1_realtime/bridge.py

# Expected output:
# Bridge active — forwarding sim.snapshots and sim.obs.>
```

### Test: Topics created in Kafka
```bash
docker exec ats-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list

# Expected:
# sim-observations
# sim-snapshots
```

### 3b. Start the PySpark streaming job
```bash
# Terminal B (JAVA_HOME must be set)
python3 analytics/ext1_realtime/streaming_job.py --local

# Expected: Spark starts, no errors. Output files appear in data/streaming/
```

### Test: Streaming output files appearing
```bash
# In a new terminal — wait ~30 seconds after starting the job
watch -n 5 'find data/streaming -name "*.json" | wc -l'

# Count should increase every 5 seconds
```

Inspect a sample output:
```bash
cat $(ls data/streaming/congestion/*.json | head -1)
# Expected: JSON lines with lane_id, congestion_score, window_start, window_end
```

---

## Step 4 — Extension 2: Historical Analytics

### 4a. Record a simulation run

```bash
# Run for as long as you want (Ctrl-C to stop)
python3 analytics/ext2_historical/recorder.py --run-id run-001

# Expected output every ~30s:
# Flushed 1000 rows → observations/minute=.../obs-000001.parquet
# Flushed 1000 rows → snapshots/minute=.../snap-000001.parquet
```

Minimum recommended: **60 seconds** (~4800 observation rows across 4 vehicles).

### Test: Parquet files written
```bash
find data/runs/run-001 -name "*.parquet" | head -10
python3 -c "
import pandas as pd
df = pd.read_parquet('data/runs/run-001/observations/')
print(f'Rows: {len(df)}, Columns: {list(df.columns)}')
"
```

### 4b. Run batch analytics jobs

```bash
# All jobs (lateral deviation, speed profile, junction slowdown, dead-end incidents)
python3 analytics/ext2_historical/run_jobs.py \
  --run-id run-001 \
  --jobs lateral_deviation speed_profile junction_slowdown dead_end_incidents

# Each job prints a result table. ~2 minutes total.
```

Run a single job:
```bash
python3 analytics/ext2_historical/run_jobs.py \
  --run-id run-001 \
  --jobs lateral_deviation
```

### Test: Results written
```bash
ls data/results/run-001/
# Expected: lateral_deviation.parquet, speed_profile.parquet, junction_slowdown.parquet, ...

python3 -c "
import pandas as pd
df = pd.read_parquet('data/results/run-001/lateral_deviation.parquet')
print(df[['road_id','p50_m','p95_m','p99_m']].to_string())
"
```

### Regression comparison (two runs)
```bash
# Record a second run first
python3 analytics/ext2_historical/recorder.py --run-id run-002

# Then compare
python3 analytics/ext2_historical/run_jobs.py \
  --run-id run-002 \
  --jobs regression_detector \
  --compare-run run-001
```

---

## Step 5 — Extension 3: ML Training Pipeline

Needs at least one recorded run from Step 4a.

```bash
python3 analytics/ext3_ml_pipeline/run_pipeline.py \
  --runs data/runs/ \
  --output data/ml_dataset/

# Expected output:
# Stage 2: Feature engineering → N rows
# Stage 3: Dataset preparation
#   total rows : 2374
#   train/val/test: 1726 / 332 / 316
#   positive label ratio: 67.1%
```

### Run only feature engineering
```bash
python3 analytics/ext3_ml_pipeline/run_pipeline.py \
  --runs data/runs/ --output data/ml_dataset/ --stage features
```

### Run only dataset prep (on existing features)
```bash
python3 analytics/ext3_ml_pipeline/run_pipeline.py \
  --runs data/runs/ --output data/ml_dataset/ --stage dataset
```

### Test: Dataset files written
```bash
ls data/ml_dataset/
# Expected: features/, train/, val/, test/, quality.json

cat data/ml_dataset/quality.json

python3 -c "
import pandas as pd
train = pd.read_parquet('data/ml_dataset/train/')
print(f'Train rows: {len(train)}')
print(train[['lateral_dev_t','heading_error','speed_ratio','label_good_cmd']].describe())
"
```

---

## Step 6 — Extension 4: Scenario Testing

Needs Step 1 (ATS stack) running. Records a separate simulation run per scenario.

### Dry run first (no actual simulation)
```bash
python3 analytics/ext4_scenario_testing/run_scenarios.py \
  --type hyperparameter_sweep --dry-run

# Prints 35 scenarios (7 K_LATERAL × 5 PREBRAKE_HORIZON values)
```

### Run a small scenario set
```bash
python3 analytics/ext4_scenario_testing/run_scenarios.py \
  --type density_stress \
  --output data/scenarios/density-001/

# Runs 4 scenarios: 5, 10, 15, 20 vehicles
# Takes ~10 minutes total (4 × ~2.5 min including startup time)
```

Other scenario types:
```bash
# A/B controller comparison
python3 analytics/ext4_scenario_testing/run_scenarios.py \
  --type ab_comparison \
  --output data/scenarios/ab-001/

# Map robustness testing
python3 analytics/ext4_scenario_testing/run_scenarios.py \
  --type map_robustness \
  --output data/scenarios/robust-001/
```

### Test: Results written
```bash
cat data/scenarios/density-001/results.json | python3 -m json.tool | head -30

python3 -c "
import pandas as pd
df = pd.read_parquet('data/scenarios/density-001/scenario_summary.parquet')
print(df.to_string())
"
```

---

## Step 7 — Unified Dashboard

Start the dashboard server to see all metrics in one place.

```bash
python3 analytics/dashboard/server.py
# Expected: Dashboard: http://localhost:8091
```

Open http://localhost:8091 in a browser.

| Tab | Shows | Needs |
|-----|-------|-------|
| REAL-TIME | Live congestion, throughput, density | Step 3 (bridge + streaming job) |
| HISTORICAL | Lateral deviation, speed profile, junction slowdown | Step 4 |
| ML PIPELINE | Dataset quality, feature distributions | Step 5 |
| SCENARIOS | Scenario comparison charts | Step 6 |

### Test: API endpoints
```bash
# Real-time congestion (live data from streaming job)
curl -s http://localhost:8091/api/ext1/congestion | python3 -m json.tool | head -20

# Available runs
curl -s http://localhost:8091/api/ext2/runs

# Lateral deviation results
curl -s "http://localhost:8091/api/ext2/lateral_deviation?run_id=run-001" | python3 -m json.tool

# ML dataset quality
curl -s http://localhost:8091/api/ext3/quality | python3 -m json.tool

# Scenario types
curl -s http://localhost:8091/api/ext4/scenario_types | python3 -m json.tool
```

---

## Full stack startup (all at once)

Run these in separate terminals, in order:

```bash
# Terminal 1 — ATS stack
docker compose up

# Terminal 2 — Analytics infrastructure
docker compose -f analytics/docker-compose.analytics.yml up

# Terminal 3 — Upload map (run once)
python3 scripts/upload-map.py

# Terminal 4 — NATS→Kafka bridge
python3 analytics/ext1_realtime/bridge.py

# Terminal 5 — PySpark streaming (needs JAVA_HOME)
export JAVA_HOME=/home/elam/.antigravity/extensions/redhat.java-1.53.0-linux-x64/jre/21.0.10-linux-x86_64
export PATH=$JAVA_HOME/bin:$PATH
python3 analytics/ext1_realtime/streaming_job.py --local

# Terminal 6 — Dashboard
python3 analytics/dashboard/server.py
# Open: http://localhost:8091
```

---

## Teardown

```bash
# Stop background processes
pkill -f "ext1_realtime/bridge.py"
pkill -f "ext1_realtime/streaming_job.py"
pkill -f "dashboard/server.py"

# Stop Docker stacks
docker compose -f analytics/docker-compose.analytics.yml down
docker compose down
```

---

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| `JAVA_GATEWAY_EXITED` | JAVA_HOME not set. Run the export commands above. |
| `No brokers available` | Kafka not running. `docker compose -f analytics/docker-compose.analytics.yml up -d` |
| `Topic not found in cluster metadata` | Normal at startup — Kafka auto-creates topics on first message |
| `Run directory not found` | Run the recorder (Step 4a) before running batch jobs |
| Dashboard shows `—` everywhere | Run Steps 4 and 5 first to generate results data |
| REAL-TIME tab shows OFFLINE | Start the bridge (3a) and streaming job (3b) |
