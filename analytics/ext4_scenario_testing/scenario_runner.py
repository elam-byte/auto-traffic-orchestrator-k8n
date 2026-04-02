"""
Extension 4 — Sequential scenario runner + PySpark aggregation.

Each scenario:
  1. Writes docker-compose.override-{scenario_id}.yml with env var overrides
  2. Starts the ATS stack with the override (docker compose up --detach)
  3. Starts the recorder for `duration_sec` seconds
  4. Stops the stack (docker compose down)
  5. Returns path to collected Parquet data

After all scenarios, PySpark aggregates all results into a summary DataFrame.
"""
from __future__ import annotations

import json
import logging
import subprocess
import sys
import time
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from analytics.ext4_scenario_testing.scenarios import Scenario
from analytics.shared.config import SCENARIOS_DIR, RUNS_DIR

log = logging.getLogger("scenario_runner")

# Path to the repo root docker-compose.yml
_REPO_ROOT = Path(__file__).resolve().parents[2]
_COMPOSE_FILE = _REPO_ROOT / "docker-compose.yml"


def _write_override(scenario: Scenario, override_path: Path) -> None:
    """Write a docker-compose.override file with scenario parameters."""
    env = {
        "K_LATERAL":        str(scenario.k_lateral),
        "K_SOFT":           str(scenario.k_soft),
        "PREBRAKE_HORIZON": str(scenario.prebrake_horizon),
        **scenario.extra_env,
    }
    override = {
        "version": "3.9",
        "services": {
            "vehicle-agent": {
                "environment": env,
            }
        },
    }
    override_path.write_text(yaml.dump(override))


def _run_docker(compose_file: Path, override_file: Path, project: str, action: str) -> int:
    cmd = [
        "docker", "compose",
        "-f", str(compose_file),
        "-f", str(override_file),
        "--project-name", project,
        action,
    ]
    if action == "up":
        cmd += ["--detach", "--wait"]
    elif action == "down":
        cmd += ["--volumes", "--remove-orphans"]

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        log.warning("docker compose %s failed: %s", action, result.stderr[:500])
    return result.returncode


def run_scenario(scenario: Scenario, output_dir: Path) -> dict:
    """
    Run one scenario and return a metrics dict.
    Returns empty dict on failure.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    override_path = output_dir / "docker-compose.override.yml"
    project_name  = f"ats-{scenario.scenario_id}"
    run_id        = scenario.scenario_id

    log.info("▶ scenario=%s  %s", scenario.scenario_id, scenario.description)

    _write_override(scenario, override_path)

    # Start ATS stack with scenario overrides
    rc = _run_docker(_COMPOSE_FILE, override_path, project_name, "up")
    if rc != 0:
        log.error("Failed to start stack for scenario %s", scenario.scenario_id)
        return {"scenario_id": scenario.scenario_id, "error": "stack_start_failed"}

    # Wait for services to stabilise
    time.sleep(5)

    # Run the recorder as a subprocess for duration_sec
    recorder_cmd = [
        sys.executable, str(_REPO_ROOT / "analytics" / "ext2_historical" / "recorder.py"),
        "--run-id", run_id,
        "--nats", "nats://localhost:4222",
    ]

    try:
        proc = subprocess.Popen(recorder_cmd)
        time.sleep(scenario.duration_sec)
        proc.terminate()
        proc.wait(timeout=10)
    except Exception as e:
        log.warning("Recorder error for %s: %s", scenario.scenario_id, e)

    # Stop the stack
    _run_docker(_COMPOSE_FILE, override_path, project_name, "down")

    # Collect basic metrics from parquet if available
    run_dir = RUNS_DIR / run_id
    metrics = {
        "scenario_id":     scenario.scenario_id,
        "description":     scenario.description,
        "k_lateral":       scenario.k_lateral,
        "prebrake_horizon": scenario.prebrake_horizon,
        "vehicle_count":   scenario.vehicle_count,
        "duration_sec":    scenario.duration_sec,
        "run_dir":         str(run_dir),
    }
    metrics["params"] = json.dumps({
        "k_lateral": scenario.k_lateral,
        "k_soft": scenario.k_soft,
        "prebrake_horizon": scenario.prebrake_horizon,
    })

    log.info("✓ scenario=%s complete", scenario.scenario_id)
    return metrics


def aggregate_results(spark, results_dir: Path):
    """
    Use PySpark to aggregate all scenario observation Parquet files and compute
    cross-scenario comparison metrics (p95 lateral deviation, avg speed, etc.).
    """
    import math
    import json as _json
    from pyspark.sql import functions as F
    from pyspark.sql.types import DoubleType

    def _lateral_dev(x, y, corridor_json):
        if not corridor_json: return None
        try: pts = _json.loads(corridor_json)
        except: return None
        if not pts: return None
        best = float("inf")
        for p in pts:
            d = math.sqrt((x - p["x"])**2 + (y - p["y"])**2)
            if d < best: best = d
        return best

    lateral_udf = F.udf(_lateral_dev, DoubleType())

    # Read all scenario observation parquets
    obs_pattern = str(RUNS_DIR / "*/observations")
    try:
        df = spark.read.parquet(obs_pattern)
    except Exception as e:
        log.warning("Could not read scenario observations: %s", e)
        return None

    summary = (
        df
        .filter(F.col("speed") > 0.1)
        .filter(F.col("corridor_len") > 0)
        .withColumn("lat_dev", lateral_udf(F.col("x"), F.col("y"), F.col("corridor_json")))
        .filter(F.col("lat_dev").isNotNull())
        .groupBy("vehicle_id")    # vehicle_id holds run_id in scenario context
        .agg(
            F.count("*").alias("tick_count"),
            F.mean("speed").alias("avg_speed_ms"),
            F.percentile_approx("lat_dev", 0.50).alias("p50_lateral_m"),
            F.percentile_approx("lat_dev", 0.95).alias("p95_lateral_m"),
            F.percentile_approx("lat_dev", 0.99).alias("p99_lateral_m"),
            F.mean("lat_dev").alias("mean_lateral_m"),
        )
        .orderBy("p95_lateral_m")
    )

    out_path = str(results_dir / "scenario_summary.parquet")
    summary.write.mode("overwrite").parquet(out_path)
    log.info("Scenario summary written to %s", out_path)
    return summary
