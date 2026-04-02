"""
Extension 2 — Batch analytics job runner.

Usage:
    # Run all available jobs on run-001
    python analytics/ext2_historical/run_jobs.py --run-id run-001

    # Run specific jobs
    python analytics/ext2_historical/run_jobs.py \\
        --run-id run-001 \\
        --jobs lateral_deviation speed_profile junction_slowdown

    # Regression comparison
    python analytics/ext2_historical/run_jobs.py \\
        --run-id run-002 \\
        --jobs regression_detector \\
        --compare-run run-001

Results are written to data/results/{run_id}/.
Dashboard: python analytics/dashboard/server.py  →  http://localhost:8091 (HISTORICAL tab)
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from analytics.shared.config import RUNS_DIR, RESULTS_DIR

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("run_jobs")

ALL_JOBS = [
    "lateral_deviation",
    "speed_profile",
    "junction_slowdown",
    "dead_end_incidents",
    "regression_detector",
]


def build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def main() -> None:
    ap = argparse.ArgumentParser(description="ATS historical analytics runner")
    ap.add_argument("--run-id",     required=True, help="Run ID to analyse, e.g. run-001")
    ap.add_argument("--jobs",       nargs="+", default=ALL_JOBS,
                    choices=ALL_JOBS, help="Which jobs to run (default: all)")
    ap.add_argument("--compare-run", default=None,
                    help="Second run ID for regression_detector")
    args = ap.parse_args()

    run_dir = RUNS_DIR / args.run_id
    if not run_dir.exists():
        log.error("Run directory not found: %s", run_dir)
        sys.exit(1)

    out_dir = RESULTS_DIR / args.run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    spark = build_spark(f"ats-ext2-{args.run_id}")
    spark.sparkContext.setLogLevel("WARN")

    for job_name in args.jobs:
        log.info("▶ Running job: %s", job_name)
        try:
            if job_name == "lateral_deviation":
                from analytics.ext2_historical.jobs.lateral_deviation import run
                df = run(spark, run_dir, out_dir)
                df.show(20, truncate=False)

            elif job_name == "speed_profile":
                from analytics.ext2_historical.jobs.speed_profile import run
                df = run(spark, run_dir, out_dir)
                df.show(20, truncate=False)

            elif job_name == "junction_slowdown":
                from analytics.ext2_historical.jobs.junction_slowdown import run
                df = run(spark, run_dir, out_dir)
                df.show(20, truncate=False)

            elif job_name == "dead_end_incidents":
                from analytics.ext2_historical.jobs.dead_end_incidents import run
                df = run(spark, run_dir, out_dir)
                df.show(truncate=False)

            elif job_name == "regression_detector":
                from analytics.ext2_historical.jobs.regression_detector import run
                compare_dir = RUNS_DIR / args.compare_run if args.compare_run else None
                df = run(spark, run_dir, out_dir, compare_run_dir=compare_dir)
                df.show(truncate=False)

            log.info("✓ %s  →  %s", job_name, out_dir)

        except Exception as e:
            log.error("✗ %s failed: %s", job_name, e, exc_info=True)

    spark.stop()
    log.info("All jobs complete. Results in: %s", out_dir)


if __name__ == "__main__":
    main()
