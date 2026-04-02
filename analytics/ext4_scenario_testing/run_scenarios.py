"""
Extension 4 — Distributed scenario testing CLI.

Usage:
    # Hyperparameter sweep
    python analytics/ext4_scenario_testing/run_scenarios.py \\
        --type hyperparameter_sweep \\
        --output data/scenarios/sweep-001/

    # Vehicle density stress test
    python analytics/ext4_scenario_testing/run_scenarios.py \\
        --type density_stress \\
        --output data/scenarios/density-001/

    # A/B controller comparison (10 runs each)
    python analytics/ext4_scenario_testing/run_scenarios.py \\
        --type ab_comparison \\
        --output data/scenarios/ab-001/

    # Dry run (prints scenarios without running)
    python analytics/ext4_scenario_testing/run_scenarios.py \\
        --type hyperparameter_sweep --dry-run

Dashboard: python analytics/dashboard/server.py  →  http://localhost:8091 (SCENARIOS tab)
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from analytics.ext4_scenario_testing.scenarios import SCENARIO_TYPES
from analytics.ext4_scenario_testing.scenario_runner import run_scenario, aggregate_results
from analytics.shared.config import SCENARIOS_DIR

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("run_scenarios")


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("ats-ext4-scenario-testing")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def main() -> None:
    ap = argparse.ArgumentParser(description="ATS distributed scenario testing")
    ap.add_argument("--type",    required=True, choices=list(SCENARIO_TYPES.keys()),
                    help="Scenario type to run")
    ap.add_argument("--output",  default=None,
                    help="Output directory (default: data/scenarios/{type}/)")
    ap.add_argument("--dry-run", action="store_true",
                    help="Print scenarios without executing")
    args = ap.parse_args()

    scenarios = SCENARIO_TYPES[args.type]()
    output_dir = Path(args.output) if args.output else SCENARIOS_DIR / args.type
    output_dir.mkdir(parents=True, exist_ok=True)

    log.info("Scenario type: %s  (%d scenarios)", args.type, len(scenarios))

    if args.dry_run:
        for s in scenarios:
            log.info("  [dry-run] %s — %s", s.scenario_id, s.description)
        sys.exit(0)

    results = []
    for i, scenario in enumerate(scenarios, 1):
        log.info("[%d/%d] %s", i, len(scenarios), scenario.scenario_id)
        scenario_dir = output_dir / scenario.scenario_id
        metrics = run_scenario(scenario, scenario_dir)
        results.append(metrics)

        # Save incremental results JSON
        (output_dir / "results.json").write_text(json.dumps(results, indent=2))

    log.info("All %d scenarios complete. Aggregating with PySpark...", len(scenarios))

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    summary_df = aggregate_results(spark, output_dir)
    if summary_df is not None:
        log.info("Top 10 scenarios by p95 lateral deviation:")
        summary_df.show(10, truncate=False)

    spark.stop()
    log.info("Results: %s", output_dir / "results.json")
    log.info("Summary: %s", output_dir / "scenario_summary.parquet")


if __name__ == "__main__":
    main()
