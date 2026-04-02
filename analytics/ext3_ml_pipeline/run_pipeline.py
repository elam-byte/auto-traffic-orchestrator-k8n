"""
Extension 3 — ML training data pipeline CLI.

Runs stages 2 and 3 (feature engineering + dataset prep) using PySpark.

Usage:
    python analytics/ext3_ml_pipeline/run_pipeline.py \\
        --runs data/runs/ \\
        --output data/ml_dataset/

    # Or just feature engineering:
    python analytics/ext3_ml_pipeline/run_pipeline.py --stage features

    # Or just dataset prep (on existing features):
    python analytics/ext3_ml_pipeline/run_pipeline.py --stage dataset

Dashboard: python analytics/dashboard/server.py  →  http://localhost:8091 (ML PIPELINE tab)
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from analytics.shared.config import RUNS_DIR, ML_DATASET_DIR

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("ml_pipeline")


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("ats-ext3-ml-pipeline")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "3g")
        .getOrCreate()
    )


def main() -> None:
    ap = argparse.ArgumentParser(description="ATS ML training data pipeline")
    ap.add_argument("--runs",   default=str(RUNS_DIR),
                    help="Path to recorded runs directory (default: data/runs/)")
    ap.add_argument("--output", default=str(ML_DATASET_DIR),
                    help="Output path for ML dataset (default: data/ml_dataset/)")
    ap.add_argument("--stage",  choices=["features", "dataset", "all"], default="all",
                    help="Which pipeline stage to run (default: all)")
    args = ap.parse_args()

    runs_dir    = Path(args.runs)
    dataset_dir = Path(args.output)
    features_dir = dataset_dir / "features"

    if not runs_dir.exists() or not any(runs_dir.iterdir()):
        log.error("No run data found at %s", runs_dir)
        log.error("Record a simulation first: python analytics/ext2_historical/recorder.py --run-id run-001")
        sys.exit(1)

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    if args.stage in ("features", "all"):
        log.info("Stage 2: Feature engineering")
        from analytics.ext3_ml_pipeline.feature_engineering import run as run_features
        df = run_features(spark, runs_dir, features_dir)
        count = df.count()
        log.info("Features written: %d rows → %s", count, features_dir)

    if args.stage in ("dataset", "all"):
        log.info("Stage 3: Dataset preparation")
        from analytics.ext3_ml_pipeline.dataset_prep import run as run_prep
        quality = run_prep(spark, features_dir, dataset_dir)
        log.info("Dataset ready:")
        log.info("  total rows : %d", quality["total_rows"])
        log.info("  train/val/test: %d / %d / %d",
                 quality["train_rows"], quality["val_rows"], quality["test_rows"])
        log.info("  positive label ratio: %.1f%%", quality["positive_label_ratio"] * 100)
        log.info("  quality.json → %s", dataset_dir / "quality.json")

    spark.stop()
    log.info("Pipeline complete. Dataset at: %s", dataset_dir)


if __name__ == "__main__":
    main()
