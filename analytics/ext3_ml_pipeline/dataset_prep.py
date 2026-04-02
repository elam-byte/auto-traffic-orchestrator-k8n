"""
Extension 3 — Dataset preparation (Stage 3).

Reads feature Parquet from feature_engineering output, then:
  1. Filters out startup ticks (speed_ratio < 0.05)
  2. Balances arc vs line road types (undersample majority)
  3. Shuffles globally
  4. Splits 70/15/15 → train / val / test
  5. Writes to data/ml_dataset/train/, val/, test/

Writes quality metrics JSON to data/ml_dataset/quality.json.
"""
from __future__ import annotations

import json
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


def run(spark: SparkSession, features_dir: Path, dataset_dir: Path) -> dict:
    df = spark.read.parquet(str(features_dir))

    # ── Filter startup ticks ───────────────────────────────────────────────────
    df = df.filter(F.col("speed_ratio") >= 0.05)

    # ── Balance by road type ───────────────────────────────────────────────────
    # We don't have explicit arc/line field — use road_id as group key.
    # Balance so no single road_id contributes more than 3× the minimum.
    counts = df.groupBy("road_id").count().collect()
    if counts:
        min_count = min(r["count"] for r in counts)
        cap = min_count * 3
        # Fraction needed per group
        fractions = {r["road_id"]: min(1.0, cap / r["count"]) for r in counts}
        df = df.sampleBy("road_id", fractions, seed=42)

    # ── Global shuffle ─────────────────────────────────────────────────────────
    df = df.orderBy(F.rand(seed=42))

    # ── Train / val / test split ───────────────────────────────────────────────
    total = df.count()
    train_df, val_df, test_df = df.randomSplit([0.70, 0.15, 0.15], seed=42)

    dataset_dir.mkdir(parents=True, exist_ok=True)
    train_df.write.mode("overwrite").parquet(str(dataset_dir / "train"))
    val_df.write.mode("overwrite").parquet(str(dataset_dir / "val"))
    test_df.write.mode("overwrite").parquet(str(dataset_dir / "test"))

    # ── Quality metrics ────────────────────────────────────────────────────────
    pos_ratio = df.filter(F.col("label_good_cmd") == 1).count() / max(total, 1)
    quality = {
        "total_rows":      total,
        "train_rows":      train_df.count(),
        "val_rows":        val_df.count(),
        "test_rows":       test_df.count(),
        "positive_label_ratio": round(pos_ratio, 4),
        "features": [
            "lateral_dev_t", "lateral_dev_t1", "heading_error",
            "speed_ratio", "upcoming_curv", "at_junction",
        ],
        "label": "label_good_cmd",
    }

    (dataset_dir / "quality.json").write_text(json.dumps(quality, indent=2))
    return quality
