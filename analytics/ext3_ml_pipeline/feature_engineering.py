"""
Extension 3 — Feature engineering (Stage 2).

Reads observation Parquet from data/runs/, joins each observation with the
next-tick observation to build (state, action, next_state) triples, then
computes the ML feature set:

  Features:
    - lateral_dev_t         lateral deviation at time t (m)
    - lateral_dev_t1        lateral deviation at t+1 (outcome)
    - heading_error         angle between vehicle heading and corridor heading
    - speed_ratio           speed / speed_limit (clipped 0–1.5)
    - upcoming_curvature    min speed_limit in next 20 corridor points
    - at_junction           bool
    - label_good_cmd        1 if lateral_dev_t1 < lateral_dev_t else 0

Writes intermediate features to data/ml_dataset/features/.
"""
from __future__ import annotations

import json
import math
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, FloatType


def _lateral_dev(x: float, y: float, corridor_json: str) -> float | None:
    if not corridor_json:
        return None
    try:
        pts = json.loads(corridor_json)
    except Exception:
        return None
    if not pts:
        return None
    best = float("inf")
    for p in pts:
        d = math.sqrt((x - p["x"]) ** 2 + (y - p["y"]) ** 2)
        if d < best:
            best = d
    return best


def _heading_error(heading: float, corridor_json: str) -> float | None:
    """Angle (rad) between vehicle heading and first corridor point tangent."""
    if not corridor_json:
        return None
    try:
        pts = json.loads(corridor_json)
    except Exception:
        return None
    if not pts:
        return None
    target = pts[0].get("heading", 0.0)
    err = heading - target
    # Normalize to [-pi, pi]
    while err >  math.pi: err -= 2 * math.pi
    while err < -math.pi: err += 2 * math.pi
    return abs(err)


def _upcoming_curvature(corridor_json: str, horizon: int = 20) -> float | None:
    """Minimum speed_limit in the next `horizon` corridor points (proxy for curvature)."""
    if not corridor_json:
        return None
    try:
        pts = json.loads(corridor_json)
    except Exception:
        return None
    if not pts:
        return None
    limits = [p.get("speed_limit", 999.0) for p in pts[:horizon]]
    return min(limits) if limits else None


def run(spark: SparkSession, runs_dir: Path, features_dir: Path) -> DataFrame:
    lateral_udf   = F.udf(_lateral_dev,         DoubleType())
    heading_udf   = F.udf(_heading_error,        DoubleType())
    curvature_udf = F.udf(_upcoming_curvature,   DoubleType())

    # Read all run observations (may span multiple runs)
    obs_path = str(runs_dir / "*/observations")
    df = spark.read.parquet(obs_path)

    # Window per vehicle ordered by time for lead/lag
    w = Window.partitionBy("vehicle_id").orderBy("t")

    df = (
        df
        .withColumn("lateral_dev_t",    lateral_udf(F.col("x"), F.col("y"), F.col("corridor_json")))
        .withColumn("lateral_dev_t1",   F.lead("lateral_dev_t").over(w))
        .withColumn("heading_error",    heading_udf(F.col("heading"), F.col("corridor_json")))
        .withColumn("speed_ratio",      F.when(F.col("speed_limit") > 0,
                                            F.col("speed") / F.col("speed_limit"))
                                         .otherwise(0.0))
        .withColumn("speed_ratio",      F.least(F.col("speed_ratio"), F.lit(1.5)))
        .withColumn("upcoming_curv",    curvature_udf(F.col("corridor_json")))
        .withColumn("label_good_cmd",   F.when(
            F.col("lateral_dev_t1") < F.col("lateral_dev_t"), F.lit(1)
        ).otherwise(F.lit(0)))
    )

    # Filter out rows we can't use
    features = (
        df
        .filter(F.col("lateral_dev_t").isNotNull())
        .filter(F.col("lateral_dev_t1").isNotNull())
        .filter(F.col("speed") > 0.1)       # skip stopped
        .filter(F.col("corridor_len") > 0)
        .select(
            "vehicle_id", "t", "road_id", "lane_id",
            "lateral_dev_t", "lateral_dev_t1",
            "heading_error", "speed_ratio", "upcoming_curv",
            "at_junction", "label_good_cmd",
        )
    )

    features_dir.mkdir(parents=True, exist_ok=True)
    features.write.mode("overwrite").parquet(str(features_dir))
    return features
