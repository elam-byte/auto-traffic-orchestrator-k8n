"""
Job: Controller regression detector.

Compare lateral deviation distributions between two run sets (run_id_a vs
run_id_b). Flags if p95 lateral deviation increased by more than a threshold.

Usage via run_jobs.py:
    python analytics/ext2_historical/run_jobs.py \\
        --run-id run-001 \\
        --jobs regression_detector \\
        --compare-run run-002

Writes: data/results/{run_id}/regression_report.parquet
"""
from __future__ import annotations

import json
import math
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType


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


def _compute_p95(spark: SparkSession, obs_path: str) -> float:
    lateral_udf = F.udf(_lateral_dev, DoubleType())
    df = (
        spark.read.parquet(obs_path)
        .filter(F.col("speed") > 0.1)
        .filter(F.col("corridor_len") > 0)
        .withColumn("lat_dev", lateral_udf(F.col("x"), F.col("y"), F.col("corridor_json")))
        .filter(F.col("lat_dev").isNotNull())
    )
    row = df.agg(F.percentile_approx("lat_dev", 0.95).alias("p95")).first()
    return row["p95"] if row else float("nan")


def run(
    spark: SparkSession,
    run_dir: Path,
    out_dir: Path,
    compare_run_dir: Path | None = None,
    threshold_pct: float = 10.0,
) -> DataFrame:
    if compare_run_dir is None:
        raise ValueError("regression_detector requires --compare-run <run_id>")

    p95_a = _compute_p95(spark, str(run_dir / "observations"))
    p95_b = _compute_p95(spark, str(compare_run_dir / "observations"))

    change_pct = ((p95_b - p95_a) / p95_a * 100) if p95_a else float("nan")
    regressed = change_pct > threshold_pct

    report = [{
        "run_a":         run_dir.name,
        "run_b":         compare_run_dir.name,
        "p95_lateral_a": round(p95_a, 4),
        "p95_lateral_b": round(p95_b, 4),
        "change_pct":    round(change_pct, 2),
        "threshold_pct": threshold_pct,
        "regressed":     regressed,
    }]

    result = spark.createDataFrame(report)
    out_dir.mkdir(parents=True, exist_ok=True)
    result.write.mode("overwrite").parquet(str(out_dir / "regression_report.parquet"))
    return result
