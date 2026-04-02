"""
Job: Junction slowdown ranking.

For each junction edge (at_junction == True), compare the vehicle's speed
10 ticks before and after the junction crossing. Ranks junctions by average
speed reduction. Identifies map geometry problems.

Writes: data/results/{run_id}/junction_slowdown.parquet
"""
from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F


def run(spark: SparkSession, run_dir: Path, out_dir: Path) -> DataFrame:
    obs_path = str(run_dir / "observations")

    df = spark.read.parquet(obs_path)

    # Window per vehicle ordered by time
    w_vehicle = Window.partitionBy("vehicle_id").orderBy("t")

    df = (
        df
        .withColumn("speed_lag10",  F.lag("speed",  10).over(w_vehicle))  # 10 ticks before
        .withColumn("speed_lead10", F.lead("speed", 10).over(w_vehicle))  # 10 ticks after
        .filter(F.col("at_junction") == True)
        .filter(F.col("speed_lag10").isNotNull())
        .filter(F.col("speed_lead10").isNotNull())
        .withColumn("slowdown_ms", F.col("speed_lag10") - F.col("speed_lead10"))
    )

    ranking = (
        df.groupBy("junction_edge")
        .agg(
            F.count("*").alias("crossing_count"),
            F.mean("slowdown_ms").alias("avg_slowdown_ms"),
            F.mean("speed").alias("avg_junction_speed_ms"),
            F.mean("speed_lag10").alias("avg_approach_speed_ms"),
            F.mean("speed_lead10").alias("avg_exit_speed_ms"),
        )
        .filter(F.col("junction_edge") != "")
        .orderBy(F.col("avg_slowdown_ms").desc())
    )

    out_dir.mkdir(parents=True, exist_ok=True)
    ranking.write.mode("overwrite").parquet(str(out_dir / "junction_slowdown.parquet"))
    return ranking
