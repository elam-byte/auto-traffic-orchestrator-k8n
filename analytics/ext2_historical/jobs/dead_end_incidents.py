"""
Job: Dead-end incident report.

Finds ticks where a vehicle had an empty lane_corridor (corridor_len == 0),
which indicates it lost its path. Reports per-vehicle and per-edge counts.

Writes: data/results/{run_id}/dead_end_incidents.parquet
"""
from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


def run(spark: SparkSession, run_dir: Path, out_dir: Path) -> DataFrame:
    obs_path = str(run_dir / "observations")

    df = spark.read.parquet(obs_path)

    incidents = df.filter(F.col("corridor_len") == 0)

    per_vehicle = (
        incidents.groupBy("vehicle_id")
        .agg(
            F.count("*").alias("incident_count"),
            F.min("t").alias("first_incident_t"),
            F.max("t").alias("last_incident_t"),
        )
        .orderBy(F.col("incident_count").desc())
    )

    per_edge = (
        incidents.filter(F.col("road_id") != "")
        .groupBy("road_id", "lane_id")
        .agg(
            F.count("*").alias("incident_count"),
            F.countDistinct("vehicle_id").alias("affected_vehicles"),
        )
        .orderBy(F.col("incident_count").desc())
    )

    total = incidents.count()

    out_dir.mkdir(parents=True, exist_ok=True)
    per_vehicle.write.mode("overwrite").parquet(str(out_dir / "dead_end_per_vehicle.parquet"))
    per_edge.write.mode("overwrite").parquet(str(out_dir / "dead_end_per_edge.parquet"))

    # Write a summary parquet combining both
    summary = spark.createDataFrame([{
        "total_incidents": total,
        "affected_vehicles": incidents.select("vehicle_id").distinct().count(),
        "affected_edges": incidents.filter(F.col("road_id") != "").select("road_id").distinct().count(),
    }])
    summary.write.mode("overwrite").parquet(str(out_dir / "dead_end_incidents.parquet"))
    return summary
