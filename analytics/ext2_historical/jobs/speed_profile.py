"""
Job: Speed profile per lap.

Bins observations by corridor position index (0–99 m from the vehicle's
current position) and computes average speed at each bin. Reveals
overshoot, undershoot, and oscillation.

Writes: data/results/{run_id}/speed_profile.parquet
"""
from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StructType, StructField, DoubleType, StringType


_CORRIDOR_SCHEMA = ArrayType(StructType([
    StructField("x",           DoubleType()),
    StructField("y",           DoubleType()),
    StructField("heading",     DoubleType()),
    StructField("width",       DoubleType()),
    StructField("speed_limit", DoubleType()),
    StructField("lane_id",     StringType()),
    StructField("road_id",     StringType()),
]))


def run(spark: SparkSession, run_dir: Path, out_dir: Path) -> DataFrame:
    obs_path = str(run_dir / "observations")

    df = spark.read.parquet(obs_path).filter(F.col("speed") > 0.1)

    # Explode corridor to get per-point speed_limit and position index
    df_corr = (
        df.withColumn("corridor", F.from_json(F.col("corridor_json"), _CORRIDOR_SCHEMA))
        .select(
            "vehicle_id", "t", "speed",
            F.posexplode(F.col("corridor")).alias("pos_m", "corridor_pt")
        )
        .withColumn("speed_limit", F.col("corridor_pt.speed_limit"))
        .withColumn("lane_id",     F.col("corridor_pt.lane_id"))
    )

    # Bin into 5 m buckets (0–4 = 0 m, 5–9 = 5 m, …)
    df_binned = df_corr.withColumn("pos_bin_m", (F.col("pos_m") / 5).cast("int") * 5)

    profile = (
        df_binned.groupBy("pos_bin_m", "lane_id")
        .agg(
            F.mean("speed").alias("avg_speed_ms"),
            F.mean("speed_limit").alias("avg_speed_limit_ms"),
            F.count("*").alias("sample_count"),
        )
        .withColumn("speed_ratio", F.col("avg_speed_ms") / F.col("avg_speed_limit_ms"))
        .orderBy("pos_bin_m")
    )

    out_dir.mkdir(parents=True, exist_ok=True)
    profile.write.mode("overwrite").parquet(str(out_dir / "speed_profile.parquet"))
    return profile
