"""
Extension 1 — PySpark Structured Streaming job.

Reads sim.snapshots from Kafka (topic: sim-snapshots) and computes 4 real-time
metrics in sliding 10-second windows, writing results as JSON to data/streaming/:

  congestion/      — avg speed / speed_limit per lane edge (from observations)
  throughput/      — vehicles/min at each junction edge
  collisions/      — near-collision pairs (same lane, < NEAR_COLLISION_DIST_M)
  density/         — vehicle count per grid cell (50×50 cells over world bounds)

Usage:
    spark-submit \\
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \\
      analytics/ext1_realtime/streaming_job.py

    # Or in local mode (no cluster):
    python analytics/ext1_realtime/streaming_job.py --local
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, LongType, ArrayType,
)

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from analytics.shared.config import (
    KAFKA_BOOTSTRAP, KAFKA_TOPIC_SNAPSHOTS, KAFKA_TOPIC_OBSERVATIONS,
    STREAMING_DIR, STREAM_WINDOW_SEC,
)

# ── Schemas ────────────────────────────────────────────────────────────────────

SNAPSHOT_SCHEMA = StructType([
    StructField("t",   LongType()),
    StructField("t_iso", StringType()),
    StructField("vehicles", ArrayType(StructType([
        StructField("id",      StringType()),
        StructField("x",       DoubleType()),
        StructField("y",       DoubleType()),
        StructField("heading", DoubleType()),
        StructField("length",  DoubleType()),
        StructField("width",   DoubleType()),
        StructField("color",   StringType()),
    ]))),
])

OBS_SCHEMA = StructType([
    StructField("t",     LongType()),
    StructField("t_iso", StringType()),
    StructField("id",    StringType()),
    StructField("x",     DoubleType()),
    StructField("y",     DoubleType()),
    StructField("speed", DoubleType()),
    StructField("lane_corridor", ArrayType(StructType([
        StructField("lane_id",     StringType()),
        StructField("road_id",     StringType()),
        StructField("speed_limit", DoubleType()),
        StructField("x",           DoubleType()),
        StructField("y",           DoubleType()),
    ]))),
    StructField("junction", StructType([
        StructField("at_edge",        StringType()),
        StructField("choices",        ArrayType(StringType())),
        StructField("current_choice", StringType()),
    ])),
])

# World bounds for density grid (from default map)
WORLD_W = 500.0
WORLD_H = 281.0
GRID_COLS = 50
GRID_ROWS = 28


def build_spark(local: bool) -> SparkSession:
    master = "local[*]" if local else None
    builder = SparkSession.builder.appName("ats-ext1-streaming")
    if master:
        builder = builder.master(master)
    return (
        builder
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5")
        .getOrCreate()
    )


def run(spark: SparkSession) -> None:
    STREAMING_DIR.mkdir(parents=True, exist_ok=True)
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    window_dur = f"{STREAM_WINDOW_SEC} seconds"
    slide_dur  = "5 seconds"

    # ── Read Kafka snapshot stream ─────────────────────────────────────────────
    snap_raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC_SNAPSHOTS)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    snaps = (
        snap_raw
        .select(F.from_json(F.col("value").cast("string"), SNAPSHOT_SCHEMA).alias("d"))
        .select(
            F.col("d.t").alias("t"),
            (F.col("d.t") / 1000).cast("timestamp").alias("event_time"),
            F.explode("d.vehicles").alias("v"),
        )
        .select(
            "event_time",
            F.col("v.id").alias("vehicle_id"),
            F.col("v.x").alias("x"),
            F.col("v.y").alias("y"),
        )
        .withWatermark("event_time", "10 seconds")
    )

    # ── Read Kafka observation stream ──────────────────────────────────────────
    obs_raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC_OBSERVATIONS)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    obs = (
        obs_raw
        .select(F.from_json(F.col("value").cast("string"), OBS_SCHEMA).alias("d"))
        .select(
            (F.col("d.t") / 1000).cast("timestamp").alias("event_time"),
            F.col("d.id").alias("vehicle_id"),
            F.col("d.speed").alias("speed"),
            F.col("d.x").alias("x"),
            F.col("d.y").alias("y"),
            F.col("d.lane_corridor")[0].alias("first_point"),
            F.col("d.junction").alias("junction"),
        )
        .withColumn("lane_id",     F.col("first_point.lane_id"))
        .withColumn("speed_limit", F.col("first_point.speed_limit"))
        .withColumn("at_junction", F.col("junction").isNotNull())
        .withColumn("junction_edge", F.col("junction.at_edge"))
        .withWatermark("event_time", "10 seconds")
    )

    # ── Metric 1: Congestion score per lane edge ───────────────────────────────
    congestion = (
        obs
        .filter(F.col("lane_id").isNotNull())
        .filter(F.col("speed_limit") > 0)
        .groupBy(F.window("event_time", window_dur, slide_dur), "lane_id")
        .agg(
            F.mean("speed").alias("avg_speed_ms"),
            F.mean("speed_limit").alias("avg_speed_limit_ms"),
            F.count("*").alias("sample_count"),
        )
        .withColumn("congestion_score",
                    F.lit(1.0) - (F.col("avg_speed_ms") / F.col("avg_speed_limit_ms")))
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end",   F.col("window.end"))
        .drop("window")
    )

    # ── Metric 2: Junction throughput ──────────────────────────────────────────
    throughput = (
        obs
        .filter(F.col("at_junction") == True)
        .filter(F.col("junction_edge").isNotNull())
        .groupBy(F.window("event_time", window_dur, slide_dur), "junction_edge")
        .agg(
            F.approx_count_distinct("vehicle_id").alias("vehicles_per_window"),
        )
        .withColumn("vehicles_per_min",
                    F.col("vehicles_per_window") * (60.0 / STREAM_WINDOW_SEC))
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end",   F.col("window.end"))
        .drop("window")
    )

    # ── Metric 3: Vehicle density heatmap ─────────────────────────────────────
    density = (
        snaps
        .withColumn("grid_col", ((F.col("x") / WORLD_W) * GRID_COLS).cast("int"))
        .withColumn("grid_row", ((F.col("y") / WORLD_H) * GRID_ROWS).cast("int"))
        .filter((F.col("grid_col") >= 0) & (F.col("grid_col") < GRID_COLS))
        .filter((F.col("grid_row") >= 0) & (F.col("grid_row") < GRID_ROWS))
        .groupBy(F.window("event_time", window_dur, slide_dur), "grid_col", "grid_row")
        .agg(F.count("*").alias("vehicle_count"))
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end",   F.col("window.end"))
        .drop("window")
    )

    # ── Write sinks (JSON files, one per micro-batch) ──────────────────────────
    def _write_query(df, name: str):
        return (
            df.writeStream
            .outputMode("append")
            .format("json")
            .option("path", str(STREAMING_DIR / name))
            .option("checkpointLocation", str(STREAMING_DIR / f"_checkpoint_{name}"))
            .trigger(processingTime="5 seconds")
            .start()
        )

    _write_query(congestion, "congestion")
    _write_query(throughput, "throughput")
    _write_query(density,    "density")

    spark.streams.awaitAnyTermination()


def main() -> None:
    ap = argparse.ArgumentParser(description="ATS real-time streaming job")
    ap.add_argument("--local", action="store_true",
                    help="Run in local Spark mode (no cluster needed)")
    args = ap.parse_args()

    spark = build_spark(local=args.local)
    run(spark)


if __name__ == "__main__":
    main()
