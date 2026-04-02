"""
Extension 2 — Parquet recorder.

Subscribes to NATS sim.snapshots and sim.obs.> and writes buffered
Parquet files partitioned by run_id.

Usage:
    python analytics/ext2_historical/recorder.py --run-id run-001
    python analytics/ext2_historical/recorder.py --run-id run-001 --nats nats://localhost:4222
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import signal
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import nats
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from analytics.shared.config import (
    NATS_URL, RUNS_DIR,
    RECORDER_FLUSH_SEC, RECORDER_FLUSH_ROWS,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("recorder")


# ── Parquet schemas ────────────────────────────────────────────────────────────

SNAPSHOT_SCHEMA = pa.schema([
    pa.field("t",          pa.int64()),
    pa.field("t_iso",      pa.string()),
    pa.field("vehicle_id", pa.string()),
    pa.field("x",          pa.float64()),
    pa.field("y",          pa.float64()),
    pa.field("heading",    pa.float64()),
    pa.field("length",     pa.float64()),
    pa.field("width",      pa.float64()),
    pa.field("color",      pa.string()),
])

OBS_SCHEMA = pa.schema([
    pa.field("t",                pa.int64()),
    pa.field("t_iso",            pa.string()),
    pa.field("vehicle_id",       pa.string()),
    pa.field("x",                pa.float64()),
    pa.field("y",                pa.float64()),
    pa.field("heading",          pa.float64()),
    pa.field("speed",            pa.float64()),
    pa.field("corridor_len",     pa.int32()),     # number of lane points
    pa.field("lane_id",          pa.string()),     # first corridor point lane_id
    pa.field("road_id",          pa.string()),     # first corridor point road_id
    pa.field("speed_limit",      pa.float64()),    # first corridor point speed_limit
    pa.field("at_junction",      pa.bool_()),
    pa.field("junction_edge",    pa.string()),
    pa.field("corridor_json",    pa.string()),     # full corridor as JSON string
])


class ParquetBuffer:
    """Accumulates rows and flushes to partitioned Parquet files."""

    def __init__(self, base_dir: Path, schema: pa.Schema, name: str):
        self.base_dir = base_dir
        self.schema = schema
        self.name = name
        self._rows: list[dict] = []
        self._last_flush = time.monotonic()
        self._file_index = 0

    def add(self, row: dict) -> None:
        self._rows.append(row)

    def should_flush(self) -> bool:
        elapsed = time.monotonic() - self._last_flush
        return elapsed >= RECORDER_FLUSH_SEC or len(self._rows) >= RECORDER_FLUSH_ROWS

    def flush(self) -> int:
        if not self._rows:
            return 0
        rows = self._rows
        self._rows = []
        self._last_flush = time.monotonic()

        minute = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M")
        out_dir = self.base_dir / f"minute={minute}"
        out_dir.mkdir(parents=True, exist_ok=True)

        self._file_index += 1
        path = out_dir / f"{self.name}-{self._file_index:06d}.parquet"

        table = pa.Table.from_pylist(rows, schema=self.schema)
        pq.write_table(table, path, compression="snappy")
        log.info("Flushed %d rows → %s", len(rows), path.relative_to(self.base_dir.parent))
        return len(rows)


class Recorder:
    def __init__(self, run_id: str, nats_url: str):
        self.run_id = run_id
        self.nats_url = nats_url

        run_dir = RUNS_DIR / run_id
        self._snap_buf = ParquetBuffer(run_dir / "snapshots",   SNAPSHOT_SCHEMA, "snap")
        self._obs_buf  = ParquetBuffer(run_dir / "observations", OBS_SCHEMA,      "obs")

        (run_dir / "snapshots").mkdir(parents=True, exist_ok=True)
        (run_dir / "observations").mkdir(parents=True, exist_ok=True)

        meta = {
            "run_id":    run_id,
            "started":   datetime.now(timezone.utc).isoformat(),
            "nats_url":  nats_url,
        }
        (run_dir / "meta.json").write_text(json.dumps(meta, indent=2))
        log.info("Run dir: %s", run_dir)

        self._nc = None
        self._stop = asyncio.Event()
        self._snap_count = 0
        self._obs_count  = 0

    # ── NATS handlers ─────────────────────────────────────────────────────────

    async def _on_snapshot(self, msg) -> None:
        try:
            data = json.loads(msg.data.decode())
            t     = data.get("t", 0)
            t_iso = data.get("t_iso", "")
            for v in data.get("vehicles", []):
                self._snap_buf.add({
                    "t":          t,
                    "t_iso":      t_iso,
                    "vehicle_id": v["id"],
                    "x":          v["x"],
                    "y":          v["y"],
                    "heading":    v["heading"],
                    "length":     v.get("length", 0.0),
                    "width":      v.get("width", 0.0),
                    "color":      v.get("color", ""),
                })
            self._snap_count += 1
        except Exception as e:
            log.warning("snapshot parse error: %s", e)

        if self._snap_buf.should_flush():
            self._snap_buf.flush()

    async def _on_observation(self, msg) -> None:
        try:
            data = json.loads(msg.data.decode())
            corridor = data.get("lane_corridor", [])
            first = corridor[0] if corridor else {}
            junction = data.get("junction") or {}

            self._obs_buf.add({
                "t":             data.get("t", 0),
                "t_iso":         data.get("t_iso", ""),
                "vehicle_id":    data.get("id", ""),
                "x":             data.get("x", 0.0),
                "y":             data.get("y", 0.0),
                "heading":       data.get("heading", 0.0),
                "speed":         data.get("speed", 0.0),
                "corridor_len":  len(corridor),
                "lane_id":       first.get("lane_id", ""),
                "road_id":       first.get("road_id", ""),
                "speed_limit":   first.get("speed_limit", 0.0),
                "at_junction":   bool(junction),
                "junction_edge": junction.get("at_edge", ""),
                "corridor_json": json.dumps(corridor),
            })
            self._obs_count += 1
        except Exception as e:
            log.warning("observation parse error: %s", e)

        if self._obs_buf.should_flush():
            self._obs_buf.flush()

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    async def run(self) -> None:
        self._nc = await nats.connect(self.nats_url)
        log.info("Connected to NATS: %s", self.nats_url)

        await self._nc.subscribe("sim.snapshots", cb=self._on_snapshot)
        await self._nc.subscribe("sim.obs.>",     cb=self._on_observation)
        log.info("Recording run=%s  (Ctrl-C to stop)", self.run_id)

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self._stop.set)

        await self._stop.wait()
        await self._flush_all()
        await self._nc.drain()
        log.info("Recorder stopped. snapshots=%d obs=%d", self._snap_count, self._obs_count)

    async def _flush_all(self) -> None:
        self._snap_buf.flush()
        self._obs_buf.flush()


def main() -> None:
    ap = argparse.ArgumentParser(description="ATS Parquet recorder")
    ap.add_argument("--run-id", required=True, help="Unique run identifier, e.g. run-001")
    ap.add_argument("--nats",   default=NATS_URL, help="NATS server URL")
    args = ap.parse_args()

    recorder = Recorder(run_id=args.run_id, nats_url=args.nats)
    asyncio.run(recorder.run())


if __name__ == "__main__":
    main()
