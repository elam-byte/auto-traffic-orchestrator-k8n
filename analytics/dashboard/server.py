"""
Unified ATS Analytics Dashboard server.

Serves the single-page dashboard at http://localhost:8091
with API endpoints for all 4 extensions.

Usage:
    python analytics/dashboard/server.py
    python analytics/dashboard/server.py --port 8091 --run-id run-001
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import AsyncGenerator

import pandas as pd
import uvicorn
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from sse_starlette.sse import EventSourceResponse

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from analytics.shared.config import (
    RUNS_DIR, RESULTS_DIR, STREAMING_DIR, ML_DATASET_DIR, SCENARIOS_DIR,
    DASHBOARD_PORT,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("dashboard")

app = FastAPI(title="ATS Analytics Dashboard", docs_url="/api/docs")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

_STATIC_DIR = Path(__file__).parent / "static"

# ── Helpers ────────────────────────────────────────────────────────────────────

def _parquet_to_json(path: Path) -> list[dict]:
    """Read a Parquet file and return as list of dicts (safe for JSON)."""
    if not path.exists():
        return []
    try:
        df = pd.read_parquet(path)
        # Convert non-serialisable types
        for col in df.columns:
            if df[col].dtype == "bool":
                df[col] = df[col].astype(int)
        return df.fillna(0).to_dict(orient="records")
    except Exception as e:
        log.warning("Failed to read %s: %s", path, e)
        return []


def _latest_run_id() -> str | None:
    if not RUNS_DIR.exists():
        return None
    runs = sorted(RUNS_DIR.iterdir(), key=lambda p: p.stat().st_mtime, reverse=True)
    return runs[0].name if runs else None


def _list_runs() -> list[str]:
    if not RUNS_DIR.exists():
        return []
    return sorted([p.name for p in RUNS_DIR.iterdir() if p.is_dir()])


# ── Root ───────────────────────────────────────────────────────────────────────

@app.get("/")
async def root():
    from fastapi.responses import FileResponse
    return FileResponse(str(_STATIC_DIR / "index.html"))


# ── Extension 1 — Real-time endpoints ─────────────────────────────────────────

@app.get("/api/ext1/congestion")
async def ext1_congestion():
    """Latest congestion data from streaming output."""
    out = _read_latest_streaming("congestion")
    return JSONResponse(out)


@app.get("/api/ext1/throughput")
async def ext1_throughput():
    out = _read_latest_streaming("throughput")
    return JSONResponse(out)


@app.get("/api/ext1/density")
async def ext1_density():
    out = _read_latest_streaming("density")
    return JSONResponse(out)


@app.get("/api/ext1/stream")
async def ext1_stream():
    """SSE endpoint — pushes all 3 metric types every 3 seconds."""
    async def generator() -> AsyncGenerator[dict, None]:
        while True:
            payload = {
                "congestion":  _read_latest_streaming("congestion"),
                "throughput":  _read_latest_streaming("throughput"),
                "density":     _read_latest_streaming("density"),
            }
            yield {"data": json.dumps(payload)}
            await asyncio.sleep(3)
    return EventSourceResponse(generator())


def _read_latest_streaming(metric: str) -> list[dict]:
    """Read the most recent JSON files from a streaming output dir."""
    d = STREAMING_DIR / metric
    if not d.exists():
        return []
    files = sorted(d.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    rows = []
    for f in files[:5]:           # last 5 micro-batch files
        try:
            for line in f.read_text().strip().splitlines():
                if line.strip():
                    obj = json.loads(line)
                    # Convert timestamp objects to strings
                    for k in ("window_start", "window_end"):
                        if k in obj:
                            obj[k] = str(obj[k])
                    rows.append(obj)
        except Exception:
            pass
    return rows


# ── Extension 2 — Historical endpoints ────────────────────────────────────────

@app.get("/api/ext2/runs")
async def ext2_runs():
    return JSONResponse(_list_runs())


@app.get("/api/ext2/lateral_deviation")
async def ext2_lateral_deviation(run_id: str = Query(default=None)):
    rid = run_id or _latest_run_id()
    if not rid:
        return JSONResponse([])
    return JSONResponse(_parquet_to_json(RESULTS_DIR / rid / "lateral_deviation.parquet"))


@app.get("/api/ext2/speed_profile")
async def ext2_speed_profile(run_id: str = Query(default=None)):
    rid = run_id or _latest_run_id()
    if not rid:
        return JSONResponse([])
    return JSONResponse(_parquet_to_json(RESULTS_DIR / rid / "speed_profile.parquet"))


@app.get("/api/ext2/junction_slowdown")
async def ext2_junction_slowdown(run_id: str = Query(default=None)):
    rid = run_id or _latest_run_id()
    if not rid:
        return JSONResponse([])
    return JSONResponse(_parquet_to_json(RESULTS_DIR / rid / "junction_slowdown.parquet"))


@app.get("/api/ext2/dead_ends")
async def ext2_dead_ends(run_id: str = Query(default=None)):
    rid = run_id or _latest_run_id()
    if not rid:
        return JSONResponse([])
    data = _parquet_to_json(RESULTS_DIR / rid / "dead_end_incidents.parquet")
    per_vehicle = _parquet_to_json(RESULTS_DIR / rid / "dead_end_per_vehicle.parquet")
    per_edge = _parquet_to_json(RESULTS_DIR / rid / "dead_end_per_edge.parquet")
    return JSONResponse({"summary": data, "per_vehicle": per_vehicle, "per_edge": per_edge})


@app.get("/api/ext2/regression")
async def ext2_regression(run_id: str = Query(default=None)):
    rid = run_id or _latest_run_id()
    if not rid:
        return JSONResponse([])
    return JSONResponse(_parquet_to_json(RESULTS_DIR / rid / "regression_report.parquet"))


# ── Extension 3 — ML pipeline endpoints ───────────────────────────────────────

@app.get("/api/ext3/quality")
async def ext3_quality():
    q_path = ML_DATASET_DIR / "quality.json"
    if not q_path.exists():
        return JSONResponse({"error": "No ML dataset found. Run the pipeline first."})
    return JSONResponse(json.loads(q_path.read_text()))


@app.get("/api/ext3/features")
async def ext3_features():
    """Sample of feature statistics from the train set."""
    train_path = ML_DATASET_DIR / "train"
    if not train_path.exists():
        return JSONResponse([])
    try:
        df = pd.read_parquet(str(train_path))
        stats = []
        for col in ["lateral_dev_t", "heading_error", "speed_ratio", "upcoming_curv"]:
            if col in df.columns:
                stats.append({
                    "feature": col,
                    "mean":    round(float(df[col].mean()), 4),
                    "std":     round(float(df[col].std()),  4),
                    "p50":     round(float(df[col].median()), 4),
                    "p95":     round(float(df[col].quantile(0.95)), 4),
                })
        label_dist = [
            {"label": "good (1)", "count": int((df["label_good_cmd"] == 1).sum())},
            {"label": "bad (0)",  "count": int((df["label_good_cmd"] == 0).sum())},
        ]
        road_dist = df.groupby("road_id").size().reset_index(name="count").to_dict(orient="records")
        return JSONResponse({"feature_stats": stats, "label_dist": label_dist, "road_dist": road_dist})
    except Exception as e:
        return JSONResponse({"error": str(e)})


# ── Extension 4 — Scenario testing endpoints ──────────────────────────────────

@app.get("/api/ext4/scenario_types")
async def ext4_scenario_types():
    from analytics.ext4_scenario_testing.scenarios import SCENARIO_TYPES
    types = []
    for name, fn in SCENARIO_TYPES.items():
        scenarios = fn()
        types.append({"type": name, "count": len(scenarios)})
    return JSONResponse(types)


@app.get("/api/ext4/results")
async def ext4_results(scenario_type: str = Query(default=None)):
    """Return scenario results JSON."""
    if scenario_type:
        results_path = SCENARIOS_DIR / scenario_type / "results.json"
    else:
        # Find the most recently modified results.json
        candidates = sorted(
            SCENARIOS_DIR.glob("*/results.json"),
            key=lambda p: p.stat().st_mtime, reverse=True
        ) if SCENARIOS_DIR.exists() else []
        results_path = candidates[0] if candidates else None

    if not results_path or not results_path.exists():
        return JSONResponse([])
    return JSONResponse(json.loads(results_path.read_text()))


@app.get("/api/ext4/summary")
async def ext4_summary(scenario_type: str = Query(default=None)):
    if scenario_type:
        path = SCENARIOS_DIR / scenario_type / "scenario_summary.parquet"
    else:
        candidates = sorted(
            SCENARIOS_DIR.glob("*/scenario_summary.parquet"),
            key=lambda p: p.stat().st_mtime, reverse=True
        ) if SCENARIOS_DIR.exists() else []
        path = candidates[0] if candidates else None

    if not path or not path.exists():
        return JSONResponse([])
    return JSONResponse(_parquet_to_json(path))


# ── Static files ───────────────────────────────────────────────────────────────

_STATIC_DIR.mkdir(parents=True, exist_ok=True)
app.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")


def main() -> None:
    ap = argparse.ArgumentParser(description="ATS Analytics Dashboard")
    ap.add_argument("--port", type=int, default=DASHBOARD_PORT)
    ap.add_argument("--host", default="0.0.0.0")
    args = ap.parse_args()

    log.info("Dashboard: http://localhost:%d", args.port)
    uvicorn.run(app, host=args.host, port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
