"""Shared configuration for all ATS analytics extensions."""
import os
from pathlib import Path

# ── NATS ──────────────────────────────────────────────────────────────────────
NATS_URL = os.getenv("NATS_URL", "nats://localhost:4222")

# ── Kafka ─────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC_SNAPSHOTS = "sim-snapshots"
KAFKA_TOPIC_OBSERVATIONS = "sim-observations"

# ── Data paths ────────────────────────────────────────────────────────────────
# Resolved relative to repo root (two levels up from this file)
_REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = Path(os.getenv("ATS_DATA_DIR", str(_REPO_ROOT / "data")))

RUNS_DIR       = DATA_DIR / "runs"
RESULTS_DIR    = DATA_DIR / "results"
STREAMING_DIR  = DATA_DIR / "streaming"
ML_DATASET_DIR = DATA_DIR / "ml_dataset"
SCENARIOS_DIR  = DATA_DIR / "scenarios"

# ── Dashboard ─────────────────────────────────────────────────────────────────
DASHBOARD_PORT = int(os.getenv("DASHBOARD_PORT", "8091"))

# ── Simulation constants ──────────────────────────────────────────────────────
TICK_HZ = 20                # ticks per second
TICK_MS = 1000 // TICK_HZ  # 50 ms per tick

# Near-collision threshold (metres)
NEAR_COLLISION_DIST_M = float(os.getenv("NEAR_COLLISION_DIST_M", "5.0"))

# Streaming window for real-time aggregates
STREAM_WINDOW_SEC = int(os.getenv("STREAM_WINDOW_SEC", "10"))

# Recorder flush interval
RECORDER_FLUSH_SEC = int(os.getenv("RECORDER_FLUSH_SEC", "30"))
RECORDER_FLUSH_ROWS = int(os.getenv("RECORDER_FLUSH_ROWS", "1000"))


def ensure_dirs() -> None:
    """Create all data directories if they don't exist."""
    for d in (RUNS_DIR, RESULTS_DIR, STREAMING_DIR, ML_DATASET_DIR, SCENARIOS_DIR):
        d.mkdir(parents=True, exist_ok=True)
