"""
Extension 4 — Scenario definitions.

Each scenario is a dict that parameterises one simulation run. The runner
writes a docker-compose.override.yml and runs the stack for a fixed duration,
then collects metrics via the recorder (Extension 2).
"""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any


@dataclass
class Scenario:
    """One simulation scenario."""
    scenario_id: str
    description: str

    # Vehicle agent controller params (written as env vars to override defaults)
    k_lateral: float = 2.5          # Stanley lateral gain (default)
    k_soft: float    = 0.3          # Softening at low speed
    prebrake_horizon: int = 20      # Look-ahead metres for speed limit braking

    # Simulation topology
    vehicle_count: int = 1          # how many vehicles to spawn
    duration_sec: int  = 60         # how long to run the scenario

    # Extra override env vars to inject into vehicle-agent container
    extra_env: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


# ── Hyperparameter sweep ───────────────────────────────────────────────────────

def hyperparameter_sweep() -> list[Scenario]:
    """Grid search over K_LATERAL (1.0–4.0) × PREBRAKE_HORIZON (10–30)."""
    scenarios = []
    for k in [1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0]:
        for h in [10, 15, 20, 25, 30]:
            sid = f"sweep_k{k:.1f}_h{h}"
            scenarios.append(Scenario(
                scenario_id=sid,
                description=f"K_LATERAL={k} PREBRAKE_HORIZON={h}",
                k_lateral=k,
                prebrake_horizon=h,
                vehicle_count=1,
                duration_sec=90,
            ))
    return scenarios


# ── Vehicle density stress test ────────────────────────────────────────────────

def density_stress() -> list[Scenario]:
    """Test 5, 10, 15, 20 vehicles and measure junction throughput."""
    scenarios = []
    for n in [5, 10, 15, 20]:
        scenarios.append(Scenario(
            scenario_id=f"density_{n}v",
            description=f"{n} vehicles — junction throughput stress",
            vehicle_count=n,
            duration_sec=120,
        ))
    return scenarios


# ── A/B controller comparison ──────────────────────────────────────────────────

def ab_comparison(n_each: int = 10) -> list[Scenario]:
    """
    Compare two controller configurations.
    Controller A: default (K_LATERAL=2.5, PREBRAKE_HORIZON=20)
    Controller B: aggressive (K_LATERAL=3.5, PREBRAKE_HORIZON=15)
    """
    scenarios = []
    for i in range(n_each):
        scenarios.append(Scenario(
            scenario_id=f"ctrl_a_{i:03d}",
            description="Controller A (default)",
            k_lateral=2.5, prebrake_horizon=20,
            vehicle_count=1, duration_sec=60,
        ))
        scenarios.append(Scenario(
            scenario_id=f"ctrl_b_{i:03d}",
            description="Controller B (aggressive)",
            k_lateral=3.5, prebrake_horizon=15,
            vehicle_count=1, duration_sec=60,
        ))
    return scenarios


# ── Map robustness test ────────────────────────────────────────────────────────

def map_robustness(n: int = 20) -> list[Scenario]:
    """Run with small random start-position perturbations to test corridor recovery."""
    import random
    random.seed(99)
    scenarios = []
    for i in range(n):
        scenarios.append(Scenario(
            scenario_id=f"robust_{i:03d}",
            description=f"Start-position perturbation #{i}",
            vehicle_count=1,
            duration_sec=60,
            extra_env={"ATS_START_JITTER_M": str(round(random.uniform(0.5, 3.0), 2))},
        ))
    return scenarios


SCENARIO_TYPES: dict[str, callable] = {
    "hyperparameter_sweep": hyperparameter_sweep,
    "density_stress":       density_stress,
    "ab_comparison":        ab_comparison,
    "map_robustness":       map_robustness,
}
