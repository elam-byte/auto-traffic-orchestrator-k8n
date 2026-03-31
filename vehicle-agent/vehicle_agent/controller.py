import math
import random
from .types import VehicleObservation, VehicleCommand, LanePoint, JunctionChoice

TARGET_SPEED = float(__import__("os").environ.get("TARGET_SPEED", "8.0"))
SPEED_KP = 0.8      # proportional gain for speed control
ACCEL_MIN = -4.0
ACCEL_MAX = 2.0
STEER_MIN = -0.5
STEER_MAX = 0.5
MIN_LOOKAHEAD = 8.0  # meters

# In-process sticky junction choices: at_edge → chosen next edge ID
# This state lives in the container (not persisted) — valid per CLAUDE.md design.
_junction_choices: dict[str, str] = {}


def _normalize_angle(a: float) -> float:
    """Map angle to (-π, π]."""
    r = a % (2 * math.pi)
    if r > math.pi:
        r -= 2 * math.pi
    if r <= -math.pi:
        r += 2 * math.pi
    return r


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _find_lookahead(
    obs: VehicleObservation, corridor: list[LanePoint], distance: float
) -> LanePoint:
    """Walk the corridor until cumulative arc-length >= distance."""
    cumulative = 0.0
    prev_x, prev_y = obs.x, obs.y
    for pt in corridor:
        dx = pt.x - prev_x
        dy = pt.y - prev_y
        cumulative += math.sqrt(dx * dx + dy * dy)
        prev_x, prev_y = pt.x, pt.y
        if cumulative >= distance:
            return pt
    return corridor[-1]


def compute_command(obs: VehicleObservation) -> VehicleCommand:
    corridor = obs.lane_corridor

    if not corridor:
        # No corridor — gentle brake, hold straight
        return VehicleCommand(
            t=obs.t, id=obs.id, desired_accel=-2.0, desired_steer=0.0
        )

    # ── Junction routing ──────────────────────────────────────────────────────
    junction_choice: JunctionChoice | None = None
    if obs.junction is not None:
        j = obs.junction
        if j.at_edge not in _junction_choices:
            # First time seeing this junction — pick randomly and remember
            pick = random.choice(j.choices)
            _junction_choices[j.at_edge] = pick
        committed = _junction_choices[j.at_edge]
        # Always echo back the choice so ats-env can record/update it
        junction_choice = JunctionChoice(at_edge=j.at_edge, choice=committed)

    # ── Adaptive lookahead ────────────────────────────────────────────────────
    lookahead_dist = max(MIN_LOOKAHEAD, obs.speed * 1.5)
    target = _find_lookahead(obs, corridor, lookahead_dist)

    # ── Pure pursuit steering ─────────────────────────────────────────────────
    dx = target.x - obs.x
    dy = target.y - obs.y
    desired_heading = math.atan2(dy, dx)
    heading_error = _normalize_angle(desired_heading - obs.heading)

    if obs.speed < 0.5:
        # At very low speed, steer directly toward the path heading
        desired_steer = _clamp(heading_error * 0.3, STEER_MIN, STEER_MAX)
    else:
        # Pure pursuit: κ = 2*sin(α) / L,  ω = κ*v  →  steer ≈ ω (for bicycle model)
        curvature = 2.0 * math.sin(heading_error) / lookahead_dist
        desired_steer = _clamp(curvature * obs.speed, STEER_MIN, STEER_MAX)

    # ── Speed control (proportional) ──────────────────────────────────────────
    target_speed = min(TARGET_SPEED, corridor[0].speed_limit)
    speed_error = target_speed - obs.speed
    desired_accel = _clamp(speed_error * SPEED_KP, ACCEL_MIN, ACCEL_MAX)

    return VehicleCommand(
        t=obs.t,
        id=obs.id,
        desired_accel=desired_accel,
        desired_steer=desired_steer,
        junction_choice=junction_choice,
    )
