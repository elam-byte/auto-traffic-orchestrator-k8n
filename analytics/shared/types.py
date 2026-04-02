"""Shared Pydantic models for ATS analytics — mirrors vehicle_agent/types.py."""
from __future__ import annotations

from pydantic import BaseModel


class LanePoint(BaseModel):
    x: float
    y: float
    heading: float
    width: float
    speed_limit: float
    lane_id: str
    road_id: str


class JunctionInfo(BaseModel):
    at_edge: str
    choices: list[str]
    current_choice: str


class VehicleObservation(BaseModel):
    t: int                            # Unix ms
    t_iso: str | None = None          # ISO-8601 (may be absent in older captures)
    id: str
    x: float
    y: float
    heading: float
    speed: float
    lane_corridor: list[LanePoint]
    junction: JunctionInfo | None = None


class JunctionChoice(BaseModel):
    at_edge: str
    choice: str


class VehicleCommand(BaseModel):
    t: int
    id: str
    desired_accel: float
    desired_steer: float
    junction_choice: JunctionChoice | None = None


class SnapshotVehicle(BaseModel):
    id: str
    x: float
    y: float
    heading: float
    length: float
    width: float
    color: str | None = None


class WorldSnapshot(BaseModel):
    t: int
    t_iso: str | None = None
    vehicles: list[SnapshotVehicle]
