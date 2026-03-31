from pydantic import BaseModel


class LanePoint(BaseModel):
    x: float
    y: float
    heading: float
    width: float
    speed_limit: float
    lane_id: str  # e.g. "r-1:right"
    road_id: str  # e.g. "r-1"


class JunctionInfo(BaseModel):
    at_edge: str         # edge ID where the fork occurs
    choices: list[str]   # available next edge IDs
    current_choice: str  # currently committed next edge ID


class VehicleObservation(BaseModel):
    t: int
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
