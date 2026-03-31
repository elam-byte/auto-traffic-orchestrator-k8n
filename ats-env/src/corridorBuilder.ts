import { findLaneAtPoint, walkForward } from "./roadGraph.js";
import type { RoadGraph, VehicleState, LanePoint, JunctionInfo } from "./types.js";

const CORRIDOR_LENGTH = 100; // meters

export function buildCorridor(
  graph: RoadGraph,
  vehicle: VehicleState,
  routeChoices?: Map<string, string>
): { corridor: LanePoint[]; junction?: JunctionInfo } {
  const cursor = findLaneAtPoint(graph, vehicle.x, vehicle.y, vehicle.heading);
  if (!cursor) {
    console.warn(
      `[corridor] Could not locate vehicle ${vehicle.id} on any lane — returning empty corridor`
    );
    return { corridor: [] };
  }
  const { points, junction } = walkForward(graph, cursor, CORRIDOR_LENGTH, routeChoices);
  return { corridor: points, junction };
}
