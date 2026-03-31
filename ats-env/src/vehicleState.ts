import type { AtsMap, VehicleState, SimVehicleEntry } from "./types.js";

export class VehicleStore {
  private states: Map<string, VehicleState>;
  // Per-vehicle junction routing: vehicleId → (edgeId → chosen nextEdgeId)
  private routeChoices: Map<string, Map<string, string>>;

  constructor(map: AtsMap) {
    this.states = new Map();
    this.routeChoices = new Map();
    for (const v of map.vehicles) {
      this.states.set(v.id, { ...v, speed: 0 });
      this.routeChoices.set(v.id, new Map());
    }
  }

  get(id: string): VehicleState | undefined {
    return this.states.get(id);
  }

  getAll(): VehicleState[] {
    return [...this.states.values()];
  }

  apply(id: string, patch: Partial<VehicleState>): void {
    const current = this.states.get(id);
    if (!current) return;
    this.states.set(id, { ...current, ...patch });
  }

  ids(): string[] {
    return [...this.states.keys()];
  }

  getRouteChoices(vehicleId: string): Map<string, string> {
    return this.routeChoices.get(vehicleId) ?? new Map();
  }

  setRouteChoice(vehicleId: string, atEdge: string, choice: string): void {
    let choices = this.routeChoices.get(vehicleId);
    if (!choices) {
      choices = new Map();
      this.routeChoices.set(vehicleId, choices);
    }
    choices.set(atEdge, choice);
  }

  getVehicleMeta(): SimVehicleEntry[] {
    return [...this.states.values()].map((v) => ({
      id: v.id,
      color: v.color,
      length: v.length,
      width: v.width,
    }));
  }
}
