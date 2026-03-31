import { readFile } from "fs/promises";
import { parseMap } from "./mapLoader.js";
import { buildGraph } from "./roadGraph.js";
import { VehicleStore } from "./vehicleState.js";
import {
  connectNats,
  subscribeCommands,
  subscribeMap,
  publishVehicles,
} from "./natsClient.js";
import { startTickLoop, recordCommand, stopTickLoop } from "./tickLoop.js";

const NATS_URL = process.env.NATS_URL ?? "nats://localhost:4222";

async function main() {
  console.log("[ats-env] Connecting to NATS:", NATS_URL);
  const nc = await connectNats(NATS_URL);

  console.log("[ats-env] Waiting for map upload (sim.map)...");
  console.log("[ats-env] Upload a map via the viewer or:  python scripts/upload-map.py");

  // Subscribe to map uploads — re-initialise simulation on each new map
  subscribeMap(nc, async (mapRef) => {
    try {
      console.log(`[ats-env] Map reference received: ${mapRef} — loading from disk...`);
      const raw = await readFile(mapRef, "utf-8");
      const map = parseMap(JSON.parse(raw));
      console.log(
        `[ats-env] Map loaded: ${map.roads.length} roads, ${map.vehicles.length} vehicles`
      );

      console.log("[ats-env] Building road graph...");
      const graph = buildGraph(map);
      console.log(`[ats-env] Road graph: ${graph.edges.size} lane edges`);

      // Stop any existing tick loop before re-initialising
      stopTickLoop();

      const store = new VehicleStore(map);

      // Publish vehicle list so the orchestrator can spin up containers
      publishVehicles(nc, { vehicles: store.getVehicleMeta() });

      // Subscribe to vehicle commands (re-subscribe for new vehicle IDs)
      subscribeCommands(nc, store.ids(), recordCommand);

      // Start the 20 Hz simulation loop
      startTickLoop(nc, graph, store);
      console.log("[ats-env] Simulation started.");
    } catch (err) {
      console.error("[ats-env] Failed to initialise from map:", err);
    }
  });

  // Graceful shutdown
  process.on("SIGTERM", async () => {
    console.log("[ats-env] Shutting down...");
    stopTickLoop();
    await nc.drain();
    process.exit(0);
  });
  process.on("SIGINT", async () => {
    stopTickLoop();
    await nc.drain();
    process.exit(0);
  });
}

main().catch((err) => {
  console.error("[ats-env] Fatal:", err);
  process.exit(1);
});
