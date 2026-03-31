import {
  connect,
  StringCodec,
  type NatsConnection,
} from "nats";
import type {
  VehicleObservation,
  VehicleCommand,
  WorldSnapshot,
  SimMapMessage,
  SimVehiclesMessage,
} from "./types.js";

const sc = StringCodec();

export async function connectNats(url: string): Promise<NatsConnection> {
  const nc = await connect({
    servers: url,
    maxReconnectAttempts: -1, // retry forever
    reconnectTimeWait: 1000,
  });
  console.log(`[nats] Connected to ${url}`);
  return nc;
}

export function publishObservation(nc: NatsConnection, obs: VehicleObservation): void {
  nc.publish(`sim.obs.${obs.id}`, sc.encode(JSON.stringify(obs)));
}

export function publishSnapshot(nc: NatsConnection, snapshot: WorldSnapshot): void {
  nc.publish("sim.snapshots", sc.encode(JSON.stringify(snapshot)));
}

export function publishVehicles(nc: NatsConnection, msg: SimVehiclesMessage): void {
  nc.publish("sim.vehicles", sc.encode(JSON.stringify(msg)));
  console.log(`[nats] Published sim.vehicles (${msg.vehicles.length} vehicles)`);
}

export function subscribeCommands(
  nc: NatsConnection,
  ids: string[],
  handler: (cmd: VehicleCommand) => void
): void {
  for (const id of ids) {
    const sub = nc.subscribe(`sim.cmd.${id}`);
    (async () => {
      for await (const msg of sub) {
        try {
          const cmd = JSON.parse(sc.decode(msg.data)) as VehicleCommand;
          handler(cmd);
        } catch (e) {
          console.warn(`[nats] Failed to parse command for ${id}:`, e);
        }
      }
    })();
  }
}

/**
 * Subscribe to sim.map — published by viz-gateway when the user uploads a map.
 * The message contains { mapRef: "/maps/filename.json" }.
 * Calls handler with the filesystem path each time a new map arrives.
 */
export function subscribeMap(
  nc: NatsConnection,
  handler: (mapRef: string) => void
): void {
  const sub = nc.subscribe("sim.map");
  (async () => {
    for await (const msg of sub) {
      try {
        const parsed = JSON.parse(sc.decode(msg.data)) as SimMapMessage;
        if (!parsed.mapRef) {
          console.warn("[nats] sim.map message missing mapRef field");
          continue;
        }
        handler(parsed.mapRef);
      } catch (e) {
        console.warn("[nats] Failed to parse sim.map message:", e);
      }
    }
  })();
  console.log("[nats] Subscribed to sim.map");
}
