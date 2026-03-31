"use strict";
/**
 * Vehicle Orchestrator
 *
 * Subscribes to `sim.vehicles` on NATS. When ats-env loads a new map it publishes
 * the desired vehicle list. This service diffs it against currently running
 * containers/pods and creates or removes them so exactly one container exists per
 * vehicle defined in the map.
 *
 * Modes (set ORCHESTRATION_MODE env var):
 *   docker      — uses Docker API via dockerode (default, for local compose)
 *   kubernetes  — uses Kubernetes API via @kubernetes/client-node (for production)
 */

const { connect, StringCodec } = require("nats");

const NATS_URL          = process.env.NATS_URL          ?? "nats://localhost:4222";
const ORCHESTRATION_MODE = process.env.ORCHESTRATION_MODE ?? "docker";
const VEHICLE_IMAGE     = process.env.VEHICLE_IMAGE     ?? "vehicle-agent:latest";
const VEHICLE_NAMESPACE = process.env.VEHICLE_NAMESPACE ?? "ats";
const VEHICLE_NETWORK   = process.env.VEHICLE_NETWORK   ?? "ats_default";

const sc = StringCodec();

// ── Docker orchestrator ───────────────────────────────────────────────────────

async function createDockerOrchestrator() {
  const Docker = require("dockerode");
  const docker = new Docker({ socketPath: "/var/run/docker.sock" });

  // Track containers this orchestrator manages
  const managed = new Set(); // container names currently running

  async function listManagedContainers() {
    const containers = await docker.listContainers({ all: true });
    const ours = containers.filter((c) =>
      c.Names.some((n) => n.startsWith("/vehicle-"))
    );
    return ours;
  }

  async function applyVehicles(desiredVehicles) {
    const desiredIds = new Set(desiredVehicles.map((v) => v.id));
    const existing = await listManagedContainers();

    // Remove containers for vehicles no longer in the map
    for (const c of existing) {
      const name = c.Names[0].replace(/^\//, "");
      const vehicleId = name.replace(/^vehicle-/, "");
      if (!desiredIds.has(vehicleId)) {
        console.log(`[orchestrator] Removing container ${name}`);
        const container = docker.getContainer(c.Id);
        try {
          if (c.State === "running") await container.stop({ t: 5 });
          await container.remove();
          managed.delete(name);
        } catch (e) {
          console.warn(`[orchestrator] Failed to remove ${name}:`, e.message);
        }
      }
    }

    // Create containers for new vehicles
    const existingNames = new Set(existing.map((c) => c.Names[0].replace(/^\//, "")));
    for (const v of desiredVehicles) {
      const name = `vehicle-${v.id}`;
      if (existingNames.has(name)) {
        // Ensure it's running
        const c = existing.find((x) => x.Names[0] === `/${name}`);
        if (c && c.State !== "running") {
          console.log(`[orchestrator] Starting existing container ${name}`);
          await docker.getContainer(c.Id).start().catch((e) =>
            console.warn(`[orchestrator] Start failed for ${name}:`, e.message)
          );
        }
        continue;
      }

      console.log(`[orchestrator] Creating container ${name}`);
      try {
        const container = await docker.createContainer({
          name,
          Image: VEHICLE_IMAGE,
          Env: [
            `VEHICLE_ID=${v.id}`,
            `NATS_URL=${NATS_URL}`,
            `TARGET_SPEED=8.0`,
          ],
          HostConfig: {
            NetworkMode: VEHICLE_NETWORK,
            RestartPolicy: { Name: "unless-stopped" },
          },
        });
        await container.start();
        managed.add(name);
        console.log(`[orchestrator] Container ${name} started`);
      } catch (e) {
        console.error(`[orchestrator] Failed to create ${name}:`, e.message);
      }
    }
  }

  return { applyVehicles };
}

// ── Kubernetes orchestrator ───────────────────────────────────────────────────

async function createK8sOrchestrator() {
  const k8s = require("@kubernetes/client-node");
  const kc = new k8s.KubeConfig();
  kc.loadFromDefault(); // reads in-cluster SA token or ~/.kube/config

  const appsApi = kc.makeApiClient(k8s.AppsV1Api);
  const LABEL_SELECTOR = "app=vehicle-agent,managed-by=vehicle-orchestrator";

  function vehicleDeployment(vehicleId) {
    return {
      apiVersion: "apps/v1",
      kind: "Deployment",
      metadata: {
        name: `vehicle-${vehicleId}`,
        namespace: VEHICLE_NAMESPACE,
        labels: {
          app: "vehicle-agent",
          "vehicle-id": vehicleId,
          "managed-by": "vehicle-orchestrator",
        },
      },
      spec: {
        replicas: 1,
        selector: {
          matchLabels: { app: "vehicle-agent", "vehicle-id": vehicleId },
        },
        template: {
          metadata: {
            labels: { app: "vehicle-agent", "vehicle-id": vehicleId },
          },
          spec: {
            containers: [
              {
                name: "vehicle-agent",
                image: VEHICLE_IMAGE,
                env: [
                  { name: "VEHICLE_ID", value: vehicleId },
                  { name: "NATS_URL", value: NATS_URL },
                  { name: "TARGET_SPEED", value: "8.0" },
                ],
                resources: {
                  requests: { cpu: "50m", memory: "64Mi" },
                  limits:   { cpu: "200m", memory: "128Mi" },
                },
              },
            ],
            restartPolicy: "Always",
          },
        },
      },
    };
  }

  async function applyVehicles(desiredVehicles) {
    const desiredIds = new Set(desiredVehicles.map((v) => v.id));

    // List existing managed deployments
    const { body } = await appsApi.listNamespacedDeployment(
      VEHICLE_NAMESPACE, undefined, undefined, undefined, undefined,
      "managed-by=vehicle-orchestrator"
    );
    const existing = body.items;
    const existingIds = new Set(
      existing.map((d) => d.metadata.labels["vehicle-id"])
    );

    // Delete deployments for removed vehicles
    for (const d of existing) {
      const vid = d.metadata.labels["vehicle-id"];
      if (!desiredIds.has(vid)) {
        console.log(`[orchestrator] Deleting Deployment vehicle-${vid}`);
        await appsApi.deleteNamespacedDeployment(
          `vehicle-${vid}`, VEHICLE_NAMESPACE
        ).catch((e) => console.warn(`[orchestrator] Delete failed:`, e.message));
      }
    }

    // Create deployments for new vehicles
    for (const v of desiredVehicles) {
      if (existingIds.has(v.id)) continue;
      console.log(`[orchestrator] Creating Deployment vehicle-${v.id}`);
      await appsApi.createNamespacedDeployment(
        VEHICLE_NAMESPACE, vehicleDeployment(v.id)
      ).catch((e) =>
        console.error(`[orchestrator] Create failed for vehicle-${v.id}:`, e.message)
      );
    }
  }

  return { applyVehicles };
}

// ── Main ──────────────────────────────────────────────────────────────────────

async function main() {
  console.log(`[orchestrator] Mode: ${ORCHESTRATION_MODE}, image: ${VEHICLE_IMAGE}`);

  let orchestrator;
  if (ORCHESTRATION_MODE === "kubernetes") {
    orchestrator = await createK8sOrchestrator();
    console.log(`[orchestrator] K8s namespace: ${VEHICLE_NAMESPACE}`);
  } else {
    orchestrator = await createDockerOrchestrator();
    console.log(`[orchestrator] Docker network: ${VEHICLE_NETWORK}`);
  }

  const nc = await connect({
    servers: NATS_URL,
    maxReconnectAttempts: -1,
    reconnectTimeWait: 1000,
  });
  console.log(`[orchestrator] NATS connected to ${NATS_URL}`);

  const sub = nc.subscribe("sim.vehicles");
  console.log("[orchestrator] Subscribed to sim.vehicles");

  (async () => {
    for await (const msg of sub) {
      try {
        const { vehicles } = JSON.parse(sc.decode(msg.data));
        console.log(`[orchestrator] sim.vehicles received: ${vehicles.length} vehicles`);
        await orchestrator.applyVehicles(vehicles);
      } catch (e) {
        console.error("[orchestrator] Error handling sim.vehicles:", e.message);
      }
    }
  })();

  async function shutdown() {
    console.log("[orchestrator] Shutting down...");
    await nc.drain();
    process.exit(0);
  }
  process.on("SIGTERM", shutdown);
  process.on("SIGINT", shutdown);
}

main().catch((err) => {
  console.error("[orchestrator] Fatal:", err);
  process.exit(1);
});
