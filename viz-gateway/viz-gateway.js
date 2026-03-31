"use strict";
// Viz-Gateway: NATS sim.snapshots → WebSocket broadcast (latest-only, 20 Hz)
// Also serves the built viewer app from dist/ at http://localhost:8090/

const http = require("http");
const fs   = require("fs");
const path = require("path");
const MAPS_DIR = process.env.MAPS_DIR ?? "/maps";
const { connect, StringCodec } = require("nats");
const { WebSocketServer } = require("ws");

const NATS_URL    = process.env.NATS_URL ?? "nats://localhost:4222";
const WS_PORT     = parseInt(process.env.WS_PORT ?? "8090", 10);
const BROADCAST_HZ = 20;
const DIST_DIR    = path.join(__dirname, "dist");

const MIME = {
  ".html": "text/html",
  ".js":   "application/javascript",
  ".css":  "text/css",
  ".svg":  "image/svg+xml",
  ".ico":  "image/x-icon",
  ".png":  "image/png",
  ".woff2": "font/woff2",
};

const sc = StringCodec();
let latestSnapshot = null;

async function main() {
  // ── NATS ──────────────────────────────────────────────────────────────────
  const nc = await connect({
    servers: NATS_URL,
    maxReconnectAttempts: -1,
    reconnectTimeWait: 1000,
  });
  console.log(`[viz-gateway] NATS connected to ${NATS_URL}`);

  const sub = nc.subscribe("sim.snapshots");
  (async () => {
    for await (const msg of sub) {
      try { latestSnapshot = sc.decode(msg.data); } catch (_) {}
    }
  })();

  // ── HTTP server (serves viewer SPA from dist/) ────────────────────────────
  const server = http.createServer((req, res) => {
    // CORS headers — allow the viewer to POST from any origin during dev
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "Content-Type");
    if (req.method === "OPTIONS") { res.writeHead(204); res.end(); return; }

    // Strip query string
    const urlPath = (req.url ?? "/").split("?")[0];

    // ── POST /map — receive map JSON from viewer, save to /maps/, publish path ──
    if (req.method === "POST" && urlPath === "/map") {
      let body = "";
      req.on("data", (chunk) => { body += chunk; });
      req.on("end", () => {
        try {
          JSON.parse(body); // validate JSON before writing

          // Derive a safe filename from X-Map-Filename header or timestamp
          const rawName = req.headers["x-map-filename"] || "";
          const safeName = path.basename(rawName).replace(/[^a-zA-Z0-9._-]/g, "_") ||
            `map-${Date.now()}.json`;
          const mapPath = path.join(MAPS_DIR, safeName);

          // Save to shared /maps/ volume so ats-env can load it from disk
          fs.mkdirSync(MAPS_DIR, { recursive: true });
          fs.writeFileSync(mapPath, body, "utf-8");
          console.log(`[viz-gateway] Map saved to ${mapPath} (${body.length} bytes)`);

          // Publish only the path reference — not the full JSON payload
          nc.publish("sim.map", sc.encode(JSON.stringify({ mapRef: mapPath })));
          console.log(`[viz-gateway] Published { mapRef: "${mapPath}" } → sim.map`);

          res.writeHead(200, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ ok: true, mapRef: mapPath }));
        } catch (e) {
          console.warn("[viz-gateway] POST /map error:", e.message);
          res.writeHead(400, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ ok: false, error: e.message }));
        }
      });
      return;
    }

    // Resolve file path, default to index.html for "/"
    let filePath = path.join(DIST_DIR, urlPath === "/" ? "index.html" : urlPath);

    // Security: prevent path traversal
    if (!filePath.startsWith(DIST_DIR)) {
      res.writeHead(403); res.end(); return;
    }

    fs.readFile(filePath, (err, data) => {
      if (err) {
        // SPA fallback: serve index.html for unknown routes
        fs.readFile(path.join(DIST_DIR, "index.html"), (err2, html) => {
          if (err2) { res.writeHead(404); res.end("Not found"); return; }
          res.writeHead(200, { "Content-Type": "text/html" });
          res.end(html);
        });
        return;
      }
      const ext = path.extname(filePath);
      const contentType = MIME[ext] ?? "application/octet-stream";
      res.writeHead(200, { "Content-Type": contentType });
      res.end(data);
    });
  });

  // ── WebSocket server (attached to HTTP server, any path) ──────────────────
  const wss = new WebSocketServer({ server });

  wss.on("connection", (ws) => {
    console.log(`[viz-gateway] Client connected (total: ${wss.clients.size})`);
    ws.on("close", () => {
      console.log(`[viz-gateway] Client disconnected (total: ${wss.clients.size})`);
    });
    ws.on("error", () => {});
  });

  server.listen(WS_PORT, () => {
    console.log(`[viz-gateway] Listening on http://localhost:${WS_PORT}`);
  });

  // ── Broadcast loop ────────────────────────────────────────────────────────
  setInterval(() => {
    if (!latestSnapshot) return;
    const payload = latestSnapshot;
    for (const ws of wss.clients) {
      if (ws.readyState !== 1 /* OPEN */) continue;
      if (ws.bufferedAmount > 0) continue; // skip slow clients
      try { ws.send(payload); } catch (_) {}
    }
  }, 1000 / BROADCAST_HZ);

  // ── Graceful shutdown ─────────────────────────────────────────────────────
  async function shutdown() {
    console.log("[viz-gateway] Shutting down...");
    server.close();
    await nc.drain();
    process.exit(0);
  }
  process.on("SIGTERM", shutdown);
  process.on("SIGINT", shutdown);
}

main().catch((err) => {
  console.error("[viz-gateway] Fatal:", err);
  process.exit(1);
});
