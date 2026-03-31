import * as THREE from 'three';
import { OrbitControls } from 'three/addons/controls/OrbitControls.js';
import { safeParseMapModel } from '@shared/validation';
import type { MapModel } from '@shared/types';
import { SceneBuilder } from './SceneBuilder';
import { VehicleInstancer } from './VehicleInstancer';
import { WSClient } from './WSClient';

// ── Renderer ──────────────────────────────────────────────────────────────────

const app = document.getElementById('app')!;
const statusEl = document.getElementById('status')!;

const renderer = new THREE.WebGLRenderer({ antialias: true });
renderer.setPixelRatio(window.devicePixelRatio);
renderer.setSize(window.innerWidth, window.innerHeight);
app.appendChild(renderer.domElement);
renderer.setClearColor(0x0d0d1a);

const scene = new THREE.Scene();

// ── Lighting ──────────────────────────────────────────────────────────────────

scene.add(new THREE.AmbientLight(0xffffff, 0.45));
const keyLight = new THREE.DirectionalLight(0xffffff, 1.0);
keyLight.position.set(1, -0.5, 2);
scene.add(keyLight);
const fillLight = new THREE.DirectionalLight(0x8090ff, 0.3);
fillLight.position.set(-1, 1, 0.5);
scene.add(fillLight);

// ── Camera ────────────────────────────────────────────────────────────────────

const camera = new THREE.PerspectiveCamera(55, window.innerWidth / window.innerHeight, 1, 10000);
camera.up.set(0, 0, 1);

function fitCamera(worldW: number, worldH: number) {
  const cx = worldW / 2, cy = worldH / 2;
  const d  = Math.max(worldW, worldH);
  camera.aspect = window.innerWidth / window.innerHeight;
  camera.far    = d * 8;
  camera.near   = 1;
  camera.position.set(cx, cy - d * 0.5, d * 0.65);
  camera.lookAt(cx, cy, 0);
  camera.updateProjectionMatrix();
  controls.target.set(cx, cy, 0);
  controls.update();
}

const controls = new OrbitControls(camera, renderer.domElement);
controls.enableRotate = true;
controls.enableZoom   = true;
controls.enablePan    = true;
controls.zoomSpeed    = 1.2;

fitCamera(500, 281);

// ── Scene objects ─────────────────────────────────────────────────────────────

const sceneBuilder     = new SceneBuilder();
const vehicleInstancer = new VehicleInstancer(scene);
let currentModel: MapModel | null = null;

function setStatus(msg: string) { statusEl.textContent = msg; }

function loadMap(model: MapModel) {
  currentModel = model;
  const { width, height } = model.meta.world;
  sceneBuilder.build(scene, model);
  vehicleInstancer.reset();
  if (model.vehicles.length > 0) {
    vehicleInstancer.updateFromSnapshot({ t: 0, vehicles: model.vehicles });
  }
  fitCamera(width, height);
  setStatus(
    `Map: ${width}×${height} m | Roads: ${model.roads.length} ` +
    `| Junctions: ${model.junctions.length} | Vehicles: ${model.vehicles.length}`,
  );
}

// ── UI wiring ─────────────────────────────────────────────────────────────────

const mapFileInput = document.getElementById('map-file-input') as HTMLInputElement;
const loadMapBtn   = document.getElementById('load-map-btn')   as HTMLButtonElement;
const wsUrlInput   = document.getElementById('ws-url-input')   as HTMLInputElement;
const connectWsBtn = document.getElementById('connect-ws-btn') as HTMLButtonElement;

loadMapBtn.addEventListener('click', () => mapFileInput.click());

mapFileInput.addEventListener('change', (e) => {
  const file = (e.target as HTMLInputElement).files?.[0];
  if (!file) return;
  const reader = new FileReader();
  reader.onload = async (ev) => {
    const text = ev.target?.result;
    if (typeof text !== 'string') return;
    try {
      const result = safeParseMapModel(JSON.parse(text));
      if (!result.ok) { setStatus(`Invalid map: ${result.error}`); return; }

      // Render the map locally
      loadMap(result.model);

      // Send map to ats-env via viz-gateway so the simulation can start
      setStatus('Uploading map to simulator…');
      try {
        const res = await fetch('/map', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: text,
        });
        if (res.ok) {
          setStatus(
            `Map sent to simulator | Roads: ${result.model.roads.length}` +
            ` | Vehicles: ${result.model.vehicles.length}`,
          );
        } else {
          const err = await res.json().catch(() => ({ error: res.statusText }));
          setStatus(`Map rendered locally but upload failed: ${err.error}`);
        }
      } catch {
        setStatus('Map rendered locally — could not reach viz-gateway to upload to simulator.');
      }
    } catch {
      setStatus('Failed to parse JSON.');
    }
  };
  reader.readAsText(file);
  (e.target as HTMLInputElement).value = '';
});

const wsClient = new WSClient(setStatus);

connectWsBtn.addEventListener('click', () => {
  wsClient.connect(wsUrlInput.value.trim() || 'ws://localhost:8090');
});

// ── Resize ────────────────────────────────────────────────────────────────────

window.addEventListener('resize', () => {
  renderer.setSize(window.innerWidth, window.innerHeight);
  const w = currentModel?.meta.world.width  ?? 500;
  const h = currentModel?.meta.world.height ?? 281;
  fitCamera(w, h);
});

// ── Render loop ───────────────────────────────────────────────────────────────

function animate() {
  requestAnimationFrame(animate);
  const snapshot = wsClient.getLatestAndClear();
  if (snapshot) vehicleInstancer.updateFromSnapshot(snapshot);
  controls.update();
  renderer.render(scene, camera);
}

animate();
setStatus('Ready — load Map JSON then click Connect WebSocket.');
