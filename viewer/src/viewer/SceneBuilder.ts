import * as THREE from 'three';
import type { MapModel, Road, RoadLine, RoadArc, Junction } from '@shared/types';
import { JUNCTION_ARM_LENGTH } from '@shared/types';
import { sampleArc, roadHalfWidth, junctionConnectionPoints } from '@shared/geometry';

const ROAD_COLOR        = new THREE.Color('#2c2c4a');
const CENTERLINE_COLOR  = new THREE.Color('#c8a020');
const LANE_EDGE_COLOR   = new THREE.Color('#ffffff');
const LANE_CENTER_COLOR = new THREE.Color('#888888');
const JUNCTION_COLOR    = new THREE.Color('#c47a10');
const BORDER_COLOR      = new THREE.Color('#4a9eff');
const GRID_MINOR_COLOR  = new THREE.Color('#ffffff').multiplyScalar(0.06);
const GRID_MAJOR_COLOR  = new THREE.Color('#ffffff').multiplyScalar(0.15);

export class SceneBuilder {
  private group: THREE.Group | null = null;

  build(scene: THREE.Scene, model: MapModel): void {
    if (this.group) {
      scene.remove(this.group);
      disposeGroup(this.group);
    }
    const group = new THREE.Group();
    const { world } = model.meta;
    addGrid(group, world.width, world.height);
    addBorder(group, world.width, world.height);
    for (const road of model.roads)         addRoad(group, road);
    for (const junction of model.junctions) addJunction(group, junction);
    scene.add(group);
    this.group = group;
  }

  dispose(): void {
    if (this.group) disposeGroup(this.group);
  }
}

function addGrid(group: THREE.Group, width: number, height: number) {
  const addLines = (step: number, color: THREE.Color) => {
    const positions: number[] = [];
    for (let x = 0; x <= width; x += step)  { positions.push(x, 0, 0.001, x, height, 0.001); }
    for (let y = 0; y <= height; y += step)  { positions.push(0, y, 0.001, width, y, 0.001); }
    const geo = new THREE.BufferGeometry();
    geo.setAttribute('position', new THREE.Float32BufferAttribute(positions, 3));
    group.add(new THREE.LineSegments(geo, new THREE.LineBasicMaterial({ color, transparent: true, opacity: 1 })));
  };
  addLines(10, GRID_MINOR_COLOR);
  addLines(100, GRID_MAJOR_COLOR);
}

function addBorder(group: THREE.Group, width: number, height: number) {
  const pts = [
    new THREE.Vector3(0,     0,      0.01),
    new THREE.Vector3(width, 0,      0.01),
    new THREE.Vector3(width, height, 0.01),
    new THREE.Vector3(0,     height, 0.01),
    new THREE.Vector3(0,     0,      0.01),
  ];
  group.add(new THREE.Line(new THREE.BufferGeometry().setFromPoints(pts), new THREE.LineBasicMaterial({ color: BORDER_COLOR })));
}

function addRoad(group: THREE.Group, road: Road) {
  if (road.kind === 'line') addLineRoad(group, road);
  else                      addArcRoad(group, road);
}

function addLineRoad(group: THREE.Group, road: RoadLine) {
  const { start, end, lanes } = road;
  const dx = end.x - start.x, dy = end.y - start.y;
  const len = Math.sqrt(dx * dx + dy * dy);
  if (len < 0.01) return;
  const nx = -dy / len, ny = dx / len;
  const hw = roadHalfWidth(lanes);

  if (hw > 0) {
    const verts = [
      start.x + nx * hw, start.y + ny * hw, 0,
      end.x   + nx * hw, end.y   + ny * hw, 0,
      start.x - nx * hw, start.y - ny * hw, 0,
      end.x   - nx * hw, end.y   - ny * hw, 0,
    ];
    const geo = new THREE.BufferGeometry();
    geo.setAttribute('position', new THREE.Float32BufferAttribute(verts, 3));
    geo.setIndex([0, 1, 2, 1, 3, 2]);
    group.add(new THREE.Mesh(geo, new THREE.MeshBasicMaterial({ color: ROAD_COLOR, side: THREE.DoubleSide })));
  }

  const totalLeft  = lanes.left  * lanes.laneWidth;
  const totalRight = lanes.right * lanes.laneWidth;
  const centerOffset = (totalLeft - totalRight) / 2;
  const edgePositions: number[] = [];

  for (let i = 0; i <= lanes.left; i++) {
    const o = centerOffset + i * lanes.laneWidth;
    edgePositions.push(start.x + nx * o, start.y + ny * o, 0.02, end.x + nx * o, end.y + ny * o, 0.02);
  }
  for (let i = 1; i <= lanes.right; i++) {
    const o = centerOffset - i * lanes.laneWidth;
    edgePositions.push(start.x + nx * o, start.y + ny * o, 0.02, end.x + nx * o, end.y + ny * o, 0.02);
  }
  const edgeGeo = new THREE.BufferGeometry();
  edgeGeo.setAttribute('position', new THREE.Float32BufferAttribute(edgePositions, 3));
  group.add(new THREE.LineSegments(edgeGeo, new THREE.LineBasicMaterial({ color: LANE_EDGE_COLOR })));

  // Centerline
  const cx = nx * centerOffset, cy = ny * centerOffset;
  group.add(new THREE.Line(
    new THREE.BufferGeometry().setFromPoints([
      new THREE.Vector3(start.x + cx, start.y + cy, 0.03),
      new THREE.Vector3(end.x   + cx, end.y   + cy, 0.03),
    ]),
    new THREE.LineBasicMaterial({ color: CENTERLINE_COLOR }),
  ));

  // Per-lane center dashes
  const dashMat = new THREE.LineDashedMaterial({ color: LANE_CENTER_COLOR, dashSize: 0.6, gapSize: 2.4 });
  const addDash = (o: number) => {
    const geo = new THREE.BufferGeometry().setFromPoints([
      new THREE.Vector3(start.x + nx * o, start.y + ny * o, 0.025),
      new THREE.Vector3(end.x   + nx * o, end.y   + ny * o, 0.025),
    ]);
    const line = new THREE.Line(geo, dashMat);
    line.computeLineDistances();
    group.add(line);
  };
  for (let i = 0; i < lanes.left;  i++) addDash(centerOffset + (i + 0.5) * lanes.laneWidth);
  for (let i = 0; i < lanes.right; i++) addDash(centerOffset - (i + 0.5) * lanes.laneWidth);
}

function addArcRoad(group: THREE.Group, road: RoadArc) {
  const { center, radius, startAngle, endAngle, clockwise, lanes } = road;
  if (radius < 0.01) return;

  const hw = roadHalfWidth(lanes);
  const totalLeft   = lanes.left  * lanes.laneWidth;
  const totalRight  = lanes.right * lanes.laneWidth;
  const centerOffset = (totalLeft - totalRight) / 2;
  const centerPts = sampleArc(center, radius + centerOffset, startAngle, endAngle, clockwise, 0.5);

  if (hw > 0 && centerPts.length >= 2) {
    const outerPts = sampleArc(center, radius + centerOffset + hw,            startAngle, endAngle, clockwise, 0.5);
    const innerPts = sampleArc(center, Math.max(0, radius + centerOffset - hw), startAngle, endAngle, clockwise, 0.5);
    const n = Math.min(outerPts.length, innerPts.length);
    const verts: number[] = [], indices: number[] = [];
    for (let i = 0; i < n; i++) {
      verts.push(outerPts[i].x, outerPts[i].y, 0, innerPts[i].x, innerPts[i].y, 0);
      if (i < n - 1) {
        const b = i * 2;
        indices.push(b, b + 1, b + 2, b + 1, b + 3, b + 2);
      }
    }
    const geo = new THREE.BufferGeometry();
    geo.setAttribute('position', new THREE.Float32BufferAttribute(verts, 3));
    geo.setIndex(indices);
    group.add(new THREE.Mesh(geo, new THREE.MeshBasicMaterial({ color: ROAD_COLOR, side: THREE.DoubleSide })));
  }

  // Lane edge arcs
  const edgePositions: number[] = [];
  const addEdgeArc = (r: number) => {
    const pts = sampleArc(center, r, startAngle, endAngle, clockwise, 0.5);
    for (let j = 0; j < pts.length - 1; j++) {
      edgePositions.push(pts[j].x, pts[j].y, 0.02, pts[j+1].x, pts[j+1].y, 0.02);
    }
  };
  for (let i = 0; i <= lanes.left;  i++) addEdgeArc(radius + centerOffset + i * lanes.laneWidth);
  for (let i = 1; i <= lanes.right; i++) addEdgeArc(Math.max(0, radius + centerOffset - i * lanes.laneWidth));
  if (edgePositions.length > 0) {
    const edgeGeo = new THREE.BufferGeometry();
    edgeGeo.setAttribute('position', new THREE.Float32BufferAttribute(edgePositions, 3));
    group.add(new THREE.LineSegments(edgeGeo, new THREE.LineBasicMaterial({ color: LANE_EDGE_COLOR })));
  }

  // Centerline
  if (centerPts.length >= 2) {
    const clPos: number[] = [];
    for (let i = 0; i < centerPts.length - 1; i++) {
      clPos.push(centerPts[i].x, centerPts[i].y, 0.03, centerPts[i+1].x, centerPts[i+1].y, 0.03);
    }
    const clGeo = new THREE.BufferGeometry();
    clGeo.setAttribute('position', new THREE.Float32BufferAttribute(clPos, 3));
    group.add(new THREE.LineSegments(clGeo, new THREE.LineBasicMaterial({ color: CENTERLINE_COLOR })));
  }

  // Per-lane dashes
  const dashMat = new THREE.LineDashedMaterial({ color: LANE_CENTER_COLOR, dashSize: 0.6, gapSize: 2.4 });
  const addArcDash = (r: number) => {
    if (r <= 0) return;
    const pts = sampleArc(center, r, startAngle, endAngle, clockwise, 0.5);
    if (pts.length < 2) return;
    const geo = new THREE.BufferGeometry().setFromPoints(pts.map(p => new THREE.Vector3(p.x, p.y, 0.025)));
    const line = new THREE.Line(geo, dashMat);
    line.computeLineDistances();
    group.add(line);
  };
  for (let i = 0; i < lanes.left;  i++) addArcDash(radius + centerOffset + (i + 0.5) * lanes.laneWidth);
  for (let i = 0; i < lanes.right; i++) addArcDash(radius + centerOffset - (i + 0.5) * lanes.laneWidth);
}

function addJunction(group: THREE.Group, junction: Junction) {
  const { x, y, laneWidth } = junction;
  const hw = laneWidth, armL = JUNCTION_ARM_LENGTH;
  const roadMat = new THREE.MeshBasicMaterial({ color: ROAD_COLOR, side: THREE.DoubleSide });

  const padGeo = new THREE.PlaneGeometry(hw * 2, hw * 2);
  const pad = new THREE.Mesh(padGeo, roadMat);
  pad.position.set(x, y, 0);
  group.add(pad);

  for (const tip of junctionConnectionPoints(junction)) {
    const dirX = tip.x - x, dirY = tip.y - y;
    const len = Math.sqrt(dirX * dirX + dirY * dirY);
    if (len < 0.01) continue;
    const ux = dirX / len, uy = dirY / len;
    const px = -uy, py = ux;
    const s = hw, e = armL;
    const verts = [
      x + ux * s + px * hw, y + uy * s + py * hw, 0,
      x + ux * e + px * hw, y + uy * e + py * hw, 0,
      x + ux * s - px * hw, y + uy * s - py * hw, 0,
      x + ux * e - px * hw, y + uy * e - py * hw, 0,
    ];
    const geo = new THREE.BufferGeometry();
    geo.setAttribute('position', new THREE.Float32BufferAttribute(verts, 3));
    geo.setIndex([0, 1, 2, 1, 3, 2]);
    group.add(new THREE.Mesh(geo, roadMat));
  }

  const dot = new THREE.Mesh(new THREE.CircleGeometry(1.2, 12), new THREE.MeshBasicMaterial({ color: JUNCTION_COLOR }));
  dot.position.set(x, y, 0.01);
  group.add(dot);
}

function disposeGroup(group: THREE.Group) {
  group.traverse((obj) => {
    if (obj instanceof THREE.Mesh || obj instanceof THREE.Line || obj instanceof THREE.LineSegments) {
      obj.geometry.dispose();
      if (Array.isArray(obj.material)) obj.material.forEach((m) => m.dispose());
      else obj.material.dispose();
    }
  });
}
