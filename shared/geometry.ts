import type { Point, RoadLanes, Junction, MapModel } from './types';
import { JUNCTION_ARM_LENGTH } from './types';

export function clamp(v: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, v));
}

export function snapToGrid(pt: Point, gridSize: number): Point {
  return {
    x: Math.round(pt.x / gridSize) * gridSize,
    y: Math.round(pt.y / gridSize) * gridSize,
  };
}

export function distance(a: Point, b: Point): number {
  const dx = b.x - a.x;
  const dy = b.y - a.y;
  return Math.sqrt(dx * dx + dy * dy);
}

export function lerp(a: number, b: number, t: number): number {
  return a + (b - a) * t;
}

export function roadHalfWidth(lanes: RoadLanes): number {
  return ((lanes.left + lanes.right) * lanes.laneWidth) / 2;
}

export function squaredDistanceToSegment(p: Point, a: Point, b: Point): number {
  const dx = b.x - a.x;
  const dy = b.y - a.y;
  const lenSq = dx * dx + dy * dy;
  if (lenSq === 0) {
    const ex = p.x - a.x;
    const ey = p.y - a.y;
    return ex * ex + ey * ey;
  }
  const t = clamp(((p.x - a.x) * dx + (p.y - a.y) * dy) / lenSq, 0, 1);
  const cx = a.x + t * dx - p.x;
  const cy = a.y + t * dy - p.y;
  return cx * cx + cy * cy;
}

export function distanceToLineSegment(p: Point, a: Point, b: Point): number {
  return Math.sqrt(squaredDistanceToSegment(p, a, b));
}

export function normaliseAngle(a: number): number {
  a = a % (2 * Math.PI);
  if (a < 0) a += 2 * Math.PI;
  return a;
}

export function angleInArc(
  theta: number,
  startAngle: number,
  endAngle: number,
  clockwise: boolean,
): boolean {
  const s = normaliseAngle(startAngle);
  const e = normaliseAngle(endAngle);
  const t = normaliseAngle(theta);

  if (!clockwise) {
    if (s <= e) return t >= s && t <= e;
    return t >= s || t <= e;
  } else {
    if (s >= e) return t <= s && t >= e;
    return t <= s || t >= e;
  }
}

export function distanceToArc(
  p: Point,
  center: Point,
  radius: number,
  startAngle: number,
  endAngle: number,
  clockwise: boolean,
): number {
  const theta = Math.atan2(p.y - center.y, p.x - center.x);
  if (angleInArc(theta, startAngle, endAngle, clockwise)) {
    return Math.abs(distance(p, center) - radius);
  }
  const pStart: Point = {
    x: center.x + radius * Math.cos(startAngle),
    y: center.y + radius * Math.sin(startAngle),
  };
  const pEnd: Point = {
    x: center.x + radius * Math.cos(endAngle),
    y: center.y + radius * Math.sin(endAngle),
  };
  return Math.min(distance(p, pStart), distance(p, pEnd));
}

export function arcSweep(
  startAngle: number,
  endAngle: number,
  clockwise: boolean,
): number {
  const s = normaliseAngle(startAngle);
  const e = normaliseAngle(endAngle);
  if (!clockwise) {
    return s <= e ? e - s : 2 * Math.PI - s + e;
  } else {
    return s >= e ? s - e : 2 * Math.PI - e + s;
  }
}

export function arcFromThreePoints(
  center: Point,
  startPt: Point,
  endPt: Point,
  _clockwise: boolean,
): { radius: number; startAngle: number; endAngle: number } {
  const radius = distance(center, startPt);
  const startAngle = Math.atan2(startPt.y - center.y, startPt.x - center.x);
  const endAngle = Math.atan2(endPt.y - center.y, endPt.x - center.x);
  return { radius, startAngle, endAngle };
}

export function sampleArc(
  center: Point,
  radius: number,
  startAngle: number,
  endAngle: number,
  clockwise: boolean,
  step = 0.5,
): Point[] {
  const sweep = arcSweep(startAngle, endAngle, clockwise);
  const numSteps = Math.max(2, Math.ceil((sweep * radius) / step));
  const points: Point[] = [];
  const dir = clockwise ? -1 : 1;
  for (let i = 0; i <= numSteps; i++) {
    const angle = startAngle + dir * (sweep * i) / numSteps;
    points.push({
      x: center.x + radius * Math.cos(angle),
      y: center.y + radius * Math.sin(angle),
    });
  }
  return points;
}

export function closedSideIndex(rotation: number): number {
  const idx = Math.round((rotation / (Math.PI / 2)) % 4);
  return ((idx % 4) + 4) % 4;
}

export function junctionConnectionPoints(j: Junction): Point[] {
  const L = JUNCTION_ARM_LENGTH;
  const allTips: Point[] = [
    { x: j.x,     y: j.y - L },
    { x: j.x + L, y: j.y     },
    { x: j.x,     y: j.y + L },
    { x: j.x - L, y: j.y     },
  ];
  if (j.junctionType === '4-way') return allTips;
  const closedIdx = closedSideIndex(j.rotation);
  return allTips.filter((_, i) => i !== closedIdx);
}

export function snapToJunction(
  pt: Point,
  junctions: Junction[],
  threshold: number,
): Point | null {
  let best: Point | null = null;
  let bestDist = threshold;
  for (const j of junctions) {
    for (const cp of junctionConnectionPoints(j)) {
      const d = distance(pt, cp);
      if (d < bestDist) { bestDist = d; best = cp; }
    }
  }
  return best;
}

export function pointInJunction(pt: Point, j: Junction): boolean {
  const hw = j.laneWidth;
  const L = JUNCTION_ARM_LENGTH;
  const dx = pt.x - j.x;
  const dy = pt.y - j.y;
  if (Math.abs(dx) <= hw && Math.abs(dy) <= hw) return true;
  const ci = closedSideIndex(j.rotation);
  if (ci !== 0 && Math.abs(dx) <= hw && dy >= -L && dy <= 0) return true;
  if (ci !== 1 && Math.abs(dy) <= hw && dx >= 0 && dx <= L) return true;
  if (ci !== 2 && Math.abs(dx) <= hw && dy >= 0 && dy <= L) return true;
  if (ci !== 3 && Math.abs(dy) <= hw && dx >= -L && dx <= 0) return true;
  return false;
}

export function getFeatureConnectionPoints(model: MapModel): Point[] {
  const points: Point[] = [];
  for (const j of model.junctions) points.push(...junctionConnectionPoints(j));
  for (const r of model.roads) {
    if (r.kind === 'line') {
      points.push(r.start, r.end);
    } else {
      points.push(
        { x: r.center.x + r.radius * Math.cos(r.startAngle), y: r.center.y + r.radius * Math.sin(r.startAngle) },
        { x: r.center.x + r.radius * Math.cos(r.endAngle),   y: r.center.y + r.radius * Math.sin(r.endAngle)   },
      );
    }
  }
  return points;
}

export function snapToFeatures(pt: Point, model: MapModel, threshold: number): Point | null {
  let best: Point | null = null;
  let bestDist = threshold;
  for (const cp of getFeatureConnectionPoints(model)) {
    const d = distance(pt, cp);
    if (d < bestDist) { bestDist = d; best = cp; }
  }
  return best;
}

export function pointInOrientedRect(
  p: Point,
  center: Point,
  halfLength: number,
  halfWidth: number,
  angle: number,
): boolean {
  const cos = Math.cos(-angle);
  const sin = Math.sin(-angle);
  const lx = p.x - center.x;
  const ly = p.y - center.y;
  const rx = lx * cos - ly * sin;
  const ry = lx * sin + ly * cos;
  return Math.abs(rx) <= halfLength && Math.abs(ry) <= halfWidth;
}
