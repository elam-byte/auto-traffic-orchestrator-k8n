import { z } from 'zod';
import type { MapModel } from './types';

const PointSchema = z.object({
  x: z.number().finite(),
  y: z.number().finite(),
});

const RoadLanesSchema = z.object({
  left: z.number().int().min(0).max(3),
  right: z.number().int().min(0).max(3),
  laneWidth: z.number().min(2.8).max(4.0),
});

const RoadLineSchema = z.object({
  id: z.string().min(1),
  kind: z.literal('line'),
  start: PointSchema,
  end: PointSchema,
  lanes: RoadLanesSchema,
  speedLimit: z.number().positive().optional(),
});

const RoadArcSchema = z.object({
  id: z.string().min(1),
  kind: z.literal('arc'),
  center: PointSchema,
  radius: z.number().min(0).finite(),
  startAngle: z.number().finite(),
  endAngle: z.number().finite(),
  clockwise: z.boolean(),
  lanes: RoadLanesSchema,
  speedLimit: z.number().positive().optional(),
});

const RoadSchema = z.discriminatedUnion('kind', [RoadLineSchema, RoadArcSchema]);

const VehicleSchema = z.object({
  id: z.string().min(1),
  x: z.number().finite(),
  y: z.number().finite(),
  heading: z.number().finite(),
  length: z.number().positive().finite(),
  width: z.number().positive().finite(),
  color: z.string().regex(/^#[0-9a-fA-F]{6}$/).optional(),
});

const WorldMetaSchema = z.object({
  version: z.literal('ats-map-v1'),
  unit: z.literal('m'),
  origin: z.literal('bottom-left'),
  world: z.object({
    width: z.number().positive().finite(),
    height: z.number().positive().finite(),
  }),
});

const JunctionSchema = z.object({
  id: z.string().min(1),
  x: z.number().finite(),
  y: z.number().finite(),
  junctionType: z.enum(['4-way', 't-junction']).default('4-way'),
  rotation: z.number().finite().default(0),
  laneWidth: z.number().min(2.8).max(4.0).default(3.5),
});

export const MapModelSchema = z.object({
  meta: WorldMetaSchema,
  junctions: z.array(JunctionSchema),
  roads: z.array(RoadSchema),
  vehicles: z.array(VehicleSchema),
});

export function parseMapModel(json: unknown): MapModel {
  return MapModelSchema.parse(json) as MapModel;
}

export type ParseResult =
  | { ok: true; model: MapModel }
  | { ok: false; error: string };

export function safeParseMapModel(json: unknown): ParseResult {
  const result = MapModelSchema.safeParse(json);
  if (result.success) return { ok: true, model: result.data as MapModel };
  const issues = result.error.issues
    .map((i) => `${i.path.join('.')}: ${i.message}`)
    .join('\n');
  return { ok: false, error: issues };
}
