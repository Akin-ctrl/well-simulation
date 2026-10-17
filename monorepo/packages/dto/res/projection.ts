import { executionOriginSchema } from './forecast-schedule';
import z from 'zod';
import { branchOriginSchema } from './snapshots';

export const alarmRiskSchema = z.enum(['none', 'warning', 'critical', 'unknown']);

export const projectionPointSchema = z.object({
  elapsedSeconds: z.number().int().positive(),
  timestamp: z.string(),
  values: z.record(z.string(), z.number().finite()),
  alarmRisk: alarmRiskSchema,
  triggeredRuleIds: z.array(z.number().int().positive()),
  baselineValues: z.record(z.string(), z.number().finite()).optional(),
  baselineAlarmRisk: alarmRiskSchema.optional(),
});

/** Shared response shape for a fresh run and its later database read. */
export const projectionResponseSchema = z.object({
  runId: z.uuid(),
  kind: z.enum(['forecast', 'scenario']),
  wellheadId: z.number().int().positive(),
  name: z.string().nullable(),
  source: z.literal('synthetic_reduced_order_model'),
  fieldCalibrated: z.literal(false),
  modelVersion: z.string(),
  baseSimulatedAt: z.string(),
  generatedAt: z.string(),
  branchOrigin: branchOriginSchema.default({ kind: 'current' }),
  executionOrigin: executionOriginSchema.default({ kind: 'manual' }),
  horizonSeconds: z.number().int().min(300).max(7200),
  stepSeconds: z.number().int().min(5).max(300),
  confidence: z.object({
    method: z.literal('heuristic_uncalibrated'),
    score: z.number().min(0).max(1),
    label: z.enum(['moderate', 'low']),
    reasons: z.array(z.string()),
  }),
  points: z.array(projectionPointSchema).min(1).max(1440),
});

export type ProjectionResponse = z.infer<typeof projectionResponseSchema>;
