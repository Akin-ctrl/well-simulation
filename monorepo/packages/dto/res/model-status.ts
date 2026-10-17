import z from 'zod';

const time = z.string().datetime({ offset: true });

/** Lifecycle and process freshness, computed by the model's own clock. */
export const modelStatusSchema = z.object({
  wellheadId: z.number().int().positive(),
  modelVersion: z.string(),
  status: z.string(),
  reason: z.string(),
  lastTickAt: time.nullable(),
  simulatedAt: time.nullable(),
  processAgeSeconds: z.number().finite().nullable(),
  current: z.boolean(),
  skippedTicks: z.number().int().nonnegative(),
  restoredFrom: time.nullable(),
  lastRecoveryEvent: z.object({ type: z.string(), time }).nullable(),
});

export const fleetModelStatusSchema = z.object({
  checkedAt: time,
  available: z.boolean(),
  wellheads: z.array(modelStatusSchema).max(1000),
});
export type ModelStatus = z.infer<typeof modelStatusSchema>;
export type FleetModelStatus = z.infer<typeof fleetModelStatusSchema>;
