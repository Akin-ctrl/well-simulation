import z from 'zod';

export const executionOriginSchema = z.discriminatedUnion('kind', [
  z.object({ kind: z.literal('manual') }),
  z.object({
    kind: z.literal('scheduled'),
    scheduleId: z.uuid(),
    jobId: z.uuid(),
    scheduledFor: z.iso.datetime({ offset: true }),
  }),
]);
export function storedExecutionOrigin(state: Record<string, unknown>) {
  return executionOriginSchema.parse(state.executionOrigin ?? { kind: 'manual' });
}
export const forecastScheduleSchema = z.object({
  scheduleId: z.uuid(),
  wellheadId: z.number().int().positive(),
  ownerUserId: z.number().int().positive(),
  ownerRole: z.literal('ADMIN'),
  enabled: z.boolean(),
  intervalSeconds: z.number().int(),
  horizonSeconds: z.number().int(),
  stepSeconds: z.literal(60),
  revision: z.number().int().positive(),
  nextDueAt: z.string().nullable(),
  reason: z.string(),
  updatedAt: z.string(),
});
export const forecastJobSchema = z.object({
  jobId: z.uuid(),
  scheduleId: z.uuid(),
  scheduleRevision: z.number().int(),
  wellheadId: z.number().int(),
  ownerUserId: z.number().int(),
  ownerRole: z.literal('ADMIN'),
  intervalSeconds: z.number().int(),
  horizonSeconds: z.number().int(),
  stepSeconds: z.literal(60),
  scheduledFor: z.string(),
  startedAt: z.string(),
  leaseExpiresAt: z.string(),
  missedIntervals: z.number().int().min(0),
  status: z.enum(['running', 'complete', 'skipped', 'failed']),
  completedAt: z.string().nullable(),
  reasonCode: z.string().nullable(),
  runId: z.uuid().nullable(),
});
export const forecastScheduleViewSchema = z.object({
  schedule: forecastScheduleSchema.nullable(),
  latestJob: forecastJobSchema.nullable(),
  latestCompleted: forecastJobSchema.nullable(),
});
export type ForecastScheduleView = z.infer<typeof forecastScheduleViewSchema>;
