import type { executionOriginSchema } from './forecast-schedule';
import z from 'zod';
import type { BranchOrigin } from './snapshots';
import { controlChangeSchema } from '../req/projection';
import { projectionResponseSchema } from './projection';

/** Metadata comes from the historian catalogue, including wells with no readings. */
export const projectionSignalSchema = z.object({
  code: z.string(),
  displayName: z.string(),
  canonicalUnit: z.string().nullable(),
  dataType: z.string(),
});

/** Extra saved-run information for explaining an immutable result in the UI. */
export const projectionDetailSchema = projectionResponseSchema.extend({
  baselineControls: z.record(z.string(), z.union([z.boolean(), z.number().finite()])),
  scheduledControls: z.array(
    z.object({
      offsetMinutes: z.number().int().nonnegative(),
      input: controlChangeSchema,
    })
  ),
  signals: z.array(projectionSignalSchema),
});
export type ProjectionDetail = z.infer<typeof projectionDetailSchema>;

export const runHistoryQuerySchema = z
  .object({
    kind: z.enum(['forecast', 'scenario']).default('scenario'),
    limit: z.coerce.number().int().min(1).max(50).default(20),
    offset: z.coerce.number().int().min(0).max(10000).default(0),
  })
  .strict();
export type RunSummary = {
  runId: string;
  kind: 'forecast' | 'scenario';
  wellheadId: number;
  name: string | null;
  createdAt: string;
  baseSimulatedAt: string;
  modelVersion: string;
  branchOrigin: BranchOrigin;
  executionOrigin: z.infer<typeof executionOriginSchema>;
  horizonSeconds: number;
  stepSeconds: number;
  confidenceScore: number;
};
export type RunHistory = { items: RunSummary[]; hasMore: boolean };
/** Persisted model schedule, validated before conversion to public input names. */
export const storedScheduledControlSchema = z.object({
  offset_seconds: z.number().int().nonnegative().multipleOf(60),
  change: z.record(z.string(), z.union([z.number().finite(), z.boolean(), z.null()])),
});
