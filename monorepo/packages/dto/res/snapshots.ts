import z from 'zod';
import { snapshotSelectionSchema, snapshotTimeSchema } from '../req/snapshots';

export const branchOriginSchema = z.discriminatedUnion('kind', [
  z.object({ kind: z.literal('current') }).strict(),
  snapshotSelectionSchema.extend({ kind: z.literal('snapshot') }).strict(),
]);
export type BranchOrigin = z.infer<typeof branchOriginSchema>;

export const snapshotCandidateSchema = snapshotSelectionSchema
  .extend({
    modelVersion: z.string(),
    compatible: z.boolean(),
    reason: z.enum(['compatible', 'model_version_changed', 'parameters_changed']),
  })
  .refine((value) => value.compatible === (value.reason === 'compatible'));

export const snapshotListSchema = z.object({
  wellheadId: z.number().int().positive(),
  checkedAt: snapshotTimeSchema,
  hasMore: z.boolean(),
  items: z.array(snapshotCandidateSchema).max(20),
});
export type SnapshotList = z.infer<typeof snapshotListSchema>;

/** Only absent metadata on legacy current-only rows receives the old origin. */
export function storedBranchOrigin(baseState: Record<string, unknown>): BranchOrigin {
  return baseState.branchOrigin === undefined
    ? { kind: 'current' }
    : branchOriginSchema.parse(baseState.branchOrigin);
}
