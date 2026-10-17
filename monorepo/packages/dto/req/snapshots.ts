import z from 'zod';

/** PostgreSQL bigint identities must survive JSON without Number rounding. */
export const snapshotIdSchema = z
  .string()
  .regex(/^[1-9][0-9]{0,18}$/)
  .refine(
    (value) =>
      /^[1-9][0-9]{0,18}$/.test(value) && BigInt(value) <= 9_223_372_036_854_775_807n,
    'Invalid snapshot id'
  );

export const snapshotTimeSchema = z
  .string()
  .datetime({ offset: true })
  .regex(
    /T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?(?:Z|\+00:00)$/,
    'Use UTC with at most six fractional digits'
  );

export const snapshotSelectionSchema = z
  .object({ snapshotId: snapshotIdSchema, simulatedAt: snapshotTimeSchema })
  .strict();
export const snapshotSearchSchema = z.object({ at: snapshotTimeSchema }).strict();
export type SnapshotSelection = z.infer<typeof snapshotSelectionSchema>;

/** Compare PostgreSQL microseconds without converting them to JS milliseconds. */
export function snapshotTimeKey(value: string): string {
  const [seconds, fraction = ''] = snapshotTimeSchema
    .parse(value)
    .replace(/(?:Z|\+00:00)$/, '')
    .split('.');
  return `${seconds}.${fraction.padEnd(6, '0')}Z`;
}
