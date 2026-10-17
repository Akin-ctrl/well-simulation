import z from 'zod';

/** PostgreSQL bigint identities must survive JSON without Number rounding. */
export const databaseIdSchema = z
  .string()
  .regex(/^[1-9][0-9]{0,18}$/)
  .refine(
    (value) =>
      /^[1-9][0-9]{0,18}$/.test(value) && BigInt(value) <= 9_223_372_036_854_775_807n,
    'Invalid database id'
  );

export const databaseTimeSchema = z
  .string()
  .datetime({ offset: true })
  .regex(
    /T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?(?:Z|\+00:00)$/,
    'Use UTC with at most six fractional digits'
  );

/** Compare PostgreSQL microseconds without converting them to JS milliseconds. */
export function databaseTimeKey(value: string): string {
  const [seconds, fraction = ''] = databaseTimeSchema
    .parse(value)
    .replace(/(?:Z|\+00:00)$/, '')
    .split('.');
  return `${seconds}.${fraction.padEnd(6, '0')}Z`;
}
