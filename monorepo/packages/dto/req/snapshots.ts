import z from 'zod';
import { databaseIdSchema, databaseTimeSchema } from './database-identity';
export { databaseTimeKey as snapshotTimeKey } from './database-identity';

export const snapshotIdSchema = databaseIdSchema;
export const snapshotTimeSchema = databaseTimeSchema;

export const snapshotSelectionSchema = z
  .object({ snapshotId: snapshotIdSchema, simulatedAt: snapshotTimeSchema })
  .strict();
export const snapshotSearchSchema = z.object({ at: snapshotTimeSchema }).strict();
export type SnapshotSelection = z.infer<typeof snapshotSelectionSchema>;
