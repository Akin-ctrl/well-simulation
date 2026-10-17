import z from 'zod';
import { databaseIdSchema, databaseTimeSchema } from '../req/database-identity';

export const alarmTargetSchema = z.object({
  wellheadId: z.number().int().positive(),
  wellheadName: z.string(),
  locationName: z.string(),
  ruleId: z.number().int().positive(),
  parameterName: z.string(),
  unit: z.string().nullable(),
  operator: z.string(),
  threshold: z.number().finite(),
  severity: z.string(),
  triggerAfter: z.number().int(),
  clearAfter: z.number().int(),
});
export const alarmShelfStateSchema = z.object({
  revision: z.number().int().positive(),
  until: databaseTimeSchema.nullable(),
  active: z.boolean(),
  updatedAt: databaseTimeSchema,
  actorName: z.string(),
  reason: z.string(),
});
export const alarmReceiptSchema = z.object({
  actionId: z.uuid(),
  action: z.enum(['acknowledge', 'shelve', 'unshelve']),
  occurredAt: databaseTimeSchema,
  actorUserId: z.number().int().positive(),
  actorName: z.string(),
  reason: z.string(),
  target: alarmTargetSchema,
  eventId: databaseIdSchema.nullable(),
  triggeredAt: databaseTimeSchema.nullable(),
  revision: z.number().int().positive().nullable(),
  shelvedUntil: databaseTimeSchema.nullable(),
});
export const alarmEventSchema = z.object({
  eventId: databaseIdSchema,
  triggeredAt: databaseTimeSchema,
  clearedAt: databaseTimeSchema.nullable(),
  severity: z.string(),
  triggeredValue: z.number().finite(),
  target: alarmTargetSchema,
  acknowledgement: z
    .object({ at: databaseTimeSchema, actorName: z.string(), reason: z.string() })
    .nullable(),
  shelf: alarmShelfStateSchema.nullable(),
});
export const alarmSummarySchema = z.object({
  active: z.number().int().min(0),
  critical: z.number().int().min(0),
  attention: z.number().int().min(0),
  shelves: z.number().int().min(0),
  retained: z.number().int().min(0),
});
const pageHeader = {
  wells: z.array(
    z.object({ wellheadId: z.number().int().positive(), wellheadName: z.string() })
  ),
  summary: alarmSummarySchema,
};
export const alarmListSchema = z.object({
  ...pageHeader,
  observedAt: databaseTimeSchema,
  items: z.array(alarmEventSchema),
  nextCursor: z.string().nullable(),
});
export const alarmShelfListSchema = z.object({
  ...pageHeader,
  observedAt: databaseTimeSchema,
  items: z.array(z.object({ target: alarmTargetSchema, state: alarmShelfStateSchema })),
  nextCursor: z.string().nullable(),
  total: z.number().int().min(0),
});
export const alarmDetailSchema = z.object({
  observedAt: databaseTimeSchema,
  event: alarmEventSchema,
  actions: z.array(alarmReceiptSchema),
});
export type AlarmTarget = z.infer<typeof alarmTargetSchema>;
export type AlarmEvent = z.infer<typeof alarmEventSchema>;
export type AlarmReceipt = z.infer<typeof alarmReceiptSchema>;
export type AlarmList = z.infer<typeof alarmListSchema>;
export type AlarmShelfList = z.infer<typeof alarmShelfListSchema>;
