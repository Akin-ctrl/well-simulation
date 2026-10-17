import z from 'zod';
import { ALARM_SEVERITIES } from '../res/dashboard';
import {
  databaseIdSchema,
  databaseTimeSchema,
  databaseTimeKey,
} from './database-identity';

export const ALARM_PAGE_SIZE = 50;
export const SHELF_MIN_MINUTES = 5;
export const SHELF_MAX_MINUTES = 480;
export const alarmViewSchema = z.enum(['attention', 'active', 'history']);
const assetId = z.number().int().positive().max(2_147_483_647);
const revision = z.number().int().min(0).max(2_147_483_646);
const common = {
  reason: z.string().trim().min(1).max(500),
  confirmed: z.literal(true),
};
export const alarmIdentitySchema = z
  .object({
    eventId: databaseIdSchema,
    triggeredAt: databaseTimeSchema.transform(databaseTimeKey),
  })
  .strict();
export const alarmActionInputSchema = z.discriminatedUnion('action', [
  z
    .object({
      action: z.literal('acknowledge'),
      ...alarmIdentitySchema.shape,
      ...common,
    })
    .strict(),
  z
    .object({
      action: z.literal('shelve'),
      wellheadId: assetId,
      ruleId: assetId,
      expectedRevision: revision,
      durationMinutes: z.number().int().min(SHELF_MIN_MINUTES).max(SHELF_MAX_MINUTES),
      ...common,
    })
    .strict(),
  z
    .object({
      action: z.literal('unshelve'),
      wellheadId: assetId,
      ruleId: assetId,
      expectedRevision: revision,
      ...common,
    })
    .strict(),
]);
export const alarmQuerySchema = z
  .object({
    view: alarmViewSchema.default('attention'),
    wellheadId: z.coerce.number().int().positive().max(2_147_483_647).optional(),
    cursor: z.string().min(1).max(600).optional(),
  })
  .strict();
export const shelfQuerySchema = alarmQuerySchema.omit({ view: true });
export type AlarmActionInput = z.infer<typeof alarmActionInputSchema>;
export type AlarmQuery = z.infer<typeof alarmQuerySchema>;
export type ShelfQuery = z.infer<typeof shelfQuerySchema>;

export const alarmRequestKeySchema = z.uuid();
export const alarmDetailQuerySchema = alarmIdentitySchema.pick({ triggeredAt: true });
const cursorScope = { wellheadId: z.number().int().positive().nullable() };
export const eventCursorSchema = z
  .object({
    ...cursorScope,
    view: alarmViewSchema,
    rank: z.number().int().min(0).max(ALARM_SEVERITIES.length),
    at: databaseTimeSchema,
    id: databaseIdSchema,
  })
  .strict();
export const shelfCursorSchema = z
  .object({
    ...cursorScope,
    well: z.number().int().positive(),
    rule: z.number().int().positive(),
  })
  .strict();
