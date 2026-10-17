/** Typed mirror of the canonical SQL; schema.sql owns installation and constraints. */
import {
  bigint,
  index,
  integer,
  jsonb,
  pgTable,
  primaryKey,
  text,
  timestamp,
  unique,
  uuid,
  varchar,
} from 'drizzle-orm/pg-core';
import { wellhead, alarmrule } from './schema';

export const alarmAction = pgTable(
  'alarm_action',
  {
    actionId: uuid('action_id').primaryKey(),
    actorUserId: integer('actor_user_id').notNull(),
    actorName: varchar('actor_name', { length: 256 }).notNull(),
    actorRole: text('actor_role').notNull(),
    requestKey: uuid('request_key').notNull(),
    inputHash: varchar('input_hash', { length: 64 }).notNull(),
    action: text('action').notNull(),
    wellheadId: integer('wellhead_id').notNull(),
    ruleId: integer('alarm_rule_id').notNull(),
    occurredAt: timestamp('occurred_at', {
      withTimezone: true,
      mode: 'string',
    }).notNull(),
    input: jsonb('input').$type<Record<string, unknown>>().notNull(),
    result: jsonb('result').$type<Record<string, unknown>>().notNull(),
  },
  (t) => [
    unique().on(t.actorUserId, t.requestKey),
    index('ix_alarm_action_target').on(
      t.wellheadId,
      t.ruleId,
      t.occurredAt.desc(),
      t.actionId.desc()
    ),
  ]
);
export const alarmAcknowledgement = pgTable(
  'alarm_acknowledgement',
  {
    eventId: bigint('event_id', { mode: 'bigint' }).notNull(),
    triggeredAt: timestamp('triggered_at', {
      withTimezone: true,
      mode: 'string',
    }).notNull(),
    acknowledgedAt: timestamp('acknowledged_at', {
      withTimezone: true,
      mode: 'string',
    }).notNull(),
    actionId: uuid('action_id')
      .notNull()
      .unique()
      .references(() => alarmAction.actionId),
  },
  (t) => [primaryKey({ columns: [t.eventId, t.triggeredAt] })]
);
export const alarmShelf = pgTable(
  'alarm_shelf',
  {
    wellheadId: integer('wellhead_id')
      .notNull()
      .references(() => wellhead.wellheadId, { onDelete: 'cascade' }),
    ruleId: integer('alarm_rule_id')
      .notNull()
      .references(() => alarmrule.alarmRuleId, { onDelete: 'cascade' }),
    revision: integer('revision').notNull(),
    shelvedUntil: timestamp('shelved_until', { withTimezone: true, mode: 'string' }),
    updatedAt: timestamp('updated_at', {
      withTimezone: true,
      mode: 'string',
    }).notNull(),
    actionId: uuid('action_id')
      .notNull()
      .references(() => alarmAction.actionId),
  },
  (t) => [primaryKey({ columns: [t.wellheadId, t.ruleId] })]
);
