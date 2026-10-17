import {
  pgTable,
  uuid,
  integer,
  text,
  varchar,
  boolean,
  doublePrecision,
  timestamp,
  bigserial,
  uniqueIndex,
  index,
} from 'drizzle-orm/pg-core';
import { sql } from 'drizzle-orm';
import { users } from './user';
import { wellhead, wellheadModelParameter } from './schema';

/** Typed mirror of data/sql/schema.sql, which owns checks, triggers and grants. */
const instant = (name: string) =>
  timestamp(name, { withTimezone: true, mode: 'string' });
const targets = () => ({
  chokePositionPercent: doublePrecision('choke_position_percent'),
  masterValveOpen: boolean('master_valve_open'),
  wingValveOpen: boolean('wing_valve_open'),
  pumpOn: boolean('pump_on'),
  emergencyShutdown: boolean('emergency_shutdown'),
});

export const controlCommand = pgTable(
  'control_command',
  {
    commandId: uuid('command_id').primaryKey(),
    wellheadId: integer('wellhead_id')
      .notNull()
      .references(() => wellhead.wellheadId),
    createdBy: integer('created_by')
      .notNull()
      .references(() => users.id),
    idempotencyKey: uuid('idempotency_key').notNull(),
    reason: varchar('reason', { length: 500 }).notNull(),
    expectedRevision: integer('expected_revision').notNull(),
    modelVersion: text('model_version').notNull(),
    parameterFingerprint: text('parameter_fingerprint').notNull(),
    ...targets(),
    status: text('status').notNull().default('draft'),
    createdAt: instant('created_at').notNull().defaultNow(),
    expiresAt: instant('expires_at')
      .notNull()
      .default(sql`clock_timestamp() + interval '24 hours'`),
    queuedAt: instant('queued_at'),
    activatedBy: integer('activated_by').references(() => users.id),
    activatedRole: text('activated_role'),
    activatedSessionVersion: integer('activated_session_version'),
    resultAt: instant('result_at'),
    resultRevision: integer('result_revision'),
    resultReason: varchar('result_reason', { length: 64 }),
    requestId: varchar('request_id', { length: 64 }),
  },
  (t) => [
    uniqueIndex('control_command_created_by_idempotency_key_key').on(
      t.createdBy,
      t.idempotencyKey
    ),
    uniqueIndex('ix_control_one_draft')
      .on(t.createdBy, t.wellheadId)
      .where(sql`${t.status} = 'draft'`),
    uniqueIndex('ix_control_one_queued')
      .on(t.wellheadId)
      .where(sql`${t.status} = 'queued'`),
  ]
);

export const wellheadControlState = pgTable('wellhead_control_state', {
  wellheadId: integer('wellhead_id')
    .primaryKey()
    .references(() => wellhead.wellheadId),
  revision: integer('revision').notNull().default(0),
  modelVersion: text('model_version').notNull(),
  parameterFingerprint: text('parameter_fingerprint').notNull(),
  chokePositionPercent: doublePrecision('choke_position_percent').notNull(),
  masterValveOpen: boolean('master_valve_open').notNull(),
  wingValveOpen: boolean('wing_valve_open').notNull(),
  swabValveOpen: boolean('swab_valve_open').notNull(),
  pumpOn: boolean('pump_on').notNull(),
  emergencyShutdown: boolean('emergency_shutdown').notNull(),
  appliedCommandId: uuid('applied_command_id').references(
    () => controlCommand.commandId
  ),
  appliedAt: instant('applied_at').notNull().defaultNow(),
});

export const controlCommandEvent = pgTable(
  'control_command_event',
  {
    eventId: bigserial('event_id', { mode: 'number' }).primaryKey(),
    commandId: uuid('command_id')
      .notNull()
      .references(() => controlCommand.commandId),
    eventType: varchar('event_type', { length: 32 }).notNull(),
    occurredAt: instant('occurred_at').notNull().defaultNow(),
    actorUserId: integer('actor_user_id').references(() => users.id),
    reasonCode: varchar('reason_code', { length: 64 }).notNull(),
    requestId: varchar('request_id', { length: 64 }),
  },
  (t) => [index('ix_control_event_command').on(t.commandId, t.eventId)]
);

export const modelParameterChange = pgTable(
  'model_parameter_change',
  {
    changeId: uuid('change_id').primaryKey(),
    wellheadId: integer('wellhead_id')
      .notNull()
      .references(() => wellhead.wellheadId),
    createdBy: integer('created_by')
      .notNull()
      .references(() => users.id),
    idempotencyKey: uuid('idempotency_key').notNull(),
    modelVersion: text('model_version').notNull(),
    parameterKey: text('parameter_key').notNull(),
    value: doublePrecision('value'),
    unit: text('unit'),
    reason: varchar('reason', { length: 500 }).notNull(),
    expectedOverrideId: integer('expected_override_id').references(
      () => wellheadModelParameter.parameterOverrideId
    ),
    status: text('status').notNull().default('draft'),
    createdAt: instant('created_at').notNull().defaultNow(),
    expiresAt: instant('expires_at')
      .notNull()
      .default(sql`clock_timestamp() + interval '24 hours'`),
    resultAt: instant('result_at'),
    publishedOverrideId: integer('published_override_id').references(
      () => wellheadModelParameter.parameterOverrideId
    ),
  },
  (t) => [
    uniqueIndex('model_parameter_change_created_by_idempotency_key_key').on(
      t.createdBy,
      t.idempotencyKey
    ),
    uniqueIndex('ix_parameter_one_draft')
      .on(t.createdBy, t.wellheadId, t.parameterKey)
      .where(sql`${t.status} = 'draft'`),
  ]
);
