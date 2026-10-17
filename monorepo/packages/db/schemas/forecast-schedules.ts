import {
  pgTable,
  uuid,
  integer,
  text,
  boolean,
  varchar,
  timestamp,
} from 'drizzle-orm/pg-core';
import { users } from './user';
import { wellhead, predictionRun } from './schema';
const instant = (name: string) =>
  timestamp(name, { withTimezone: true, mode: 'string' });
/** SQL owns constraints and indexes; these fields mirror its named columns. */
export const forecastSchedule = pgTable('forecast_schedule', {
  scheduleId: uuid('schedule_id').primaryKey(),
  wellheadId: integer('wellhead_id')
    .notNull()
    .unique()
    .references(() => wellhead.wellheadId),
  ownerUserId: integer('owner_user_id')
    .notNull()
    .references(() => users.id),
  ownerRole: text('owner_role').notNull(),
  enabled: boolean('enabled').notNull().default(false),
  intervalSeconds: integer('interval_seconds').notNull(),
  horizonSeconds: integer('horizon_seconds').notNull(),
  stepSeconds: integer('step_seconds').notNull().default(60),
  revision: integer('revision').notNull(),
  nextDueAt: instant('next_due_at'),
  reason: varchar('reason', { length: 500 }).notNull(),
  updatedAt: instant('updated_at').notNull().defaultNow(),
});
export const forecastJob = pgTable('forecast_job', {
  jobId: uuid('job_id').primaryKey(),
  scheduleId: uuid('schedule_id')
    .notNull()
    .references(() => forecastSchedule.scheduleId),
  scheduleRevision: integer('schedule_revision').notNull(),
  wellheadId: integer('wellhead_id')
    .notNull()
    .references(() => wellhead.wellheadId),
  ownerUserId: integer('owner_user_id')
    .notNull()
    .references(() => users.id),
  ownerRole: text('owner_role').notNull(),
  intervalSeconds: integer('interval_seconds').notNull(),
  horizonSeconds: integer('horizon_seconds').notNull(),
  stepSeconds: integer('step_seconds').notNull(),
  scheduledFor: instant('scheduled_for').notNull(),
  startedAt: instant('started_at').notNull().defaultNow(),
  leaseExpiresAt: instant('lease_expires_at').notNull(),
  missedIntervals: integer('missed_intervals').notNull(),
  status: text('status').notNull(),
  completedAt: instant('completed_at'),
  reasonCode: varchar('reason_code', { length: 64 }),
  runId: uuid('run_id').references(() => predictionRun.runId),
});
