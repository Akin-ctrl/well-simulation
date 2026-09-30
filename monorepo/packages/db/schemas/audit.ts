import {
  bigserial,
  index,
  integer,
  jsonb,
  pgTable,
  primaryKey,
  timestamp,
  varchar,
} from 'drizzle-orm/pg-core';

/**
 * The audit trail defined by ADR 0034.
 *
 * Mirrors `data/sql/migrations/0006_audit_event.sql`, which is canonical. The
 * table is a TimescaleDB hypertable there, which Drizzle cannot express, so
 * this declaration covers the columns only.
 */
export const auditEvent = pgTable(
  'auditevent',
  {
    auditEventId: bigserial('audit_event_id', { mode: 'number' }).notNull(),
    occurredAt: timestamp('occurred_at', {
      withTimezone: true,
      mode: 'string',
    }).notNull(),

    /** Null when no known user acted, such as a login for an unknown account. */
    actorUserId: integer('actor_user_id'),

    /**
     * What the caller submitted. Recorded whether or not it matches an account,
     * so a row cannot be used to discover which accounts exist. Never populated
     * from the password field.
     */
    actorIdentifier: varchar('actor_identifier', { length: 256 }),

    action: varchar('action', { length: 64 }).notNull(),
    subjectType: varchar('subject_type', { length: 64 }),
    subjectId: varchar('subject_id', { length: 64 }),
    outcome: varchar('outcome', { length: 16 }).notNull(),

    /** Ties the row to one request and to the logs that request produced. */
    requestId: varchar('request_id', { length: 64 }),

    detail: jsonb('detail').notNull().default({}),
  },
  (table) => [
    primaryKey({ columns: [table.auditEventId, table.occurredAt] }),
    index('ix_audit_event_actor').on(table.actorUserId, table.occurredAt),
    index('ix_audit_event_action').on(table.action, table.occurredAt),
  ]
);
