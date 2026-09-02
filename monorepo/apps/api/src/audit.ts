import type { Context } from 'hono';
import { db } from '@well-simulation/db/query';
import { auditEvent } from '@well-simulation/db/schemas/audit';

import { currentRequestId } from './middleware/request-id';

/**
 * Writing the audit trail defined by ADR 0034.
 *
 * The trail answers who did what, when, and to which record. It is built for
 * the person operating this system, not for a regulator, so there is no hash
 * chain and no separate store. ADR 0034 records why.
 */

/** Actions recorded so far. The control API adds its own when it arrives. */
export type AuditAction =
  | 'auth.login'
  | 'auth.logout'
  | 'user.create'
  | 'user.disable'
  | 'user.role_change'
  | 'config.change';

export type AuditOutcome = 'success' | 'failure';

type AuditInput = {
  action: AuditAction;
  outcome: AuditOutcome;
  /** The user who acted, where one is known. */
  actorUserId?: number | null;
  /**
   * The identifier the caller submitted. Recorded for failed logins whether or
   * not the account exists, so the table cannot be used to discover which
   * accounts are real. Never populated from the password field.
   */
  actorIdentifier?: string | null;
  subjectType?: string | null;
  subjectId?: string | number | null;
  detail?: Record<string, unknown>;
};

/**
 * Record one event.
 *
 * Never throws. An audit write that fails must not turn a successful login into
 * an error for the user, so a failure is logged loudly and swallowed. The
 * alternative, letting it propagate, would make the audit trail a way to take
 * the service down.
 */
export async function recordAuditEvent(c: Context, input: AuditInput): Promise<void> {
  try {
    await db.insert(auditEvent).values({
      occurredAt: new Date().toISOString(),
      actorUserId: input.actorUserId ?? null,
      actorIdentifier: input.actorIdentifier ?? null,
      action: input.action,
      subjectType: input.subjectType ?? null,
      subjectId: input.subjectId === undefined ? null : String(input.subjectId ?? ''),
      outcome: input.outcome,
      requestId: currentRequestId(c),
      detail: input.detail ?? {},
    });
  } catch (error) {
    console.error(
      JSON.stringify({
        level: 'error',
        service: 'api',
        event: 'audit_write_failed',
        action: input.action,
        outcome: input.outcome,
        requestId: currentRequestId(c),
        error: error instanceof Error ? error.message : String(error),
      })
    );
  }
}
