import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';

import { createUser, deleteTestUsers, login, request, withDb } from './helpers/api';

/**
 * The audit trail from ADR 0034.
 *
 * These assertions are the reason the trail exists. A row that leaks a
 * credential, or that reveals which accounts are real, is worse than no row.
 */

type AuditRow = {
  action: string;
  outcome: string;
  actor_user_id: number | null;
  actor_identifier: string | null;
  subject_type: string | null;
  subject_id: string | null;
  request_id: string | null;
  detail: Record<string, unknown>;
};

async function auditRows(action?: string): Promise<AuditRow[]> {
  return withDb(async (client) => {
    const result = action
      ? await client.query<AuditRow>(
          'SELECT * FROM auditEvent WHERE action = $1 ORDER BY occurred_at DESC',
          [action]
        )
      : await client.query<AuditRow>(
          'SELECT * FROM auditEvent ORDER BY occurred_at DESC'
        );
    return result.rows;
  });
}

async function clearAudit(): Promise<void> {
  await withDb((client) => client.query('DELETE FROM auditEvent'));
}

describe('audit trail', () => {
  beforeAll(deleteTestUsers);
  beforeEach(clearAudit);
  afterAll(async () => {
    await clearAudit();
    await deleteTestUsers();
  });

  it('records a successful login against the acting user', async () => {
    const user = await createUser('USER');
    await login(user);

    const rows = await auditRows('auth.login');
    expect(rows).toHaveLength(1);
    expect(rows[0]!.outcome).toBe('success');
    expect(rows[0]!.actor_user_id).toBe(user.id);
  });

  it('records a failed login for a real account', async () => {
    const user = await createUser('USER');
    await request('/auth/login', {
      method: 'POST',
      body: { emailOrUsername: user.email, password: 'wrong-password-here' },
    });

    const rows = await auditRows('auth.login');
    expect(rows).toHaveLength(1);
    expect(rows[0]!.outcome).toBe('failure');
    expect(rows[0]!.actor_user_id).toBe(user.id);
    expect(rows[0]!.detail).toMatchObject({ reason: 'bad_password' });
  });

  it('records a failed login for an account that does not exist', async () => {
    // If a row only appeared for real accounts, its presence would disclose
    // that the account exists.
    await request('/auth/login', {
      method: 'POST',
      body: { emailOrUsername: 'nobody@test.local', password: 'wrong-password-here' },
    });

    const rows = await auditRows('auth.login');
    expect(rows).toHaveLength(1);
    expect(rows[0]!.outcome).toBe('failure');
    expect(rows[0]!.actor_user_id).toBeNull();
    expect(rows[0]!.actor_identifier).toBe('nobody@test.local');
  });

  it('never writes the submitted password into the trail', async () => {
    const secret = 'the-password-that-was-typed';
    await request('/auth/login', {
      method: 'POST',
      body: { emailOrUsername: 'nobody@test.local', password: secret },
    });

    const serialised = JSON.stringify(await auditRows());
    expect(serialised).not.toContain(secret);
    expect(serialised).not.toContain('$2b$');
  });

  it('records a logout against the user who was signed in', async () => {
    const user = await createUser('USER');
    const cookie = await login(user);
    await clearAudit();

    await request('/auth/logout', { method: 'POST', cookie });

    const rows = await auditRows('auth.logout');
    expect(rows).toHaveLength(1);
    expect(rows[0]!.actor_user_id).toBe(user.id);
  });

  it('records user creation against the administrator who did it', async () => {
    const admin = await createUser('ADMIN');
    const cookie = await login(admin);
    await clearAudit();

    const response = await request('/auth/register', {
      method: 'POST',
      cookie,
      body: {
        username: 'made-by-admin',
        email: 'made-by-admin@test.local',
        password: 'a-long-enough-password',
        confirmPassword: 'a-long-enough-password',
      },
    });
    expect(response.status).toBe(200);

    const rows = await auditRows('user.create');
    expect(rows).toHaveLength(1);
    expect(rows[0]!.actor_user_id).toBe(admin.id);
    expect(rows[0]!.subject_type).toBe('user');
    expect(rows[0]!.subject_id).not.toBeNull();
  });

  it('carries the request id, and honours one supplied by the caller', async () => {
    const user = await createUser('USER');
    const response = await request('/auth/login', {
      method: 'POST',
      body: { emailOrUsername: user.email, password: user.password },
      headers: { 'x-request-id': 'known-request-id' },
    });

    expect(response.headers.get('x-request-id')).toBe('known-request-id');

    const rows = await auditRows('auth.login');
    expect(rows[0]!.request_id).toBe('known-request-id');
  });

  it('generates a request id when the supplied one is unusable', async () => {
    const user = await createUser('USER');
    // Attacker-controlled, and it reaches a database column and every log line.
    await request('/auth/login', {
      method: 'POST',
      body: { emailOrUsername: user.email, password: user.password },
      headers: { 'x-request-id': 'not ok; drop table'.repeat(20) },
    });

    const rows = await auditRows('auth.login');
    expect(rows[0]!.request_id).not.toContain('drop table');
    expect(rows[0]!.request_id!.length).toBeLessThanOrEqual(64);
  });

  it('does not fail the request when nothing can be audited', async () => {
    // The trail must never be a way to take the service down.
    const user = await createUser('USER');
    await withDb((client) =>
      client.query('ALTER TABLE auditEvent RENAME TO auditevent_hidden')
    );
    try {
      const response = await request('/auth/login', {
        method: 'POST',
        body: { emailOrUsername: user.email, password: user.password },
      });
      expect(response.status).toBe(200);
    } finally {
      await withDb((client) =>
        client.query('ALTER TABLE auditevent_hidden RENAME TO auditEvent')
      );
    }
  });
});
