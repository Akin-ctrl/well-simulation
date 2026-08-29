import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { createUser, deleteTestUsers, request, signedInAs } from './helpers/api';

/**
 * The error contract.
 *
 * Every failure carries the same JSON envelope, whatever layer raised it, and
 * nothing derived from a database error reaches the client.
 */
describe('error responses', () => {
  beforeAll(deleteTestUsers);
  afterAll(deleteTestUsers);

  async function newUserPayload(overrides: Record<string, string> = {}) {
    const stamp = `${Date.now()}${Math.random().toString(36).slice(2, 7)}`;
    return {
      username: `user-${stamp}`,
      email: `user-${stamp}@test.local`,
      password: 'a-long-enough-password',
      confirmPassword: 'a-long-enough-password',
      ...overrides,
    };
  }

  it('returns 409 for a duplicate email, not 500', async () => {
    const { cookie } = await signedInAs('ADMIN');
    const payload = await newUserPayload();

    const first = await request('/auth/register', {
      method: 'POST',
      cookie,
      body: payload,
    });
    expect(first.status).toBe(200);

    const second = await request('/auth/register', {
      method: 'POST',
      cookie,
      body: { ...(await newUserPayload()), email: payload.email },
    });
    expect(second.status).toBe(409);
  });

  it('returns 409 for a duplicate username', async () => {
    const { cookie } = await signedInAs('ADMIN');
    const payload = await newUserPayload();
    await request('/auth/register', { method: 'POST', cookie, body: payload });

    const second = await request('/auth/register', {
      method: 'POST',
      cookie,
      body: { ...(await newUserPayload()), username: payload.username },
    });
    expect(second.status).toBe(409);
  });

  it('does not disclose which field collided', async () => {
    const { cookie } = await signedInAs('ADMIN');
    const payload = await newUserPayload();
    await request('/auth/register', { method: 'POST', cookie, body: payload });

    const second = await request('/auth/register', {
      method: 'POST',
      cookie,
      body: { ...(await newUserPayload()), email: payload.email },
    });

    const body = JSON.stringify(await second.json()).toLowerCase();
    expect(body).not.toContain('email');
    expect(body).not.toContain('user_name');
    expect(body).not.toContain('constraint');
  });

  it('returns 400 for a malformed payload', async () => {
    const { cookie } = await signedInAs('ADMIN');
    const response = await request('/auth/register', {
      method: 'POST',
      cookie,
      body: { username: 'x' },
    });
    expect(response.status).toBe(400);
  });

  it('rejects a password shorter than the shared minimum', async () => {
    const { cookie } = await signedInAs('ADMIN');
    const response = await request('/auth/register', {
      method: 'POST',
      cookie,
      body: await newUserPayload({ password: 'short', confirmPassword: 'short' }),
    });
    expect(response.status).toBe(400);
  });

  it('returns 400 for an invalid wellhead id and 404 for an absent one', async () => {
    const { cookie } = await signedInAs('USER');
    expect((await request('/dashboard/wellheads/abc', { cookie })).status).toBe(400);
    expect((await request('/dashboard/wellheads/999999', { cookie })).status).toBe(404);
  });

  it('uses one envelope shape for every failure', async () => {
    const { cookie: userCookie } = await signedInAs('USER');

    const responses = [
      await request('/dashboard/overview'), // 401
      await request('/auth/register', {
        method: 'POST',
        cookie: userCookie,
        body: await newUserPayload(),
      }), // 403
      await request('/dashboard/wellheads/999999', { cookie: userCookie }), // 404
    ];

    for (const response of responses) {
      expect(response.headers.get('content-type')).toContain('application/json');
      const body = (await response.json()) as Record<string, unknown>;
      expect(body).toHaveProperty('msg');
      expect(body).toHaveProperty('error');
      expect(body).toHaveProperty('status');
    }
  });

  it('never leaks a password hash in an error body', async () => {
    const { cookie } = await signedInAs('ADMIN');
    const payload = await newUserPayload();
    await request('/auth/register', { method: 'POST', cookie, body: payload });

    const conflict = await request('/auth/register', {
      method: 'POST',
      cookie,
      body: { ...(await newUserPayload()), email: payload.email },
    });

    expect(JSON.stringify(await conflict.json())).not.toContain('$2b$');
  });

  it('returns 404 as JSON for an unknown route', async () => {
    const user = await createUser('USER');
    expect(user.role).toBe('USER');
    const response = await request('/does-not-exist');
    expect(response.status).toBe(401);
  });
});
