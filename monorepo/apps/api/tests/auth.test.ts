import { sign } from 'hono/jwt';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { config } from '../src/config';

import {
  createUser,
  deleteTestUsers,
  login,
  request,
  sessionCookie,
} from './helpers/api';

describe('authentication', () => {
  beforeAll(deleteTestUsers);
  afterAll(deleteTestUsers);

  it('reports health without a session', async () => {
    const response = await request('/health');
    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toMatchObject({ status: 'ok' });
  });

  it('rejects an unauthenticated dashboard read', async () => {
    const response = await request('/dashboard/overview');
    expect(response.status).toBe(401);
  });

  it('issues an httpOnly session cookie on login', async () => {
    const user = await createUser('USER');
    const response = await request('/auth/login', {
      method: 'POST',
      body: { emailOrUsername: user.email, password: user.password },
    });

    expect(response.status).toBe(200);
    const header = response.headers.get('set-cookie') ?? '';
    expect(header).toContain('HttpOnly');
    expect(header).toContain('SameSite=Lax');
  });

  it('accepts a username as well as an email', async () => {
    const user = await createUser('USER');
    const response = await request('/auth/login', {
      method: 'POST',
      body: { emailOrUsername: user.username, password: user.password },
    });
    expect(response.status).toBe(200);
  });

  it('never returns the password hash', async () => {
    const user = await createUser('USER');
    const response = await request('/auth/login', {
      method: 'POST',
      body: { emailOrUsername: user.email, password: user.password },
    });

    const body = JSON.stringify(await response.json());
    expect(body).not.toContain('encryptedPassword');
    expect(body).not.toContain('$2b$');
  });

  it('rejects a wrong password without revealing whether the user exists', async () => {
    const user = await createUser('USER');

    const wrongPassword = await request('/auth/login', {
      method: 'POST',
      body: { emailOrUsername: user.email, password: 'not-the-password' },
    });
    const noSuchUser = await request('/auth/login', {
      method: 'POST',
      body: { emailOrUsername: 'nobody@test.local', password: 'not-the-password' },
    });

    expect(wrongPassword.status).toBe(401);
    expect(noSuchUser.status).toBe(401);
    expect(await wrongPassword.json()).toEqual(await noSuchUser.json());
  });

  it('returns the current user for a valid session', async () => {
    const user = await createUser('OPERATIONS');
    const cookie = await login(user);

    const response = await request('/auth/me', { cookie });
    expect(response.status).toBe(200);

    const body = (await response.json()) as { data: { user: { email: string } } };
    expect(body.data.user.email).toBe(user.email);
  });

  it('rejects a tampered session token', async () => {
    const user = await createUser('USER');
    const cookie = await login(user);
    const tampered = `${cookie.slice(0, -2)}xx`;

    const response = await request('/auth/me', { cookie: tampered });
    expect(response.status).toBe(401);
  });

  it('clears the cookie on logout', async () => {
    const user = await createUser('USER');
    const cookie = await login(user);

    const response = await request('/auth/logout', { method: 'POST', cookie });
    expect(response.status).toBe(200);
    expect(sessionCookie(response)).toMatch(/auth_token=$/);
  });

  it('requires a session to log out', async () => {
    const response = await request('/auth/logout', { method: 'POST' });
    expect(response.status).toBe(401);
  });
});

describe('session revocation (ADR 0037)', () => {
  beforeAll(deleteTestUsers);
  afterAll(deleteTestUsers);

  it('ends every session of the user on logout, including copies', async () => {
    const user = await createUser('USER');
    const first = await login(user);
    // A second device, or a copied cookie: both must stop working.
    const second = await login(user);

    const logout = await request('/auth/logout', { method: 'POST', cookie: first });
    expect(logout.status).toBe(200);

    expect((await request('/auth/me', { cookie: first })).status).toBe(401);
    expect((await request('/auth/me', { cookie: second })).status).toBe(401);
    expect((await request('/dashboard/overview', { cookie: second })).status).toBe(401);
  });

  it('lets the user sign in again after logging out', async () => {
    const user = await createUser('USER');
    const before = await login(user);
    await request('/auth/logout', { method: 'POST', cookie: before });

    const after = await login(user);
    expect((await request('/auth/me', { cookie: after })).status).toBe(200);
  });

  it('refuses a correctly signed token that carries no session version', async () => {
    // Shaped like a token issued before ADR 0037: valid signature, no `sv`.
    const user = await createUser('USER');
    const token = await sign(
      {
        sub: String(user.id),
        email: user.email,
        role: user.role,
        exp: Math.floor(Date.now() / 1000) + 3600,
      },
      config.jwtSecret,
      config.jwtAlgorithm
    );

    const response = await request('/auth/me', { cookie: `auth_token=${token}` });
    expect(response.status).toBe(401);
  });

  it('does not expose the session counter', async () => {
    const user = await createUser('USER');
    const cookie = await login(user);

    const body = (await (await request('/auth/me', { cookie })).json()) as {
      data: { user: Record<string, unknown> };
    };
    expect(body.data.user).not.toHaveProperty('sessionVersion');
    expect(body.data.user).not.toHaveProperty('encryptedPassword');
  });
});
