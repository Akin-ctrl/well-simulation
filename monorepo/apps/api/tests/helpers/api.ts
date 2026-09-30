import bcryptjs from 'bcryptjs';
import { Client } from 'pg';

import { TEST_DATABASE, databaseUrl } from '../setup/env';
import { appRouter } from '../../src/_app';
import type { Role } from '@well-simulation/dto/auth/roles';

/**
 * Helpers for driving the API in tests.
 *
 * Requests go through `appRouter.request`, so every middleware the real service
 * mounts runs exactly as it does in production: security headers, rate
 * limiting, the origin guard, and authentication. Testing the routers in isolation would
 * skip the layers most worth covering.
 */

/** Matches the rounds used by the register route. */
const BCRYPT_ROUNDS = 11;

const ORIGIN = 'http://localhost';

export async function withDb<T>(fn: (client: Client) => Promise<T>): Promise<T> {
  const client = new Client({ connectionString: databaseUrl(TEST_DATABASE) });
  await client.connect();
  try {
    return await fn(client);
  } finally {
    await client.end();
  }
}

export type TestUser = {
  id: number;
  email: string;
  username: string;
  password: string;
  role: Role;
};

let sequence = 0;

/** Insert a user directly, bypassing the API, with a known password. */
export async function createUser(
  role: Role,
  password = 'test-password-1234'
): Promise<TestUser> {
  sequence += 1;
  const email = `user${sequence}@test.local`;
  const username = `user${sequence}`;
  const hash = await bcryptjs.hash(password, BCRYPT_ROUNDS);

  const id = await withDb(async (client) => {
    const result = await client.query<{ id: number }>(
      `INSERT INTO users (email, user_name, encrypted_password, role)
       VALUES ($1, $2, $3, $4) RETURNING id`,
      [email, username, hash, role]
    );
    return result.rows[0]!.id;
  });

  return { id, email, username, password, role };
}

/** Remove everything this suite created, leaving the seeded accounts alone. */
export async function deleteTestUsers(): Promise<void> {
  await withDb((client) =>
    client.query(`DELETE FROM users WHERE email LIKE '%@test.local'`)
  );
}

type RequestOptions = {
  method?: string;
  body?: unknown;
  cookie?: string;
  origin?: string | null;
  headers?: Record<string, string>;
};

export async function request(
  path: string,
  options: RequestOptions = {}
): Promise<Response> {
  const { method = 'GET', body, cookie, origin = ORIGIN, headers = {} } = options;

  const finalHeaders: Record<string, string> = { ...headers };
  if (body !== undefined) finalHeaders['content-type'] = 'application/json';
  if (cookie) finalHeaders.cookie = cookie;
  if (origin !== null) finalHeaders.origin = origin;

  // Hono's request() is typed as sync-or-async; await normalises it.
  return await appRouter.request(`${ORIGIN}${path}`, {
    method,
    headers: finalHeaders,
    body: body === undefined ? undefined : JSON.stringify(body),
  });
}

/** Extract the session cookie from a login response. */
export function sessionCookie(response: Response): string {
  const header = response.headers.get('set-cookie');
  if (!header) throw new Error('Response carried no Set-Cookie header');
  return header.split(';')[0]!;
}

/** Log in and return the cookie a subsequent request should carry. */
export async function login(user: TestUser): Promise<string> {
  const response = await request('/auth/login', {
    method: 'POST',
    body: { emailOrUsername: user.email, password: user.password },
  });

  if (response.status !== 200) {
    throw new Error(`Login failed for ${user.email}: ${response.status}`);
  }
  return sessionCookie(response);
}

/** Create a user of the given role and return a ready-to-use session cookie. */
export async function signedInAs(
  role: Role
): Promise<{ user: TestUser; cookie: string }> {
  const user = await createUser(role);
  return { user, cookie: await login(user) };
}
