import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import {
  CAPABILITIES,
  ROLES,
  ROLE_CAPABILITIES,
  hasCapability,
  isRole,
  rolesWithCapability,
  type Capability,
  type Role,
} from '@corsight/dto/auth/roles';

import { deleteTestUsers, request, signedInAs } from './helpers/api';

/**
 * The authorization model from ADR 0033.
 *
 * The route tests are driven from ROLE_CAPABILITIES rather than repeating the
 * matrix, so adding a role cannot leave a route silently untested. The matrix
 * itself is pinned separately below against the table in the ADR.
 */

/** Routes that declare a capability, and how to exercise them. */
const GUARDED_ROUTES: ReadonlyArray<{
  capability: Capability;
  description: string;
  send: (cookie: string) => Promise<Response>;
}> = [
  {
    capability: 'dashboard:read',
    description: 'GET /dashboard/overview',
    send: (cookie) => request('/dashboard/overview', { cookie }),
  },
  {
    capability: 'user:manage',
    description: 'POST /auth/register',
    send: (cookie) =>
      request('/auth/register', {
        method: 'POST',
        cookie,
        body: {
          username: `made-by-${Date.now()}`,
          email: `made-by-${Date.now()}@test.local`,
          password: 'a-long-enough-password',
          confirmPassword: 'a-long-enough-password',
        },
      }),
  },
];

describe('authorization', () => {
  beforeAll(deleteTestUsers);
  afterAll(deleteTestUsers);

  describe('the capability matrix matches ADR 0033', () => {
    it('separates operating authority from administrative authority', () => {
      // The decision ADR 0033 records: an administrator cannot operate the
      // plant, and an operator cannot manage users.
      expect(hasCapability('ADMIN', 'control:issue')).toBe(false);
      expect(hasCapability('ADMIN', 'alarm:acknowledge')).toBe(false);
      expect(hasCapability('OPERATIONS', 'user:manage')).toBe(false);
      expect(hasCapability('OPERATIONS', 'control:issue')).toBe(true);
      expect(hasCapability('ADMIN', 'user:manage')).toBe(true);
    });

    it('gives every role the baseline read capabilities', () => {
      for (const role of ROLES) {
        expect(hasCapability(role, 'dashboard:read')).toBe(true);
      }
    });

    it('grants no capability outside the declared set', () => {
      for (const granted of Object.values(ROLE_CAPABILITIES)) {
        for (const capability of granted) {
          expect(CAPABILITIES).toContain(capability);
        }
      }
    });

    it('leaves no capability unreachable by every role', () => {
      for (const capability of CAPABILITIES) {
        expect(rolesWithCapability(capability).length).toBeGreaterThan(0);
      }
    });
  });

  describe('unrecognised roles', () => {
    it('does not treat an unknown value as a role', () => {
      expect(isRole('SUPERUSER')).toBe(false);
      expect(isRole(undefined)).toBe(false);
      expect(isRole('ADMIN')).toBe(true);
    });
  });

  describe.each(GUARDED_ROUTES)(
    '$description requires $capability',
    ({ capability, send }) => {
      const permitted = ROLES.filter((role) => hasCapability(role, capability));
      const denied = ROLES.filter((role) => !hasCapability(role, capability));

      it.each(permitted)('allows %s', async (role: Role) => {
        const { cookie } = await signedInAs(role);
        const response = await send(cookie);
        expect(response.status).not.toBe(403);
        expect(response.status).not.toBe(401);
      });

      it.each(denied)('denies %s with 403', async (role: Role) => {
        const { cookie } = await signedInAs(role);
        const response = await send(cookie);
        expect(response.status).toBe(403);
      });

      it('denies an unauthenticated caller with 401', async () => {
        const response = await send('');
        expect(response.status).toBe(401);
      });
    }
  );

  it('does not disclose which roles would be permitted', async () => {
    const { cookie } = await signedInAs('USER');
    const response = await request('/auth/register', {
      method: 'POST',
      cookie,
      body: {
        username: 'escalate',
        email: 'escalate@test.local',
        password: 'a-long-enough-password',
        confirmPassword: 'a-long-enough-password',
      },
    });

    expect(response.status).toBe(403);
    const body = JSON.stringify(await response.json());
    expect(body).not.toContain('ADMIN');
    expect(body).not.toContain('user:manage');
  });
});
