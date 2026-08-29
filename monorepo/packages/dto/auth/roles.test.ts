import { describe, expect, it } from 'vitest';

import {
  CAPABILITIES,
  DEFAULT_ROLE,
  ROLES,
  ROLE_CAPABILITIES,
  hasCapability,
  isRole,
  rolesWithCapability,
  type Capability,
} from './roles';

describe('role model (ADR 0033)', () => {
  it('defaults to the least privileged role', () => {
    expect(DEFAULT_ROLE).toBe('USER');
    expect(ROLE_CAPABILITIES.USER).toEqual(['dashboard:read', 'scenario:run']);
  });

  it('grants every role a capability set drawn from the declared list', () => {
    for (const role of ROLES) {
      for (const capability of ROLE_CAPABILITIES[role]) {
        expect(CAPABILITIES).toContain(capability);
      }
    }
  });

  it('gives every role read access to the dashboard', () => {
    for (const role of ROLES) {
      expect(hasCapability(role, 'dashboard:read')).toBe(true);
    }
  });
});

describe('separation of operating and administrative authority', () => {
  it.each<Capability>(['control:issue', 'alarm:acknowledge'])(
    'withholds %s from ADMIN',
    (capability) => {
      // The heart of ADR 0033: the account that manages users cannot move
      // equipment. If this fails, the model has silently become hierarchical.
      expect(hasCapability('ADMIN', capability)).toBe(false);
      expect(hasCapability('OPERATIONS', capability)).toBe(true);
    }
  );

  it.each<Capability>(['user:manage', 'alarmRule:manage', 'asset:manage'])(
    'withholds %s from OPERATIONS',
    (capability) => {
      expect(hasCapability('OPERATIONS', capability)).toBe(false);
      expect(hasCapability('ADMIN', capability)).toBe(true);
    }
  );

  it('grants no privileged capability to USER', () => {
    const privileged: Capability[] = [
      'alarm:acknowledge',
      'control:issue',
      'user:manage',
      'alarmRule:manage',
      'asset:manage',
    ];
    for (const capability of privileged) {
      expect(hasCapability('USER', capability)).toBe(false);
    }
  });

  it('is not a hierarchy in either direction', () => {
    const ops = new Set(ROLE_CAPABILITIES.OPERATIONS);
    const admin = new Set(ROLE_CAPABILITIES.ADMIN);
    const adminHasAllOps = [...ops].every((c) => admin.has(c));
    const opsHasAllAdmin = [...admin].every((c) => ops.has(c));
    expect(adminHasAllOps).toBe(false);
    expect(opsHasAllAdmin).toBe(false);
  });

  it('reports which roles hold a capability', () => {
    expect(rolesWithCapability('control:issue')).toEqual(['OPERATIONS']);
    expect(rolesWithCapability('user:manage')).toEqual(['ADMIN']);
    expect(rolesWithCapability('scenario:run')).toEqual([
      'USER',
      'OPERATIONS',
      'ADMIN',
    ]);
  });
});

describe('isRole', () => {
  it.each(ROLES)('accepts %s', (role) => {
    expect(isRole(role)).toBe(true);
  });

  it.each([['user'], ['SUPERUSER'], [''], [null], [undefined], [42], [{}]])(
    'rejects %s',
    (value) => {
      expect(isRole(value)).toBe(false);
    }
  );
});
