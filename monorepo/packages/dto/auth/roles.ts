/**
 * The authorization model defined by ADR 0033.
 *
 * Operating authority and administrative authority are deliberately separate:
 * ADMIN does not inherit OPERATIONS. The account that manages users and edits
 * alarm thresholds cannot issue a control command to a wellhead.
 *
 * This is the single definition of who may do what. Routes declare the
 * capability they need rather than listing roles, so adding a role does not
 * mean auditing every route.
 */

export const ROLES = ['USER', 'OPERATIONS', 'ADMIN'] as const;

export type Role = (typeof ROLES)[number];

/** Applied when a record carries no role. */
export const DEFAULT_ROLE: Role = 'USER';

export const CAPABILITIES = [
  'dashboard:read',
  'scenario:run',
  'alarm:acknowledge',
  'control:issue',
  'user:manage',
  'alarmRule:manage',
  'asset:manage',
] as const;

export type Capability = (typeof CAPABILITIES)[number];

/**
 * The permission matrix from ADR 0033.
 *
 * Note what ADMIN does *not* hold: `alarm:acknowledge` and `control:issue`.
 * That omission is the decision, not an oversight. An administrator who must
 * operate the plant is granted OPERATIONS explicitly.
 */
export const ROLE_CAPABILITIES: Record<Role, readonly Capability[]> = {
  USER: ['dashboard:read', 'scenario:run'],
  OPERATIONS: ['dashboard:read', 'scenario:run', 'alarm:acknowledge', 'control:issue'],
  ADMIN: [
    'dashboard:read',
    'scenario:run',
    'user:manage',
    'alarmRule:manage',
    'asset:manage',
  ],
};

export function isRole(value: unknown): value is Role {
  return typeof value === 'string' && (ROLES as readonly string[]).includes(value);
}

export function hasCapability(role: Role, capability: Capability): boolean {
  return ROLE_CAPABILITIES[role].includes(capability);
}

/** Roles holding a capability. Useful for error messages and documentation. */
export function rolesWithCapability(capability: Capability): Role[] {
  return ROLES.filter((role) => hasCapability(role, capability));
}
