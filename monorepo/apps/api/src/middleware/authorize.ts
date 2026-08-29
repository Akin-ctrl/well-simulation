import type { Context, MiddlewareHandler } from 'hono';
import { HTTPException } from 'hono/http-exception';
import {
  DEFAULT_ROLE,
  hasCapability,
  isRole,
  type Capability,
  type Role,
} from '@corsight/dto/auth/roles';

/**
 * Capability checks for the model in ADR 0033.
 *
 * The session role is read from the verified JWT rather than the database, so
 * a role change does not take effect until the token is reissued. That is
 * acceptable for expanding access and deliberate for revoking it: session
 * revocation is tracked separately in the remediation roadmap.
 */

type SessionClaims = {
  sub: string;
  email?: string;
  role: Role;
};

function readSession(c: Context): SessionClaims {
  const session = c.get('session') as unknown;

  if (typeof session !== 'object' || session === null) {
    throw new HTTPException(401, { message: 'Unauthorized' });
  }

  const claims = session as Record<string, unknown>;
  if (typeof claims.sub !== 'string') {
    throw new HTTPException(401, { message: 'Unauthorized' });
  }

  return {
    sub: claims.sub,
    email: typeof claims.email === 'string' ? claims.email : undefined,
    // An unrecognised role must not silently widen access, so anything that is
    // not a known role falls back to the least-privileged one.
    role: isRole(claims.role) ? claims.role : DEFAULT_ROLE,
  };
}

export function sessionRole(c: Context): Role {
  return readSession(c).role;
}

export function sessionUserId(c: Context): number {
  const userId = Number.parseInt(readSession(c).sub, 10);
  if (!Number.isInteger(userId)) {
    throw new HTTPException(401, { message: 'Unauthorized' });
  }
  return userId;
}

/**
 * Require a capability on a route.
 *
 * The response deliberately does not name the roles that would be permitted:
 * that tells an unauthorised caller how the system is segmented.
 */
export function requireCapability(capability: Capability): MiddlewareHandler {
  return async (c, next) => {
    const { sub, role } = readSession(c);

    if (!hasCapability(role, capability)) {
      console.warn(
        JSON.stringify({
          level: 'warn',
          service: 'api',
          event: 'authorization_denied',
          capability,
          role,
          subject: sub,
          path: new URL(c.req.url).pathname,
        })
      );
      throw new HTTPException(403, { message: 'Forbidden' });
    }

    return next();
  };
}
