import type { MiddlewareHandler } from 'hono';
import { deleteCookie, getCookie } from 'hono/cookie';
import { verify } from 'hono/jwt';
import { HTTPException } from 'hono/http-exception';
import { db, eq } from '@well-simulation/db/query';
import { users } from '@well-simulation/db/schemas/user';

import { config } from '../config';

/**
 * Routes reachable without a session.
 *
 * `/auth/register` is deliberately absent: ADR 0033 makes creating a user an
 * administrative action. `/auth/logout` requires a session too, so clearing a
 * cookie cannot be triggered by an unauthenticated caller.
 */
const publicRoutes: string[] = ['/auth/login', '/metrics'];

/**
 * Whether a correctly signed token is still current (ADR 0037).
 *
 * A signature only proves the API issued the token. Logout moves the user's
 * session_version on, so a token issued before that, including a copied
 * cookie, no longer matches. A token with no version predates this check and
 * is refused. A deleted user has no row and is refused too.
 */
async function isCurrentSession(payload: Record<string, unknown>): Promise<boolean> {
  const userId = Number.parseInt(String(payload.sub), 10);
  if (!Number.isInteger(userId) || typeof payload.sv !== 'number') {
    return false;
  }

  const [row] = await db
    .select({ sessionVersion: users.sessionVersion })
    .from(users)
    .where(eq(users.id, userId))
    .limit(1);
  return row?.sessionVersion === payload.sv;
}

export const authMiddleware: MiddlewareHandler = async (c, next) => {
  const token = getCookie(c, 'auth_token');

  const url = new URL(c.req.url);
  const pathname = url.pathname ?? '';

  const isPublic = publicRoutes.some((route) => {
    if (route.endsWith('/*')) {
      const base = route.replace('/*', '');
      return pathname === base || pathname?.startsWith(`${base}/`);
    }
    return pathname === route;
  });

  if (!token && !isPublic) {
    throw new HTTPException(401, { message: 'Unauthorized' });
  }

  if (token) {
    let payload: Record<string, unknown> | undefined;
    try {
      payload = await verify(token, config.jwtSecret, config.jwtAlgorithm);
    } catch {
      payload = undefined;
    }

    // A database error is not caught here. Refusing the request is safer than
    // letting a session through without knowing whether it was revoked.
    if (!payload || !(await isCurrentSession(payload))) {
      deleteCookie(c, 'auth_token');
      if (isPublic) {
        return next();
      }
      throw new HTTPException(401, { message: 'Unauthorized' });
    }

    c.set('session', payload);
  }

  return next();
};
