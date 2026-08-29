import type { MiddlewareHandler } from 'hono';
import { deleteCookie, getCookie } from 'hono/cookie';
import { verify } from 'hono/jwt';
import { HTTPException } from 'hono/http-exception';

import { config } from '../config';

/**
 * Routes reachable without a session.
 *
 * `/auth/register` is deliberately absent: ADR 0033 makes creating a user an
 * administrative action. `/auth/logout` requires a session too, so clearing a
 * cookie cannot be triggered by an unauthenticated caller.
 */
const publicRoutes: string[] = ['/auth/login'];

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
    try {
      const payload = await verify(token, config.jwtSecret);
      c.set('session', payload);
    } catch {
      deleteCookie(c, 'auth_token');
      if (isPublic) {
        return next();
      }
      throw new HTTPException(401, { message: 'Unauthorized' });
    }
  }

  return next();
};
