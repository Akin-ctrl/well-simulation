import type { MiddlewareHandler } from 'hono';
import { deleteCookie, getCookie } from 'hono/cookie';
import { verify } from 'hono/jwt';

const publicRoutes: string[] = ['/auth/*'];

export const authMiddleware: MiddlewareHandler = async (c, next) => {
  const cookieToken = getCookie(c, 'auth_token');
  const token = cookieToken ?? c.req.header('Authorization')?.split(' ')[1];

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
    return c.json({ msg: 'Unauthorized' }, 401);
  }

  if (token) {
    try {
      const payload = await verify(token, process.env.JWT_SECRET!);
      c.set('session', payload);
    } catch (e) {
      deleteCookie(c, 'auth_token');
      return c.json({ msg: 'Unauthorized: Invalid or expired token' }, 401);
    }
  }

  await next();
};
