import type { Context, MiddlewareHandler } from 'hono';
import { HTTPException } from 'hono/http-exception';

import { config } from '../config';

/**
 * Origin check for state-changing requests.
 *
 * Sessions are cookie-based, so a cross-site form post would otherwise carry
 * the user's credentials. `SameSite=Lax` blocks the common cases, but nothing
 * pinned the allowed origin, and Lax still permits top-level navigations.
 *
 * This is an Origin check rather than a synchroniser token because the API is
 * same-origin with the dashboard: nginx proxies `/api`, so a legitimate
 * state-changing request always carries a matching `Origin` or none at all
 * (same-origin GET-like navigations, and non-browser clients such as curl).
 */

const SAFE_METHODS = new Set(['GET', 'HEAD', 'OPTIONS']);

/**
 * Compare an Origin against the host this request was addressed to.
 *
 * Behind the dashboard's nginx the request URL carries the internal host, so
 * comparing against it would reject every legitimate same-origin request. The
 * forwarded host and the Host header both describe the address the browser
 * actually used.
 */
function isAllowed(origin: string, c: Context): boolean {
  if (config.allowedOrigins.includes(origin)) {
    return true;
  }

  let originHost: string;
  try {
    originHost = new URL(origin).host;
  } catch {
    return false;
  }

  const candidates = [
    c.req.header('x-forwarded-host'),
    c.req.header('host'),
    (() => {
      try {
        return new URL(c.req.url).host;
      } catch {
        return undefined;
      }
    })(),
  ].filter((value): value is string => Boolean(value));

  return candidates.includes(originHost);
}

export function originGuard(): MiddlewareHandler {
  return async (c, next) => {
    if (SAFE_METHODS.has(c.req.method)) {
      return next();
    }

    const origin = c.req.header('origin');

    // Browsers always send Origin on cross-site state-changing requests. Its
    // absence means a same-origin request or a non-browser client, neither of
    // which is the attack this guards against.
    if (origin && !isAllowed(origin, c)) {
      console.warn(
        JSON.stringify({
          level: 'warn',
          service: 'api',
          event: 'origin_rejected',
          origin,
          path: new URL(c.req.url).pathname,
        })
      );
      throw new HTTPException(403, { message: 'Forbidden' });
    }

    return next();
  };
}
