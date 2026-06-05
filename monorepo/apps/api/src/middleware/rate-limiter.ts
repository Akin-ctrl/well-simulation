import type { MiddlewareHandler } from 'hono';

type RateLimitStore = Map<string, { count: number; resetTime: number }>;

const rateLimitStore: RateLimitStore = new Map();

setInterval(() => {
  const now = Date.now();
  for (const [ip, entry] of rateLimitStore.entries()) {
    if (now > entry.resetTime) {
      rateLimitStore.delete(ip);
    }
  }
}, 60_000);

export function customRateLimit(props?: {
  windowMs?: number;
  limit?: number;
  message?: string;
}): MiddlewareHandler {
  const {
    windowMs = 60_000,
    limit = 100,
    message = 'Too many requests',
  } = props || {
    windowMs: 60_000,
    limit: 100,
    message: 'Too many requests',
  };

  return async (c, next) => {
    const ip =
      c.req.header('x-forwarded-for') ||
      c.req.header('cf-connecting-ip') ||
      c.req.header('x-real-ip') ||
      'unknown-ip';
    const now = Date.now();

    let entry = rateLimitStore.get(ip);

    if (!entry || now > entry.resetTime) {
      entry = { count: 1, resetTime: now + windowMs };
      rateLimitStore.set(ip, entry);
    } else {
      entry.count++;
    }

    if (entry.count > limit) {
      return c.text(message ?? 'Too many requests', 429);
    }

    return next();
  };
}
