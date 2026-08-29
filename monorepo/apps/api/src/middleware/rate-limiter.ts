import type { Context, MiddlewareHandler } from 'hono';
import { getConnInfo } from '@hono/node-server/conninfo';

/**
 * Fixed-window rate limiting keyed on client address.
 *
 * The previous implementation read `x-forwarded-for` unconditionally, so any
 * caller could spoof the header per request and never be limited. Everything
 * without the header shared a single `'unknown-ip'` bucket, so genuine direct
 * traffic throttled itself collectively.
 *
 * The header is now honoured only when the immediate peer is a configured
 * trusted proxy, which in this deployment is the nginx container that serves
 * the dashboard and proxies `/api`.
 *
 * State is per-process and in memory. That is sufficient for a single API
 * container; running more than one would need a shared store.
 */

type Bucket = { count: number; resetAt: number };

const WINDOW_MS = 60_000;
const DEFAULT_LIMIT = 100;
/** Credential endpoints get a tighter budget than dashboard reads. */
const AUTH_LIMIT = 10;
const SWEEP_INTERVAL_MS = 60_000;

const buckets = new Map<string, Bucket>();

const sweep = setInterval(() => {
  const now = Date.now();
  for (const [key, bucket] of buckets) {
    if (now > bucket.resetAt) buckets.delete(key);
  }
}, SWEEP_INTERVAL_MS);
// Do not hold the process open for a cache sweep.
sweep.unref?.();

/**
 * Clear all rate-limit state.
 *
 * The store is module-level, so without this the auth budget carries between
 * test files and unrelated cases start seeing 429. Not used by the running
 * service.
 */
export function resetRateLimits(): void {
  buckets.clear();
}

/**
 * Hosts permitted to speak for their clients via `x-forwarded-for`.
 *
 * Defaults to none: trusting a proxy that is not there is how the spoofing
 * problem arises in the first place.
 */
function trustedProxies(): Set<string> {
  return new Set(
    (process.env.TRUSTED_PROXY_IPS ?? '')
      .split(',')
      .map((value) => value.trim())
      .filter(Boolean)
  );
}

function peerAddress(c: Context): string | undefined {
  try {
    return getConnInfo(c).remote.address;
  } catch {
    return undefined;
  }
}

/**
 * Resolve the address to rate limit.
 *
 * Returns the peer address unless that peer is a trusted proxy, in which case
 * the first entry of `x-forwarded-for` is the real client.
 */
export function clientKey(c: Context): string {
  const peer = peerAddress(c);
  if (!peer) {
    // Without a peer address there is nothing trustworthy to key on. Fall back
    // to a shared bucket, which throttles conservatively rather than not at all.
    return 'unattributed';
  }

  if (!trustedProxies().has(peer)) {
    return peer;
  }

  const forwarded = c.req.header('x-forwarded-for');
  const client = forwarded?.split(',')[0]?.trim();
  return client && client.length > 0 ? client : peer;
}

export function customRateLimit(props?: {
  windowMs?: number;
  limit?: number;
  message?: string;
}): MiddlewareHandler {
  const {
    windowMs = WINDOW_MS,
    limit = DEFAULT_LIMIT,
    message = 'Too many requests',
  } = props ?? {};

  return async (c, next) => {
    const pathname = new URL(c.req.url).pathname;
    const effectiveLimit = pathname.startsWith('/auth/') ? AUTH_LIMIT : limit;
    const key = `${clientKey(c)}:${pathname.startsWith('/auth/') ? 'auth' : 'api'}`;
    const now = Date.now();

    let bucket = buckets.get(key);
    if (!bucket || now > bucket.resetAt) {
      bucket = { count: 0, resetAt: now + windowMs };
      buckets.set(key, bucket);
    }
    bucket.count += 1;

    const remaining = Math.max(0, effectiveLimit - bucket.count);
    c.header('RateLimit-Limit', String(effectiveLimit));
    c.header('RateLimit-Remaining', String(remaining));
    c.header('RateLimit-Reset', String(Math.ceil((bucket.resetAt - now) / 1000)));

    if (bucket.count > effectiveLimit) {
      c.header('Retry-After', String(Math.ceil((bucket.resetAt - now) / 1000)));
      return c.text(message, 429);
    }

    return next();
  };
}
