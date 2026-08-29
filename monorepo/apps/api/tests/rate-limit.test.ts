import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

/**
 * Rate limiting.
 *
 * The previous implementation read `x-forwarded-for` unconditionally, so any
 * caller could spoof it per request and never be limited, while all traffic
 * without the header shared one bucket. These tests pin both halves of the fix:
 * the header is honoured only from a configured proxy, and the auth endpoints
 * carry a tighter budget than dashboard reads.
 */

const remoteAddress = vi.hoisted(() => ({ value: '203.0.113.10' }));

vi.mock('@hono/node-server/conninfo', () => ({
  getConnInfo: () => ({ remote: { address: remoteAddress.value } }),
}));

const { clientKey, customRateLimit, resetRateLimits } = await import(
  '../src/middleware/rate-limiter'
);

/** Minimal Context stand-in exposing the two things clientKey reads. */
function contextWith(
  headers: Record<string, string>,
  url = 'http://localhost/auth/login'
) {
  return {
    req: {
      url,
      header: (name: string) => headers[name.toLowerCase()],
    },
  } as unknown as Parameters<typeof clientKey>[0];
}

describe('client attribution', () => {
  beforeEach(() => {
    resetRateLimits();
    remoteAddress.value = '203.0.113.10';
    process.env.TRUSTED_PROXY_IPS = '';
  });

  afterEach(() => {
    process.env.TRUSTED_PROXY_IPS = '';
  });

  it('keys on the peer address by default', () => {
    expect(clientKey(contextWith({}))).toBe('203.0.113.10');
  });

  it('ignores x-forwarded-for from an untrusted peer', () => {
    const key = clientKey(contextWith({ 'x-forwarded-for': '198.51.100.1' }));
    expect(key).toBe('203.0.113.10');
  });

  it('honours x-forwarded-for from a trusted proxy', () => {
    process.env.TRUSTED_PROXY_IPS = '203.0.113.10';
    const key = clientKey(contextWith({ 'x-forwarded-for': '198.51.100.1' }));
    expect(key).toBe('198.51.100.1');
  });

  it('takes the first entry of a forwarded chain', () => {
    process.env.TRUSTED_PROXY_IPS = '203.0.113.10';
    const key = clientKey(
      contextWith({ 'x-forwarded-for': '198.51.100.1, 10.0.0.1, 10.0.0.2' })
    );
    expect(key).toBe('198.51.100.1');
  });

  it('falls back to the peer when a trusted proxy sends an empty header', () => {
    process.env.TRUSTED_PROXY_IPS = '203.0.113.10';
    expect(clientKey(contextWith({ 'x-forwarded-for': '   ' }))).toBe('203.0.113.10');
  });

  it('separates two clients behind the same trusted proxy', () => {
    process.env.TRUSTED_PROXY_IPS = '203.0.113.10';
    const first = clientKey(contextWith({ 'x-forwarded-for': '198.51.100.1' }));
    const second = clientKey(contextWith({ 'x-forwarded-for': '198.51.100.2' }));
    expect(first).not.toBe(second);
  });
});

describe('budget enforcement', () => {
  beforeEach(() => {
    resetRateLimits();
    remoteAddress.value = '203.0.113.10';
    process.env.TRUSTED_PROXY_IPS = '';
  });

  /** Drive the middleware directly so the budget under test is explicit. */
  async function send(limit: number, path = '/dashboard/overview') {
    const middleware = customRateLimit({ limit, windowMs: 60_000 });
    const headers = new Map<string, string>();
    const context = {
      req: { url: `http://localhost${path}`, method: 'GET', header: () => undefined },
      header: (name: string, value: string) => headers.set(name, value),
      text: (body: string, status: number) => new Response(body, { status }),
    } as unknown as Parameters<typeof middleware>[0];

    const response = await middleware(context, async () => undefined);
    return { response, headers };
  }

  it('allows requests up to the limit', async () => {
    for (let i = 0; i < 3; i += 1) {
      const { response } = await send(3);
      expect(response).toBeUndefined();
    }
  });

  it('returns 429 once the limit is exceeded', async () => {
    for (let i = 0; i < 3; i += 1) await send(3);
    const { response } = await send(3);
    expect(response?.status).toBe(429);
  });

  it('advertises the remaining budget', async () => {
    const { headers } = await send(5);
    expect(headers.get('RateLimit-Limit')).toBe('5');
    expect(headers.get('RateLimit-Remaining')).toBe('4');
  });

  it('sets Retry-After when throttling', async () => {
    for (let i = 0; i < 2; i += 1) await send(1);
    const { headers } = await send(1);
    expect(Number(headers.get('Retry-After'))).toBeGreaterThan(0);
  });

  it('gives credential endpoints a tighter budget than dashboard reads', async () => {
    // The auth budget is 10; a dashboard budget of 100 must not apply there.
    for (let i = 0; i < 10; i += 1) {
      const { response } = await send(100, '/auth/login');
      expect(response).toBeUndefined();
    }
    const { response } = await send(100, '/auth/login');
    expect(response?.status).toBe(429);
  });

  it('keeps auth and dashboard budgets in separate buckets', async () => {
    for (let i = 0; i < 10; i += 1) await send(100, '/auth/login');
    const { response } = await send(100, '/dashboard/overview');
    expect(response).toBeUndefined();
  });
});
