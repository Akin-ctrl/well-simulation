import { describe, expect, it } from 'vitest';

import { request } from './helpers/api';

/**
 * Security headers are set by the API itself, not only by the dashboard's
 * nginx, so the service is not dependent on being deployed behind it.
 */
describe('security headers', () => {
  it('sets them on a successful response', async () => {
    const response = await request('/health');
    expect(response.headers.get('x-content-type-options')).toBe('nosniff');
    expect(response.headers.get('x-frame-options')).toBe('DENY');
    expect(response.headers.get('referrer-policy')).toBe('no-referrer');
    expect(response.headers.get('content-security-policy')).toContain(
      "frame-ancestors 'none'"
    );
  });

  it('sets them on an error response too', async () => {
    const response = await request('/dashboard/overview');
    expect(response.status).toBe(401);
    expect(response.headers.get('x-content-type-options')).toBe('nosniff');
  });
});
