import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { deleteTestUsers, request, signedInAs } from './helpers/api';

/**
 * Origin checks on state-changing requests.
 *
 * Sessions are cookie-based, so without this a cross-site form post would carry
 * the user's credentials. The guard compares Origin against the host the
 * request was addressed to, because behind the dashboard's nginx the API's own
 * URL carries the internal hostname.
 */
describe('origin guard', () => {
  beforeAll(deleteTestUsers);
  afterAll(deleteTestUsers);

  it('rejects a cross-site state-changing request', async () => {
    const { cookie } = await signedInAs('USER');
    const response = await request('/auth/logout', {
      method: 'POST',
      cookie,
      origin: 'http://attacker.example',
    });
    expect(response.status).toBe(403);
  });

  it('allows a same-origin state-changing request', async () => {
    const { cookie } = await signedInAs('USER');
    const response = await request('/auth/logout', {
      method: 'POST',
      cookie,
      origin: 'http://localhost',
    });
    expect(response.status).toBe(200);
  });

  it('allows a request with no Origin header', async () => {
    // Non-browser clients send none, and they are not the attack this guards.
    const { cookie } = await signedInAs('USER');
    const response = await request('/auth/logout', {
      method: 'POST',
      cookie,
      origin: null,
    });
    expect(response.status).toBe(200);
  });

  it('honours the forwarded host when behind a proxy', async () => {
    // nginx forwards the external host; the API's own URL is the internal one.
    const { cookie } = await signedInAs('USER');
    const response = await request('/auth/logout', {
      method: 'POST',
      cookie,
      origin: 'http://dashboard.example:8090',
      headers: { 'x-forwarded-host': 'dashboard.example:8090' },
    });
    expect(response.status).toBe(200);
  });

  it('does not guard safe methods', async () => {
    const { cookie } = await signedInAs('USER');
    const response = await request('/dashboard/overview', {
      cookie,
      origin: 'http://attacker.example',
    });
    expect(response.status).not.toBe(403);
  });

  it('rejects a malformed Origin rather than trusting it', async () => {
    const { cookie } = await signedInAs('USER');
    const response = await request('/auth/logout', {
      method: 'POST',
      cookie,
      origin: 'not-a-url',
    });
    expect(response.status).toBe(403);
  });
});
