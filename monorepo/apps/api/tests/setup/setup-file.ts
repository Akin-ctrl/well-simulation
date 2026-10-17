import { beforeEach } from 'vitest';

import { TEST_DATABASE, databaseUrl, loadRepoEnv } from './env';

/**
 * Configure the process before any application module is imported.
 *
 * `config.ts` validates JWT_SECRET and `@well-simulation/db/query` reads DATABASE_URL
 * at import time, so these must be set before a test file's imports evaluate.
 * Vitest runs setup files ahead of the test module, which is what makes this
 * work.
 */

loadRepoEnv();

process.env.DATABASE_URL = databaseUrl(TEST_DATABASE);
// Requests use the deployed grants; fixture setup keeps its separate admin connection.
const applicationUser = process.env.TEST_API_DB_USER ?? process.env.API_DB_USER;
if (!applicationUser || !process.env.API_DB_PASSWORD) {
  throw new Error(
    'API tests require API_DB_USER and API_DB_PASSWORD for a restricted application role'
  );
}
const applicationUrl = new URL(process.env.DATABASE_URL);
applicationUrl.username = applicationUser;
applicationUrl.password = process.env.API_DB_PASSWORD;
process.env.DATABASE_URL = applicationUrl.toString();
process.env.NODE_ENV = 'test';
process.env.JWT_SECRET =
  process.env.TEST_JWT_SECRET ?? 'test-secret-of-at-least-thirty-two-characters';
process.env.TWIN_INTERNAL_TOKEN =
  process.env.TEST_TWIN_INTERNAL_TOKEN ??
  'test-twin-token-of-at-least-thirty-two-characters';
process.env.COOKIE_SECURE = 'false';
process.env.TRUSTED_PROXY_IPS = '';
process.env.ALLOWED_ORIGINS = '';

// Rate limit state is module-level and would otherwise leak across tests: the
// auth budget is deliberately small, so a dozen login attempts in one file
// would start returning 429 to unrelated cases.
beforeEach(async () => {
  const { resetRateLimits } = await import('../../src/middleware/rate-limiter');
  resetRateLimits();
});
