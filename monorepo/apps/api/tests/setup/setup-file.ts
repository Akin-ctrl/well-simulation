import { beforeEach } from 'vitest';

import { TEST_DATABASE, databaseUrl, loadRepoEnv } from './env';

/**
 * Configure the process before any application module is imported.
 *
 * `config.ts` validates JWT_SECRET and `@corsight/db/query` reads DATABASE_URL
 * at import time, so these must be set before a test file's imports evaluate.
 * Vitest runs setup files ahead of the test module, which is what makes this
 * work.
 */

loadRepoEnv();

process.env.DATABASE_URL = databaseUrl(TEST_DATABASE);
process.env.NODE_ENV = 'test';
process.env.JWT_SECRET =
  process.env.TEST_JWT_SECRET ?? 'test-secret-of-at-least-thirty-two-characters';
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
