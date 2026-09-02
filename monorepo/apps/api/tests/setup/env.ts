import { existsSync, readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

/**
 * Test environment derivation, shared by the global setup and the per-file
 * setup so both agree on which database the suite is talking to.
 *
 * These tests run against a real PostgreSQL/TimescaleDB instance. The dashboard
 * queries continuous aggregates and the alarm evaluation lives in a trigger, so
 * a mocked database would verify almost nothing that matters (Standard 8 asks
 * for tests reflecting the real backing systems).
 */

const HERE = dirname(fileURLToPath(import.meta.url));
export const REPO_ROOT = resolve(HERE, '../../../../..');

/**
 * Populate process.env from the repository .env when a value is absent.
 *
 * Locally the database credentials live there; in CI they are supplied as real
 * environment variables and this is a no-op. Existing values always win.
 */
export function loadRepoEnv(): void {
  const envPath = resolve(REPO_ROOT, '.env');
  if (!existsSync(envPath)) return;

  for (const line of readFileSync(envPath, 'utf8').split('\n')) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) continue;

    const eq = trimmed.indexOf('=');
    if (eq === -1) continue;

    const key = trimmed.slice(0, eq).trim();
    const value = trimmed.slice(eq + 1).trim();
    if (process.env[key] === undefined) process.env[key] = value;
  }
}

export type DatabaseTarget = {
  host: string;
  port: string;
  user: string;
  password: string;
  database: string;
};

/** Connection details for the server hosting the test database. */
export function serverTarget(): Omit<DatabaseTarget, 'database'> & {
  maintenanceDatabase: string;
} {
  loadRepoEnv();
  return {
    host: process.env.TEST_POSTGRES_HOST ?? 'localhost',
    port: process.env.TEST_POSTGRES_PORT ?? '5434',
    user: process.env.POSTGRES_USER ?? 'admin',
    password: process.env.POSTGRES_PASSWORD ?? '',
    maintenanceDatabase: 'postgres',
  };
}

/**
 * The disposable database this run uses.
 *
 * Fixed rather than random so a crashed run leaves exactly one stale database
 * to clean up instead of accumulating them, and so the name is obvious when
 * inspecting the server by hand.
 */
export const TEST_DATABASE =
  process.env.TEST_DATABASE_NAME ?? 'well_simulation_api_test';

export function databaseUrl(database: string): string {
  const { host, port, user, password } = serverTarget();
  return `postgres://${user}:${encodeURIComponent(password)}@${host}:${port}/${database}`;
}
