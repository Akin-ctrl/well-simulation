import { execFileSync } from 'node:child_process';
import { Client } from 'pg';

import { REPO_ROOT, TEST_DATABASE, databaseUrl, serverTarget } from './env';

/**
 * Create a disposable database, apply the canonical migrations, and drop it
 * again when the suite finishes.
 *
 * Migrations are applied by running `data/src/schema.py`, the same migrator the
 * compose stack uses, rather than by a second implementation living in the test
 * harness. That keeps one applier (Standard 6) and means the suite also
 * proves the migrations apply cleanly to an empty database.
 */

async function withMaintenanceClient<T>(
  fn: (client: Client) => Promise<T>
): Promise<T> {
  const { maintenanceDatabase } = serverTarget();
  const client = new Client({ connectionString: databaseUrl(maintenanceDatabase) });

  try {
    await client.connect();
  } catch (cause) {
    throw new Error(
      `Cannot reach PostgreSQL for API tests at ${serverTarget().host}:${serverTarget().port}.\n` +
        `Start it with:  docker compose up -d db\n` +
        `Original error: ${cause instanceof Error ? cause.message : String(cause)}`
    );
  }

  try {
    return await fn(client);
  } finally {
    await client.end();
  }
}

async function dropTestDatabase(client: Client): Promise<void> {
  // Terminate stragglers first: a leaked pool connection blocks DROP DATABASE.
  await client.query(
    'SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()',
    [TEST_DATABASE]
  );
  await client.query(`DROP DATABASE IF EXISTS "${TEST_DATABASE}"`);
}

function applyMigrations(): void {
  const { host, port, user, password } = serverTarget();
  const python = process.env.PYTHON_BIN ?? 'python3';

  execFileSync(python, ['data/src/schema.py'], {
    cwd: REPO_ROOT,
    stdio: process.env.TEST_MIGRATION_LOGS ? 'inherit' : 'pipe',
    env: {
      ...process.env,
      POSTGRES_HOST: host,
      POSTGRES_PORT: port,
      POSTGRES_USER: user,
      POSTGRES_PASSWORD: password,
      POSTGRES_DB: TEST_DATABASE,
    },
  });
}

export async function setup(): Promise<void> {
  await withMaintenanceClient(async (client) => {
    await dropTestDatabase(client);
    await client.query(`CREATE DATABASE "${TEST_DATABASE}"`);
  });

  try {
    applyMigrations();
  } catch (cause) {
    const detail =
      cause instanceof Error && 'stderr' in cause
        ? String((cause as { stderr?: Buffer }).stderr ?? '')
        : '';
    throw new Error(
      `Failed to apply migrations to the test database.\n` +
        `Ensure the migrator's dependencies are installed:  pip install -r requirements-dev.txt\n${detail}`
    );
  }
}

export async function teardown(): Promise<void> {
  if (process.env.TEST_KEEP_DATABASE) return;
  await withMaintenanceClient(dropTestDatabase);
}
