import { defineConfig } from 'drizzle-kit';

/**
 * Drizzle is the typed read layer, not the schema source.
 *
 * ADR 0006 makes the SQL migrations in `data/sql/migrations` canonical. They
 * are applied by the `migrator` compose service, which owns hypertables,
 * continuous aggregates, and retention policies. Drizzle cannot express any of
 * those. The generated migration journal that used to live in `./drizzle`
 * has been removed: it was never applied, because Compose only ever ran the
 * SQL bootstrap, and it defined a `users` table the SQL schema also defined.
 *
 * This config exists only for introspection and `drizzle-kit studio`. There is
 * deliberately no `out` directory and no generate or migrate script, so
 * drizzle-kit cannot emit DDL that would compete with the migrations.
 */

const databaseUrl = process.env.DATABASE_URL;

if (!databaseUrl) {
  throw new Error('DATABASE_URL is required');
}

export default defineConfig({
  dialect: 'postgresql',
  schema: './schemas/*',
  verbose: true,
  strict: true,
  dbCredentials: {
    url: databaseUrl,
  },
});
