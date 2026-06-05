import { sql } from 'drizzle-orm';
import type { AnyColumn } from 'drizzle-orm';
import { alias } from 'drizzle-orm/pg-core';
import { drizzle } from 'drizzle-orm/node-postgres';
import type { NodePgDatabase } from 'drizzle-orm/node-postgres';
import { schema } from './schemas';

declare global {
  var _db: NodePgDatabase<typeof schema>;
}

let db: NodePgDatabase<typeof schema>;
const databaseUrl = process.env.DATABASE_URL;

if (!databaseUrl) {
  throw new Error('DATABASE_URL is required');
}

if (process.env.NODE_ENV === 'production') {
  db = drizzle(databaseUrl, {
    schema,
  });
} else {
  if (!global._db)
    global._db = drizzle(databaseUrl, {
      schema,
    });

  db = global._db;
}

export * from 'drizzle-orm';

export { alias, db };

export const increment = (column: AnyColumn, value = 1) => {
  return sql`${column} + ${value}`;
};
