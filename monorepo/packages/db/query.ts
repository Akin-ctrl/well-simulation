import { AnyColumn, sql } from 'drizzle-orm';
import { alias } from 'drizzle-orm/pg-core';
import { drizzle, NodePgDatabase } from 'drizzle-orm/node-postgres';
import { schema } from './schemas';

declare global {
  var _db: NodePgDatabase<typeof schema>;
}

let db: NodePgDatabase<typeof schema>;

if (process.env.NODE_ENV === 'production') {
  db = drizzle(process.env.DATABASE_URL!, {
    schema,
  });
} else {
  if (!global._db)
    global._db = drizzle(process.env.DATABASE_URL!, {
      schema,
    });

  db = global._db;
}

export * from 'drizzle-orm';

export { alias, db };

export const increment = (column: AnyColumn, value = 1) => {
  return sql`${column} + ${value}`;
};
