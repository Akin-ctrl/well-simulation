import { defineConfig } from 'drizzle-kit';

const databaseUrl = process.env.DATABASE_URL;

if (!databaseUrl) {
  throw new Error('DATABASE_URL is required');
}

export default defineConfig({
  dialect: 'postgresql',
  schema: './schemas/*',
  out: './drizzle',
  verbose: true,
  strict: true,
  dbCredentials: {
    url: databaseUrl,
  },
});
