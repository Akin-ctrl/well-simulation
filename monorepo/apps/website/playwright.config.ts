import { defineConfig } from '@playwright/test';

/** Run against an already healthy local Compose stack. Model runs remain isolated. */
export default defineConfig({
  testDir: './e2e',
  timeout: 240_000,
  expect: { timeout: 15_000 },
  workers: 1,
  retries: 0,
  reporter: 'list',
  use: {
    actionTimeout: 15_000,
    navigationTimeout: 30_000,
    baseURL: process.env.DASHBOARD_TEST_URL ?? 'http://127.0.0.1:8090',
    viewport: { width: 1440, height: 1000 },
    trace: 'retain-on-failure',
  },
});
