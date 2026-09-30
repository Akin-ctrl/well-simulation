import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    include: ['tests/**/*.test.ts'],
    globalSetup: ['tests/setup/global-setup.ts'],
    setupFiles: ['tests/setup/setup-file.ts'],
    // The suite shares one disposable database and the middleware keeps
    // module-level state, so parallel files would race each other.
    fileParallelism: false,
    testTimeout: 20_000,
    hookTimeout: 60_000,
  },
});
