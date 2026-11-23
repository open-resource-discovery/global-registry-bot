import { fileURLToPath } from 'node:url';
import { defineConfig } from 'vitest/config';

export default defineConfig({
  resolve: {
    alias: {
      src: fileURLToPath(new URL('./src', import.meta.url)),
      actions: fileURLToPath(new URL('./actions', import.meta.url)),
    },
  },
  test: {
    environment: 'node',
    globals: true,
    include: ['test/**/*.test.ts'],
    setupFiles: ['./vitest.setup.ts'],
    testTimeout: 30000,
    clearMocks: true,
    unstubEnvs: true,
    retry: 0,
    coverage: {
      provider: 'v8',
      include: ['src/**/*.ts'],
      exclude: ['test/**', 'types/**'],
      reportsDirectory: 'reports/jest-coverage',
      reporter: ['text', 'lcov'],
      thresholds: {
        branches: 65,
        functions: 90,
        lines: 90,
        statements: 80,
      },
    },
  },
});
