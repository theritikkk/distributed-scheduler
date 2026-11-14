import { defineConfig } from 'vitest/config';

const databaseUrl =
  process.env.TEST_DATABASE_URL ??
  process.env.DATABASE_URL ??
  'postgresql://scheduler:scheduler_secret@localhost:5432/distributed_scheduler_test';

export default defineConfig({
  test: {
    environment: 'node',
    include: ['tests/**/*.test.ts'],
    env: {
      NODE_ENV: 'test',
      JWT_SECRET: 'test-only-jwt-secret-change-me-32chars',
      DATABASE_URL: databaseUrl,
      RABBITMQ_URL: 'amqp://scheduler:scheduler_secret@localhost:5672',
      LOG_REQUESTS: 'false',
      SCHEDULER_JITTER_ENABLED: 'true',
      DEFAULT_RECURRING_JITTER_SEC: '0',
      MAX_JITTER_SEC: '300',
    },
    hookTimeout: 10_000,
    coverage: {
      provider: 'v8',
      reporter: ['text', 'lcov'],
      include: ['src/**/*.ts'],
      exclude: ['src/index.ts'],
      thresholds: {
        statements: 50,
        branches: 60,
        functions: 60,
        lines: 50,
      },
    },
  },
});
