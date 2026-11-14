import { describe, expect, it } from 'vitest';
import { ConfigError, loadConfig } from '../src/config';

const baseDevEnv = {
  NODE_ENV: 'development',
  DATABASE_URL: 'postgresql://scheduler:scheduler_secret@localhost:5432/scheduler',
  RABBITMQ_URL: 'amqp://scheduler:scheduler_secret@localhost:5672',
  JWT_SECRET: 'test-only-jwt-secret-change-me-32chars',
};

describe('loadConfig', () => {
  it('loads a valid development configuration', () => {
    const config = loadConfig({ ...baseDevEnv, PORT: '4000' });

    expect(config.nodeEnv).toBe('development');
    expect(config.port).toBe(4000);
    expect(config.isProduction).toBe(false);
    expect(config.rateLimitMax).toBe(10_000);
    expect(config.idempotencyEnforce).toBe(true);
    expect(config.outboxEnabled).toBe(true);
    expect(config.rabbitmqQueueType).toBe('classic');
  });

  it('rejects missing required values in production', () => {
    expect(() => loadConfig({ NODE_ENV: 'production' })).toThrow(ConfigError);
  });

  it('rejects short JWT secrets', () => {
    expect(() =>
      loadConfig({ ...baseDevEnv, JWT_SECRET: 'too-short' }),
    ).toThrow(/at least 32/);
  });

  it('rejects documented default JWT secrets in production', () => {
    expect(() =>
      loadConfig({
        NODE_ENV: 'production',
        DATABASE_URL: baseDevEnv.DATABASE_URL,
        RABBITMQ_URL: baseDevEnv.RABBITMQ_URL,
        JWT_SECRET: 'your_long_random_secret_here_min_32_chars',
      }),
    ).toThrow(/default value/);
  });

  it('rejects invalid integer values', () => {
    expect(() => loadConfig({ ...baseDevEnv, PORT: 'not-a-port' })).toThrow(
      /integer/,
    );
    expect(() => loadConfig({ ...baseDevEnv, PORT: '70000' })).toThrow(
      /between/,
    );
  });

  it('parses trust proxy and boolean values', () => {
    const config = loadConfig({ ...baseDevEnv, TRUST_PROXY: '1', LOG_REQUESTS: 'false' });

    expect(config.trustProxy).toBe(1);
    expect(config.logRequests).toBe(false);
  });

  it('parses scheduling jitter settings', () => {
    const config = loadConfig({
      ...baseDevEnv,
      SCHEDULER_JITTER_ENABLED: 'true',
      MAX_JITTER_SEC: '120',
      DEFAULT_RECURRING_JITTER_SEC: '30',
    });

    expect(config.schedulerJitterEnabled).toBe(true);
    expect(config.maxJitterSec).toBe(120);
    expect(config.defaultRecurringJitterSec).toBe(30);
  });

  it('validates rate-limit mode and admission thresholds', () => {
    const config = loadConfig({
      ...baseDevEnv,
      RATE_LIMIT_ENABLED: 'true',
      RATE_LIMIT_MODE: 'enforce',
      REDIS_URL: 'redis://localhost:6379/0',
      ADMISSION_ENABLED: 'true',
      DB_POOL_HIGH_WATERMARK: '0.75',
    });

    expect(config.rateLimitEnabled).toBe(true);
    expect(config.rateLimitMode).toBe('enforce');
    expect(config.redisUrl).toBe('redis://localhost:6379/0');
    expect(config.admissionEnabled).toBe(true);
    expect(config.dbPoolHighWatermark).toBe(0.75);

    expect(() =>
      loadConfig({ ...baseDevEnv, RATE_LIMIT_MODE: 'drop' }),
    ).toThrow(/RATE_LIMIT_MODE/);

    expect(() =>
      loadConfig({
        ...baseDevEnv,
        QUEUE_BACKLOG_HIGH: '100',
        QUEUE_BACKLOG_LOW: '100',
      }),
    ).toThrow(/QUEUE_BACKLOG_LOW/);
  });

  it('parses Redis Cluster seed nodes and prefers them over REDIS_URL', () => {
    const config = loadConfig({
      ...baseDevEnv,
      RATE_LIMIT_ENABLED: 'true',
      REDIS_URL: 'redis://localhost:6379/0',
      REDIS_CLUSTER_NODES:
        '10.0.0.1:7001, 10.0.0.2:7002,10.0.0.1:7001,[::1]:7003',
    });

    expect(config.redisUrl).toBe('redis://localhost:6379/0');
    expect(config.redisClusterNodes).toEqual([
      { host: '10.0.0.1', port: 7001 },
      { host: '10.0.0.2', port: 7002 },
      { host: '::1', port: 7003 },
    ]);
  });

  it('rejects malformed Redis Cluster seed nodes', () => {
    expect(() =>
      loadConfig({ ...baseDevEnv, REDIS_CLUSTER_NODES: 'redis-1' }),
    ).toThrow(/host:port/);
    expect(() =>
      loadConfig({ ...baseDevEnv, REDIS_CLUSTER_NODES: 'redis-1:0' }),
    ).toThrow(/port/);
    expect(() =>
      loadConfig({ ...baseDevEnv, REDIS_CLUSTER_NODES: 'redis-1:70000' }),
    ).toThrow(/port/);
  });

  it('can disable idempotency enforcement for rollback', () => {
    const config = loadConfig({
      ...baseDevEnv,
      IDEMPOTENCY_ENFORCE: 'false',
    });

    expect(config.idempotencyEnforce).toBe(false);
  });

  it('can disable the outbox for direct-confirm rollback', () => {
    const config = loadConfig({
      ...baseDevEnv,
      OUTBOX_ENABLED: 'false',
    });

    expect(config.outboxEnabled).toBe(false);
  });

  it('requires the outbox for quorum queue mode', () => {
    const config = loadConfig({
      ...baseDevEnv,
      RABBITMQ_QUEUE_TYPE: 'quorum',
    });
    expect(config.rabbitmqQueueType).toBe('quorum');

    expect(() =>
      loadConfig({
        ...baseDevEnv,
        RABBITMQ_QUEUE_TYPE: 'quorum',
        OUTBOX_ENABLED: 'false',
      }),
    ).toThrow(/OUTBOX_ENABLED/);
  });

  it('rejects a recurring jitter default larger than the maximum', () => {
    expect(() =>
      loadConfig({
        ...baseDevEnv,
        MAX_JITTER_SEC: '10',
        DEFAULT_RECURRING_JITTER_SEC: '30',
      }),
    ).toThrow(/between/);
  });

  it('defaults Phase 7 features to disabled with a 16-shard ingress limiter', () => {
    const config = loadConfig(baseDevEnv);

    expect(config.cacheEnabled).toBe(false);
    expect(config.cacheTtlSec).toBe(15);
    expect(config.idempotencyCacheEnabled).toBe(false);
    expect(config.tenantQuotasEnabled).toBe(false);
    expect(config.globalIngressShards).toBe(16);
    expect(config.otelEnabled).toBe(false);
    expect(config.otelSamplerArg).toBe(0.1);
  });

  it('parses Phase 7 cache, quota, and tracing settings', () => {
    const config = loadConfig({
      ...baseDevEnv,
      REDIS_URL: 'redis://localhost:6379/0',
      CACHE_ENABLED: 'true',
      CACHE_TTL_SEC: '30',
      CACHE_REDIS_TIMEOUT_MS: '25',
      IDEMPOTENCY_CACHE_ENABLED: 'true',
      IDEMPOTENCY_CACHE_TTL_SEC: '120',
      TENANT_QUOTAS_ENABLED: 'true',
      TENANT_QUOTA_CACHE_TTL_SEC: '10',
      TENANT_DEFAULT_WEIGHT: '1.5',
      TENANT_DEFAULT_TASKS_PER_MIN: '120',
      TENANT_DEFAULT_BURST: '20',
      GLOBAL_INGRESS_SHARDS: '8',
      OTEL_ENABLED: 'true',
      OTEL_SERVICE_NAME: 'scheduler-api-test',
      OTEL_EXPORTER_OTLP_ENDPOINT: 'http://collector:4318/v1/traces',
      OTEL_SAMPLER_ARG: '0.5',
    });

    expect(config.cacheEnabled).toBe(true);
    expect(config.cacheTtlSec).toBe(30);
    expect(config.cacheRedisTimeoutMs).toBe(25);
    expect(config.idempotencyCacheEnabled).toBe(true);
    expect(config.idempotencyCacheTtlSec).toBe(120);
    expect(config.tenantQuotasEnabled).toBe(true);
    expect(config.tenantQuotaCacheTtlSec).toBe(10);
    expect(config.tenantDefaultWeight).toBe(1.5);
    expect(config.tenantDefaultTasksPerMin).toBe(120);
    expect(config.tenantDefaultBurst).toBe(20);
    expect(config.globalIngressShards).toBe(8);
    expect(config.otelEnabled).toBe(true);
    expect(config.otelServiceName).toBe('scheduler-api-test');
    expect(config.otelExporterEndpoint).toBe(
      'http://collector:4318/v1/traces',
    );
    expect(config.otelSamplerArg).toBe(0.5);
  });

  it('rejects out-of-range Phase 7 settings', () => {
    expect(() =>
      loadConfig({ ...baseDevEnv, GLOBAL_INGRESS_SHARDS: '0' }),
    ).toThrow(/GLOBAL_INGRESS_SHARDS/);
    expect(() =>
      loadConfig({ ...baseDevEnv, OTEL_SAMPLER_ARG: '1.5' }),
    ).toThrow(/OTEL_SAMPLER_ARG/);
    expect(() =>
      loadConfig({ ...baseDevEnv, TENANT_DEFAULT_WEIGHT: '0' }),
    ).toThrow(/TENANT_DEFAULT_WEIGHT/);
  });
});
