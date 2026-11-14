export type NodeEnvironment = 'development' | 'test' | 'production';

export interface RedisClusterNode {
  host: string;
  port: number;
}

export class ConfigError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'ConfigError';
  }
}

export interface AppConfig {
  nodeEnv: NodeEnvironment;
  isProduction: boolean;
  port: number;
  databaseUrl: string;
  rabbitmqUrl: string;
  jwtSecret: string;
  corsOrigin: string;
  trustProxy: number | false;
  dbPoolMax: number;
  dbIdleTimeoutMs: number;
  dbConnectionTimeoutMs: number;
  rateLimitWindowMs: number;
  rateLimitMax: number;
  rateLimitEnabled: boolean;
  rateLimitMode: 'observe' | 'enforce';
  redisUrl?: string;
  redisClusterNodes?: RedisClusterNode[];
  rateLimitRedisTimeoutMs: number;
  rateLimitFailureThreshold: number;
  rateLimitFailureWindowMs: number;
  rateLimitOpenMs: number;
  rateLimitFallbackRps: number;
  globalIngressRps: number;
  globalIngressBurst: number;
  globalIngressShards: number;
  loginPerMin: number;
  loginBurst: number;
  userTasksPerMin: number;
  userTasksBurst: number;
  cacheEnabled: boolean;
  cacheTtlSec: number;
  cacheRedisTimeoutMs: number;
  idempotencyCacheEnabled: boolean;
  idempotencyCacheTtlSec: number;
  tenantQuotasEnabled: boolean;
  tenantQuotaCacheTtlSec: number;
  tenantDefaultWeight: number;
  tenantDefaultTasksPerMin: number;
  tenantDefaultBurst: number;
  otelEnabled: boolean;
  otelServiceName: string;
  otelExporterEndpoint: string;
  otelSamplerArg: number;
  admissionEnabled: boolean;
  dbPoolHighWatermark: number;
  dbPoolMaxWaiters: number;
  queueBacklogHigh: number;
  queueBacklogLow: number;
  schedulerJitterEnabled: boolean;
  defaultRecurringJitterSec: number;
  maxJitterSec: number;
  idempotencyEnforce: boolean;
  outboxEnabled: boolean;
  rabbitmqQueueType: 'classic' | 'quorum';
  logRequests: boolean;
  serviceVersion: string;
}

const DEV_DEFAULTS = {
  databaseUrl:
    'postgresql://scheduler:scheduler_secret@localhost:5432/distributed_scheduler',
  rabbitmqUrl: 'amqp://scheduler:scheduler_secret@localhost:5672',
  jwtSecret: 'dev-only-jwt-secret-change-me-32chars',
};

const INSECURE_JWT_SECRETS = new Set([
  DEV_DEFAULTS.jwtSecret,
  'change-me-in-production',
  'your_long_random_secret_here_min_32_chars',
]);

function readString(env: NodeJS.ProcessEnv, key: string): string | undefined {
  const value = env[key]?.trim();
  return value ? value : undefined;
}

function readInt(
  env: NodeJS.ProcessEnv,
  key: string,
  defaultValue: number,
  min: number,
  max: number,
): number {
  const raw = readString(env, key);
  if (raw === undefined) {
    return defaultValue;
  }
  if (!/^\d+$/.test(raw)) {
    throw new ConfigError(`${key} must be an integer`);
  }
  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value < min || value > max) {
    throw new ConfigError(`${key} must be between ${min} and ${max}`);
  }
  return value;
}

function readFloat(
  env: NodeJS.ProcessEnv,
  key: string,
  defaultValue: number,
  min: number,
  max: number,
): number {
  const raw = readString(env, key);
  if (raw === undefined) {
    return defaultValue;
  }
  const value = Number(raw);
  if (!Number.isFinite(value) || value < min || value > max) {
    throw new ConfigError(`${key} must be between ${min} and ${max}`);
  }
  return value;
}

function readChoice<T extends string>(
  env: NodeJS.ProcessEnv,
  key: string,
  defaultValue: T,
  allowed: readonly T[],
): T {
  const raw = (readString(env, key) ?? defaultValue).toLowerCase();
  if (!allowed.includes(raw as T)) {
    throw new ConfigError(`${key} must be one of ${allowed.join(', ')}`);
  }
  return raw as T;
}

function readBool(
  env: NodeJS.ProcessEnv,
  key: string,
  defaultValue: boolean,
): boolean {
  const raw = readString(env, key)?.toLowerCase();
  if (raw === undefined) {
    return defaultValue;
  }
  if (['1', 'true', 'yes', 'on'].includes(raw)) {
    return true;
  }
  if (['0', 'false', 'no', 'off'].includes(raw)) {
    return false;
  }
  throw new ConfigError(`${key} must be true or false`);
}

function readTrustProxy(env: NodeJS.ProcessEnv): number | false {
  const raw = readString(env, 'TRUST_PROXY')?.toLowerCase();
  if (raw === undefined || raw === 'false' || raw === '0' || raw === 'off') {
    return false;
  }
  if (raw === 'true') {
    return 1;
  }
  if (!/^\d+$/.test(raw)) {
    throw new ConfigError('TRUST_PROXY must be false, true, or a hop count');
  }
  return Number(raw);
}

function requiredOrDev(
  env: NodeJS.ProcessEnv,
  key: string,
  devDefault: string,
  isProduction: boolean,
  warnings: string[],
): string {
  const value = readString(env, key);
  if (value) {
    return value;
  }
  if (isProduction) {
    throw new ConfigError(`${key} is required when NODE_ENV=production`);
  }
  warnings.push(`${key} is not set; using a development-only default`);
  return devDefault;
}

function parseNodeEnv(env: NodeJS.ProcessEnv): NodeEnvironment {
  const raw = (readString(env, 'NODE_ENV') ?? 'development').toLowerCase();
  if (raw === 'development' || raw === 'test' || raw === 'production') {
    return raw;
  }
  throw new ConfigError('NODE_ENV must be development, test, or production');
}

/**
 * Parse REDIS_CLUSTER_NODES as a comma-separated list of host:port seeds.
 *
 * The list is only used to bootstrap cluster discovery; ioredis learns the
 * full slot map from CLUSTER SLOTS. Bracketed IPv6 literals are accepted.
 */
export function parseRedisClusterNodes(
  env: NodeJS.ProcessEnv,
): RedisClusterNode[] | undefined {
  const raw = readString(env, 'REDIS_CLUSTER_NODES');
  if (!raw) {
    return undefined;
  }

  const nodes: RedisClusterNode[] = [];
  const seen = new Set<string>();
  for (const entry of raw.split(',')) {
    const value = entry.trim();
    if (!value) {
      continue;
    }
    const match =
      /^(\[[0-9a-fA-F:.]+\]|[A-Za-z0-9](?:[A-Za-z0-9._-]*[A-Za-z0-9])?):(\d{1,5})$/.exec(
        value,
      );
    if (!match) {
      throw new ConfigError(
        'REDIS_CLUSTER_NODES must be a comma-separated list of host:port',
      );
    }
    const port = Number(match[2]);
    if (port < 1 || port > 65_535) {
      throw new ConfigError(
        'REDIS_CLUSTER_NODES port must be between 1 and 65535',
      );
    }
    const host = match[1].replace(/^\[|\]$/g, '');
    const key = `${host}:${port}`;
    if (!seen.has(key)) {
      seen.add(key);
      nodes.push({ host, port });
    }
  }

  if (nodes.length === 0) {
    throw new ConfigError(
      'REDIS_CLUSTER_NODES must contain at least one host:port',
    );
  }
  return nodes;
}

export function loadConfig(env: NodeJS.ProcessEnv = process.env): AppConfig {
  const nodeEnv = parseNodeEnv(env);
  const isProduction = nodeEnv === 'production';
  const warnings: string[] = [];

  const databaseUrl = requiredOrDev(
    env,
    'DATABASE_URL',
    DEV_DEFAULTS.databaseUrl,
    isProduction,
    warnings,
  );
  const rabbitmqUrl = requiredOrDev(
    env,
    'RABBITMQ_URL',
    DEV_DEFAULTS.rabbitmqUrl,
    isProduction,
    warnings,
  );
  const jwtSecret = requiredOrDev(
    env,
    'JWT_SECRET',
    DEV_DEFAULTS.jwtSecret,
    isProduction,
    warnings,
  );

  if (jwtSecret.length < 32) {
    throw new ConfigError('JWT_SECRET must be at least 32 characters');
  }
  if (isProduction && INSECURE_JWT_SECRETS.has(jwtSecret)) {
    throw new ConfigError('JWT_SECRET must not use a documented/default value');
  }

  const redisUrl = readString(env, 'REDIS_URL');
  const redisClusterNodes = parseRedisClusterNodes(env);
  if (redisUrl && redisClusterNodes) {
    warnings.push('REDIS_CLUSTER_NODES is set; REDIS_URL is ignored');
  }

  const cacheEnabled = readBool(env, 'CACHE_ENABLED', false);
  const idempotencyCacheEnabled = readBool(
    env,
    'IDEMPOTENCY_CACHE_ENABLED',
    false,
  );
  const tenantQuotasEnabled = readBool(env, 'TENANT_QUOTAS_ENABLED', false);
  if (
    (cacheEnabled || idempotencyCacheEnabled || tenantQuotasEnabled) &&
    !redisUrl &&
    !redisClusterNodes?.length
  ) {
    warnings.push(
      'Phase 7 Redis features are enabled but REDIS_URL/REDIS_CLUSTER_NODES is not set; the features stay disabled',
    );
  }

  if (warnings.length > 0) {
    for (const warning of warnings) {
      console.warn(`[config] ${warning}`);
    }
  }

  const maxJitterSec = readInt(env, 'MAX_JITTER_SEC', 300, 0, 3600);
  const defaultRecurringJitterSec = readInt(
    env,
    'DEFAULT_RECURRING_JITTER_SEC',
    0,
    0,
    maxJitterSec,
  );

  const queueBacklogHigh = readInt(
    env,
    'QUEUE_BACKLOG_HIGH',
    5_000,
    1,
    10_000_000,
  );
  const queueBacklogLow = readInt(
    env,
    'QUEUE_BACKLOG_LOW',
    1_000,
    0,
    10_000_000,
  );
  if (queueBacklogLow >= queueBacklogHigh) {
    throw new ConfigError(
      'QUEUE_BACKLOG_LOW must be lower than QUEUE_BACKLOG_HIGH',
    );
  }

  const outboxEnabled = readBool(env, 'OUTBOX_ENABLED', true);
  const rabbitmqQueueType = readChoice(env, 'RABBITMQ_QUEUE_TYPE', 'classic', [
    'classic',
    'quorum',
  ] as const);
  if (rabbitmqQueueType === 'quorum' && !outboxEnabled) {
    throw new ConfigError(
      'RABBITMQ_QUEUE_TYPE=quorum requires OUTBOX_ENABLED=true',
    );
  }

  return {
    nodeEnv,
    isProduction,
    port: readInt(env, 'PORT', 3000, 1, 65535),
    databaseUrl,
    rabbitmqUrl,
    jwtSecret,
    corsOrigin: readString(env, 'CORS_ORIGIN') ?? '*',
    trustProxy: readTrustProxy(env),
    dbPoolMax: readInt(env, 'DB_POOL_MAX', 20, 1, 200),
    dbIdleTimeoutMs: readInt(env, 'DB_IDLE_TIMEOUT_MS', 30_000, 1_000, 300_000),
    dbConnectionTimeoutMs: readInt(
      env,
      'DB_CONNECTION_TIMEOUT_MS',
      2_000,
      100,
      60_000,
    ),
    rateLimitWindowMs: readInt(
      env,
      'RATE_LIMIT_WINDOW_MS',
      15 * 60 * 1000,
      1_000,
      24 * 60 * 60 * 1000,
    ),
    rateLimitMax: readInt(env, 'RATE_LIMIT_MAX', 10_000, 1, 1_000_000),
    rateLimitEnabled: readBool(env, 'RATE_LIMIT_ENABLED', false),
    rateLimitMode: readChoice(env, 'RATE_LIMIT_MODE', 'observe', [
      'observe',
      'enforce',
    ] as const),
    redisUrl,
    redisClusterNodes,
    rateLimitRedisTimeoutMs: readInt(
      env,
      'RATE_LIMIT_REDIS_TIMEOUT_MS',
      50,
      5,
      2_000,
    ),
    rateLimitFailureThreshold: readInt(
      env,
      'RATE_LIMIT_FAILURE_THRESHOLD',
      5,
      1,
      1_000,
    ),
    rateLimitFailureWindowMs: readInt(
      env,
      'RATE_LIMIT_FAILURE_WINDOW_MS',
      10_000,
      100,
      600_000,
    ),
    rateLimitOpenMs: readInt(
      env,
      'RATE_LIMIT_OPEN_MS',
      30_000,
      100,
      3_600_000,
    ),
    rateLimitFallbackRps: readInt(
      env,
      'LOCAL_FALLBACK_RPS',
      20,
      1,
      100_000,
    ),
    globalIngressRps: readInt(
      env,
      'GLOBAL_INGRESS_RPS',
      2_000,
      1,
      1_000_000,
    ),
    globalIngressBurst: readInt(
      env,
      'GLOBAL_INGRESS_BURST',
      4_000,
      1,
      2_000_000,
    ),
    globalIngressShards: readInt(
      env,
      'GLOBAL_INGRESS_SHARDS',
      16,
      1,
      256,
    ),
    loginPerMin: readInt(env, 'RATE_LIMIT_LOGIN_PER_MIN', 5, 1, 100_000),
    loginBurst: readInt(env, 'RATE_LIMIT_LOGIN_BURST', 2, 1, 100_000),
    userTasksPerMin: readInt(
      env,
      'RATE_LIMIT_USER_TASKS_PER_MIN',
      60,
      1,
      1_000_000,
    ),
    userTasksBurst: readInt(
      env,
      'RATE_LIMIT_USER_TASKS_BURST',
      10,
      1,
      1_000_000,
    ),
    cacheEnabled,
    cacheTtlSec: readInt(env, 'CACHE_TTL_SEC', 15, 1, 3_600),
    cacheRedisTimeoutMs: readInt(
      env,
      'CACHE_REDIS_TIMEOUT_MS',
      50,
      5,
      2_000,
    ),
    idempotencyCacheEnabled,
    idempotencyCacheTtlSec: readInt(
      env,
      'IDEMPOTENCY_CACHE_TTL_SEC',
      900,
      1,
      86_400,
    ),
    tenantQuotasEnabled,
    tenantQuotaCacheTtlSec: readInt(
      env,
      'TENANT_QUOTA_CACHE_TTL_SEC',
      30,
      1,
      3_600,
    ),
    tenantDefaultWeight: readFloat(
      env,
      'TENANT_DEFAULT_WEIGHT',
      1,
      0.1,
      100,
    ),
    tenantDefaultTasksPerMin: readInt(
      env,
      'TENANT_DEFAULT_TASKS_PER_MIN',
      60,
      1,
      1_000_000,
    ),
    tenantDefaultBurst: readInt(
      env,
      'TENANT_DEFAULT_BURST',
      10,
      1,
      1_000_000,
    ),
    otelEnabled: readBool(env, 'OTEL_ENABLED', false),
    otelServiceName: readString(env, 'OTEL_SERVICE_NAME') ?? 'api-gateway',
    otelExporterEndpoint:
      readString(env, 'OTEL_EXPORTER_OTLP_ENDPOINT') ??
      'http://localhost:4318/v1/traces',
    otelSamplerArg: readFloat(env, 'OTEL_SAMPLER_ARG', 0.1, 0, 1),
    admissionEnabled: readBool(env, 'ADMISSION_ENABLED', false),
    dbPoolHighWatermark: readFloat(
      env,
      'DB_POOL_HIGH_WATERMARK',
      0.8,
      0.1,
      1,
    ),
    dbPoolMaxWaiters: readInt(env, 'DB_POOL_MAX_WAITERS', 5, 0, 10_000),
    queueBacklogHigh,
    queueBacklogLow,
    schedulerJitterEnabled: readBool(env, 'SCHEDULER_JITTER_ENABLED', false),
    defaultRecurringJitterSec,
    maxJitterSec,
    idempotencyEnforce: readBool(env, 'IDEMPOTENCY_ENFORCE', true),
    outboxEnabled,
    rabbitmqQueueType,
    logRequests: readBool(env, 'LOG_REQUESTS', nodeEnv !== 'test'),
    serviceVersion: readString(env, 'APP_VERSION') ?? 'dev',
  };
}

export const config = loadConfig();
