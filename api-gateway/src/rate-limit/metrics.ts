import { Counter, Gauge, Histogram } from 'prom-client';

export const rateLimitAllowed = new Counter({
  name: 'scheduler_ratelimit_allowed_total',
  help: 'Requests allowed by a rate-limit scope',
  labelNames: ['scope', 'source'],
});

export const rateLimitDenied = new Counter({
  name: 'scheduler_ratelimit_denied_total',
  help: 'Requests denied by a rate-limit scope',
  labelNames: ['scope', 'source', 'reason'],
});

export const rateLimitRedisErrors = new Counter({
  name: 'scheduler_ratelimit_redis_errors_total',
  help: 'Redis rate-limit errors',
  labelNames: ['reason'],
});

export const rateLimitFallbackActive = new Gauge({
  name: 'scheduler_ratelimit_fallback_active',
  help: '1 while the local rate-limit fallback is serving requests',
  labelNames: ['scope'],
});

export const rateLimitCircuitState = new Gauge({
  name: 'scheduler_ratelimit_circuit_state',
  help: 'Rate-limit Redis circuit breaker state (one-hot)',
  labelNames: ['state'],
});

export const rateLimitLatency = new Histogram({
  name: 'scheduler_ratelimit_latency_seconds',
  help: 'Rate-limit decision latency',
  labelNames: ['scope', 'source'],
  buckets: [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5],
});

export const rateLimitShards = new Gauge({
  name: 'scheduler_ratelimit_shards',
  help: 'Number of Redis keys the shared global limiter is sharded across',
  labelNames: ['scope'],
});

export const admissionRejected = new Counter({
  name: 'scheduler_admission_rejected_total',
  help: 'Requests rejected by admission control',
  labelNames: ['reason'],
});

export const cacheHitsTotal = new Counter({
  name: 'scheduler_cache_hits_total',
  help: 'Cache lookups served from Redis',
  labelNames: ['namespace'],
});

export const cacheMissesTotal = new Counter({
  name: 'scheduler_cache_misses_total',
  help: 'Cache lookups that missed and fell through to PostgreSQL',
  labelNames: ['namespace'],
});

export const cacheErrorsTotal = new Counter({
  name: 'scheduler_cache_errors_total',
  help: 'Cache operations that failed and were treated as a miss',
  labelNames: ['namespace', 'op'],
});

export const cacheLatencySeconds = new Histogram({
  name: 'scheduler_cache_latency_seconds',
  help: 'Cache operation latency',
  labelNames: ['namespace', 'op'],
  buckets: [0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25],
});

export const tenantQuotaLookupsTotal = new Counter({
  name: 'scheduler_tenant_quota_lookups_total',
  help: 'Tenant quota resolutions by source',
  labelNames: ['source'],
});

export const tenantQuotaLookupErrors = new Counter({
  name: 'scheduler_tenant_quota_lookup_errors_total',
  help: 'Tenant quota lookups that failed and used a default/local value',
});

export const dbPoolUtilization = new Gauge({
  name: 'scheduler_db_pool_utilization',
  help: 'PostgreSQL pool utilization (0-1)',
});

export const dbPoolWaiting = new Gauge({
  name: 'scheduler_db_pool_waiting',
  help: 'Requests waiting for a PostgreSQL pool connection',
});
