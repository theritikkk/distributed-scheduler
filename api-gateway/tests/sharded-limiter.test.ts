import express from 'express';
import request from 'supertest';
import { describe, expect, it } from 'vitest';
import { CircuitBreaker } from '../src/rate-limit/circuitBreaker';
import { createLimiter } from '../src/rate-limit/limiterFactory';
import { ShardedRateLimiter, shardHash } from '../src/rate-limit/shardedLimiter';
import { TenantQuotaLimiterPool } from '../src/rate-limit/tenantQuotaLimiter';
import { tenantQuotaRateLimit } from '../src/rate-limit/tenantQuotaMiddleware';
import type { LimitDecision, RateLimiter } from '../src/rate-limit/types';
import type { TenantQuota } from '../src/tenant-quota';

function decision(overrides: Partial<LimitDecision> = {}): LimitDecision {
  return {
    allowed: true,
    limit: 10,
    remaining: 9,
    retryAfterMs: 0,
    resetAtMs: Date.now() + 1_000,
    source: 'local',
    ...overrides,
  };
}

class RecordingLimiter implements RateLimiter {
  keys: string[] = [];

  constructor(private readonly reply: LimitDecision = decision()) {}

  async consume(key: string): Promise<LimitDecision> {
    this.keys.push(key);
    return this.reply;
  }
}

function quota(overrides: Partial<TenantQuota> = {}): TenantQuota {
  return {
    userId: 'user-1',
    weight: 2,
    baseTasksPerMin: 60,
    baseBurst: 10,
    effectiveTasksPerMin: 120,
    effectiveBurst: 20,
    source: 'database',
    ...overrides,
  };
}

describe('ShardedRateLimiter', () => {
  it('routes a key deterministically to one shard and suffixes the Redis key', async () => {
    const shards = [
      new RecordingLimiter(),
      new RecordingLimiter(),
      new RecordingLimiter(),
      new RecordingLimiter(),
    ];
    const limiter = new ShardedRateLimiter(shards, 400);
    const expected = shardHash('client-a') % shards.length;

    const result = await limiter.consume('client-a');

    expect(result.limit).toBe(400);
    expect(shards[expected].keys).toEqual([
      `client-a:shard:${expected}`,
    ]);
    for (let index = 0; index < shards.length; index += 1) {
      if (index !== expected) {
        expect(shards[index].keys).toEqual([]);
      }
    }
  });

  it('reports the aggregate capacity while delegating the remaining count', async () => {
    const shards = [
      new RecordingLimiter(decision({ remaining: 3 })),
      new RecordingLimiter(decision({ remaining: 7 })),
    ];
    const limiter = new ShardedRateLimiter(shards, 100, () => 1);

    const result = await limiter.consume('client-b');

    expect(result.limit).toBe(100);
    expect(result.remaining).toBe(7);
    expect(shards[1].keys).toEqual(['client-b:shard:1']);
  });

  it('requires at least one shard', () => {
    expect(() => new ShardedRateLimiter([], 10)).toThrow(/at least one/);
  });
});

describe('TenantQuotaLimiterPool', () => {
  const deps = {
    redis: null,
    breaker: new CircuitBreaker(5, 1_000, 10_000),
    timeoutMs: 50,
    fallbackRps: 100,
  };

  it('reuses a limiter for the same tenant and quota', () => {
    const pool = new TenantQuotaLimiterPool(deps);
    const first = pool.limiterFor('user-1', quota());
    const second = pool.limiterFor('user-1', quota());
    expect(second).toBe(first);
    expect(pool.size).toBe(1);
  });

  it('creates a separate limiter when the quota changes', () => {
    const pool = new TenantQuotaLimiterPool(deps);
    const first = pool.limiterFor('user-1', quota());
    const second = pool.limiterFor('user-1', quota({ effectiveBurst: 40 }));
    expect(second).not.toBe(first);
    expect(pool.size).toBe(2);
  });

  it('creates a separate limiter for each tenant even with equal quotas', () => {
    const pool = new TenantQuotaLimiterPool(deps);
    expect(pool.limiterFor('user-1', quota())).not.toBe(
      pool.limiterFor('user-2', quota()),
    );
  });

  it('evicts the oldest entry at the configured bound', () => {
    const pool = new TenantQuotaLimiterPool(deps, 2);
    pool.limiterFor('user-1', quota());
    pool.limiterFor('user-2', quota());
    pool.limiterFor('user-3', quota());
    expect(pool.size).toBe(2);
  });
});

describe('createLimiter', () => {
  it('uses the local fallback when Redis is unavailable', async () => {
    const limiter = createLimiter('test', 2, 1, {
      redis: null,
      breaker: new CircuitBreaker(1, 1_000, 10_000),
      timeoutMs: 50,
      fallbackRps: 10,
    });

    const first = await limiter.consume('k');
    expect(first.source).toBe('local');
    expect(first.allowed).toBe(true);
  });
});

describe('tenantQuotaRateLimit', () => {
  it('sets quota headers and allows a request under the limit', async () => {
    const app = express();
    app.get(
      '/tasks',
      (req, _res, next) => {
        (req as typeof req & { user?: { userId: string } }).user = {
          userId: 'user-1',
        };
        next();
      },
      tenantQuotaRateLimit({
        scope: 'tenant:tasks',
        mode: 'enforce',
        resolveQuota: async () => quota(),
        limiterFor: () => new RecordingLimiter(decision({ remaining: 4 })),
      }),
      (_req, res) => res.json({ ok: true }),
    );

    const response = await request(app).get('/tasks');
    expect(response.status).toBe(200);
    expect(response.headers['x-quota-source']).toBe('database');
    expect(response.headers['x-quota-weight']).toBe('2');
    expect(response.headers['x-quota-tasks-per-min']).toBe('120');
    expect(response.headers['x-quota-burst']).toBe('20');
    expect(response.headers['x-ratelimit-remaining']).toBe('4');
  });

  it('returns 429 with the effective quota when denied', async () => {
    const app = express();
    app.get(
      '/tasks',
      (req, _res, next) => {
        (req as typeof req & { user?: { userId: string } }).user = {
          userId: 'user-1',
        };
        next();
      },
      tenantQuotaRateLimit({
        scope: 'tenant:tasks',
        mode: 'enforce',
        resolveQuota: async () => quota(),
        limiterFor: () =>
          new RecordingLimiter(
            decision({ allowed: false, remaining: 0, retryAfterMs: 2_500 }),
          ),
      }),
      (_req, res) => res.json({ ok: true }),
    );

    const response = await request(app).get('/tasks');
    expect(response.status).toBe(429);
    expect(response.headers['retry-after']).toBe('3');
    expect(response.body.quota).toEqual({
      tasksPerMin: 120,
      burst: 20,
      weight: 2,
    });
  });

  it('only reports headers in observe mode', async () => {
    const app = express();
    app.get(
      '/tasks',
      (req, _res, next) => {
        (req as typeof req & { user?: { userId: string } }).user = {
          userId: 'user-1',
        };
        next();
      },
      tenantQuotaRateLimit({
        scope: 'tenant:tasks',
        mode: 'observe',
        resolveQuota: async () => quota(),
        limiterFor: () =>
          new RecordingLimiter(decision({ allowed: false, remaining: 0 })),
      }),
      (_req, res) => res.json({ ok: true }),
    );

    const response = await request(app).get('/tasks');
    expect(response.status).toBe(200);
    expect(response.headers['x-ratelimit-remaining']).toBe('0');
  });
});
