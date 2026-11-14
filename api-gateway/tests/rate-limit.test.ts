import express from 'express';
import request from 'supertest';
import { describe, expect, it } from 'vitest';
import { CircuitBreaker } from '../src/rate-limit/circuitBreaker';
import { FallbackRateLimiter } from '../src/rate-limit/fallbackLimiter';
import { LocalTokenBucket } from '../src/rate-limit/localTokenBucket';
import { rateLimitMiddleware } from '../src/rate-limit/middleware';
import { RedisTokenBucket } from '../src/rate-limit/redisTokenBucket';
import type { RedisCommandClient } from '../src/rate-limit/redisTokenBucket';
import type { LimitDecision, RateLimiter } from '../src/rate-limit/types';

function decision(allowed: boolean, source: 'redis' | 'local' = 'local'): LimitDecision {
  return {
    allowed,
    limit: 1,
    remaining: 0,
    retryAfterMs: allowed ? 0 : 1_000,
    resetAtMs: Date.now() + 1_000,
    source,
  };
}

describe('LocalTokenBucket', () => {
  it('refills at the configured rate without exceeding capacity', async () => {
    let now = 0;
    const bucket = new LocalTokenBucket(2, 1, 100, () => now);

    expect((await bucket.consume('k')).allowed).toBe(true);
    expect((await bucket.consume('k')).allowed).toBe(true);
    expect((await bucket.consume('k')).allowed).toBe(false);

    now = 1_000;
    const refilled = await bucket.consume('k');
    expect(refilled.allowed).toBe(true);
    expect(refilled.remaining).toBe(0);
  });

  it('evicts the least recently used key', async () => {
    const bucket = new LocalTokenBucket(1, 1, 2);
    await bucket.consume('a');
    await bucket.consume('b');
    await bucket.consume('a');
    const result = await bucket.consume('c');
    expect(result.allowed).toBe(true);
  });
});

describe('RedisTokenBucket', () => {
  it('defines the Lua command and maps the shared reply', async () => {
    const calls: unknown[][] = [];
    let registered: { name: string; definition: any } | null = null;
    const fake = {
      defineCommand: (name: string, definition: unknown) => {
        registered = { name, definition };
      },
      schedulerTokenBucket: async (...args: unknown[]) => {
        calls.push(args);
        return [1, 7, 0];
      },
    };

    const bucket = new RedisTokenBucket(
      fake as unknown as RedisCommandClient,
      10,
      2,
    );
    const result = await bucket.consume('rl:test', 3);

    expect(registered?.name).toBe('schedulerTokenBucket');
    expect(registered?.definition.numberOfKeys).toBe(1);
    expect(calls[0]).toEqual(['rl:test', 2, 10, 3]);
    expect(result).toEqual({
      allowed: true,
      limit: 10,
      remaining: 7,
      retryAfterMs: 0,
      resetAtMs: expect.any(Number),
      source: 'redis',
    });
  });

  it('maps a denial to a retry-after decision', async () => {
    const fake = {
      defineCommand: () => undefined,
      schedulerTokenBucket: async () => [0, 0, 250],
    };
    const bucket = new RedisTokenBucket(
      fake as unknown as RedisCommandClient,
      5,
      1,
    );

    const before = Date.now();
    const result = await bucket.consume('rl:denied');
    expect(result.allowed).toBe(false);
    expect(result.remaining).toBe(0);
    expect(result.retryAfterMs).toBe(250);
    expect(result.resetAtMs).toBeGreaterThanOrEqual(before + 250);
    expect(result.resetAtMs).toBeLessThanOrEqual(Date.now() + 250);
  });
});

describe('CircuitBreaker', () => {
  it('opens after the threshold and allows one half-open probe', () => {
    let now = 0;
    const breaker = new CircuitBreaker(2, 10_000, 1_000, () => now);
    breaker.onFailure();
    expect(breaker.currentState).toBe('closed');
    breaker.onFailure();
    expect(breaker.currentState).toBe('open');
    expect(breaker.canAttempt()).toBe(false);

    now = 1_001;
    expect(breaker.canAttempt()).toBe(true);
    expect(breaker.currentState).toBe('half_open');
    expect(breaker.canAttempt()).toBe(false);
    breaker.onSuccess();
    expect(breaker.currentState).toBe('closed');
  });
});

describe('FallbackRateLimiter', () => {
  it('uses the local bucket when Redis fails', async () => {
    const primary: RateLimiter = {
      consume: async () => {
        throw new Error('redis down');
      },
    };
    const fallback: RateLimiter = { consume: async () => decision(true) };
    const limiter = new FallbackRateLimiter(
      primary,
      fallback,
      new CircuitBreaker(1, 1_000, 10_000),
      5,
      'test',
    );

    const result = await limiter.consume('k');
    expect(result.allowed).toBe(true);
    expect(result.source).toBe('local');
  });

  it('does not open the circuit on a Redis denial', async () => {
    const primary: RateLimiter = { consume: async () => decision(false, 'redis') };
    const fallback: RateLimiter = { consume: async () => decision(true) };
    const breaker = new CircuitBreaker(1, 1_000, 10_000);
    const limiter = new FallbackRateLimiter(primary, fallback, breaker, 5, 'test');

    const result = await limiter.consume('k');
    expect(result.allowed).toBe(false);
    expect(breaker.currentState).toBe('closed');
  });
});

describe('rateLimitMiddleware', () => {
  it('returns 429 and Retry-After in enforce mode', async () => {
    const app = express();
    app.get(
      '/limited',
      rateLimitMiddleware({
        limiter: { consume: async () => decision(false) },
        scope: 'test',
        key: () => 'k',
        mode: 'enforce',
      }),
      (_req, res) => res.json({ ok: true }),
    );

    const response = await request(app).get('/limited');
    expect(response.status).toBe(429);
    expect(response.headers['retry-after']).toBe('1');
    expect(response.headers['x-ratelimit-source']).toBe('local');
  });

  it('only reports headers in observe mode', async () => {
    const app = express();
    app.get(
      '/observed',
      rateLimitMiddleware({
        limiter: { consume: async () => decision(false) },
        scope: 'test',
        key: () => 'k',
        mode: 'observe',
      }),
      (_req, res) => res.json({ ok: true }),
    );

    const response = await request(app).get('/observed');
    expect(response.status).toBe(200);
    expect(response.headers['x-ratelimit-remaining']).toBe('0');
  });
});
