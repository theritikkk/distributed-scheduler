import { CircuitBreaker } from './circuitBreaker';
import { FallbackRateLimiter } from './fallbackLimiter';
import { LocalTokenBucket } from './localTokenBucket';
import { RedisTokenBucket } from './redisTokenBucket';
import type { RedisCommandClient } from './redisTokenBucket';
import type { RateLimiter } from './types';

export interface LimiterFactoryDeps {
  redis: RedisCommandClient | null;
  breaker: CircuitBreaker;
  timeoutMs: number;
  fallbackRps: number;
}

export interface FallbackLimits {
  capacity?: number;
  refillPerSec?: number;
}

/**
 * Build the standard Redis-primary/local-fallback limiter. Tests can inject a
 * null Redis client to exercise the local path deterministically.
 */
export function createLimiter(
  scope: string,
  capacity: number,
  refillPerSec: number,
  deps: LimiterFactoryDeps,
  fallback: FallbackLimits = {},
): RateLimiter {
  const normalizedCapacity = Math.max(1, capacity);
  const normalizedRefill = Math.max(0.001, refillPerSec);
  const fallbackCapacity = Math.max(
    1,
    Math.min(normalizedCapacity, fallback.capacity ?? deps.fallbackRps),
  );
  const fallbackRefill = Math.max(
    0.001,
    Math.min(normalizedRefill, fallback.refillPerSec ?? deps.fallbackRps),
  );

  return new FallbackRateLimiter(
    deps.redis === null
      ? null
      : new RedisTokenBucket(
          deps.redis,
          normalizedCapacity,
          normalizedRefill,
        ),
    new LocalTokenBucket(fallbackCapacity, fallbackRefill),
    deps.breaker,
    deps.timeoutMs,
    scope,
  );
}
