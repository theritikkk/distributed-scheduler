import type { LimitDecision, RateLimiter } from './types';

interface Bucket {
  tokens: number;
  lastRefillMs: number;
}

/**
 * In-process token bucket used when Redis is unavailable or unconfigured.
 * It is intentionally local and conservative: each API replica enforces its
 * own bucket, so operators size it below the shared budget.
 */
export class LocalTokenBucket implements RateLimiter {
  private readonly buckets = new Map<string, Bucket>();

  constructor(
    private readonly capacity: number,
    private readonly refillPerSec: number,
    private readonly maxKeys = 10_000,
    private readonly now: () => number = Date.now,
  ) {
    this.capacity = Math.max(1, capacity);
    this.refillPerSec = Math.max(0.001, refillPerSec);
  }

  async consume(key: string, cost = 1): Promise<LimitDecision> {
    const now = this.now();
    const bucket = this.buckets.get(key) ?? {
      tokens: this.capacity,
      lastRefillMs: now,
    };

    const elapsedSec = Math.max(0, (now - bucket.lastRefillMs) / 1000);
    bucket.tokens = Math.min(
      this.capacity,
      bucket.tokens + elapsedSec * this.refillPerSec,
    );
    bucket.lastRefillMs = now;

    const allowed = bucket.tokens >= cost;
    if (allowed) {
      bucket.tokens -= cost;
    }

    const retryAfterMs = allowed
      ? 0
      : Math.ceil(((cost - bucket.tokens) / this.refillPerSec) * 1000);

    // Refresh insertion order so eviction removes the least recently used key.
    this.buckets.delete(key);
    this.buckets.set(key, bucket);
    this.evictIfNeeded();

    return {
      allowed,
      limit: this.capacity,
      remaining: Math.floor(Math.max(0, bucket.tokens)),
      retryAfterMs,
      resetAtMs: now + retryAfterMs,
      source: 'local',
    };
  }

  private evictIfNeeded(): void {
    if (this.buckets.size <= this.maxKeys) return;
    const oldest = this.buckets.keys().next().value;
    if (oldest !== undefined) {
      this.buckets.delete(oldest);
    }
  }
}
