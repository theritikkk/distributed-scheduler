import { createLimiter } from './limiterFactory';
import type { LimiterFactoryDeps } from './limiterFactory';
import type { RateLimiter } from './types';

export interface TenantQuotaWindow {
  effectiveTasksPerMin: number;
  effectiveBurst: number;
}

const MAX_POOL_ENTRIES = 10_000;

/**
 * Per-tenant limiter instances. Keying by tenant (not just quota values) is
 * important: the local fallback bucket must not be shared between tenants.
 * Quota changes naturally create a new entry; the bounded LRU keeps memory
 * flat across long-lived processes.
 */
export class TenantQuotaLimiterPool {
  private readonly limiters = new Map<string, RateLimiter>();

  constructor(
    private readonly deps: LimiterFactoryDeps,
    private readonly maxEntries = MAX_POOL_ENTRIES,
  ) {}

  limiterFor(userId: string, quota: TenantQuotaWindow): RateLimiter {
    const capacity = Math.max(1, Math.round(quota.effectiveBurst));
    const refillPerSec = Math.max(0.001, quota.effectiveTasksPerMin / 60);
    const key = `${userId}:${capacity}:${refillPerSec.toFixed(4)}`;

    const existing = this.limiters.get(key);
    if (existing) {
      // Refresh insertion order for LRU eviction.
      this.limiters.delete(key);
      this.limiters.set(key, existing);
      return existing;
    }

    const limiter = createLimiter(
      'tenant:tasks',
      capacity,
      refillPerSec,
      this.deps,
    );
    if (this.limiters.size >= this.maxEntries) {
      const oldest = this.limiters.keys().next().value;
      if (oldest !== undefined) {
        this.limiters.delete(oldest);
      }
    }
    this.limiters.set(key, limiter);
    return limiter;
  }

  get size(): number {
    return this.limiters.size;
  }
}
