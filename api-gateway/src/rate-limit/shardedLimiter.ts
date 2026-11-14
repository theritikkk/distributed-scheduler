import type { LimitDecision, RateLimiter } from './types';

/** FNV-1a 32-bit hash; stable across processes and cheap to compute. */
export function shardHash(value: string): number {
  let hash = 0x811c9dc5;
  for (let i = 0; i < value.length; i += 1) {
    hash ^= value.charCodeAt(i);
    hash = Math.imul(hash, 0x01000193);
  }
  return hash >>> 0;
}

/**
 * Spread one logical limiter across N Redis keys. Each shard has capacity/N
 * and refill/N; a request is routed by hash(key) so a client stays on one
 * shard while the aggregate key load is spread across Redis slots.
 *
 * The reported remaining count is the selected shard's remaining count, which
 * is conservative because other shards may still have tokens.
 */
export class ShardedRateLimiter implements RateLimiter {
  constructor(
    private readonly shards: RateLimiter[],
    private readonly totalCapacity: number,
    private readonly hash: (value: string) => number = shardHash,
  ) {
    if (shards.length === 0) {
      throw new Error('ShardedRateLimiter requires at least one shard');
    }
  }

  get shardCount(): number {
    return this.shards.length;
  }

  async consume(key: string, cost = 1): Promise<LimitDecision> {
    const index = this.hash(key) % this.shards.length;
    const decision = await this.shards[index].consume(
      `${key}:shard:${index}`,
      cost,
    );
    return {
      ...decision,
      limit: this.totalCapacity,
    };
  }
}
