export type LimitSource = 'redis' | 'local';

export interface LimitDecision {
  allowed: boolean;
  limit: number;
  remaining: number;
  retryAfterMs: number;
  resetAtMs: number;
  source: LimitSource;
}

export interface RateLimiter {
  consume(key: string, cost?: number): Promise<LimitDecision>;
}

export interface RedisTokenBucketClient {
  schedulerTokenBucket(
    key: string,
    refillPerSec: number,
    capacity: number,
    cost: number,
  ): Promise<[number, number, number]>;
}
