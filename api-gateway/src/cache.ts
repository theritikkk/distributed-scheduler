import { config } from './config';
import {
  cacheErrorsTotal,
  cacheHitsTotal,
  cacheLatencySeconds,
  cacheMissesTotal,
} from './rate-limit/metrics';
import { withTimeout } from './rate-limit/fallbackLimiter';
import {
  createRedisClient,
  createRedisClusterClient,
} from './rate-limit/redisTokenBucket';
import type { RedisCommandClient } from './rate-limit/redisTokenBucket';

export type CacheNamespace =
  | 'task'
  | 'executions'
  | 'idempotency'
  | 'quota'
  | 'task-version';

const PREFIX = 'sched:cache';

function createCacheRedisClient(): RedisCommandClient | null {
  if (!config.redisUrl && !config.redisClusterNodes?.length) {
    return null;
  }

  const client = config.redisClusterNodes?.length
    ? createRedisClusterClient({
        nodes: config.redisClusterNodes,
        timeoutMs: config.cacheRedisTimeoutMs,
      })
    : createRedisClient({
        url: config.redisUrl as string,
        timeoutMs: config.cacheRedisTimeoutMs,
      });

  // Connect in the background. Every cache operation fails open to PostgreSQL
  // until Redis becomes available.
  void client.connect().catch(() => {
    // The client's error handler logs connection details.
  });
  return client;
}

const phase7CacheNeeded =
  config.cacheEnabled ||
  config.idempotencyCacheEnabled ||
  config.tenantQuotasEnabled;

const client: RedisCommandClient | null = phase7CacheNeeded
  ? createCacheRedisClient()
  : null;

function elapsedSeconds(started: bigint): number {
  return Number(process.hrtime.bigint() - started) / 1_000_000_000;
}

/**
 * Small JSON cache on top of the same Redis deployment used by the rate
 * limiter. Redis is strictly an accelerator: every error is converted into a
 * miss so the caller falls back to authoritative PostgreSQL data.
 */
export class RedisJsonCache {
  constructor(
    private readonly redis: RedisCommandClient,
    private readonly timeoutMs: number,
    private readonly prefix = PREFIX,
  ) {}

  private key(namespace: CacheNamespace, key: string): string {
    return `${this.prefix}:${namespace}:${key}`;
  }

  async getJson<T>(namespace: CacheNamespace, key: string): Promise<T | null> {
    const started = process.hrtime.bigint();
    try {
      const raw = await withTimeout(
        this.redis.get(this.key(namespace, key)),
        this.timeoutMs,
        'cache_timeout',
      );
      cacheLatencySeconds.observe(
        { namespace, op: 'get' },
        elapsedSeconds(started),
      );
      if (raw === null) {
        cacheMissesTotal.inc({ namespace });
        return null;
      }
      cacheHitsTotal.inc({ namespace });
      try {
        return JSON.parse(raw) as T;
      } catch {
        cacheErrorsTotal.inc({ namespace, op: 'decode' });
        return null;
      }
    } catch {
      cacheErrorsTotal.inc({ namespace, op: 'get' });
      return null;
    }
  }

  async getString(
    namespace: CacheNamespace,
    key: string,
  ): Promise<string | null> {
    const started = process.hrtime.bigint();
    try {
      const raw = await withTimeout(
        this.redis.get(this.key(namespace, key)),
        this.timeoutMs,
        'cache_timeout',
      );
      cacheLatencySeconds.observe(
        { namespace, op: 'get' },
        elapsedSeconds(started),
      );
      if (raw === null) {
        cacheMissesTotal.inc({ namespace });
      } else {
        cacheHitsTotal.inc({ namespace });
      }
      return raw;
    } catch {
      cacheErrorsTotal.inc({ namespace, op: 'get' });
      return null;
    }
  }

  async setJson(
    namespace: CacheNamespace,
    key: string,
    value: unknown,
    ttlSec: number,
  ): Promise<void> {
    const started = process.hrtime.bigint();
    try {
      await withTimeout(
        this.redis.set(
          this.key(namespace, key),
          JSON.stringify(value),
          'EX',
          Math.max(1, ttlSec),
        ),
        this.timeoutMs,
        'cache_timeout',
      );
      cacheLatencySeconds.observe(
        { namespace, op: 'set' },
        elapsedSeconds(started),
      );
    } catch {
      cacheErrorsTotal.inc({ namespace, op: 'set' });
    }
  }

  async setString(
    namespace: CacheNamespace,
    key: string,
    value: string,
    ttlSec: number,
  ): Promise<void> {
    const started = process.hrtime.bigint();
    try {
      await withTimeout(
        this.redis.set(
          this.key(namespace, key),
          value,
          'EX',
          Math.max(1, ttlSec),
        ),
        this.timeoutMs,
        'cache_timeout',
      );
      cacheLatencySeconds.observe(
        { namespace, op: 'set' },
        elapsedSeconds(started),
      );
    } catch {
      cacheErrorsTotal.inc({ namespace, op: 'set' });
    }
  }

  async delete(namespace: CacheNamespace, key: string): Promise<void> {
    const started = process.hrtime.bigint();
    try {
      await withTimeout(
        this.redis.del(this.key(namespace, key)),
        this.timeoutMs,
        'cache_timeout',
      );
      cacheLatencySeconds.observe(
        { namespace, op: 'delete' },
        elapsedSeconds(started),
      );
    } catch {
      cacheErrorsTotal.inc({ namespace, op: 'delete' });
    }
  }

  /**
   * Increment a generation counter. Used to invalidate paginated execution
   * history without scanning/deleting individual page keys.
   */
  async increment(
    namespace: CacheNamespace,
    key: string,
    ttlSec: number,
  ): Promise<number | null> {
    const started = process.hrtime.bigint();
    try {
      const fullKey = this.key(namespace, key);
      const value = await withTimeout(
        this.redis.incr(fullKey),
        this.timeoutMs,
        'cache_timeout',
      );
      await withTimeout(
        this.redis.expire(fullKey, Math.max(1, ttlSec)),
        this.timeoutMs,
        'cache_timeout',
      );
      cacheLatencySeconds.observe(
        { namespace, op: 'increment' },
        elapsedSeconds(started),
      );
      return Number(value);
    } catch {
      cacheErrorsTotal.inc({ namespace, op: 'increment' });
      return null;
    }
  }
}

export const cache: RedisJsonCache | null = client
  ? new RedisJsonCache(client, config.cacheRedisTimeoutMs)
  : null;

export function cacheAvailable(): boolean {
  return cache !== null;
}
