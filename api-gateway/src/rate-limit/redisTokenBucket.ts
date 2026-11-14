import Redis, { Cluster } from 'ioredis';
import { TOKEN_BUCKET_LUA } from './lua';
import type { LimitDecision, RateLimiter, RedisTokenBucketClient } from './types';

/**
 * Both standalone Redis and Redis Cluster expose defineCommand() through
 * ioredis' shared Commander base. The token-bucket script touches one key per
 * call, so cluster slot routing needs no hash tags.
 */
export type RedisCommandClient = Redis | Cluster;

export interface RedisClientOptions {
  url: string;
  timeoutMs: number;
  retryMaxDelayMs?: number;
}

export interface RedisClusterClientOptions {
  nodes: Array<{ host: string; port: number }>;
  timeoutMs: number;
  retryMaxDelayMs?: number;
  scaleReads?: 'master' | 'slave' | 'all';
}

export function createRedisClient(options: RedisClientOptions): Redis {
  const client = new Redis(options.url, {
    enableOfflineQueue: false,
    maxRetriesPerRequest: 1,
    connectTimeout: options.timeoutMs,
    commandTimeout: options.timeoutMs,
    lazyConnect: true,
    retryStrategy: (times) =>
      Math.min(times * 200, options.retryMaxDelayMs ?? 2_000),
  });
  client.on('error', (error) => {
    console.warn('[rate-limit] redis error:', error.message);
  });
  return client;
}

/**
 * Create a cluster client from seed nodes. Seeds only bootstrap discovery;
 * ioredis learns the slot map from CLUSTER SLOTS and follows MOVED/ASK
 * redirects itself. Commands stay on masters so token accounting is
 * authoritative even if a replica lags.
 */
export function createRedisClusterClient(
  options: RedisClusterClientOptions,
): Cluster {
  const cluster = new Cluster(
    options.nodes.map(({ host, port }) => ({ host, port })),
    {
      enableOfflineQueue: false,
      lazyConnect: true,
      scaleReads: options.scaleReads ?? 'master',
      clusterRetryStrategy: (times) =>
        Math.min(times * 200, options.retryMaxDelayMs ?? 2_000),
      redisOptions: {
        maxRetriesPerRequest: 1,
        connectTimeout: options.timeoutMs,
        commandTimeout: options.timeoutMs,
      },
    },
  );
  cluster.on('error', (error) => {
    console.warn('[rate-limit] redis cluster error:', error.message);
  });
  return cluster;
}

export class RedisTokenBucket implements RateLimiter {
  private readonly commandClient: RedisTokenBucketClient;

  constructor(
    private readonly redis: RedisCommandClient,
    private readonly capacity: number,
    private readonly refillPerSec: number,
  ) {
    this.capacity = Math.max(1, capacity);
    this.refillPerSec = Math.max(0.001, refillPerSec);
    this.redis.defineCommand('schedulerTokenBucket', {
      numberOfKeys: 1,
      lua: TOKEN_BUCKET_LUA,
    });
    this.commandClient = this.redis as unknown as RedisTokenBucketClient;
  }

  async consume(key: string, cost = 1): Promise<LimitDecision> {
    const [allowed, remaining, retryAfterMs] =
      await this.commandClient.schedulerTokenBucket(
        key,
        this.refillPerSec,
        this.capacity,
        cost,
      );

    return {
      allowed: Number(allowed) === 1,
      limit: this.capacity,
      remaining: Math.max(0, Number(remaining)),
      retryAfterMs: Math.max(0, Number(retryAfterMs)),
      resetAtMs: Date.now() + Math.max(0, Number(retryAfterMs)),
      source: 'redis',
    };
  }
}
