import { createHash } from 'crypto';
import { Request, RequestHandler } from 'express';
import { config } from '../config';
import type { AuthRequest } from '../middleware/auth';
import { defaultTenantQuota, resolveTenantQuota } from '../tenant-quota';
import { CircuitBreaker } from './circuitBreaker';
import { createLimiter } from './limiterFactory';
import type { LimiterFactoryDeps } from './limiterFactory';
import { rateLimitMiddleware } from './middleware';
import { rateLimitShards } from './metrics';
import {
  createRedisClient,
  createRedisClusterClient,
} from './redisTokenBucket';
import type { RedisCommandClient } from './redisTokenBucket';
import { ShardedRateLimiter } from './shardedLimiter';
import { TenantQuotaLimiterPool } from './tenantQuotaLimiter';
import { tenantQuotaRateLimit } from './tenantQuotaMiddleware';
import type { RateLimiter } from './types';

function hashKey(value: string): string {
  return createHash('sha256').update(value).digest('hex').slice(0, 32);
}

function clientIp(req: Request): string {
  return req.ip || req.socket.remoteAddress || 'unknown';
}

const limiterRedisNeeded =
  config.rateLimitEnabled || config.tenantQuotasEnabled;

let redisClient: RedisCommandClient | null = null;
const breaker = new CircuitBreaker(
  config.rateLimitFailureThreshold,
  config.rateLimitFailureWindowMs,
  config.rateLimitOpenMs,
);

if (limiterRedisNeeded) {
  if (config.redisClusterNodes?.length) {
    redisClient = createRedisClusterClient({
      nodes: config.redisClusterNodes,
      timeoutMs: config.rateLimitRedisTimeoutMs,
    });
  } else if (config.redisUrl) {
    redisClient = createRedisClient({
      url: config.redisUrl,
      timeoutMs: config.rateLimitRedisTimeoutMs,
    });
  }
  if (redisClient) {
    // Connect in the background. Until Redis is available, the fallback bucket
    // serves requests and the circuit breaker protects against a slow Redis.
    void redisClient.connect().catch(() => {
      // The error event handler logs details; local fallback remains active.
    });
  }
}

const limiterDeps: LimiterFactoryDeps = {
  redis: redisClient,
  breaker,
  timeoutMs: config.rateLimitRedisTimeoutMs,
  fallbackRps: config.rateLimitFallbackRps,
};

function buildIngressLimiter(): RateLimiter {
  const shardCount = Math.max(1, config.globalIngressShards);
  const capacityPerShard = Math.max(
    1,
    Math.ceil(config.globalIngressBurst / shardCount),
  );
  const refillPerShard = Math.max(
    0.001,
    config.globalIngressRps / shardCount,
  );
  const fallbackCapacity = Math.max(
    1,
    Math.floor(config.rateLimitFallbackRps / shardCount),
  );
  const fallbackRefill = Math.max(
    0.001,
    config.rateLimitFallbackRps / shardCount,
  );

  const shards = Array.from({ length: shardCount }, () =>
    createLimiter(
      'global:ingress',
      capacityPerShard,
      refillPerShard,
      limiterDeps,
      { capacity: fallbackCapacity, refillPerSec: fallbackRefill },
    ),
  );
  rateLimitShards.set({ scope: 'global:ingress' }, shardCount);
  return new ShardedRateLimiter(shards, config.globalIngressBurst);
}

const disabledMiddleware: RequestHandler = (_req, _res, next) => {
  next();
};

const ingressLimiter = buildIngressLimiter();
const loginLimiter = createLimiter(
  'login',
  config.loginBurst,
  config.loginPerMin / 60,
  limiterDeps,
);
const staticTaskCreationLimiter = createLimiter(
  'user:tasks',
  config.userTasksBurst,
  config.userTasksPerMin / 60,
  limiterDeps,
);
const tenantLimiterPool = new TenantQuotaLimiterPool(limiterDeps);

export const ingressRateLimit: RequestHandler = config.rateLimitEnabled
  ? rateLimitMiddleware({
      limiter: ingressLimiter,
      scope: 'global:ingress',
      // Hashing the client address spreads one logical global limit across
      // shards instead of sending every request to a single hot Redis key.
      key: (req) => `rl:global:ingress:${hashKey(clientIp(req))}`,
      mode: config.rateLimitMode,
    })
  : disabledMiddleware;

export const loginRateLimit: RequestHandler = config.rateLimitEnabled
  ? rateLimitMiddleware({
      limiter: loginLimiter,
      scope: 'login',
      key: (req) =>
        `rl:login:${hashKey(clientIp(req))}:${hashKey(
          String(req.body?.email ?? '').toLowerCase(),
        )}`,
      mode: config.rateLimitMode,
    })
  : disabledMiddleware;

export const taskCreationRateLimit: RequestHandler = config.tenantQuotasEnabled
  ? tenantQuotaRateLimit({
      scope: 'tenant:tasks',
      mode: config.rateLimitMode,
      resolveQuota: (userId) => resolveTenantQuota(userId),
      limiterFor: (userId, quota) =>
        tenantLimiterPool.limiterFor(userId, quota),
      fallbackQuota: defaultTenantQuota,
      key: (userId) => `rl:tenant:${userId}:tasks`,
    })
  : config.rateLimitEnabled
    ? rateLimitMiddleware({
        limiter: staticTaskCreationLimiter,
        scope: 'user:tasks',
        key: (req) =>
          `rl:user:${(req as AuthRequest).user?.userId ?? 'anonymous'}:tasks`,
        mode: config.rateLimitMode,
      })
    : disabledMiddleware;
