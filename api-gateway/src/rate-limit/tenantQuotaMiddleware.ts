import { NextFunction, Request, Response } from 'express';
import type { AuthRequest } from '../middleware/auth';
import type { TenantQuota } from '../tenant-quota';
import { applyRateLimitHeaders } from './middleware';
import {
  rateLimitAllowed,
  rateLimitDenied,
  tenantQuotaLookupsTotal,
} from './metrics';
import type { RateLimiter } from './types';

export interface TenantQuotaMiddlewareOptions {
  scope: string;
  mode: 'observe' | 'enforce';
  resolveQuota: (userId: string) => Promise<TenantQuota>;
  limiterFor: (userId: string, quota: TenantQuota) => RateLimiter;
  fallbackQuota?: (userId: string) => TenantQuota;
  key?: (userId: string) => string;
}

/**
 * Resolve the tenant's weighted quota and consume from its token bucket.
 * Quota lookup is fail-open to defaults; PostgreSQL is never bypassed for
 * task state, only for the admission decision's cached inputs.
 */
export function tenantQuotaRateLimit(options: TenantQuotaMiddlewareOptions) {
  return async (
    req: Request,
    res: Response,
    next: NextFunction,
  ): Promise<void> => {
    try {
      const userId = (req as AuthRequest).user?.userId;
      if (!userId) {
        next();
        return;
      }

      let quota: TenantQuota;
      try {
        quota = await options.resolveQuota(userId);
      } catch {
        quota = options.fallbackQuota
          ? options.fallbackQuota(userId)
          : {
              userId,
              weight: 1,
              baseTasksPerMin: 1,
              baseBurst: 1,
              effectiveTasksPerMin: 1,
              effectiveBurst: 1,
              source: 'default',
            };
      }

      tenantQuotaLookupsTotal.inc({ source: quota.source });
      res.set('X-Quota-Source', quota.source);
      res.set('X-Quota-Weight', String(quota.weight));
      res.set('X-Quota-Tasks-Per-Min', String(quota.effectiveTasksPerMin));
      res.set('X-Quota-Burst', String(quota.effectiveBurst));

      const key = options.key
        ? options.key(userId)
        : `rl:tenant:${userId}:tasks`;
      const decision = await options
        .limiterFor(userId, quota)
        .consume(key);

      applyRateLimitHeaders(res, options.scope, decision);

      if (!decision.allowed) {
        rateLimitDenied.inc({
          scope: options.scope,
          source: decision.source,
          reason: options.mode === 'observe' ? 'observe' : 'limit',
        });
        if (options.mode === 'enforce') {
          res.set(
            'Retry-After',
            String(Math.max(1, Math.ceil(decision.retryAfterMs / 1_000))),
          );
          res.status(429).json({
            error: 'Tenant task quota exceeded',
            retryAfterMs: decision.retryAfterMs,
            quota: {
              tasksPerMin: quota.effectiveTasksPerMin,
              burst: quota.effectiveBurst,
              weight: quota.weight,
            },
          });
          return;
        }
      } else {
        rateLimitAllowed.inc({
          scope: options.scope,
          source: decision.source,
        });
      }

      next();
    } catch (error) {
      next(error);
    }
  };
}
