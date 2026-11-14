import { NextFunction, Request, Response } from 'express';
import {
  rateLimitAllowed,
  rateLimitDenied,
} from './metrics';
import type { LimitDecision, RateLimiter } from './types';

export function applyRateLimitHeaders(
  res: Response,
  scope: string,
  decision: LimitDecision,
): void {
  res.set('X-RateLimit-Policy', scope);
  res.set('X-RateLimit-Limit', String(decision.limit));
  res.set('X-RateLimit-Remaining', String(decision.remaining));
  res.set('X-RateLimit-Source', decision.source);
}

export interface RateLimitMiddlewareOptions {
  limiter: RateLimiter;
  scope: string;
  key: (req: Request) => string;
  cost?: (req: Request) => number;
  mode: 'observe' | 'enforce';
}

export function rateLimitMiddleware(options: RateLimitMiddlewareOptions) {
  return async (
    req: Request,
    res: Response,
    next: NextFunction,
  ): Promise<void> => {
    try {
      const decision = await options.limiter.consume(
        options.key(req),
        options.cost?.(req) ?? 1,
      );

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
            String(Math.max(1, Math.ceil(decision.retryAfterMs / 1000))),
          );
          res.status(429).json({
            error: 'Too many requests',
            retryAfterMs: decision.retryAfterMs,
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
