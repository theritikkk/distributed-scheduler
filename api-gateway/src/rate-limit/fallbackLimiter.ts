import { CircuitBreaker } from './circuitBreaker';
import {
  rateLimitCircuitState,
  rateLimitFallbackActive,
  rateLimitLatency,
  rateLimitRedisErrors,
} from './metrics';
import type { LimitDecision, RateLimiter } from './types';

export function withTimeout<T>(
  promise: Promise<T>,
  timeoutMs: number,
  reason = 'redis_timeout',
): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error(reason)), timeoutMs);
  });
  return Promise.race([promise, timeout]).finally(() => {
    if (timer) clearTimeout(timer);
  });
}

function classifyError(error: unknown): string {
  if (error instanceof Error && error.message === 'redis_timeout') {
    return 'timeout';
  }
  return 'connection';
}

/**
 * Use Redis when it is healthy, and a conservative local bucket otherwise.
 * Redis denials are successful calls and never open the circuit.
 */
export class FallbackRateLimiter implements RateLimiter {
  constructor(
    private readonly primary: RateLimiter | null,
    private readonly fallback: RateLimiter,
    private readonly breaker: CircuitBreaker,
    private readonly timeoutMs: number,
    private readonly scope: string,
  ) {}

  async consume(key: string, cost = 1): Promise<LimitDecision> {
    if (!this.primary || !this.breaker.canAttempt()) {
      rateLimitFallbackActive.set({ scope: this.scope }, 1);
      this.setCircuitMetric();
      return this.measureFallback(key, cost);
    }

    const started = process.hrtime.bigint();
    try {
      const decision = await withTimeout(
        this.primary.consume(key, cost),
        this.timeoutMs,
      );
      this.breaker.onSuccess();
      rateLimitFallbackActive.set({ scope: this.scope }, 0);
      this.setCircuitMetric();
      rateLimitLatency.observe(
        { scope: this.scope, source: decision.source },
        Number(process.hrtime.bigint() - started) / 1_000_000_000,
      );
      return decision;
    } catch (error) {
      this.breaker.onFailure();
      this.setCircuitMetric();
      rateLimitRedisErrors.inc({ reason: classifyError(error) });
      rateLimitFallbackActive.set({ scope: this.scope }, 1);
      return this.measureFallback(key, cost);
    }
  }

  private async measureFallback(
    key: string,
    cost: number,
  ): Promise<LimitDecision> {
    const started = process.hrtime.bigint();
    const decision = await this.fallback.consume(key, cost);
    rateLimitLatency.observe(
      { scope: this.scope, source: decision.source },
      Number(process.hrtime.bigint() - started) / 1_000_000_000,
    );
    return decision;
  }

  private setCircuitMetric(): void {
    const state = this.breaker.currentState;
    rateLimitCircuitState.set({ state: 'closed' }, state === 'closed' ? 1 : 0);
    rateLimitCircuitState.set({ state: 'open' }, state === 'open' ? 1 : 0);
    rateLimitCircuitState.set(
      { state: 'half_open' },
      state === 'half_open' ? 1 : 0,
    );
  }
}
