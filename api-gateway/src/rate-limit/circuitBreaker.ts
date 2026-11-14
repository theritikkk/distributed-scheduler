export type BreakerState = 'closed' | 'open' | 'half_open';

/**
 * Simple tumbling-window circuit breaker. Success resets the window; open
 * state falls through to a half-open probe after the cool-down.
 */
export class CircuitBreaker {
  private state: BreakerState = 'closed';
  private failures = 0;
  private firstFailureAt = 0;
  private openedAt = 0;
  private halfOpenInFlight = false;

  constructor(
    private readonly failureThreshold = 5,
    private readonly windowMs = 10_000,
    private readonly openMs = 30_000,
    private readonly now: () => number = Date.now,
  ) {
    this.failureThreshold = Math.max(1, failureThreshold);
  }

  canAttempt(): boolean {
    const now = this.now();
    if (this.state === 'closed') return true;
    if (this.state === 'open' && now - this.openedAt >= this.openMs) {
      this.state = 'half_open';
      this.halfOpenInFlight = false;
    }
    if (this.state === 'half_open' && !this.halfOpenInFlight) {
      this.halfOpenInFlight = true;
      return true;
    }
    return false;
  }

  onSuccess(): void {
    this.failures = 0;
    this.firstFailureAt = 0;
    this.state = 'closed';
    this.halfOpenInFlight = false;
  }

  onFailure(): void {
    const now = this.now();
    if (this.failures === 0 || now - this.firstFailureAt > this.windowMs) {
      this.failures = 0;
      this.firstFailureAt = now;
    }

    this.failures += 1;
    this.halfOpenInFlight = false;
    if (this.state === 'half_open' || this.failures >= this.failureThreshold) {
      this.state = 'open';
      this.openedAt = now;
    }
  }

  get currentState(): BreakerState {
    return this.state;
  }
}
