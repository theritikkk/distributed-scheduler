# Design 02 — Redis rate limiting with a local fail-safe fallback

- **Status:** Proposed
- **Date:** 2026-10-30
- **Scope:** API admission control, per-user/global rate limits, Redis degradation
- **Related:** [01 — Jitter](01-jitter-and-dispatch-time.md), [03 — Backpressure](03-coordinator-backpressure.md), [04 — Plan](04-implementation-plan.md)

---

## 1. Problem

The current API applies a single in-process limit of 10,000 requests per 15 minutes
(`api-gateway/src/index.ts:29-37`). That is not a meaningful protection:

- It is per API instance. With N replicas, the effective limit is N × 10,000.
- It is endpoint-agnostic. Login, task creation, and reads all share the same budget.
- It is not user-aware. One tenant can consume the entire budget.
- Behind Nginx, `trust proxy` is not configured, so the limiter may see the proxy IP
  instead of the client IP.
- If the limiter were moved to Redis and Redis failed, a naive fallback to Postgres would
  turn a Redis outage into a database outage.

This design adds a layered limiter with an explicit degradation policy:

```text
Nginx edge limits
  → global/IP limiter
  → JWT auth
  → per-user limiter
  → validation
  → idempotency
  → DB pool / queue-depth admission
  → Postgres
```

**Core rule:** rate-limit state is ephemeral. If Redis dies, losing the counters is
acceptable. Letting every request through is not. Postgres must never become the
rate-limit store.

---

## 2. Goals and non-goals

### Goals

- Global and per-user limits that are consistent across API replicas.
- Atomic check-and-consume in Redis via Lua.
- Per-route scopes: login, task creation, reads, admin/DLQ replay.
- A local, conservative fallback limiter when Redis is unavailable or slow.
- A circuit breaker so Redis latency does not become API latency.
- Admission control in front of Postgres, so the DB only sees accepted work.
- Clear `429` vs `503` behavior and `Retry-After` headers.
- Metrics and alerts for every degraded state.
- No duplicate side effects when Redis loses rate-limit state.

### Non-goals

- Billing-grade quotas or invoice reconciliation.
- Distributed locks for correctness. Use Postgres advisory locks for that.
- Making Redis the source of truth for task state or idempotency.
- Replacing the coordinator's queue-depth backpressure (see design 03).
- Per-user fair scheduling inside workers (a later feature).

---

## 3. Layers and scopes

| Layer | Where | What it protects | Shared state? |
|-------|-------|------------------|---------------|
| Edge | Nginx | Crude IP floods, connection exhaustion | No |
| Ingress | API | Global request rate, auth abuse | Redis + local fallback |
| User | API | Per-user task creation, active tasks | Redis + local fallback |
| Replay | API | Idempotent replays | Redis + local fallback |
| Admission | API | Postgres pool, queue backlog | Local signals |
| Dispatch | Coordinator | Worker/DB capacity | Local token bucket + queue depth |
| Execution | Worker | Worker CPU/memory | Local semaphore |

### Scope definitions

| Scope key | Limit (example) | Key |
|-----------|-----------------|-----|
| `global:ingress` | 2,000 req/s | `rl:global:ingress` |
| `ip:auth` | 20/min, burst 5 | `rl:{ip:<hash>}:auth` |
| `login` | 5/min, burst 2 | `rl:{login:<ip>:<email_hash>}` |
| `user:tasks` | 60/min, burst 10 | `rl:{user:<uuid>}:tasks` |
| `user:replay` | 20/min, burst 5 | `rl:{user:<uuid>}:replay` |
| `admin:dlq` | 10/min | `rl:{admin:<uuid>}:dlq` |
| `global:task_admission` | measured capacity × 0.7 | `rl:global:task_admission` |

These are starting points, not final values. Calibrate them with a load test
(Phase 0/4 in the implementation plan).

### Request order

1. Nginx `limit_req` / `limit_conn`.
2. Global/IP limiter (cheap, before parsing).
3. JWT authentication.
4. Per-user limiter.
5. Input validation (cheap checks before consuming more budget).
6. Idempotency lookup.
7. DB pool / queue-depth admission.
8. Database write.

For `POST /auth/login`, the order is: edge → IP limiter → login limiter → bcrypt →
DB. Never let bcrypt run before the login limiter.

For `POST /tasks`, a legitimate idempotent replay should use the `user:replay` budget
rather than the main creation budget. See §9.

---

## 4. Algorithm: token bucket, not a fixed window

Use a **token bucket** (or GCRA) per key:

- `capacity` = burst size.
- `refillPerSec` = sustained rate.
- `cost` = tokens consumed by one request (usually 1; bulk creation uses N).
- Denied requests return `retryAfterMs`.

Why not a fixed window:

- A fixed window allows a 2× burst at the boundary (e.g., 60 requests at 00:59 and 60 at
  01:00).
- All clients reset at the same instant, creating a synchronized herd.
- A sliding-window counter is acceptable, but a token bucket is simpler to reason about
  and gives a natural `Retry-After`.

### 4.1 Lua script

```lua
-- KEYS[1] = bucket key
-- ARGV[1] = refill rate (tokens/second)
-- ARGV[2] = burst capacity
-- ARGV[3] = cost (tokens)
--
-- Returns: { allowed, remaining, retry_after_ms }

local key   = KEYS[1]
local rate  = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local cost  = tonumber(ARGV[3]) or 1

if not rate or rate <= 0 then
  return redis.error_reply('rate must be > 0')
end
if not burst or burst <= 0 then
  return redis.error_reply('burst must be > 0')
end
if not cost or cost <= 0 then
  return redis.error_reply('cost must be > 0')
end

-- Use the Redis clock so API replicas cannot disagree.
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

local data   = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts     = tonumber(data[2])

if tokens == nil then
  tokens = burst
  ts = now
end

local delta = math.max(0, now - ts)
tokens = math.min(burst, tokens + (delta / 1000) * rate)

local allowed = 0
local retry_after_ms = 0

if tokens >= cost then
  tokens = tokens - cost
  allowed = 1
else
  retry_after_ms = math.ceil(((cost - tokens) / rate) * 1000)
end

redis.call('HSET', key, 'tokens', tokens, 'ts', now)

-- Keep the key long enough to refill a full bucket, then let it expire.
local ttl_ms = math.ceil((burst / rate) * 1000) + 1000
redis.call('PEXPIRE', key, ttl_ms)

return { allowed, math.floor(tokens), retry_after_ms }
```

Notes:

- `EVALSHA` is handled automatically by `ioredis.defineCommand` and `redis-py`
  `register_script`; both fall back on `NOSCRIPT`.
- In Redis Cluster, all keys referenced by one script must share a hash slot. This script
  uses one key, so it is cluster-safe. Use hash tags such as `{user:<uuid>}` when you
  later need multi-key scripts for the same user.
- A single hot global key can become a Redis bottleneck. At very high throughput, shard
  the global limiter into 16 keys and sum the decisions, or rely on local admission.
- Do not use `KEYS` to clean up; TTL handles it.

### 4.2 GCRA alternative

If you want smoother pacing and lower memory, use GCRA (one timestamp per key) instead
of a token bucket. The interface and fallback logic are identical. Token bucket is
recommended initially because the math and `Retry-After` are easy to explain.

---

## 5. Node implementation

### 5.1 Module layout

```text
api-gateway/src/rate-limit/
├── types.ts                 # Decision, RateLimiter, config types
├── redisTokenBucket.ts      # ioredis + Lua
├── localTokenBucket.ts      # in-process Map, no dependencies
├── circuitBreaker.ts        # closed / open / half-open
├── fallbackLimiter.ts       # primary + fallback + breaker
├── admission.ts             # DB pool and queue-depth guards
├── middleware.ts            # Express middleware, headers, 429
├── metrics.ts               # prom-client counters/gauges/histograms
└── index.ts                 # factory + config wiring
```

### 5.2 Interfaces

```ts
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
```

### 5.3 Redis client

```ts
import Redis from 'ioredis';

export const redis = new Redis(process.env.REDIS_URL!, {
  enableOfflineQueue: false,      // fail fast; do not grow an in-memory backlog
  maxRetriesPerRequest: 1,
  connectTimeout: 50,
  commandTimeout: 50,
  lazyConnect: true,
  retryStrategy: (times) => Math.min(times * 200, 2_000),
});
```

Use a **separate Redis connection** for rate limiting and for caching. A slow cache
command must not block the limiter (bulkhead). Set `RATE_LIMIT_REDIS_TIMEOUT_MS=50`.

### 5.4 Redis token bucket

```ts
import type Redis from 'ioredis';
import { TOKEN_BUCKET_LUA } from './lua';

export class RedisTokenBucket {
  constructor(
    private readonly redis: Redis,
    private readonly capacity: number,
    private readonly refillPerSec: number,
  ) {
    this.redis.defineCommand('schedulerTokenBucket', {
      numberOfKeys: 1,
      lua: TOKEN_BUCKET_LUA,
    });
  }

  async consume(key: string, cost = 1): Promise<LimitDecision> {
    const [allowed, remaining, retryAfterMs] =
      await (this.redis as any).schedulerTokenBucket(
        key,
        this.refillPerSec,
        this.capacity,
        cost,
      );

    return {
      allowed: allowed === 1,
      limit: this.capacity,
      remaining,
      retryAfterMs,
      resetAtMs: Date.now() + retryAfterMs,
      source: 'redis',
    };
  }
}
```

Add a `declare module 'ioredis'` type augmentation for
`schedulerTokenBucket(key, rate, burst, cost)` to avoid `any`.

### 5.5 Local token bucket

The fallback must be **conservative**, because each replica enforces its own bucket.

```ts
interface Bucket {
  tokens: number;
  lastRefillMs: number;
}

export class LocalTokenBucket implements RateLimiter {
  private readonly buckets = new Map<string, Bucket>();

  constructor(
    private readonly capacity: number,
    private readonly refillPerSec: number,
    private readonly maxKeys = 10_000,
  ) {}

  async consume(key: string, cost = 1): Promise<LimitDecision> {
    const now = Date.now();
    const bucket = this.buckets.get(key) ?? {
      tokens: this.capacity,
      lastRefillMs: now,
    };

    const elapsedSec = Math.max(0, (now - bucket.lastRefillMs) / 1000);
    bucket.tokens = Math.min(
      this.capacity,
      bucket.tokens + elapsedSec * this.refillPerSec,
    );
    bucket.lastRefillMs = now;

    const allowed = bucket.tokens >= cost;
    if (allowed) bucket.tokens -= cost;

    const retryAfterMs = allowed
      ? 0
      : Math.ceil(((cost - bucket.tokens) / this.refillPerSec) * 1000);

    this.buckets.set(key, bucket);
    this.evictIfNeeded();

    return {
      allowed,
      limit: this.capacity,
      remaining: Math.floor(bucket.tokens),
      retryAfterMs,
      resetAtMs: now + retryAfterMs,
      source: 'local',
    };
  }

  private evictIfNeeded(): void {
    if (this.buckets.size <= this.maxKeys) return;
    const oldest = this.buckets.keys().next().value;
    if (oldest !== undefined) this.buckets.delete(oldest);
  }
}
```

For the fallback, size it to a fraction of the global limit:

```text
LOCAL_FALLBACK_RPS = GLOBAL_ADMISSION_RPS / expected_max_replicas × 0.8
```

If the replica count is unknown, use a static conservative value from
`LOCAL_FALLBACK_RPS` and monitor. Under-admitting during an outage is safe;
over-admitting is not.

### 5.6 Circuit breaker

```ts
type BreakerState = 'closed' | 'open' | 'half_open';

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
  ) {}

  canAttempt(now = Date.now()): boolean {
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

  onFailure(now = Date.now()): void {
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
```

The failure window is a tumbling window starting at the first failure in a burst. It is
deliberately simple; if you need a precise sliding window, keep a ring buffer of failure
timestamps. Redis **denials** are successful calls and must not count as breaker failures.

### 5.7 Fallback limiter

```ts
export class FallbackRateLimiter implements RateLimiter {
  constructor(
    private readonly primary: RateLimiter,
    private readonly fallback: RateLimiter,
    private readonly breaker: CircuitBreaker,
    private readonly timeoutMs: number,
  ) {}

  async consume(key: string, cost = 1): Promise<LimitDecision> {
    if (!this.breaker.canAttempt()) {
      return this.fallback.consume(key, cost);
    }

    try {
      const decision = await withTimeout(
        this.primary.consume(key, cost),
        this.timeoutMs,
      );
      this.breaker.onSuccess();
      return decision;
    } catch (error) {
      this.breaker.onFailure();
      metrics.redisErrors.inc({ reason: classify(error) });
      return this.fallback.consume(key, cost);
    }
  }
}
```

`withTimeout` must clear its timer and avoid unhandled rejections:

```ts
export function withTimeout<T>(promise: Promise<T>, ms: number): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error('redis_timeout')), ms);
  });
  return Promise.race([promise, timeout]).finally(() => {
    if (timer) clearTimeout(timer);
  });
}
```

### 5.8 Express middleware

```ts
export function rateLimitMiddleware(options: {
  limiter: RateLimiter;
  scope: string;
  key: (req: AuthRequest) => string;
  cost?: (req: AuthRequest) => number;
  mode: 'observe' | 'enforce';
}) {
  return async (req: AuthRequest, res: Response, next: NextFunction) => {
    const decision = await options.limiter.consume(
      options.key(req),
      options.cost?.(req) ?? 1,
    );

    res.set('X-RateLimit-Policy', options.scope);
    res.set('X-RateLimit-Limit', String(decision.limit));
    res.set('X-RateLimit-Remaining', String(decision.remaining));
    res.set('X-RateLimit-Source', decision.source);

    if (!decision.allowed && options.mode === 'enforce') {
      res.set('Retry-After', String(Math.ceil(decision.retryAfterMs / 1000)));
      return res.status(429).json({
        error: 'Too many requests',
        retryAfterMs: decision.retryAfterMs,
      });
    }

    next();
  };
}
```

In `observe` mode, always call `next()` but still emit metrics and headers. This lets you
roll out limits without rejecting traffic.

### 5.9 `trust proxy`

Behind the Nginx example in the README, set exactly one trusted proxy hop:

```ts
app.set('trust proxy', 1);
```

Do not set `trust proxy = true` blindly; it lets clients spoof `X-Forwarded-For` if the
app is reachable directly.

---

## 6. Python implementation (coordinator)

The coordinator does **not** need Redis in its hot path. Its dispatch limit can be local:

- The queue is the buffer.
- Queue-depth backpressure (design 03) is the primary protection.
- A local token bucket enforces a per-replica dispatch ceiling.

If a shared global dispatch limit is later required, provide the same wrapper in Python:

```python
# coordinator/internal/ratelimit.py
from dataclasses import dataclass

@dataclass
class LimitDecision:
    allowed: bool
    limit: int
    remaining: int
    retry_after_ms: int
    source: str  # "redis" | "local"
```

Implement:

- `LocalTokenBucket` with `threading.Lock`.
- `RedisTokenBucket` with `redis-py` `Script` and the same Lua payload.
- `CircuitBreaker` using `time.monotonic()`.
- `FallbackRateLimiter` with the same semantics as Node.

Use it for dispatching only if you need a global ceiling. Otherwise, use the local bucket
and let design 03 pause/resume the consumer based on queue depth and DB pool utilization.

---

## 7. Failure fallback matrix

| Dependency / use | Normal | On failure | Correctness |
|------------------|--------|------------|-------------|
| Redis rate limit | Global Lua token bucket | Local conservative limiter + `429` | Approximate, safe |
| Redis idempotency cache | `SET NX EX` fast path | Postgres unique constraint | Exact |
| Redis distributed lock | Optional optimization | Postgres advisory lock / fail closed | Exact |
| Redis cache | Serve cached reads | Read DB behind pool guard | Eventual |
| Postgres | Primary | Reject writes, pause consumers, queue buffers | Durable |
| RabbitMQ | Publish | Outbox buffers, retry, resume | Durable |
| Queue backlog | Normal dispatch | API `429`, coordinator pauses | Safe |

### 7.1 Redis unavailable or slow

1. Redis client fails fast (`enableOfflineQueue=false`, 50ms timeout).
2. Circuit breaker opens after `N` failures.
3. Requests use the local limiter.
4. If the local bucket denies, return `429` with `Retry-After`.
5. If the local bucket allows, continue to validation, idempotency, and DB admission.
6. The DB pool guard is the final backstop; if it is saturated, return `503`.
7. Emit `scheduler_ratelimit_fallback_active=1` and an alert.

Do **not**:

- Write rate-limit counters to Postgres.
- Retry Redis aggressively.
- Bypass the limiter and let all traffic through.
- Block the request for the full Redis timeout on every call.

### 7.2 Redis latency

- 50ms command timeout.
- One retry at most.
- Circuit opens after repeated timeouts.
- API P99 should stay bounded; measure `ratelimit_latency_seconds{source}`.

### 7.3 Postgres unavailable or saturated

- `/health/ready` fails; the load balancer removes the instance.
- API returns `503` for writes; reads may continue if a replica is available.
- Coordinator pauses consumers; do not ack and drop messages.
- Outbox rows remain in the DB; the relay retries.
- Recovery ramp starts at 10–25% capacity.

### 7.4 RabbitMQ unavailable

- Outbox buffers events in Postgres.
- Coordinator cannot consume; API can still accept tasks up to DB capacity.
- If outbox lag or queue health exceeds a threshold, reject new task creation with
  `503` rather than growing the backlog indefinitely.

### 7.5 Recovery storm

When a dependency returns, all clients retry at once. Mitigate with:

- Randomized `Retry-After` on the client side (full jitter).
- Server-side recovery ramp: 10% → 25% → 50% → 100% over 30–60s.
- Circuit breaker half-open probes, not instant full traffic.
- Per-user limits so one tenant cannot consume the whole recovery budget.

---

## 8. Admission control in front of Postgres

Rate limiting controls request rate; admission control controls **work in progress**.

### 8.1 DB pool guard

Node `pg` exposes `pool.totalCount`, `pool.idleCount`, and `pool.waitingCount`.
Before a write:

```ts
if (pool.waitingCount > config.poolMaxWaiters ||
    pool.totalCount / config.poolMax > config.poolHighWatermark) {
  throw new AppError(503, 'Database is busy; retry later');
}
```

Also set:

```ts
const pool = new Pool({
  connectionString,
  max: config.poolMax,
  idleTimeoutMillis: 30_000,
  connectionTimeoutMillis: 1_000,
  statement_timeout: 5_000,
  query_timeout: 5_000,
});
```

`statement_timeout` is a Postgres setting; set it per connection or via the role.
`query_timeout` is a `pg` client option. Both prevent a slow query from holding a
connection forever.

For Python, use `psycopg_pool.ConnectionPool` and inspect `pool.get_stats()`:
`requests_waiting`, `pool_available`, `pool_size`.

### 8.2 Queue-depth guard

The API should not accept tasks when workers are already far behind. Use a cached signal:

- A background poller in the API passively declares `scheduler.tasks` on a dedicated
  channel every 5s and caches `message_count`.
- Or the coordinator publishes the depth to Redis every 5s. If using Redis, treat a stale
  value as “unknown” and fall back to the DB pool guard.

Thresholds:

```text
queue high watermark: 5,000 messages (or ~10 minutes of work)
queue low watermark:  1,000 messages
```

If `depth > high`, return `429` with `reason: queue_backlog`. If the signal is stale,
do not fail open to unlimited traffic; use the global limiter and DB guard.

### 8.3 Priority lanes

When degraded:

- Allow reads, execution history, and cancellation.
- Shed `POST /tasks` first.
- Keep login strict (it is an attack surface).
- Reject bulk creation and DLQ replay.
- Consider a separate “critical tenant” budget only if the product requires it.

### 8.4 Response codes

| Situation | Status | Header |
|-----------|--------|--------|
| Rate limit exceeded | `429` | `Retry-After`, `X-RateLimit-*` |
| Queue backlog | `429` | `Retry-After` |
| DB pool saturated | `503` | `Retry-After` |
| Redis degraded but local limiter allowed | Normal response | `X-RateLimit-Source: local` |
| Service shutting down | `503` | `Retry-After`, `Connection: close` |

---

## 9. Idempotency interaction

Rate limiting and idempotency solve different problems:

- Rate limiting is **admission**. It may reject a request.
- Idempotency is **correctness**. It must not be bypassed.

Rules:

1. The main limiter runs before the DB write.
2. The idempotency key is stored in Postgres with a unique `(user_id, key)` constraint.
3. Redis may cache completed idempotency responses for speed, but Postgres is authoritative.
4. A legitimate replay may use a separate `user:replay` budget. If the replay budget is
   exhausted, return `429`; do not bypass it.
5. If Redis is down, the DB unique constraint still prevents duplicates.
6. If the DB is down, return `503`; do not fail open on idempotency.

Recommended order for `POST /tasks`:

```text
edge limit
  → global/IP limit
  → auth
  → user:tasks limit (or user:replay if an idempotency record exists)
  → validation
  → idempotency insert/lookup
  → DB pool/queue admission
  → insert task + outbox event
```

If checking the idempotency record before consuming `user:tasks` budget, keep the lookup
cheap and bounded (Redis or a short DB query). Do not do a full response replay before
authentication.

---

## 10. Configuration

```env
# Master switches
RATE_LIMIT_ENABLED=true
RATE_LIMIT_MODE=observe          # observe | enforce

# Redis
REDIS_URL=redis://redis:6379/0
RATE_LIMIT_REDIS_TIMEOUT_MS=50
RATE_LIMIT_FAILURE_THRESHOLD=5
RATE_LIMIT_FAILURE_WINDOW_MS=10000
RATE_LIMIT_OPEN_MS=30000

# Admission
GLOBAL_TASK_ADMISSION_RPS=200
LOCAL_FALLBACK_RPS=20
DB_POOL_MAX=20
DB_POOL_HIGH_WATERMARK=0.8
DB_POOL_MAX_WAITERS=5
QUEUE_BACKLOG_HIGH=5000
QUEUE_BACKLOG_LOW=1000

# Per-route limits (example names; load from config)
RATE_LIMIT_LOGIN_PER_MIN=5
RATE_LIMIT_USER_TASKS_PER_MIN=60
RATE_LIMIT_USER_REPLAY_PER_MIN=20
```

`RATE_LIMIT_MODE=observe` computes decisions and emits metrics but always allows the
request. Use it for at least one release before enforcing.

---

## 11. Observability

| Metric | Type | Labels | Purpose |
|--------|------|--------|---------|
| `scheduler_ratelimit_allowed_total` | Counter | `scope`, `source` | Allowed requests |
| `scheduler_ratelimit_denied_total` | Counter | `scope`, `source`, `reason` | Rejections |
| `scheduler_ratelimit_redis_errors_total` | Counter | `reason` | Timeouts, connection errors |
| `scheduler_ratelimit_fallback_active` | Gauge | `scope` | 1 while the local fallback is serving |
| `scheduler_ratelimit_circuit_state` | Gauge | `state` | closed/open/half_open |
| `scheduler_ratelimit_latency_seconds` | Histogram | `source` | Limiter latency |
| `scheduler_admission_rejected_total` | Counter | `reason` | DB pool, queue backlog, shutdown |
| `scheduler_db_pool_utilization` | Gauge | — | `totalCount / max` |
| `scheduler_db_pool_waiting` | Gauge | — | Waiting acquires |

Alerts:

- `scheduler_ratelimit_fallback_active == 1` for 1m.
- `scheduler_ratelimit_denied_total` rate > 5% of requests for 5m.
- `scheduler_admission_rejected_total` rate > 1% for 5m.
- `scheduler_db_pool_utilization > 0.9` for 2m.
- `scheduler_ratelimit_circuit_state{state="open"} == 1` for 1m.

---

## 12. Test plan

### Unit tests

- Token bucket math: refill, burst, cost > 1, retry-after calculation.
- Local bucket: same decisions as Redis for a fixed clock.
- Circuit breaker: closed → open → half-open → closed; denials do not trip it.
- Fallback: primary timeout, primary error, primary denial; correct source returned.
- Middleware: headers, 429 body, observe mode always allows.
- Key functions: user key, IP key, login email hash, no raw PII in Redis keys.

### Integration tests

- Redis available: two API replicas share a limit; total requests do not exceed it.
- Redis killed mid-load: fallback active, API returns bounded 429, DB pool stays below
  the watermark, no duplicate tasks.
- Redis latency (Toxiproxy 200ms): circuit opens, API P99 stays bounded.
- Redis flush: rate limit state lost; local limiter still protects the DB.
- DB pool saturation: admission returns 503 before queries pile up.
- Queue backlog above high watermark: new task creation returns 429.
- Recovery: ramp restores traffic without a retry storm.

### Load tests

- 10k requests/s against login and task creation; measure p95/p99 and 429 rate.
- Verify the global limit is enforced across 3 API replicas.
- Verify the DB write rate never exceeds the calibrated capacity during a Redis outage.

### Acceptance criteria

- [ ] Global and per-user limits are consistent across replicas.
- [ ] Redis outage switches to local fallback within one timeout window.
- [ ] No request path writes rate-limit state to Postgres.
- [ ] DB pool and queue-depth admission prevent overload during a Redis outage.
- [ ] `429` vs `503` semantics and `Retry-After` are correct.
- [ ] Metrics, alerts, and runbook entries exist.
- [ ] Observe mode and enforce mode are both tested.

---

## 13. Rollout

1. Add the Redis service (or managed Redis) and a dedicated connection.
2. Ship the limiter modules and `RATE_LIMIT_MODE=observe`; no rejections.
3. Validate decisions against real traffic for 1–2 days.
4. Enable `enforce` for login first, then task creation, then reads.
5. Calibrate `GLOBAL_TASK_ADMISSION_RPS` from load tests and DB capacity.
6. Enable the local fallback and test it by killing Redis in staging.
7. Add alerts and a runbook.

Rollback:

- Set `RATE_LIMIT_MODE=observe` or `RATE_LIMIT_ENABLED=false`.
- Keep Redis deployed; no schema changes are required.
- The coordinator's local dispatch limiter remains independent.

---

## 14. Open questions

1. **Redis topology:** single instance, Sentinel, or Cluster? For this project, Sentinel is
   likely enough; Cluster adds hash-tag complexity.
2. **Limit values:** calibrate from a real load test, not guesses.
3. **Replay bypass:** should an idempotent replay skip the main limiter? Proposed: use a
   separate replay budget.
4. **PII in keys:** hash emails/IPs before using them in Redis keys; decide retention.
5. **Per-tenant fairness:** if one tenant can still starve others within its limit, add
   weighted fair queuing at dispatch later.
