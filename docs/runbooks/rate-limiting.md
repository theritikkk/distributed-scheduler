# Rate Limiting and Admission Runbook

## What it does

The API applies layered token buckets:

| Scope | Purpose |
|-------|---------|
| `global:ingress` | Protects the API process and all downstream services |
| `login` | Limits credential checks before bcrypt, keyed by IP + email |
| `user:tasks` | Limits task creation per authenticated user |
| admission | Rejects task creation when DB pool or task queue is saturated |

Redis stores shared buckets. If Redis is unavailable or slow, each API replica uses a
smaller local bucket and a circuit breaker prevents repeated slow calls. Redis is never
authoritative for task state.

## Defaults

```env
RATE_LIMIT_ENABLED=false
RATE_LIMIT_MODE=observe
REDIS_URL=redis://redis:6379/0
LOCAL_FALLBACK_RPS=20
ADMISSION_ENABLED=false
```

`observe` sets `X-RateLimit-*` headers and metrics but does not reject. `enforce` returns
`429` with `Retry-After`.

## Enable safely

1. Start Redis and verify `redis-cli ping`.
2. Set `RATE_LIMIT_ENABLED=true`, keep `RATE_LIMIT_MODE=observe`, and restart one API
   replica.
3. Watch `scheduler_ratelimit_allowed_total`, `scheduler_ratelimit_denied_total`,
   `scheduler_ratelimit_fallback_active`, and `scheduler_ratelimit_circuit_state`.
4. Leave admission off while load-testing; it has no observe mode. Enable it in staging
   with conservative watermarks, then tighten them.
5. Switch one replica to `RATE_LIMIT_MODE=enforce`; verify `Retry-After` and 429 rates.
6. Roll out to all replicas.

## Symptoms and actions

### `RateLimitFallbackActive` is firing

Redis is down, unreachable, or slower than `RATE_LIMIT_REDIS_TIMEOUT_MS`.

- Check Redis health, network policy, DNS, and credentials.
- Keep the fallback active; it is intentionally conservative.
- If Redis remains down, lower `LOCAL_FALLBACK_RPS` rather than disabling limits.
- `scheduler_ratelimit_redis_errors_total{reason="timeout"}` indicates latency;
  `reason="connection"` indicates connectivity/auth.

### `RateLimitCircuitOpen` is firing

Repeated Redis failures opened the circuit for `RATE_LIMIT_OPEN_MS`. Requests use the
local fallback. After the cool-down the breaker sends one half-open probe. Do not restart
API replicas just to reset it; fix Redis first.

### `AdmissionRejectionsHigh` is firing

- `reason="db_pool"`: inspect PostgreSQL slow queries, lock waits, and
  `scheduler_db_pool_utilization` / `scheduler_db_pool_waiting`.
- `reason="queue_backlog"`: workers are behind. Admission latches at
  `QUEUE_BACKLOG_HIGH` and clears at `QUEUE_BACKLOG_LOW`, so it does not flap around a
  single watermark. Scale workers or reduce producer rate.
- 429/503 responses include `Retry-After`; clients should honor it.

### Login endpoints return 429 immediately

The limit is `RATE_LIMIT_LOGIN_PER_MIN` (default 5) with burst `RATE_LIMIT_LOGIN_BURST`
(default 2), keyed by client IP + normalized email. Verify `TRUST_PROXY` so `req.ip` is
the real client, not the proxy. Do not raise the limit as a first response to credential
stuffing; inspect `scheduler_ratelimit_denied_total{scope="login"}`.

## Rollback

- Immediate: `RATE_LIMIT_ENABLED=false`.
- Softer: `RATE_LIMIT_MODE=observe`.
- Admission only: `ADMISSION_ENABLED=false` (independent of rate limiting).
- Redis can remain running while disabled; the outbox does not depend on it.

## Metrics

- `scheduler_ratelimit_allowed_total{scope,source}`
- `scheduler_ratelimit_denied_total{scope,source,reason}`
- `scheduler_ratelimit_fallback_active{scope}`
- `scheduler_ratelimit_circuit_state{state}`
- `scheduler_ratelimit_redis_errors_total{reason}`
- `scheduler_ratelimit_latency_seconds{scope,source}`
- `scheduler_admission_rejected_total{reason}`
- `scheduler_db_pool_utilization`, `scheduler_db_pool_waiting`
