# Phase 7 Scaling Runbook

This runbook covers the opt-in Phase 7 accelerators: task/execution caching,
completed idempotency-response caching, weighted tenant quotas, sharded global
ingress limiting, OpenTelemetry tracing, and bounded archival.

**Golden rule:** none of these features is authoritative. If Redis, the OTLP
collector, or the archive job is unhealthy, turn the feature off or let it fail
open. Never "fix" a problem by moving task state, idempotency, or leases into
Redis.

## Defaults

```env
# Read cache
CACHE_ENABLED=false
CACHE_TTL_SEC=15
CACHE_REDIS_TIMEOUT_MS=50

# Completed idempotency responses
IDEMPOTENCY_CACHE_ENABLED=false
IDEMPOTENCY_CACHE_TTL_SEC=900

# Weighted tenant admission
TENANT_QUOTAS_ENABLED=false
TENANT_QUOTA_CACHE_TTL_SEC=30
TENANT_DEFAULT_WEIGHT=1
TENANT_DEFAULT_TASKS_PER_MIN=60
TENANT_DEFAULT_BURST=10

# Global ingress sharding
GLOBAL_INGRESS_SHARDS=16

# Tracing
OTEL_ENABLED=false
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318/v1/traces
OTEL_SAMPLER_ARG=0.1

# Archival (coordinator)
ARCHIVE_ENABLED=false
ARCHIVE_RETENTION_DAYS=30
ARCHIVE_BATCH_SIZE=500
ARCHIVE_INTERVAL_SEC=3600
ARCHIVE_MAX_BATCHES=20
ARCHIVE_SLEEP_MS=50
```

`RATE_LIMIT_ENABLED` still gates the shared limiter. `TENANT_QUOTAS_ENABLED` is
independent: when it is on, it replaces the static per-user task limiter.
`RATE_LIMIT_MODE=observe|enforce` applies to both.

## Pre-flight

1. Apply migrations `0011`–`0013` with `make migrate`. They are additive and can
   be applied before deploying the new images.
2. Verify the API and coordinator/worker images include this version.
3. Confirm Redis is reachable from the API replicas (`redis-cli ping`) and the
   OTLP endpoint is reachable if tracing will be enabled.
4. Run the archival dry-run before enabling the loop:

   ```bash
   DATABASE_URL=postgresql://... python scripts/archive_data.py --dry-run
   # or
   make archive-data ARCHIVE_DRY_RUN=1
   ```

5. Record the baseline for the metrics listed at the end of this page.

## Enablement order

Enable one feature at a time, one API replica or one coordinator first.

### 1. Read cache

```env
CACHE_ENABLED=true
CACHE_TTL_SEC=15
CACHE_REDIS_TIMEOUT_MS=50
```

Watch:

- `sum(rate(scheduler_cache_hits_total[5m]))`
- `sum(rate(scheduler_cache_misses_total[5m]))`
- `sum(rate(scheduler_cache_errors_total[5m]))`
- `scheduler_cache_latency_seconds{namespace,op}`

Acceptance: errors stay near zero, latency stays below
`CACHE_REDIS_TIMEOUT_MS`, and p95 task-read latency improves or stays flat.
A hit ratio below 20% with meaningful traffic triggers `CacheHitRatioLow`; it is
usually a too-short TTL or mostly unique reads, not an outage.

Verify invalidation manually:

1. `GET /api/v1/tasks/:id` twice; the second read should be a hit.
2. `PUT /api/v1/tasks/:id`; the next `GET` must return the updated row.
3. `GET /api/v1/tasks/:id/executions`; after a new execution, the first page
   must reflect it even though old page keys are still in Redis. The generation
   counter is what makes this work.

### 2. Completed idempotency-response cache

```env
IDEMPOTENCY_CACHE_ENABLED=true
IDEMPOTENCY_CACHE_TTL_SEC=900
```

This feature is inert unless `IDEMPOTENCY_ENFORCE=true` and the client sends
`Idempotency-Key`.

Watch:

- `scheduler_cache_hits_total{namespace="idempotency"}`
- `scheduler_cache_errors_total{namespace="idempotency"}`
- `409` rate on `POST /api/v1/tasks`

Verify:

1. Send the same key + body twice; expect `201` then `201` with
   `Idempotent-Replayed: true`.
2. Send the same key with a different body; expect `409` from both the cache
   path and the PostgreSQL path.
3. Stop Redis and repeat step 1; the replay must still come from
   `idempotency_keys`.

### 3. Weighted tenant quotas

```env
TENANT_QUOTAS_ENABLED=true
TENANT_QUOTA_CACHE_TTL_SEC=30
TENANT_DEFAULT_WEIGHT=1
TENANT_DEFAULT_TASKS_PER_MIN=60
TENANT_DEFAULT_BURST=10
# Keep observe for the first cycle
RATE_LIMIT_MODE=observe
```

Add rows deliberately:

```sql
INSERT INTO tenant_quotas (user_id, weight, tasks_per_min, burst, enabled)
VALUES ('<user-uuid>', 2.0, 60, 10, TRUE)
ON CONFLICT (user_id) DO UPDATE
SET weight = EXCLUDED.weight,
    tasks_per_min = EXCLUDED.tasks_per_min,
    burst = EXCLUDED.burst,
    enabled = EXCLUDED.enabled;
```

Effective values for that tenant are 120 tasks/min and burst 20. Disable a row
with `enabled = FALSE` to return it to the configured defaults; do not delete it
unless you intend to lose the record.

Watch:

- `scheduler_tenant_quota_lookups_total{source}` — expect `redis`, `database`,
  then mostly `local` as caches warm.
- `scheduler_tenant_quota_lookup_errors_total`
- `scheduler_ratelimit_denied_total{scope="tenant:tasks"}`
- Response headers `X-Quota-*` and `X-RateLimit-*`.

Verify:

1. Create a low-weight and a high-weight tenant; confirm the high-weight tenant's
   `X-Quota-Tasks-Per-Min` is proportionally larger.
2. In `observe`, drive one tenant over its limit; expect headers and denied
   metrics but no `429`.
3. Switch one replica to `enforce`; expect `429` with `Retry-After` for that
   tenant only.
4. Stop Redis; confirm requests still proceed using local/default quotas and
   `TenantQuotaLookupErrors` may rise while `scheduler_tenant_quota_lookups_total
   {source="local"}` handles the traffic.

Remember: quotas change admission, not FIFO dispatch. A paused or backlogged
`scheduler.tasks` queue still serves messages in broker order.

### 4. Sharded global ingress limiter

`GLOBAL_INGRESS_SHARDS` is independent of the other flags, but the limiter only
runs when `RATE_LIMIT_ENABLED=true`.

```env
RATE_LIMIT_ENABLED=true
GLOBAL_INGRESS_SHARDS=16
```

Watch `scheduler_ratelimit_shards{scope="global:ingress"}`. It should equal the
configured value on every API replica. If it is missing, check startup logs for
config validation errors.

Verify:

- `GLOBAL_INGRESS_SHARDS=1` restores the old single-key behavior.
- With 16 shards, the aggregate burst is still `GLOBAL_INGRESS_BURST`; only the
  per-key capacity is divided. Do not multiply the total by the shard count.
- Redis Cluster: the shard suffix keeps keys spread; no hash tag is required
  because each call touches one key.

### 5. OpenTelemetry tracing

Install the optional Python packages in the coordinator/worker images (already
listed in `requirements.txt`), then:

```env
OTEL_ENABLED=true
OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318/v1/traces
OTEL_SAMPLER_ARG=0.1
# Optional; defaults per service
OTEL_SERVICE_NAME=api-gateway
```

Roll out the API first, then the coordinator, then workers. Watch:

- `scheduler_tracing_enabled` — 1 on each instrumented service.
- `scheduler_trace_spans_exported_total`
- `scheduler_trace_export_errors_total`
- Collector/drop metrics, if available.

Verify one trace end to end:

1. Send `POST /api/v1/tasks` with `traceparent` or let the API start a trace.
2. In the collector/UI, find the API SERVER span.
3. Confirm child spans exist for the outbox materialization/publish, the due
   publish, `task.process`, and `result.process`.
4. Confirm the same trace ID appears after a scheduled timer delay (this is what
   migration `0013` enables).

Tracing failure is not a request failure. If the collector is down, spans are
dropped asynchronously and `TraceExportErrors` fires; requests continue.

### 6. Archival

Start with a dry-run, then a manual cycle, then the coordinator loop.

```bash
# Count eligible rows without moving anything
make archive-data ARCHIVE_DRY_RUN=1

# Move one bounded run manually
make archive-data ARCHIVE_RETENTION_DAYS=30 ARCHIVE_BATCH_SIZE=500 ARCHIVE_MAX_BATCHES=20

# Enable the coordinator loop
ARCHIVE_ENABLED=true
ARCHIVE_RETENTION_DAYS=30
ARCHIVE_BATCH_SIZE=500
ARCHIVE_INTERVAL_SEC=3600
ARCHIVE_MAX_BATCHES=20
ARCHIVE_SLEEP_MS=50
```

Watch:

- `scheduler_archive_rows_total{table}`
- `scheduler_archive_runs_total{status}`
- `scheduler_archive_errors_total`
- `scheduler_archive_duration_seconds`
- `scheduler_archive_oldest_row_age_seconds{table}`

Verify only terminal rows moved:

```sql
-- Must be 0: no pending/running execution was archived
SELECT COUNT(*) FROM task_execution_archive
WHERE status IN ('pending', 'running');

-- Must be 0: no pending outbox row was archived
SELECT COUNT(*) FROM outbox_archive
WHERE status = 'pending';

-- Recent terminal rows must remain live
SELECT COUNT(*) FROM task_executions
WHERE status IN ('completed', 'failed', 'timeout', 'cancelled')
  AND COALESCE(completed_at, created_at) > NOW() - INTERVAL '30 days';
```

`ArchiveErrors` fires when any cycle fails in the last hour. The loop backs off
for at most 60 seconds and then retries; live rows are never deleted without the
archive copy committing in the same transaction. `ArchiveLagHigh` means the
oldest live terminal row is over 90 days old; check that `ARCHIVE_ENABLED=true`
and that cycles are not blocked on locks or disk.

## Symptoms and actions

### `CacheErrorRateHigh`

- Check Redis health, network, credentials, and `CACHE_REDIS_TIMEOUT_MS`.
- The API is already failing open. Do not disable the cache to "fix" a
  PostgreSQL latency problem; the cache is not the authority.
- If errors are dominated by `op="decode"`, suspect a mixed value format or a
  manual Redis write. Flush only the `sched:cache:*` namespace, never the whole
  database.

### `CacheHitRatioLow`

- Confirm traffic is actually repeated reads. Short TTLs and mostly unique task
  reads legitimately produce low hit ratios.
- Raise `CACHE_TTL_SEC` only after checking invalidation behavior on update and
  delete.
- If execution-history hits are low, verify the generation counter is not being
  incremented on every request.

### `TenantQuotaDenialsHigh`

- Confirm the denial is intentional: inspect `X-Quota-*` on the response and the
  tenant's row.
- A missing or disabled row uses defaults; it is a common cause of unexpected
  denials after enabling the feature.
- Do not raise global defaults to fix one tenant. Insert a row for that tenant.
- If the denial rate is caused by Redis fallback, remember the local bucket is
  smaller and may deny earlier than the shared bucket.

### `TenantQuotaLookupErrors`

- Check PostgreSQL latency and the `tenant_quotas` table.
- Requests fall back to the last local value or configured defaults, so task
  creation remains available.
- If errors persist, the API will keep serving; fix the database before tuning
  quotas.

### `GlobalIngressShardsMissing`

- `scheduler_ratelimit_shards{scope="global:ingress"}` is below 1 on an API
  replica. Check `GLOBAL_INGRESS_SHARDS` parsing and startup logs.
- A value of `1` is valid; the alert fires only when the gauge is missing or
  non-positive.

### `TraceExportErrors`

- Check `OTEL_EXPORTER_OTLP_ENDPOINT`, collector availability, and network
  policy.
- Requests and messages continue. If the collector stays down, set
  `OTEL_ENABLED=false` to stop retrying, or leave it on and accept dropped
  spans; the feature is not on the correctness path.

### `ArchiveErrors` / `ArchiveLagHigh`

- Inspect coordinator logs for lock timeouts, disk exhaustion, or schema drift.
- Run `make archive-data ARCHIVE_DRY_RUN=1` to confirm the query still works.
- Lower `ARCHIVE_BATCH_SIZE` or increase `ARCHIVE_SLEEP_MS` if batches contend
  with live traffic.
- Never delete archive rows or truncate live terminal rows to catch up. Fix the
  job or increase capacity.

## Rollback

All rollbacks are flag-first; no schema rollback is required.

| Feature | Immediate rollback | Notes |
| --- | --- | --- |
| Read cache | `CACHE_ENABLED=false` | API serves PostgreSQL directly |
| Idempotency cache | `IDEMPOTENCY_CACHE_ENABLED=false` | durable keys still enforce replay |
| Tenant quotas | `TENANT_QUOTAS_ENABLED=false` | static per-user limiter resumes if `RATE_LIMIT_ENABLED=true` |
| Quota enforcement only | `RATE_LIMIT_MODE=observe` | records decisions without 429 |
| Sharded limiter | `GLOBAL_INGRESS_SHARDS=1` | restores single-key behavior |
| Tracing | `OTEL_ENABLED=false` | no-op fallback; no request impact |
| Archival | `ARCHIVE_ENABLED=false` | live tables resume growing; no data loss |

Schema rollback (only if you are certain the tables are not needed):

- `0013` removes `scheduled_events.headers`; traces stop surviving the timer
  boundary. Apply only after all producers/consumers use the old shape.
- `0012` drops archive tables. This **destroys archived history**; export it
  first.
- `0011` drops `tenant_quotas`. Disable `TENANT_QUOTAS_ENABLED` first, or the
  API will log lookup failures and use defaults.

## Metrics reference

| Metric | Type | Labels |
| --- | --- | --- |
| `scheduler_cache_hits_total` | counter | `namespace` |
| `scheduler_cache_misses_total` | counter | `namespace` |
| `scheduler_cache_errors_total` | counter | `namespace`, `op` |
| `scheduler_cache_latency_seconds` | histogram | `namespace`, `op` |
| `scheduler_tenant_quota_lookups_total` | counter | `source` |
| `scheduler_tenant_quota_lookup_errors_total` | counter | — |
| `scheduler_ratelimit_shards` | gauge | `scope` |
| `scheduler_tracing_enabled` | gauge | — |
| `scheduler_trace_spans_exported_total` | counter | — |
| `scheduler_trace_export_errors_total` | counter | — |
| `scheduler_archive_rows_total` | counter | `table` |
| `scheduler_archive_errors_total` | counter | — |
| `scheduler_archive_runs_total` | counter | `status` |
| `scheduler_archive_duration_seconds` | histogram | — |
| `scheduler_archive_oldest_row_age_seconds` | gauge | `table` |

Dashboard: Grafana → **Distributed Scheduler v2** → *Phase 7: Cache, Quotas,
Tracing & Archival* (panels 116–126).

## Related

- [Phase 7 internals](../internals/09-cache-quotas-tracing-archival.md)
- [v1.6.0 version record](../evolution/v1.6.0-optional-scaling.md)
- [Rate limiting runbook](rate-limiting.md)
- [Deployment guide](../DEPLOYMENT-DOCS.md)
