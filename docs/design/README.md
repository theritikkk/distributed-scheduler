# Design Notes

This directory contains the concrete design for the reliability and scale improvements
discussed for the scheduler.

| Document | Scope | Status |
|----------|-------|--------|
| [01 — Jitter and dispatch_at](01-jitter-and-dispatch-time.md) | Stable cron jitter, `dispatch_at`, execution-slot uniqueness | Implemented (Phase 1) |
| [02 — Rate limiting and Redis fallback](02-rate-limiting-and-redis-fallback.md) | Per-user/global limits, Lua token bucket, local fallback, admission control | Implemented (Phase 4), opt-in |
| [03 — Coordinator backpressure](03-coordinator-backpressure.md) | Queue-depth watermarks, pause/resume, DB gate guard | Implemented (Phase 5), observe by default |
| [04 — Implementation plan](04-implementation-plan.md) | Phases, dependencies, acceptance criteria, rollout | Phases 0–5 complete; Phase 6 lab-validated; v1.5.0 data-plane hardening locally validated; Phase 7 implemented and locally validated |

## Guiding principles

1. **Postgres is the source of truth.** Redis is an accelerator, never the authority for
   task state or idempotency.
2. **Jitter smooths; admission control protects.** Jitter alone does not prevent overload.
3. **Never fail open into Postgres.** If Redis is unavailable, use a smaller local budget
   and shed load; do not let the database become the rate limiter.
4. **At-least-once delivery plus idempotent state transitions.** Arbitrary shell commands
   cannot be exactly-once; task state can be.
5. **Additive migrations, feature flags, observe mode first.** Every change must be
   deployable and rollback-safe without a maintenance window.

## Current-state references

The designs reference the repository as of the design date:

- API: `api-gateway/src/`
- Coordinator: `coordinator/`
- Worker: `worker/`
- Schema: `database/init.sql`
- Compose: `docker-compose.yml`
- Monitoring: `monitoring/`

`database/init.sql` remains the fresh-bootstrap schema. Existing databases use the
versioned migration runner (`scripts/migrate.py`) and the SQL in `database/migrations/`.

Phases 0–5 are implemented: see
[04 — Implementation plan](04-implementation-plan.md) for details. Phase 1 adds
`dispatch_at`, stable jitter, execution-slot uniqueness, missed-slot skipping, and
jittered retry backoff. Phase 2 adds durable API idempotency keys, lease-guarded worker
claims, attempt history, stale-dispatch recovery, and an active-execution unique index.
Phase 3 adds the transactional outbox and relay. Phase 4 adds Redis/local rate limiting,
admission control, and worker concurrency. Phase 5 adds coordinator queue/DB
backpressure and reconnect loops.

Phase 6 application code is implemented: migration `0010` adds DB-backed
`scheduled_events`, the outbox relay materializes timers, a poller publishes them with
confirms, and `RABBITMQ_QUEUE_TYPE=quorum` removes the TTL queues from the primary path.
The three-node PostgreSQL/RabbitMQ/Redis lab in `deploy/ha-lab/` passed the v1.4.0
failover drills. v1.5.0 then added Redis Cluster support, quorum TTL/delivery limits,
PgBouncer transaction pooling, a PITR design and local restore drill, an nginx load
balancer, and HA metrics/alerts. A 2026-11-12 v1.6.0 re-validation exercised PgBouncer,
nginx, quorum-queue leader loss, and HA metrics/alerts in AWS; Redis Cluster and PITR
remain locally validated only.

Phase 7 is implemented and locally validated: migrations `0011`–`0013` add tenant
quotas, archive tables, and timer trace headers; the API has opt-in task/execution and
idempotency-response caches, weighted tenant admission, and a sharded global ingress
limiter; W3C tracing crosses the outbox/timer boundary into the coordinator and worker;
and the coordinator can archive terminal execution/attempt/outbox rows in bounded
batches. Every feature is default-off and fails open to PostgreSQL. Phase 7 stayed
default-off during the 2026-11-12 AWS re-validation, so its features remain locally
validated only. Operational procedures live in [docs/runbooks](../runbooks/), the
version history in [docs/evolution](../evolution/README.md), and the internal mechanics
in [docs/internals](../internals/README.md), including the
[Phase 7 page](../internals/09-cache-quotas-tracing-archival.md).
