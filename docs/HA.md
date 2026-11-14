# High Availability and Failover Design

This guide describes the HA topology, the validated three-node lab, and the remaining
production work. The default `docker-compose.yml` remains a single-node
development/demo deployment: it has one PostgreSQL instance, one RabbitMQ node, and one
Redis instance. Do not claim HA for that topology.

**Current status (2026-11-12):** the v1.6.0 code was re-validated on a freshly
provisioned three-node AWS lab across `ap-south-1a/b/c`. Four live drills passed:
PostgreSQL primary loss (720/720 tasks, 1 s promotion, RPO 0), RabbitMQ queue-leader
loss with a 200-message backlog (200/200 exactly once, 2 s election), Redis master
loss with enforce-mode rate limiting (0 5xx, Sentinel promotion in 5.95 s), and
API/HAProxy node failure through nginx (717/720 accepted, all accepted tasks
completed). The lab was **destroyed after capture**; it is not currently running. The
full AWS evidence package is
[`screenshots/deployment/test-7/`](../screenshots/deployment/test-7/README.md).

**Validated baseline (2026-11-06):** Phase 6 was deployed to 3 Spot `m7i-flex.large`
nodes across `ap-south-1a/b/c` with etcd + Patroni PostgreSQL 16.6 (synchronous
standby), a 3-node RabbitMQ cluster with quorum queues, Redis master/replica + 3
Sentinels + sentinel proxy, and per-node HAProxy. PostgreSQL RPO was 0 with a ~3 second
promotion, and losing the RabbitMQ queue leader retained every queued message. The
2026-11-06 results below are kept as the historical v1.4.0 baseline; the 2026-11-12
results are a separate v1.6.0 run and must not be mixed with them.

This is a **lab**, not a production deployment: it uses Spot instances, self-managed
Patroni/etcd, and a software load-balancer path instead of a managed NLB. The default
`docker-compose.yml` and the free-tier EC2 demo remain single-node and are **not** HA.

The validation lab was destroyed after the v1.4.0 drills and again after the
2026-11-12 re-validation to stop charges. Redeploy it with
[`deploy/ha-lab/`](../deploy/ha-lab/README.md) to rerun the procedures; the measured
results in the 2026-11-06 section are from the v1.4.0 run, and the 2026-11-12 section
is the v1.6.0 run. The v1.5.0 evidence is local (see
[evolution/v1.5.0](evolution/v1.5.0-data-plane-hardening.md) and the harnesses under
`tests/`).

For operational drills and recovery steps, see
[failover runbook](runbooks/failover.md).

## Targets

| Component | Target | RPO | RTO |
|-----------|--------|-----|-----|
| PostgreSQL | Managed Multi-AZ with PITR, or Patroni + synchronous standby + PgBouncer | 0 with sync standby; seconds with async | Minutes |
| RabbitMQ | 3-node cluster across AZs with quorum queues and a stable endpoint (HAProxy or LB) | 0 for confirmed, replicated messages | Minutes |
| Redis | Managed Redis with automatic failover, Sentinel, or Redis Cluster | Ephemeral; loss is acceptable | Seconds to minutes |
| API / coordinator / workers | Multiple replicas behind HAProxy plus nginx or a managed load balancer | None (stateless) | Seconds |

## Validated lab results (2026-11-12, v1.6.0 re-validation)

A fresh three-node Spot `m7i-flex.large` lab was provisioned across
`ap-south-1a/b/c` and deployed with the v1.6.0 code, migrations `0011`–`0013`,
per-node PgBouncer, nginx on node1, and the Patroni/HAProxy/PgBouncer/Redis
observability stack. All four drills ran against the live cluster; the full
screenshots, raw logs, and reproduction notes are in
[`screenshots/deployment/test-7/`](../screenshots/deployment/test-7/README.md).

| Drill | Injection | Result | RPO | RTO |
|-------|-----------|--------|-----|-----|
| PostgreSQL primary loss | Stopped Patroni on leader node3 during a 720-task, 4 req/s stream | node2 promoted, node1 stayed sync standby, node3 rejoined as replica; 720/720 completed exactly once, 0 duplicate slots | 0 (sync standby, 0 lag) | 1 s to leader; 5.28 s max client pause |
| RabbitMQ queue-leader loss | 200-message ready backlog, stopped the RabbitMQ app on leader node2 | Leader moved to `rabbit@node1`; all 200 retained and completed exactly once; queues drained to 0/0/0 | 0 confirmed messages lost | 2 s election; backlog drained by `14:21:32Z` |
| Redis master loss | Forced master to node1, enabled `RATE_LIMIT_MODE=enforce`, stopped node1 Redis | Sentinel promoted node3 in 5.95 s, proxy followed in 6.96 s; 0 5xx and 0 network errors; circuit opened then closed; node1 rejoined in 70.39 s | Rate-limit counters are ephemeral; fail-open by design | 6.96 s to proxy follow |
| API + HAProxy node failure | Stopped node2 API and node3 HAProxy while streaming through node1 nginx | 717/720 accepted; all 717 completed exactly once; 3 client failures during the overlapping recovery window | 0 accepted tasks lost | API healthy in 48 s; HAProxy in 57 s |

The PostgreSQL and RabbitMQ drills had no client-visible failures. The Redis drill
enforced limits as designed, so its 429 responses are policy decisions rather than
errors. The API/HAProxy drill exposed the expected limitation of node1-only nginx and
passive health checks: 3 requests failed while nginx waited for two upstream failures
before removing a just-stopped backend.

**Operational note:** the two `RabbitMQQueueConsumersMissing` alerts for
`scheduler.due` and `scheduler.results` were false positives. The coordinator consumes
those queues with `basic_get()` pull loops, so RabbitMQ reports zero consumers by design;
`scheduler.tasks` had 18 push consumers. Narrow the alert expression before production.

## Validated lab results (2026-11-06, v1.4.0 baseline)

The `deploy/ha-lab/` stack was deployed to three Spot `m7i-flex.large` instances across
`ap-south-1a/b/c` and exercised end to end. Per-node HAProxy exposes the API on `:8080`,
PostgreSQL primary/replica on `:5433`/`:5434`, RabbitMQ on `:5673`, Redis Sentinel proxy
on `:6380`, and HAProxy stats on `:8404`. There was no NLB; the v1.4.0 API was
reached through any node's HAProxy. v1.5.0 adds an nginx `:8081` entry point on node1
(not exercised in that AWS run).

| Drill | Injection | Result | Measured |
|-------|-----------|--------|----------|
| PostgreSQL primary loss | Stopped Patroni on the leader during a 90-task stream | Node1 promoted, node2 became sync standby, HAProxy rerouted writes; 89/89 accepted tasks completed exactly once, no duplicate slots | RPO 0; promotion in ~2.8 s; one client 500 before reconnect |
| RabbitMQ leader loss | Paused all 9 workers, built a 301-message backlog, stopped the queue-leader broker | Quorum leader moved to node3, all 301 messages retained, consumers reconnected; 300/300 tasks completed exactly once | No message loss, no duplicate executions |
| Redis master loss | Stopped the Redis master while enforce-mode limiting was enabled on one API replica | Sentinel promoted node2, the proxy followed, and the API switched to its local bucket for 59 requests before the circuit half-opened and closed | 5 Redis connection errors; fallback latency ~14 ms typical; all 180 requests returned 201 |
| End-to-end load | k6 through node1 HAProxy, 20 VUs for 60 s, 5 s schedule delay | 7,586 submissions at 126 req/s, 0 failures, p95 69.6 ms; 8,040 total tasks (including warm-up) completed exactly once | 0 retries, 0 duplicate slots, outbox fully drained |

The drill log with exact dates and follow-ups is in
[failover.md](runbooks/failover.md#drill-log).

## v1.5.0 hardening status (2026-11-08)

These changes were validated **locally** (Docker harnesses) or by configuration
review. The 2026-11-12 re-validation exercised PgBouncer, nginx, quorum-queue leader
loss, and the observability stack in AWS; Redis Cluster, PITR, and a quorum network
partition remain local-only.

| Area | Change | Evidence | Live AWS rerun |
|------|--------|----------|----------------|
| Redis Cluster | `REDIS_CLUSTER_NODES` selects ioredis `Cluster`; same single-key Lua token bucket | Six-node harness, including `CLUSTER FAILOVER FORCE` | Not run (lab uses Sentinel) |
| Quorum queues | Quorum-only `x-message-ttl` / `x-delivery-limit`; broker alarms and `pause_minority` | 160/160 messages exactly once across partition/recovery | Leader loss yes (2026-11-12); partition not run |
| PgBouncer | Transaction pooling per node on `:6432`, SCRAM, `DISCARD ALL`, exporter `:9127` | SCRAM, reuse, advisory locks, session reset, exporter | Yes — all app traffic during the 2026-11-12 drills |
| PITR | Production pgBackRest/WAL-G design; local base-backup + WAL replay drill | Two target times (20 and 30 rows), both promoted | Not run |
| Load balancer | nginx `:8081` in front of per-node HAProxy `:8080` | Three-backend distribution, failover, recovery | Yes — drills 1, 2, and 4 (2026-11-12) |
| Observability | Patroni/HAProxy/PgBouncer/Redis scrape jobs and HA alert rules | Metric names and local exporter checks | Yes — 29/29 targets, 54 rules (2026-11-12) |

The detailed version record is [evolution/v1.5.0](evolution/v1.5.0-data-plane-hardening.md)
and the mechanics are in [internals](internals/README.md).

The remaining production gaps are narrower than after v1.4.0 but still real: PITR and
a quorum network partition have not been run in AWS, there is no production S3 backup
bucket, no managed load balancer or DNS-based stable endpoint (nginx remains a
node1-only entry point), and no Alertmanager route test against a real pager.

## PostgreSQL

Preferred order:

1. Managed PostgreSQL (RDS/Aurora/Cloud SQL) with Multi-AZ, automated backups, PITR,
   and a read replica.
2. Self-managed Patroni + etcd/Consul with a synchronous standby, a witness, and
   PgBouncer in transaction-pooling mode.

Checklist:

- [x] Provision a primary and synchronous standby in the lab (`deploy/ha-lab/`).
      PITR/off-site backups are implemented as design + local drill, not deployed to
      the lab.
- [x] Put PgBouncer in front of the primary. The lab runs one transaction-pooling
      PgBouncer per node on `:6432`; services connect through the local pooler and
      migrations stay direct on HAProxy `:5433`. Exercised in the 2026-11-12 AWS
      re-validation.
- [x] Use `pg_advisory_xact_lock` for outbox aggregate ordering. Do **not** use
      session-scoped advisory locks behind transaction pooling.
- [x] Configure `max_connections`, `statement_timeout`, `lock_timeout`, and
      `idle_in_transaction_session_timeout`. The lab Patroni template sets
      `max_connections=200`, `statement_timeout=5s`, `lock_timeout=2s`, and
      `idle_in_transaction_session_timeout=60s`.
- [x] Route writes to the primary. Every service uses HAProxy `:5433`, whose backend is
      Patroni's `/primary` health check. `:5434` exposes replicas for read-only checks.
- [x] Run migrations against the primary. `deploy.sh` applies them through HAProxy
      `:5433` before starting the application services.
- [x] Monitor replication lag, failover events, connection saturation, disk, and PITR
      restore readiness. `prometheus.ha.yml` scrapes Patroni `:8008`, HAProxy `:8404`,
      PgBouncer `:9127`, and Redis `:9121`; HA alert rules cover these signals.
- [x] Run a restore drill and record the result in the failover runbook. The local
      two-target PITR drill passed; the quarterly production drill is still an
      operational commitment, not something the repository can prove.

The scheduler writes task state, outbox rows, and materialized timer rows in one
PostgreSQL transaction, uses execution-slot uniqueness and one-active-execution
constraints, and reconnects after DB errors. A failover therefore must preserve committed
rows; the outbox relay, timer poller, and reconciliation recover work after reconnect.

## RabbitMQ

### Quorum-queue prerequisite: solved in code

RabbitMQ quorum queues do not support per-message TTL. The scheduler now has a
DB-backed timer path:

1. The outbox relay materializes `task.scheduled` / `task.retry` rows into
   `scheduled_events` (migration `0010`).
2. The scheduled-event poller publishes due rows to `scheduler.due` or
   `scheduler.tasks` with publisher confirms and `mandatory=True`.
3. `RABBITMQ_QUEUE_TYPE=quorum` declares the non-TTL queues as quorum and deliberately
   does **not** declare `scheduler.delay` / `scheduler.retry_delay`.

The code path is implemented and tested, and the three-node broker is provisioned in
`deploy/ha-lab/` with all four queues replicated across the cluster.

### Enabling quorum mode

Do this in a maintenance window after the DB timer path has run cleanly:

```bash
# 1. Apply migration 0010 and deploy SCHEDULED_EVENTS_ENABLED=true,
#    OUTBOX_ENABLED=true to every service.
make migrate

# 2. Wait until scheduled_events has no overdue backlog and RabbitMQ shows the
#    classic delay/retry queues empty.
#    Then delete the old TTL queues (or recreate the cluster without them).
rabbitmqctl delete_queue scheduler.delay
rabbitmqctl delete_queue scheduler.retry_delay

# 3. Redeclare with quorum queues. All API/coordinator/worker replicas must use
#    the same value; mixed classic/quorum declarations fail with a queue-type
#    mismatch.
RABBITMQ_QUEUE_TYPE=quorum
```

`RABBITMQ_QUEUE_TYPE=quorum` is rejected unless `OUTBOX_ENABLED=true` and
`SCHEDULED_EVENTS_ENABLED=true`; the worker and API additionally reject
`OUTBOX_ENABLED=false` in quorum mode. This prevents an accidental fallback to TTL
queues that do not exist.

### Broker checklist

- [x] 3-node cluster across availability zones (lab: `ap-south-1a/b/c`).
- [x] Replace the TTL delay queues with DB-backed scheduled events.
- [x] Implement quorum declaration for `scheduler.tasks`, `scheduler.due`,
      `scheduler.results`, and `scheduler.tasks.dlq`.
- [x] Provision the 3-node cluster and enable `RABBITMQ_QUEUE_TYPE=quorum`.
- [x] Keep publisher confirms and manual consumer acks.
- [x] Configure queue-length and DLQ alerts.
- [x] Configure broker disk/memory alarms for the managed cluster. The lab mounts
      `rabbitmq.conf.tmpl` with resource alarms, `pause_minority`, and consumer-timeout
      protection; Prometheus also alerts on RabbitMQ alarms and partitions.
- [x] Use a stable load balancer or connection endpoint; clients reconnect with jitter.
      The lab uses each node's HAProxy `:5673`; nginx provides the external API entry
      point on `:8081`. There is no managed NLB.
- [x] Test loss of one node: messages survive, consumers reconnect, and no duplicate
      executions are produced (Drill 2: 301 messages retained, 300/300 exactly once).
- [x] Test a network partition: quorum queues require a majority; document the pause.
      The local harness isolated one and then two brokers: the majority kept serving,
      the minority paused, and 160/160 messages were consumed exactly once after
      recovery.
- [x] Set intentional queue/message TTLs; do not let retry or due queues grow forever.
      Quorum declarations set `scheduler.due` 1 h / 10 deliveries,
      `scheduler.results` 7 d / 20, `scheduler.tasks` no TTL / 20, and the DLQ 14 d.

**Important:** quorum membership is fixed when a queue is first declared. If the
application declared the queues before all three brokers joined the cluster, the queues
remain single-member even after the cluster is healthy. `deploy/ha-lab/deploy.sh` adds
missing members after startup; verify with `rabbitmqctl list_queues name type leader
members` and read the [runbook](runbooks/failover.md#quorum-queue-membership).

## Redis

Redis is an accelerator for rate limiting, never the source of truth for tasks,
executions, or idempotency.

- [x] Use managed Redis with automatic failover, or Sentinel. The lab runs one master,
      two replicas, three Sentinels, and a sentinel-aware proxy on `:6380`.
- [x] Persistence is optional; document that rate-limit state may reset. The lab disables
      RDB/AOF, so bucket state resets on failover by design.
- [x] The token-bucket Lua script uses a single key per request, so it is compatible
      with cluster mode; the client and script are implemented and tested against a
      six-node local cluster. The lab itself still covers Sentinel, not Redis Cluster.
- [x] Keep `RATE_LIMIT_MODE=observe` until Redis failover has been exercised in staging.
      Drill 3 exercised failover with enforce mode on one replica; the default remains
      disabled/observe.
- [x] Redis loss must route traffic to the local fallback and bounded 429s, never to an
      unbounded database write burst. Drill 3: 59 requests used the local bucket, no 5xx.

## Outbox, timers, and failover

- The outbox and `scheduled_events` tables are authoritative and stay in PostgreSQL.
- Pending rows survive a broker outage or coordinator restart.
- The relay uses confirms, `mandatory=True`, per-aggregate advisory locks, and terminal
  `failed` status after bounded retries. It materializes wakeups/retries into
  `scheduled_events` in the same transaction that marks the outbox row published.
- The timer poller uses `FOR UPDATE SKIP LOCKED`, confirms, `mandatory=True`, and
  bounded backoff. Multiple coordinators can run it safely.
- After a broker or DB failover, the relay and timer poller reconnect and drain pending
  rows. Do not delete pending or failed rows during recovery.
- A duplicate timer publish is safe: dispatch is guarded by `(task_id, scheduled_for)`
  and one-active-execution constraints; worker claims are lease-guarded.
- Alert on `scheduler_scheduled_events_lag_seconds`, pending depth, and failed timers.

## Load balancers and replicas

- API replicas are stateless; use a load balancer with health checks. The lab has no
  managed NLB. nginx on node1 `:8081` fronts the three per-node HAProxy API listeners
  on `:8080`; each HAProxy balances across the API replicas. Per-node `:8080` remains
  the fallback. See [internals/08](internals/08-load-balancing.md).
- Coordinator is a singleton-ish control plane but is safe to run with more than one
  instance because execution slots, leases, and outbox advisory locks are in PostgreSQL.
- Workers scale horizontally; each `WORKER_CONCURRENCY` slot uses its own connection,
  channel, and prefetch of one.
- Prometheus must scrape each replica. The Compose file assigns stable worker metric
  ports for that reason.

## Acceptance criteria

- A primary failover does not lose committed tasks or duplicate executions. **Met** in
  both AWS runs: 89/89 accepted tasks completed exactly once in v1.4.0 (RPO 0,
  promotion ~2.8 s) and 720/720 in v1.6.0 (RPO 0, promotion 1 s, 5.28 s max pause).
- A broker node loss does not lose confirmed messages. **Met** in both AWS runs: a
  301-message backlog retained in v1.4.0, and a 200-message backlog retained with
  exactly-once completion in v1.6.0.
- The outbox drains after recovery. **Met**: pending/failed outbox rows returned to 0
  after every drill and the load test, including the 2026-11-12 run.
- Backpressure and rate limiting enter observable, bounded degraded modes. **Met**:
  Redis failover produced local-fallback limits with bounded latency and no 5xx in both
  the v1.4.0 run and the 2026-11-12 enforce-mode drill.
- Alerts and runbooks are current. **Met for the new signals**: Patroni, HAProxy,
  PgBouncer, and Redis scrape jobs plus HA alert rules are added and HA-gated by
  `topology="ha"`; the 2026-11-12 run confirmed 29/29 targets and 54 rules. The two
  pull-consumer alerts need narrowing, and Alertmanager routing still needs a pager
  test.
- RTO/RPO are measured, not assumed. **Met for four application drills in AWS**
  (PostgreSQL, RabbitMQ leader loss, Redis Sentinel, API/HAProxy); PITR, quorum network
  partition, and Redis Cluster remain locally validated only.

**Not production-grade yet:** Spot instances, a node1-only nginx endpoint instead of a
managed load balancer, no production S3 backup bucket, PITR/partition/Redis Cluster not
run in AWS, and an untested pager route. Treat the lab as evidence that the application
behaves correctly under failover, not as a production sign-off.
