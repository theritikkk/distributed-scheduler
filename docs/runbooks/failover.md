# Failover and Restore Runbook

This is the drill and incident procedure for the target HA topology in
[docs/HA.md](../HA.md). The `deploy/ha-lab/` stack runs this topology; the single-node
Compose stack can run the same checks but cannot survive losing its only PostgreSQL or
RabbitMQ instance.

## Pre-flight

- [ ] For production, confirm automated backups and PITR are enabled and record the
      latest restore point. In the lab, PITR is a local drill (`tests/pitr/`); no S3
      backup target is provisioned.
- [ ] Confirm the standby is synchronous (RPO 0) or record the async lag budget.
- [ ] Confirm RabbitMQ has 3 nodes and quorum queues (after the TTL-delay replacement).
- [ ] Confirm `SCHEDULED_EVENTS_ENABLED=true`, `OUTBOX_ENABLED=true`, and
      `RABBITMQ_QUEUE_TYPE=quorum` on every service before using this runbook for HA.
      Keep `classic` for the single-node development stack.
- [ ] Confirm every node's PgBouncer answers on `:6432` and the app `DATABASE_URL`
      points at `host.docker.internal:6432`; migrations still use HAProxy `:5433`.
- [ ] Confirm nginx answers on node1 `:8081` and each node's HAProxy `:8080` is healthy.
- [ ] Confirm Redis mode (Sentinel proxy `:6380` or `REDIS_CLUSTER_NODES`) and whether
      rate limiting is in observe or enforce mode.
- [ ] Confirm Prometheus scrapes all replicas and Alertmanager routes are tested.
- [ ] Record baseline row counts:

```sql
SELECT count(*) AS tasks FROM tasks;
SELECT count(*) AS executions FROM task_executions;
SELECT count(*) AS outbox_pending FROM outbox WHERE status = 'pending';
SELECT count(*) AS outbox_failed FROM outbox WHERE status = 'failed';
SELECT count(*) AS timers_pending FROM scheduled_events WHERE status = 'pending';
SELECT count(*) AS timers_failed FROM scheduled_events WHERE status = 'failed';
```

## Drill 1: PostgreSQL primary failover during dispatch

1. Start a steady task stream (load test at expected peak).
2. Kill/fail over the primary.
3. Observe coordinator logs: DB errors, retries, and reconnects. No message may be acked
   before its DB commit succeeds.
4. After promotion, verify:
   - No duplicate active executions:
     ```sql
     SELECT task_id, count(*) FROM task_executions
     WHERE status IN ('pending', 'running')
     GROUP BY task_id HAVING count(*) > 1;
     ```
   - No duplicate `(task_id, scheduled_for)` slots.
   - Pending outbox rows drain (`scheduler_outbox_pending_rows` falls).
   - Pending timers drain or remain correctly scheduled
     (`scheduler_scheduled_events_pending`, `scheduler_scheduled_events_lag_seconds`).
   - Task completion resumes and results are applied exactly once per execution.
5. Measure RTO from failure to first successful dispatch; record it here.

## Drill 1b: PgBouncer and pooled connections during failover

Run this before or together with Drill 1 when the lab uses PgBouncer.

1. Confirm the pooler view on each node:

```bash
docker compose -f docker-compose.ha-data.yml exec -T \
  -e PGPASSWORD="<postgres-password>" pgbouncer \
  psql -U scheduler -d pgbouncer -c 'SHOW POOLS;'
```

2. Confirm `pool_mode=transaction`, `server_reset_query=DISCARD ALL`, and
   `server_reset_query_always=1`:

```bash
docker compose -f docker-compose.ha-data.yml exec -T \
  -e PGPASSWORD="<postgres-password>" pgbouncer \
  psql -U scheduler -d pgbouncer -c 'SHOW CONFIG;'
```

3. Let one API transaction commit through the pooler while the PostgreSQL primary is
   failed over. Expected: in-flight client transactions fail and are retried; PgBouncer
   drops broken server connections, reconnects through HAProxy, and serves writes on the
   promoted primary. There must be no session-state leak and no session-scoped advisory
   lock.
4. Verify advisory locking still works through the pooler:

```sql
SELECT pg_try_advisory_xact_lock(hashtext('outbox:task:drill'));
```

The function must return true in one transaction and false in a second concurrent
transaction until the first commits.

The local harness (`./tests/pgbouncer/run.sh`) validates SCRAM authentication, transaction
reuse, advisory locks, `DISCARD ALL`, `SHOW POOLS`, and exporter metrics. It does not
simulate a Patroni promotion; that part remains a live-lab drill.

## Quorum queue membership

Quorum queue replicas are **not** retroactively added when a broker joins the cluster.
Queues declared while RabbitMQ was single-node stay single-member, and losing that broker
loses the queue's messages. After any deployment where brokers joined incrementally,
verify membership before trusting the cluster:

```bash
docker compose -f docker-compose.ha-data.yml exec -T rabbitmq \
  rabbitmqctl list_queues name type leader members
```

Every queue must list all three nodes, for example:

```text
scheduler.tasks  quorum  rabbit@node1  [rabbit@node1, rabbit@node2, rabbit@node3]
```

If a queue is missing a member, add it explicitly:

```bash
docker compose -f docker-compose.ha-data.yml exec -T rabbitmq \
  rabbitmq-queues add_member --vhost / scheduler.tasks rabbit@node2
```

Repeat for `scheduler.tasks`, `scheduler.due`, `scheduler.results`, and
`scheduler.tasks.dlq` on the missing nodes. `deploy/ha-lab/deploy.sh` now runs this
check/addition after the application starts and is idempotent.

## Drill 2: RabbitMQ node loss during a backlog

1. Build a `scheduler.tasks` backlog with workers paused.
2. Stop one RabbitMQ node.
3. Confirm the quorum queue retains messages and consumers reconnect with jitter.
4. Resume workers; verify no task is lost and no execution is duplicated.
5. Verify the timer poller keeps draining `scheduled_events` and
   `scheduler_scheduled_events_lag_seconds` returns below the alert threshold.
6. If the old `scheduler.delay` / `scheduler.retry_delay` TTL queues still exist, do not
   claim broker HA is complete; drain and delete them before quorum mode.

## Drill 2b: RabbitMQ network partition

Quorum queues require a majority. The local harness partitions the Docker network so
two brokers isolate one, then two brokers isolate the last one.

```bash
./tests/rabbitmq-quorum/run.sh
```

Expected sequence:

1. **One broker isolated.** Two of three nodes keep the Raft majority. The queue stays
   available, publishes are confirmed, and consumers keep draining.
2. **Two brokers isolated.** The remaining node loses quorum. RabbitMQ logs the partition
   and `pause_minority` stops the application. A probe publish is not routed and a probe
   read returns no message; the minority does not serve stale state.
3. **Healed.** All three brokers and queue members return. The remaining backlog drains
   and every message ID is checked for exactly-once consumption.

The harness excludes its own probe message ID and asserts 160/160 expected messages were
consumed exactly once. On a live cluster, watch `rabbitmq_alarms_*`, queue leader/member
counts, and `scheduler_scheduled_events_lag_seconds`; do not ack or delete anything during
the minority pause.

## Drill 3: Redis loss

1. Enable rate limiting in observe mode, then enforce on one API replica.
2. Stop Redis.
3. Verify:
   - `scheduler_ratelimit_fallback_active{scope}` becomes 1.
   - Circuit breaker opens after the failure threshold.
   - API p99 remains bounded; admission may reject with 429/503 and `Retry-After`.
   - PostgreSQL write rate does not spike.
4. Restart Redis and verify the half-open probe closes the circuit.

## Drill 3b: Redis Cluster failover

Use this instead of Sentinel when the API is configured with `REDIS_CLUSTER_NODES`.

```bash
REDIS_CLUSTER_FAILOVER=1 ./tests/redis-cluster/run.sh
```

The harness starts three masters and three replicas, runs the real token-bucket Lua
script through two independent ioredis `Cluster` clients, and verifies:

1. Both clients share one cluster-wide bucket; a fourth request against capacity 3 is
   denied.
2. Keys route to at least three different slot owners, and each execution applies a
   positive TTL.
3. After `CLUSTER FAILOVER FORCE` promotes a replica, ioredis refreshes its slot map and
   rate limiting continues.

Expected application behavior during a real slot-owner failure: a brief burst of Redis
errors opens the circuit and uses the per-replica local bucket. No 5xx is produced by the
limiter, and the half-open probe resumes shared accounting after the cluster stabilizes.
Bucket state may reset; that is acceptable because Redis is not authoritative.

## Drill 4: network partition between coordinator and PostgreSQL

Expected behavior in enforce mode:

- DB calls fail/timeout, the DB gate blocks new acquisitions, and backpressure goes to
  `unknown`/`paused`.
- Due messages are nacked and requeued; nothing is acked after a failed commit.
- Heartbeats continue via `process_data_events` while the consumer is paused.
- On reconnect, work resumes without duplicate execution slots.

## Drill 5: nginx load balancer and API node failure

Local template validation:

```bash
./tests/nginx-lb/run.sh
```

It renders `deploy/ha-lab/ha/nginx.conf.tmpl`, sends traffic through three echo backends,
stops one backend, and verifies passive failover plus recovery after `fail_timeout`.

In the lab:

1. Confirm `http://<node1-ip>:8081/health` returns `ok`.
2. Send a steady request stream through `:8081` and stop the API container on one node.
   Expected: HAProxy removes that backend, nginx retries another node, and clients see no
   5xx beyond an in-flight request.
3. Stop HAProxy on one node. Expected: nginx marks that upstream down after `max_fails=2`
   and sends traffic to the other two nodes.
4. Restart the node and wait at least 10 seconds. Expected: it returns to the upstream
   pool.
5. Confirm node1 `:8081` is a **single point of failure** for this endpoint. Per-node
   `:8080` remains the fallback until nginx runs on every node behind DNS or a managed LB.

## PITR restore validation and production procedure

The v1.4.0 lab template did **not** enable `archive_mode`; PITR was validated with a
separate local harness. Production must add continuous WAL archiving before this section
is usable against the lab.

### Production procedure

1. **Freeze the incident.** Stop or pause writers, record the target time in UTC, and do
   not reuse the production data directory.
2. **Provision a replacement** instance/data directory. Pause Patroni on the target or
   keep it out of the cluster so it cannot be promoted prematurely.
3. **Restore the base backup and replay WAL.** With pgBackRest:

   ```bash
   pgbackrest --stanza=scheduler \
     --type=time --target="2026-11-08 09:00:00+00" \
     --target-action=promote restore
   ```

   With WAL-G, fetch the base backup and configure recovery:

   ```bash
   wal-g backup-fetch "$PGDATA" LATEST
   # postgresql.conf / postgresql.auto.conf
   # restore_command = 'wal-g wal-fetch %f %p'
   # recovery_target_time = '2026-11-08 09:00:00+00'
   # recovery_target_action = 'promote'
   ```

4. **Start PostgreSQL and wait for promotion.** `pg_is_in_recovery()` must return
   `false`. The log should show recovery stopping at the requested target.
5. **Verify before cutover.** Run the row-count and invariant queries below. If any
   duplicate or impossible state appears, stop and investigate; do not point services at
   the restored database.
6. **Cut over one tier at a time.** Repoint HAProxy/Patroni, then PgBouncer, then
   coordinator/workers, then API. Record RTO (restore + replay + promotion) and RPO
   (last archived WAL segment plus `archive_timeout`).

Restore into a **staging** database first, never over production:

```sql
SELECT count(*) FROM tasks;
SELECT count(*) FROM task_executions;
SELECT count(*) FROM outbox;
SELECT count(*) FROM workers;
```

Compare with the pre-restore snapshot. Then run:

```sql
SELECT task_id, scheduled_for, count(*)
FROM task_executions
GROUP BY task_id, scheduled_for
HAVING count(*) > 1;

SELECT task_id, count(*)
FROM task_executions
WHERE status IN ('pending', 'running')
GROUP BY task_id
HAVING count(*) > 1;
```

Both queries must return zero rows. If they do not, stop and investigate before
pointing any service at the restored database.

### Local PITR drill

`./tests/pitr/run.sh` starts a disposable PostgreSQL 16 primary with `archive_mode=on`,
takes a `pg_basebackup`, inserts rows 11–20 and 21–30 with a WAL switch after each batch,
then inserts rows 31–40. It restores the same base backup twice:

| Target | Expected rows | Max id | Post-target rows | Promote |
|--------|---------------|--------|------------------|---------|
| After batch 1 | 20 | 20 | 0 | yes |
| After batch 2 | 30 | 30 | 0 | yes |

Both restores stopped at the requested time. This proves the backup/replay mechanics and
the invariant checks; it does not prove S3 durability, encryption, or restore throughput
for the production bucket.

## Rollback and recovery notes

- Keep the outbox table and pending rows during any rollback. Set
  `OUTBOX_ENABLED=false` only when rolling back application images.
- Do not re-run a destructive migration on a promoted standby.
- Reconnect services one tier at a time: PostgreSQL/PgBouncer, then RabbitMQ, then
  API/coordinator/workers.
- Record the drill date, failure injection, RTO, RPO, anomalies, and follow-ups below.

## Drill log

All v1.4.0 rows in this table ran against the `deploy/ha-lab/` three-node AWS lab on
2026-11-06 (`ap-south-1a/b/c`, Spot `m7i-flex.large`). PostgreSQL leader at the start of
Drill 1 was node3; it was node1 for Drills 2–3 after promotion. The lab was destroyed
after the drills to stop charges; redeploy it with
[`deploy/ha-lab/`](../../deploy/ha-lab/README.md) before rerunning those procedures.
Baseline row counts were captured before Drill 1 and rechecked after every drill.

The v1.5.0 rows marked **local** ran with the Docker harnesses under `tests/` on
2026-11-08. They are not AWS drill results.

The 2026-11-12 rows are a **v1.6.0 re-validation** on a freshly provisioned three-node
lab. They are a separate run from the 2026-11-06 rows and are documented with
screenshots and raw logs in
[`screenshots/deployment/test-7/`](../../screenshots/deployment/test-7/README.md).

| Date | Drill | RTO | RPO | Result | Follow-ups |
|------|-------|-----|-----|--------|------------|
| 2026-11-06 | PostgreSQL primary loss: stopped Patroni on leader node3 during a 90-task submission stream | ~2.8 s to node1 promotion; HAProxy served writes again immediately after | 0 (node1 was the synchronous standby) | 89/89 accepted tasks completed exactly once; 1 client 500 during reconnect; no duplicate `(task_id, scheduled_for)` slots; node3 rejoined as replica | Investigate the single 500: retry at the client/proxy layer, or add a short write-retry window in the API |
| 2026-11-06 | RabbitMQ queue-leader loss: paused all 9 workers, built a 301-message backlog, added quorum members, stopped the leader broker (node1) | Leader election completed before workers resumed; backlog drained after restart | 0 confirmed messages lost | 301 messages retained with node1 down; leader moved to node3; 300/300 tasks completed exactly once, one attempt each; all queues returned to 3 members when node1 restarted | Queue membership was initially single-node because the queues predated the cluster; `deploy.sh` now enforces members |
| 2026-11-06 | Redis master loss: stopped node1 Redis while enforce-mode limiting ran on node2's API and 180 requests streamed | Sentinel promoted node2 within the request stream; proxy followed | Rate-limit state reset (acceptable; Redis is not authoritative) | 10 allowed / 5×429 with `Retry-After: 1` before failure; 59 requests used the local bucket, 5 connection errors, first fallback calls ~116 ms then ~14 ms; circuit half-opened and closed after 30 s; all 180 requests returned 201; node1 restored as replica | Scrape/alert on `scheduler_ratelimit_fallback_active` and circuit state; add Redis/Sentinel to Prometheus |
| 2026-11-06 | Load test: k6 through node1 HAProxy, 20 VUs for 60 s, 5 s schedule delay | n/a | n/a | Warm-up 454 requests at 30 req/s, p95 60.9 ms. Peak 7,586 requests at 126 req/s, p95 69.6 ms, 0 failures. 8,040 total tasks completed exactly once, 0 retries, 0 duplicate slots, outbox drained | Run a longer soak and a network-partition drill; add PgBouncer before raising worker/API connection counts |
| 2026-11-08 (local) | Redis Cluster: six nodes (3 masters, 3 replicas), shared token bucket, forced `CLUSTER FAILOVER FORCE` | n/a (client reconnects) | Bucket state resets; acceptable | Two independent `Cluster` clients shared a capacity-3 bucket; keys hit at least three slot owners; limiter kept working after failover | Re-test against managed Redis/cluster under real latency |
| 2026-11-08 (local) | RabbitMQ quorum partition: one broker isolated, then two | Majority stayed available; minority paused | 0 confirmed messages lost | 160/160 expected messages consumed exactly once after recovery; probe excluded; no split-brain | Re-run in a live 3-AZ cluster and record pause window |
| 2026-11-08 (local) | PgBouncer: SCRAM, bad password, transaction reuse, advisory lock, session reset, exporter | n/a | n/a | All checks passed; `SHOW POOLS`/`SHOW CONFIG` reported transaction mode and `DISCARD ALL`; no session state leaked | Exercise a live Patroni promotion through PgBouncer |
| 2026-11-08 (local) | PITR: `pg_basebackup` + archived WAL, two restore targets | Two promotions completed in the harness | RPO bounded by archived WAL | Target 1 had 20 rows, target 2 had 30; rows 31–40 absent from both; both `pg_is_in_recovery()=false` | Deploy S3/WAL-G or pgBackRest and run the production procedure |
| 2026-11-08 (local) | nginx LB: three backends, stop one, restart, wait `fail_timeout` | Removed after 2 failures; recovered after ~10 s | n/a | All three backends received traffic; failover used the remaining two; backend1 returned after 11 s; `nginx -t` passed | Run nginx on every node or replace with a managed LB |
| 2026-11-12 | PostgreSQL primary loss (v1.6.0): stopped Patroni on leader node3 during a 720-task stream through nginx | 1 s to node2 promotion; 5.28 s max client pause | 0 (sync standby, 0 lag) | 720/720 created and completed exactly once; 0 duplicate slots; node3 rejoined as replica; 2,160 outbox events published | None; reconnect log lines were expected during handover |
| 2026-11-12 | RabbitMQ queue-leader loss (v1.6.0): paused 9 workers, built a 200-message backlog, stopped the app on leader node2 | 2 s election; backlog drained by `14:21:32Z` | 0 confirmed messages lost | Leader moved `rabbit@node2` → `rabbit@node1`; 200/200 retained and completed exactly once; 600 outbox events; all queues 0/0/0 | None |
| 2026-11-12 | Redis master loss (v1.6.0): forced master to node1, enabled enforce-mode limiting on all API replicas, stopped node1 Redis | Sentinel 5.95 s; proxy followed 6.96 s; node1 rejoined in 70.39 s | Rate-limit counters ephemeral; fail-open by design | 0 5xx and 0 network errors; circuit opened then closed; `fallback_active` 1 → 0; limiter source `local` → `redis` | Narrow the `RabbitMQQueueConsumersMissing` rule for pull-consumed queues |
| 2026-11-12 | API + HAProxy node failure (v1.6.0): streamed through node1 nginx, stopped node2 API and node3 HAProxy | API healthy in 48 s; HAProxy in 57 s | 0 accepted tasks lost | 717/720 accepted; all 717 completed exactly once; 3 client failures (one 15 s timeout, two 502s) in the overlapping recovery window; 2,151 outbox events | Put nginx on every node or use a managed LB; document passive-health-check behaviour |

### Final counts (v1.4.0 lab)

These are totals for the whole v1.4.0 lab database, not a sum of the drill rows. The
recurring E2E task kept running once per minute through the drills and load test, so it
accounts for a large share of the execution count.

| Check | Final |
|-------|-------|
| Tasks | 8,621 |
| Executions | 8,847 |
| Duplicate execution slots | 0 |
| Outbox pending / failed | 0 / 0 |
| Timers pending | 1 (recurring E2E task) |

The recurring E2E task remains intentionally scheduled; it is not backlog.

### Final counts (v1.6.0 re-validation, 2026-11-12)

| Check | Final |
|-------|-------|
| Tasks | 1,753 |
| Executions | 1,642 |
| Duplicate execution slots | 0 |
| Duplicate active executions | 0 |
| Outbox pending / failed | 0 / 0 |
| Timers pending / failed | 111 / 0 |

The 111 pending timers are scheduled tasks whose due time had not arrived at capture;
they are not lost work. The difference between tasks and executions is exactly those
pending timers.

### Open follow-ups after the drills

- PostgreSQL/PgBouncer failover, RabbitMQ queue-leader loss, Redis Sentinel failover,
  and the nginx/API-node path were re-run in AWS on 2026-11-12; PITR, quorum network
  partition, Redis Cluster, and a full AZ-loss drill remain unrun in AWS.
- Deploy a production S3 backup bucket with pgBackRest or WAL-G (encryption,
  versioning, lifecycle) and record a real restore time.
- Put nginx on every node behind DNS or replace the node1-only endpoint with a managed
  load balancer; test TLS termination.
- Test Alertmanager routing to a real pager for the Patroni/HAProxy/PgBouncer/Redis
  alerts.
- Exercise the Redis fallback with `RATE_LIMIT_MODE=enforce` on every API replica and
  confirm the local bucket limits are acceptable for the target tenant count.
- Run a longer soak test; the v1.4.0 load test was 60 seconds.
- Decide whether to keep the Spot-based lab running; destroy it when not needed to stop
  charges.
