# Coordinator Backpressure Runbook

## What it does

The coordinator samples RabbitMQ queue depth and its DB connection gate. In `enforce`
mode it pauses due dispatch, result processing, reconciliation, and outbox relay when
the system is saturated:

| Signal | Pause when | Resume when |
|--------|------------|-------------|
| `scheduler.tasks` | `>= BACKPRESSURE_TASKS_HIGH` | `<= BACKPRESSURE_TASKS_LOW` |
| `scheduler.results` | `>= BACKPRESSURE_RESULTS_HIGH` | `<= BACKPRESSURE_RESULTS_LOW` |
| DB gate | utilization `>= DB_POOL_HIGH_WATERMARK` or waiters > `DB_POOL_MAX_WAITERS` | utilization `<= DB_POOL_RESUME_WATERMARK` and no waiters |
| queue monitor | no fresh sample for `BACKPRESSURE_STALE_PAUSE_SEC` | fresh sample |

`observe` mode evaluates the same signals and emits metrics but never blocks. Default is
`observe`.

## Operational rules

- The due consumer uses `basic_get`; it does not fetch while paused. It keeps calling
  `process_data_events`, so heartbeats and confirms stay alive.
- A message is acked only after the DB transaction commits and the outbox row is written
  (or the direct publish confirms).
- Transient DB/broker errors nack and requeue with jittered backoff.
- If the queue monitor or DB gate has no fresh signal, state becomes `unknown`. In
  `enforce` mode this fails closed: dispatch and results pause until sampling recovers.
  This is deliberate; see `CoordinatorBackpressureUnknown`.
- Results continue to drain when only `scheduler.tasks` is deep. They pause when the DB
  or `scheduler.results` is the bottleneck.
- Reconciliation is skipped while paused or when `scheduler.due` is above
  `BACKPRESSURE_DUE_HIGH`.

## Diagnose a pause

1. Check `scheduler_backpressure_state{state="paused"}` and the transition counter.
2. Check `scheduler_queue_depth{queue=...}` for tasks/results/due.
3. Check `scheduler_db_pool_utilization` and `scheduler_db_pool_waiting`.
4. Check `scheduler_dispatch_gate_blocked_total{reason}` to see which consumers paused.
5. If state is `unknown`, inspect the coordinator log for `queue depth monitor` errors and
   verify the queues exist. The monitor uses passive declares on a dedicated connection.
6. If the DB gate is saturated, look for slow queries, lock waits, or a connection leak;
   the coordinator gate bounds concurrent sessions but does not pool them.

## Actions

- Task backlog: scale workers or reduce task creation. Do not raise the watermark first.
- Result backlog: inspect result-consumer errors; results are idempotent and should drain.
- DB saturation: fix slow queries/locks, then consider increasing `DB_POOL_MAX` only with
  PostgreSQL capacity headroom.
- Broker outage: the monitor goes stale and consumers reconnect with jitter. Keep the
  coordinator running; committed state and outbox rows are safe in PostgreSQL.
- Flapping: increase `BACKPRESSURE_HIGH_CONSECUTIVE` / `LOW_CONSECUTIVE` or widen the
  high/low gap. A one-sample pause is intentionally suppressed.

## Rollback

- One env change: `BACKPRESSURE_MODE=observe`.
- If dispatch pacing is the problem: raise `DISPATCH_RATE_PER_SEC` / `DISPATCH_BURST`.
- Do not disable the queue monitor; it is needed for visibility. Observe mode is enough
  to stop pausing.
- `OUTBOX_ENABLED=false` remains the outbox rollback path and is independent of
  backpressure.

## Metrics and alerts

- `scheduler_backpressure_state{state=normal|paused|unknown}`
- `scheduler_backpressure_transitions_total{from_state,to_state}`
- `scheduler_backpressure_paused_seconds_total`
- `scheduler_dispatch_gate_blocked_total{reason}`
- `scheduler_queue_depth{queue}`
- `scheduler_db_pool_utilization`, `scheduler_db_pool_waiting`
- `scheduler_consumer_reconnects_total{consumer}`
- `scheduler_dispatch_latency_seconds{consumer}`
- `scheduler_consumer_errors_total{consumer,kind}`
- Alerts: `CoordinatorBackpressurePaused`, `CoordinatorBackpressureUnknown`,
  `CoordinatorConsumerReconnects`, `DueQueueBacklogHigh`, `DbPoolSaturation`.
