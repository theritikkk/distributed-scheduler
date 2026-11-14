# Design 03 — Coordinator queue-depth backpressure

- **Status:** Proposed
- **Date:** 2026-10-30
- **Scope:** Due consumer, result consumer, reconciliation, dispatch pacing, DB pool protection
- **Related:** [01 — Jitter](01-jitter-and-dispatch-time.md), [02 — Rate limiting](02-rate-limiting-and-redis-fallback.md), [04 — Plan](04-implementation-plan.md)

---

## 1. Problem

The coordinator currently dispatches as fast as RabbitMQ delivers messages:

- `run_due_consumer` uses `basic_qos(prefetch_count=10)` and a serial callback
  (`coordinator/internal/queue/due_consumer.py`). It has no notion of worker capacity or
  `scheduler.tasks` depth.
- The callback acknowledges in all cases, including after an exception
  (`due_consumer.py:139-140`). A database error can therefore drop a due wakeup.
- The result consumer has no QoS and acknowledges after every exception
  (`result_consumer.py:137-138`).
- Reconciliation publishes up to 100 nudges every 120s regardless of backlog
  (`reconciliation.py:36-45`).
- Every message opens a new database connection and closes it. Under load, this is a
  connection storm rather than a queue.
- `scheduler.tasks` is the only buffer between the coordinator and the workers. If workers
  slow down, the coordinator keeps writing executions and publishing work until the
  database or RabbitMQ becomes the bottleneck.

Backpressure is not rate limiting. Rate limiting decides whether a new request enters the
system. Backpressure decides whether the scheduler should **create more work** while the
existing work is not draining.

**Core rule:** when the system is saturated, stop fetching and stop publishing. Do not
acknowledge work that has not been durably handed off. Let RabbitMQ and the outbox hold
the backlog.

---

## 2. Goals and non-goals

### Goals

- Pause due dispatch when `scheduler.tasks` (or the result queue) is above a high
  watermark, or when the database pool is saturated.
- Resume below a low watermark with hysteresis, so the consumer does not flap.
- Keep RabbitMQ heartbeats alive while paused.
- Never acknowledge a message after a failed database transaction or publish.
- Bound coordinator DB concurrency with a connection pool and a dispatch token bucket.
- Make reconciliation respect backpressure.
- Make the behavior observable and rollout-safe (`observe` mode first).
- Keep the design compatible with the outbox: the outbox is the durable buffer, the
  queue depth is the throttle signal.

### Non-goals

- Auto-scaling workers or coordinators.
- Per-user fairness at dispatch (a later feature).
- Replacing RabbitMQ with a database queue.
- Exactly-once command execution.
- Fixing the worker's command sandbox (separate security work).
- Implementing the outbox itself (design/phase 3).

---

## 3. Signals and state machine

### 3.1 Signals

| Signal | Source | Why it matters |
|--------|--------|----------------|
| `task_depth` | Passive declare of `scheduler.tasks` | Work waiting for workers |
| `result_depth` | Passive declare of `scheduler.results` | Results not being applied; DB or coordinator is slow |
| `due_depth` | Passive declare of `scheduler.due` | Wakeups waiting for the coordinator; pause reconciliation, not dispatch |
| `db_utilization` | Connection pool stats | The coordinator's own DB capacity |
| `db_waiters` | Connection pool stats | Requests already queueing for a connection |
| `dispatch_latency` | Histogram timer around dispatch | Early warning before depth grows |

Do **not** use only queue depth. A slow database can leave the queue shallow while every
dispatch takes seconds. The DB pool signal catches that case.

### 3.2 States

```text
                 high watermark / DB high
    NORMAL ─────────────────────────────────▶ PAUSED
       ▲                                       │
       │        low watermark + DB low         │
       └───────────────────────────────────────┘

    either state ── monitor stale > threshold ──▶ UNKNOWN
    UNKNOWN ── fresh sample ──▶ NORMAL or PAUSED
```

| State | Due consumer | Result consumer | Reconciliation | Outbox relay |
|-------|--------------|-----------------|----------------|--------------|
| `NORMAL` | Fetch | Fetch | Run | Publish |
| `PAUSED` | Do not fetch; keep heartbeat | Pause if DB signal caused it | Skip | Pause if task queue caused it |
| `UNKNOWN` | Pause after grace period | Pause | Skip | Pause |

`UNKNOWN` is conservative: if the depth monitor has been stale for
`BACKPRESSURE_STALE_PAUSE_SEC`, assume the system is saturated. A false pause is less
harmful than an unbounded DB write burst.

### 3.3 Watermarks and hysteresis

Defaults:

```text
task high:        5,000 messages
task low:         1,000 messages
result high:      2,000 messages
result low:         500 messages
db high:          0.80 utilization
db resume:        0.60 utilization
high samples:     2 consecutive
low samples:      3 consecutive
poll interval:    1 second
```

Derive the task high watermark from worker capacity rather than guessing:

```text
high ≈ workers × concurrency × buffer_seconds / average_task_seconds
```

Example: 10 workers × 4 concurrent tasks × 300s buffer / 5s average = 2,400.
Use the larger of the formula and a floor (e.g., 1,000), then override with an env var.

The low watermark should be 20–25% of high. The gap is the hysteresis band.

### 3.4 Pause conditions

Pause if **any** of:

- `task_depth >= BACKPRESSURE_TASKS_HIGH`
- `result_depth >= BACKPRESSURE_RESULTS_HIGH`
- `db_utilization >= DB_POOL_HIGH_WATERMARK`
- `db_waiters > DB_POOL_MAX_WAITERS`
- the monitor is stale beyond the grace period

Resume only if **all** of:

- `task_depth <= BACKPRESSURE_TASKS_LOW`
- `result_depth <= BACKPRESSURE_RESULTS_LOW`
- `db_utilization <= DB_POOL_RESUME_WATERMARK`
- `db_waiters == 0`
- the monitor is fresh

`due_depth` does not pause the due consumer. It pauses reconciliation, because adding
more due messages to a backlog is counterproductive. If `due_depth` is high, the
coordinator itself is the bottleneck; increase its dispatch capacity or reduce the
number of coordinators publishing nudges.

---

## 4. Components

Recommended package layout:

```text
coordinator/internal/backpressure/
├── __init__.py
├── controller.py       # state machine + hysteresis + thread-safe snapshot
├── queue_monitor.py    # dedicated pika connection, passive declare, poll loop
├── db_gate.py          # psycopg_pool stats -> utilization/waiters
├── dispatch_gate.py    # local token bucket (shared with rate-limit module)
└── consumer.py         # pull-based consumer helper + reconnect loop
```

### 4.1 `QueueDepthMonitor`

Use a **dedicated** pika connection and channel for depth polling. Do not use the
consumer channel from another thread: pika channels are not thread-safe.

```python
class QueueDepthMonitor:
    def __init__(self, rabbit_url: str, queues: list[str], interval_sec: float = 1.0):
        self._rabbit_url = rabbit_url
        self._queues = queues
        self._interval = interval_sec
        self._lock = threading.Lock()
        self._snapshot: dict[str, int] = {}
        self._sampled_at: float = time.monotonic()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._run, name="queue-depth-monitor", daemon=True
        )
        self._thread.start()

    def snapshot(self) -> tuple[dict[str, int], float]:
        with self._lock:
            return dict(self._snapshot), self._sampled_at

    def _run(self) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            conn = None
            try:
                conn = connect_blocking(self._rabbit_url)
                ch = conn.channel()
                while not self._stop.is_set():
                    depths = {}
                    for queue in self._queues:
                        declared = ch.queue_declare(queue=queue, passive=True)
                        depths[queue] = declared.method.message_count
                    with self._lock:
                        self._snapshot = depths
                        self._sampled_at = time.monotonic()
                    backoff = 1.0
                    self._stop.wait(self._interval)
            except Exception as exc:
                logger.warning("queue depth monitor: %s", exc)
                self._stop.wait(jitter(backoff))
                backoff = min(backoff * 2, 30.0)
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
```

Notes:

- `passive=True` does not create the queue; it fails if the topology is missing. That is
  desirable: the monitor should not mask a broken deployment.
- `message_count` is the ready-message count. Unacked in-flight messages are not included;
  that is fine because they are already being processed.
- Polling every second is enough. The existing management-API poller
  (`rabbitmq_metrics.py`) runs every 15s and can remain for dashboards, but it is too slow
  for control.
- If a queue is missing during startup, retry with jitter; do not crash the coordinator.

### 4.2 `BackpressureController`

```python
class BackpressureController:
    def __init__(self, config):
        self._config = config
        self._lock = threading.Lock()
        self._state = "normal"
        self._high_count = 0
        self._low_count = 0
        self._last_transition = time.monotonic()

    def evaluate(
        self,
        task_depth: int | None,
        result_depth: int | None,
        db_util: float | None,
        db_waiters: int | None,
        sampled_at: float,
    ) -> str:
        now = time.monotonic()
        stale = (now - sampled_at) > self._config.stale_pause_sec

        with self._lock:
            if stale or task_depth is None or db_util is None:
                return self._set_state("unknown")

            high = (
                task_depth >= self._config.tasks_high
                or (result_depth or 0) >= self._config.results_high
                or db_util >= self._config.db_high
                or (db_waiters or 0) > self._config.db_max_waiters
            )
            low = (
                task_depth <= self._config.tasks_low
                and (result_depth or 0) <= self._config.results_low
                and db_util <= self._config.db_resume
                and (db_waiters or 0) == 0
            )

            if self._state in ("normal", "unknown"):
                self._high_count = self._high_count + 1 if high else 0
                if self._high_count >= self._config.high_consecutive:
                    return self._set_state("paused")
                return self._set_state("normal")
            else:
                self._low_count = self._low_count + 1 if low else 0
                if self._low_count >= self._config.low_consecutive:
                    return self._set_state("normal")
                return self._set_state("paused")

    def _set_state(self, new_state: str) -> str:
        if new_state != self._state:
            metrics.backpressure_transitions.labels(
                from_state=self._state, to_state=new_state
            ).inc()
            self._state = new_state
            self._high_count = 0
            self._low_count = 0
            self._last_transition = time.monotonic()
        return self._state
```

Important details:

- `UNKNOWN` must not immediately reset counters; it is a conservative pause. When a
  fresh sample arrives, the next `evaluate` re-enters the normal path.
- In `observe` mode, the controller still computes the state and emits metrics, but
  `allow_dispatch()` always returns `True`.
- Guard against clock jumps by using `time.monotonic()`.

### 4.3 `DbGate`

```python
class DbGate:
    def __init__(self, pool, max_size: int):
        self._pool = pool
        self._max_size = max_size

    def snapshot(self) -> tuple[float, int]:
        stats = self._pool.get_stats()
        pool_size = int(stats.get("pool_size", 0))
        available = int(stats.get("pool_available", 0))
        waiting = int(stats.get("requests_waiting", 0))
        active = max(0, pool_size - available)
        utilization = active / self._max_size if self._max_size else 0.0
        return utilization, waiting
```

`psycopg_pool` exposes `get_stats()` with these keys in current versions. Pin the version
and assert the keys in a test; do not silently treat a missing key as zero.

If the coordinator still creates one connection per message at the start of the phase,
add the pool first. Backpressure without a pool can only see `db_waiters` if you
instrument your own acquisition semaphore.

### 4.4 `DispatchGate`

A local token bucket is enough. The coordinator does not need a Redis round-trip per
message:

```python
gate = LocalTokenBucket(
    capacity=env.DISPATCH_BURST,          # e.g. 20
    refill_per_sec=env.DISPATCH_RATE_PER_SEC,  # e.g. 200
)

if not gate.consume("dispatch", cost=1).allowed:
    connection.process_data_events(time_limit=0.05)
    continue
```

If the coordinator is later scaled horizontally, each replica gets
`DISPATCH_RATE_PER_SEC / replicas` or a configured per-replica share. A shared Redis
limiter is not worth the hot-path dependency here; the queue is the real buffer.

---

## 5. Due consumer integration

### 5.1 Why switch to an explicit pull loop

The current `basic_consume` callback is convenient, but pausing it safely requires
cross-thread calls to `stop_consuming` / `basic_consume`, and prefetched messages are
already in flight. A `basic_get` loop gives a hard boundary:

- Fetch only when the gate is open.
- One message at a time; no prefetch buffer.
- Pause by simply not calling `basic_get`.
- Easy to reason about acks and reconnects.

The coordinator is a serial, DB-bound dispatcher. At a few hundred messages/second, the
polling overhead is negligible. If throughput later exceeds that, replace the loop with
a push consumer supervisor that uses `connection.add_callback_threadsafe()` to
`basic_cancel`/`basic_consume`.

### 5.2 Pseudocode

```python
def run_due_consumer(get_pool, rabbit_url: str):
    monitor = QueueDepthMonitor(rabbit_url, [TASK_QUEUE, RESULT_QUEUE, DUE_QUEUE])
    monitor.start()
    controller = BackpressureController(env)
    db_gate = DbGate(get_pool(), env.DB_POOL_MAX)
    dispatch_gate = LocalTokenBucket(env.DISPATCH_BURST, env.DISPATCH_RATE_PER_SEC)

    backoff = 1.0
    while not shutdown.is_set():
        try:
            conn = connect_blocking(rabbit_url)
            ch = conn.channel()
            declare_scheduler_topology(ch)

            while not shutdown.is_set():
                depths, sampled_at = monitor.snapshot()
                db_util, db_waiters = db_gate.snapshot()
                state = controller.evaluate(
                    task_depth=depths.get(TASK_QUEUE),
                    result_depth=depths.get(RESULT_QUEUE),
                    db_util=db_util,
                    db_waiters=db_waiters,
                    sampled_at=sampled_at,
                )

                if state != "normal" and env.BACKPRESSURE_MODE == "enforce":
                    conn.process_data_events(time_limit=env.BACKPRESSURE_POLL_SEC)
                    continue

                if not dispatch_gate.consume("dispatch").allowed:
                    conn.process_data_events(time_limit=0.05)
                    continue

                method, props, body = ch.basic_get(
                    queue=DUE_QUEUE, auto_ack=False
                )
                if method is None:
                    conn.process_data_events(time_limit=0.2)
                    continue

                try:
                    handle_due_message(get_pool(), ch, method, body)
                    ch.basic_ack(method.delivery_tag)
                except TransientError as exc:
                    logger.warning("due transient error: %s", exc)
                    ch.basic_nack(method.delivery_tag, requeue=True)
                    time.sleep(jitter(backoff))
                    backoff = min(backoff * 2, 30.0)
                except Exception as exc:
                    logger.exception("due permanent error: %s", exc)
                    ch.basic_nack(method.delivery_tag, requeue=False)
                    # Publish the body to the DLQ with the failure reason, or let
                    # a queue policy dead-letter it.
                    backoff = 1.0

        except AMQPConnectionError as exc:
            logger.warning("due consumer connection lost: %s", exc)
            time.sleep(jitter(backoff))
            backoff = min(backoff * 2, 30.0)
```

### 5.3 Message handling and ack rules

- Open a DB transaction.
- Lock the task row `FOR UPDATE`.
- Re-check status and `dispatch_at` using `NOW()`.
- Create the execution with `ON CONFLICT (task_id, scheduled_for) DO NOTHING`.
- Either publish the task (current) or write an outbox row (after Phase 3).
- Commit.
- **Ack only after commit and successful publish/outbox write.**
- On exception: rollback, `basic_nack(requeue=True)`, back off. Do not ack.
- On duplicate slot or non-scheduled status: ack without dispatch.

If publishing directly, use publisher confirms:

```python
ch.confirm_delivery()
ch.basic_publish(..., mandatory=True)
```

A publish failure must not leave a committed execution that no worker will ever see.
The durable fix is the outbox; until then, consider inserting the execution as
`pending_dispatch` and reaping stale rows.

### 5.4 Heartbeats while paused

`connection.process_data_events(time_limit=...)` processes heartbeats and connection
I/O. Do not use a plain `time.sleep()` for long pauses; the broker will eventually close
an idle connection whose heartbeat is not answered.

### 5.5 Reconnect

The current coordinator has no reconnect loop. Add one:

- Exponential backoff with full jitter: `min(30, 2**n) * random.uniform(0, 1)`.
- Re-declare topology on every reconnect.
- Log the connection generation and reset backoff after a successful idle period.
- Never spin in a tight loop; cap at one attempt every 30s.

---

## 6. Result consumer integration

Results are higher volume than due wakeups, but the same rules apply:

- `basic_qos(prefetch_count=20)` if using a push consumer, or `basic_get` in batches of
  20.
- Check the DB gate before each batch. If the DB is saturated, do not fetch.
- Ack only after the execution update, task update, and next-wakeup write are committed.
- On transient DB errors, nack/requeue with backoff.
- On poison messages, publish to a result DLQ after a bounded number of attempts.
- Use `lease_id` and `status = 'running'` in the update condition so a stale result
  cannot overwrite a newer attempt (Phase 2).
- Write the next recurring wakeup to the outbox in the same transaction (Phase 3).

Do not pause the result consumer because `scheduler.tasks` is high. Results must keep
draining so workers can free capacity. Pause it only for DB saturation, result-queue
backlog, or shutdown.

---

## 7. Reconciliation integration

Reconciliation is a safety net, not a scheduler. It must not amplify a backlog.

```python
while not shutdown:
    state = controller.state
    if state != "normal" and env.BACKPRESSURE_MODE == "enforce":
        metrics.reconciliation_skipped.labels(reason="backpressure").inc()
        time.sleep(env.RECONCILE_INTERVAL_SEC)
        continue

    # Query in batches by dispatch_at, not next_execution_time.
    ids = query_due_tasks(limit=env.RECONCILE_BATCH)  # e.g. 100

    for task_id in ids:
        if shutdown.is_set():
            break
        publish_nudge(task_id)
        time.sleep(env.RECONCILE_PACE_SEC)  # e.g. 0.01

    time.sleep(
        env.RECONCILE_INTERVAL_SEC
        * random.uniform(0.8, 1.2)
    )
```

- Query `WHERE status = 'scheduled' AND dispatch_at <= NOW()`.
- `ORDER BY dispatch_at`.
- Continue batches until fewer than `RECONCILE_BATCH` rows, with a hard cap.
- If `due_depth` is above its high watermark, skip this cycle: the due queue is already
  backed up and more nudges only add duplicates.
- Keep reconciliation idempotent: the due consumer and the unique slot index handle
  duplicate nudges.

---

## 8. Outbox relay integration (after Phase 3)

The outbox relay is another producer and must respect the same queue-depth signal:

- Before claiming a batch, check `task_depth` and `result_depth`.
- If `task_depth >= high`, pause publishing normal task events; keep the rows in the
  outbox.
- Do not pause priority events (cancellations, lease revocations) behind normal task
  events; use a priority column or a separate outbox channel.
- Claim with `FOR UPDATE SKIP LOCKED`.
- Publish with confirms, mark `published_at` after the broker confirms.
- If the broker is unavailable, back off with full jitter and keep rows pending.
- Alert on `outbox_lag_seconds` and `outbox_pending_rows`; once the lag exceeds the
  admission threshold, the API should reject new task creation with `503`.

The outbox is the durable buffer that lets the coordinator pause safely. Backpressure
without an outbox still works, but direct-publish failures can strand executions.

---

## 9. Configuration

```env
# Backpressure
BACKPRESSURE_MODE=observe            # observe | enforce
BACKPRESSURE_TASKS_HIGH=5000
BACKPRESSURE_TASKS_LOW=1000
BACKPRESSURE_RESULTS_HIGH=2000
BACKPRESSURE_RESULTS_LOW=500
BACKPRESSURE_POLL_SEC=1.0
BACKPRESSURE_HIGH_CONSECUTIVE=2
BACKPRESSURE_LOW_CONSECUTIVE=3
BACKPRESSURE_STALE_PAUSE_SEC=15

# Dispatch pacing
DISPATCH_RATE_PER_SEC=200
DISPATCH_BURST=20

# Coordinator DB pool
DB_POOL_MIN=2
DB_POOL_MAX=20
DB_POOL_MAX_WAITERS=5
DB_POOL_HIGH_WATERMARK=0.80
DB_POOL_RESUME_WATERMARK=0.60
DB_STATEMENT_TIMEOUT_MS=5000
DB_LOCK_TIMEOUT_MS=2000

# Reconciliation
RECONCILE_INTERVAL_SEC=120
RECONCILE_BATCH=100
RECONCILE_PACE_SEC=0.01

# Result consumer
RESULT_BATCH_SIZE=20
RESULT_MAX_ATTEMPTS=5
```

`BACKPRESSURE_MODE=observe` must be the default for the first rollout. It computes and
logs “would pause” transitions without changing consumption.

---

## 10. Observability

| Metric | Type | Labels | Purpose |
|--------|------|--------|---------|
| `scheduler_backpressure_state` | Gauge (one-hot) | `state` | normal/paused/unknown |
| `scheduler_backpressure_transitions_total` | Counter | `from_state`, `to_state` | Flap detection |
| `scheduler_backpressure_paused_seconds_total` | Counter | — | Time spent paused |
| `scheduler_dispatch_gate_blocked_total` | Counter | `reason` | Token bucket / state blocks |
| `scheduler_db_pool_utilization` | Gauge | — | Active / max |
| `scheduler_db_pool_waiting` | Gauge | — | Waiting acquires |
| `scheduler_queue_depth` | Gauge | `queue` | Local passive-declare depth |
| `scheduler_consumer_inflight` | Gauge | `consumer` | In-flight messages |
| `scheduler_reconciliation_skipped_total` | Counter | `reason` | Backpressure/due backlog |
| `scheduler_dispatch_latency_seconds` | Histogram | `consumer` | DB + publish time |
| `scheduler_consumer_reconnects_total` | Counter | `consumer` | Connection stability |
| `scheduler_consumer_errors_total` | Counter | `consumer`, `kind` | Transient vs permanent |
| `scheduler_outbox_lag_seconds` | Gauge | — | After Phase 3 |

Alerts:

- `scheduler_backpressure_state{state="paused"} == 1` for 10m.
- More than 6 state transitions in 1h (flapping).
- `scheduler_db_pool_waiting > 0` for 2m.
- `scheduler_queue_depth{queue="scheduler.tasks"} > high` for 10m.
- `scheduler_consumer_reconnects_total` rate > 1/5m.
- `scheduler_consumer_errors_total{kind="permanent"}` > 0 (page).

Runbook entries:

- How to confirm whether a pause is due to DB or queue depth.
- How to drain a backlog safely.
- How to roll back to `observe` mode.
- How to verify no messages were lost after a reconnect.

---

## 11. Test plan

### Unit tests

- Controller hysteresis: high for 2 samples → paused; one low sample → still paused;
  3 low samples → normal.
- Stale monitor → unknown; fresh sample → normal/paused.
- DB gate utilization/waiters math.
- Token bucket pacing.
- State transitions emit the right metrics.

### Integration tests

- Fill `scheduler.tasks` above high with a test publisher; verify the due consumer stops
  creating executions; drain the queue; verify it resumes below low.
- Slow the DB with Toxiproxy; verify the coordinator pauses, heartbeats stay alive, and
  no message is acked on error.
- Kill RabbitMQ during dispatch; verify reconnect with jitter and no duplicate executions.
- Run two coordinators against the same queues; verify both respect backpressure and the
  unique slot index prevents duplicates.
- Result queue backlog: verify due dispatch pauses if results cannot be applied, and
  resumes after the result consumer catches up.
- Reconciliation with `due_depth` high: verify it skips cycles.
- Observe mode: verify behavior is unchanged while “would pause” metrics increment.

### Load tests

- 2× worker capacity: queue depth stays bounded, DB pool utilization stays below the
  high watermark, p95 dispatch latency stays within SLO.
- Worker outage for 5 minutes: queue grows, coordinator pauses, no DB write storm; on
  recovery, workers drain and the coordinator resumes.
- Coordinator restart during a backlog: no message loss, no duplicate executions.

### Acceptance criteria

- [ ] The due consumer stops fetching when the task queue or DB pool is above threshold.
- [ ] It resumes only after the hysteresis conditions are met.
- [ ] No message is acked after a failed DB transaction or publish.
- [ ] Heartbeats remain healthy during a long pause.
- [ ] Reconnect uses jittered backoff and re-declares topology.
- [ ] No duplicate `(task_id, scheduled_for)` executions under two coordinators.
- [ ] Observe mode is behaviorally identical to the current system.
- [ ] Metrics and alerts cover pause state, DB pool, queue depth, and reconnects.

---

## 12. Rollout

1. Add the connection pool and DB gate. Ship with `BACKPRESSURE_MODE=observe`.
2. Add the queue-depth monitor and controller; emit metrics only.
3. Run a load test; calibrate high/low watermarks from actual worker throughput.
4. Enable `enforce` for the due consumer in staging, then production.
5. Add result-consumer and reconciliation gates.
6. After the outbox is live, make the outbox relay respect the same signal.
7. Keep `observe` as a one-flag rollback path.

Rollback:

- Set `BACKPRESSURE_MODE=observe`.
- The consumer returns to fetching continuously; the DB pool and unique slot still
  protect correctness.
- No schema changes are required.

---

## 13. Open questions

1. **High/low values:** derive from the real worker throughput after Phase 4 concurrency
   lands; the defaults are placeholders.
2. **Pull vs push:** `basic_get` is simple and safe for the current serial dispatcher.
   Revisit if dispatch throughput exceeds a few hundred messages/second.
3. **Result consumer:** should it use the same pull helper or keep `basic_consume` with
   `basic_qos`? The pull helper is recommended for consistency, but measure throughput.
4. **Per-user fairness:** a single noisy tenant can still fill the task queue. Consider
   weighted fair queuing or per-tenant queue quotas after the core backpressure works.
5. **Auto-scaling:** should queue depth drive worker autoscaling? That is a separate
   infrastructure feature; do not couple it to the coordinator.
