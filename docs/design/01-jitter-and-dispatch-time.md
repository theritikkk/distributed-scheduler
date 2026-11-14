# Design 01 — Jitter, `dispatch_at`, and execution-slot uniqueness

- **Status:** Implemented (Phase 1)
- **Date:** 2026-10-30
- **Scope:** Scheduling correctness, cron load spreading, duplicate-dispatch prevention
- **Related:** [02 — Rate limiting](02-rate-limiting-and-redis-fallback.md), [03 — Backpressure](03-coordinator-backpressure.md), [04 — Plan](04-implementation-plan.md)

---

## 1. Problem

The scheduler currently uses `next_execution_time` for three different jobs:

1. The user-visible scheduled time.
2. The wakeup TTL published to `scheduler.delay`.
3. The dispatch check in the due consumer.

That works for a single task, but it breaks down under load:

- **Cron herd.** Every task with `* * * * *` becomes due at the same second. The
  coordinator then dispatches as fast as it can, creating a burst of DB writes and
  `scheduler.tasks` publishes.
- **TTL head-of-line blocking.** RabbitMQ dead-letters per-message-TTL messages only when
  they reach the head of the queue. A wakeup for tomorrow published before a wakeup for
  one minute from now blocks the shorter one. Reconciliation hides this, but real
  scheduling latency becomes bounded by the reconciliation interval (120s), not by TTL.
- **Retry storms.** Worker retries use deterministic backoff
  (`worker/main.py`, `_BASE_BACKOFF_MS * 2 ** retry_count`). After a broker or worker
  outage, every failed task retries at the same instants.
- **No durable slot identity.** Duplicate wakeups are mostly handled by the
  `status = 'scheduled'` check and row lock, but there is no database-enforced guarantee
  that a given task runs once per scheduled slot.
- **Jitter cannot be applied only to the wakeup.** The due consumer republishes when
  `next_execution_time > now` (`coordinator/internal/queue/due_consumer.py:79-83`).
  If the wakeup is jittered but the task time is not, jitter is immediately undone.

This design introduces a **base schedule** and a **dispatch time**:

```text
base schedule:     next_execution_time     (cron slot / user-requested time)
dispatch time:     dispatch_at             (base + stable jitter)
execution slot:    scheduled_for           (base, for uniqueness)
```

`dispatch_at` becomes the single input for wakeups and dispatch. `scheduled_for` becomes
the database-enforced identity of an execution.

---

## 2. Goals and non-goals

### Goals

- Spread recurring task dispatch over a bounded window without changing the cadence of
  the base cron schedule.
- Make duplicate wakeups and duplicate dispatch attempts harmless at the database level.
- Add full jitter to worker retry backoff.
- Make the change deployable with an expand/contract migration and no downtime.
- Keep one-time tasks exact by default.
- Preserve current behavior when jitter is disabled (`jitter_seconds = 0`).

### Non-goals

- Fixing RabbitMQ TTL head-of-line blocking. That requires a delayed-message exchange,
  TTL-bucket queues, or a DB-driven scheduler; it is a separate workstream.
- Timezone-aware cron schedules.
- Misfire/catch-up policy for long outages (documented as an open question).
- Exactly-once execution of arbitrary shell commands.
- Fixing the API-level idempotency key flow (Phase 2 in the implementation plan).

---

## 3. Concepts and invariants

| Term | Meaning |
|------|---------|
| `next_execution_time` | Base schedule: the cron slot or the user-requested one-time time. Never jittered. |
| `dispatch_at` | When the scheduler should dispatch: `next_execution_time + stable_offset`. |
| `scheduled_for` | The base slot copied onto `task_executions`; used for uniqueness and lag metrics. |
| `jitter_seconds` | Maximum offset window, in seconds. `0` disables jitter. |
| `jitter_policy` | `none` or `stable`. `stable` means a deterministic per-task offset. |
| Stable offset | `SHA-256(task_id)[0..4] % (window + 1)`. Same task, same offset, every run. |
| Schedule lag | `started_at - scheduled_for`. `started_at` is set when the coordinator creates the execution; it includes jitter and queue wait. |
| Dispatch delay | `actual_dispatch_time - dispatch_at`. Should be near zero when healthy. |

**Invariant 1:** `dispatch_at >= next_execution_time`.
**Invariant 2:** At most one execution exists for `(task_id, scheduled_for)`.
**Invariant 3:** A recurring task's next base slot is computed from the base cron
sequence, never from the jittered dispatch time. Jitter must not cause drift.

---

## 4. Schema changes

### 4.1 Expand phase (safe for rolling deploy)

The first migration only adds nullable columns and indexes. Old code continues to work.

```sql
-- 0002_expand_jitter_dispatch.sql

ALTER TABLE tasks
  ADD COLUMN IF NOT EXISTS jitter_seconds integer NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS jitter_policy text NOT NULL DEFAULT 'stable',
  ADD COLUMN IF NOT EXISTS dispatch_at timestamptz;

ALTER TABLE task_executions
  ADD COLUMN IF NOT EXISTS scheduled_for timestamptz;

-- Production: CREATE INDEX CONCURRENTLY outside a transaction.
CREATE INDEX IF NOT EXISTS idx_tasks_dispatch_at_due
  ON tasks (dispatch_at)
  WHERE status IN ('pending', 'scheduled');

CREATE INDEX IF NOT EXISTS idx_task_executions_slot
  ON task_executions (task_id, scheduled_for);
```

During rollout, new code reads:

```sql
COALESCE(dispatch_at, next_execution_time) AS effective_dispatch_at
```

and inserts `scheduled_for = next_execution_time`.

Old coordinator code keeps using `next_execution_time`; that is safe while jitter is
disabled. Enable jitter only after the backfill and code rollout are complete.

### 4.2 Backfill phase

```sql
-- Run in batches for large tables; this is the simple form.
UPDATE tasks
SET dispatch_at = next_execution_time
WHERE dispatch_at IS NULL;

UPDATE task_executions
SET scheduled_for = started_at
WHERE scheduled_for IS NULL;
```

For large tables, loop batches:

```sql
WITH batch AS (
  SELECT id FROM tasks WHERE dispatch_at IS NULL ORDER BY id LIMIT 10000
)
UPDATE tasks t
SET dispatch_at = t.next_execution_time
FROM batch
WHERE t.id = batch.id;
```

Repeat until zero rows. Do not hold a single transaction over the whole table.

Phase 1 implements this as a bounded loop inside `0003_backfill_jitter_dispatch.sql`.
The migration runner wraps one file in one transaction, so very large installations
should run an equivalent reviewed maintenance loop with periodic commits before
applying the migration.

### 4.3 Contract phase (after code is fully rolled out)

```sql
-- 0005_contract_jitter_dispatch.sql
-- The unique slot index is created earlier, in 0004_slot_unique.sql, because
-- the new coordinator uses ON CONFLICT (task_id, scheduled_for).

ALTER TABLE tasks
  ALTER COLUMN dispatch_at SET NOT NULL,
  ADD CONSTRAINT tasks_jitter_seconds_range
    CHECK (jitter_seconds BETWEEN 0 AND 3600),
  ADD CONSTRAINT tasks_jitter_policy_valid
    CHECK (jitter_policy IN ('none', 'stable')),
  ADD CONSTRAINT tasks_jitter_policy_none_zero
    CHECK (jitter_policy <> 'none' OR jitter_seconds = 0),
  ADD CONSTRAINT tasks_dispatch_at_not_before_next
    CHECK (dispatch_at >= next_execution_time);

ALTER TABLE task_executions
  ALTER COLUMN scheduled_for SET NOT NULL;
```

The unique index is added before the code deploy:

```sql
-- 0004_slot_unique.sql
-- Safe while old code is running: old code inserts scheduled_for as NULL, and
-- PostgreSQL unique indexes allow multiple NULLs.
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS uq_task_executions_slot
  ON task_executions (task_id, scheduled_for);
```

### 4.4 Pre-flight checks

Before creating the unique index, check for existing duplicates:

```sql
SELECT task_id, scheduled_for, count(*) AS executions
FROM task_executions
GROUP BY task_id, scheduled_for
HAVING count(*) > 1;
```

If duplicates exist, decide which execution to keep (usually the latest successful or
the earliest) and mark the others `failed` with an explicit error message. Do this
manually or in a reviewed cleanup migration; do not silently delete execution history.

Also verify the base schedule is sane:

```sql
SELECT id, next_execution_time, dispatch_at
FROM tasks
WHERE dispatch_at < next_execution_time;
```

### 4.5 Rollback

All changes are additive:

1. Deploy code with `JITTER_ENABLED=false`; it writes
   `dispatch_at = next_execution_time`.
2. Drop the unique index and the new columns if necessary.
3. Old code ignores the columns.

Do not roll back by dropping `scheduled_for` while code still references it. Roll back
code first, then schema.

### 4.6 Phase 2 additions (not in this migration)

The following belong to the state-machine/idempotency phase and should not be mixed into
the jitter migration:

- `task_executions.lease_id`
- `task_executions.lease_expires_at`
- `task_executions.attempt`
- `task_execution_attempts` table
- Partial unique index `(task_id) WHERE status = 'running'`

---

## 5. Jitter algorithm

### 5.1 Stable offset

Use the first 4 bytes of a SHA-256 digest so Node and Python produce the same offset.

Python:

```python
import hashlib

def stable_offset(task_id: str, window_sec: int) -> int:
    if window_sec <= 0:
        return 0
    digest = hashlib.sha256(task_id.encode("utf-8")).digest()
    return int.from_bytes(digest[:4], "big") % (window_sec + 1)
```

TypeScript:

```ts
import { createHash } from 'crypto';

export function stableOffset(taskId: string, windowSec: number): number {
  if (windowSec <= 0) return 0;
  const digest = createHash('sha256').update(taskId).digest();
  return digest.readUInt32BE(0) % (windowSec + 1);
}
```

### 5.2 Dispatch time

```python
def compute_dispatch_at(
    base: datetime,
    task_id: str,
    jitter_seconds: int,
    period_seconds: int | None,
) -> datetime:
    if jitter_seconds <= 0:
        return base

    # Never let jitter push a run past the next cron slot.
    if period_seconds is not None and period_seconds > 1:
        window = min(jitter_seconds, period_seconds - 1)
    else:
        window = jitter_seconds

    return base + timedelta(seconds=stable_offset(task_id, window))
```

Notes:

- If `base` is naive, treat it as UTC.
- If the computed time is already in the past, leave it in the past. The dispatcher will
  fire immediately; the lag metric records how late it was.
- For one-time tasks, `period_seconds = None`; the window is `jitter_seconds`.
- For recurring tasks, derive `period_seconds` from two consecutive cron occurrences.

### 5.3 Suggested windows

| Schedule | Period | Suggested default window | Max delay |
|----------|--------|--------------------------|-----------|
| `* * * * *` | 60s | 10s | 10s |
| `*/5 * * * *` | 300s | 30s | 30s |
| `0 * * * *` | 3600s | 60s | 1 min |
| `0 0 * * *` | 86400s | 300s | 5 min |

Formula for a default:

```text
window = min(0.1 * period_seconds, MAX_JITTER_SEC)
```

`MAX_JITTER_SEC` defaults to `300` (5 minutes). The schema allows up to `3600`, but a
larger window should be an explicit product decision.

### 5.4 One-time tasks

Default `jitter_seconds = 0` for one-time tasks: users expect the requested time.
Allow an optional `jitter_seconds` field for clients that want spreading. Document that
the task may run up to `jitter_seconds` late.

### 5.5 Updates

When any of these change, recompute `dispatch_at` and republish the wakeup:

- `next_execution_time`
- `cron_expression`
- `schedule_type`
- `jitter_seconds`
- `jitter_policy`

Do not modify the `scheduled_for` of an execution already in flight. Updating a running
task only affects the next slot.

---

## 6. API write path

### 6.1 Generate the task ID in the API

The current schema generates the ID with `gen_random_uuid()`, which makes it impossible
to compute a task-id-based offset before insert. Generate the ID in the API:

```ts
import { randomUUID } from 'crypto';

const taskId = randomUUID();
```

This also makes idempotency and outbox event keys deterministic in later phases.

### 6.2 Compute the base schedule

For recurring tasks, compute the next slot from the cron expression rather than trusting
a client-supplied time. This requires a cron parser in the API (`cron-parser`):

```ts
import parser from 'cron-parser';

const base = parser
  .parseExpression(cronExpression, { currentDate: new Date(), tz: 'UTC' })
  .next()
  .toDate();
```

For one-time tasks, use the validated `next_execution_time`.

This closes the current gap where any string is accepted as a cron expression and an
invalid expression later silently falls back to “every minute”
(`coordinator/internal/queue/result_consumer.py:28-32`).

### 6.3 Compute jitter

```ts
const jitterSeconds =
  body.jitter_seconds ??
  (schedule_type === 'recurring' ? config.defaultRecurringJitterSec : 0);

const periodSeconds =
  schedule_type === 'recurring'
    ? Math.floor(
        (nextCron(base).getTime() - base.getTime()) / 1000
      )
    : undefined;

const dispatchAt = computeDispatchAt(
  base,
  taskId,
  jitterSeconds,
  periodSeconds
);
```

Validation:

- `jitter_seconds` is an integer in `[0, MAX_JITTER_SEC]`.
- `jitter_policy` is `none` or `stable`.
- If `jitter_policy = 'none'`, force `jitter_seconds = 0`.
- `dispatch_at >= next_execution_time`.

### 6.4 Insert

```sql
INSERT INTO tasks (
  id, user_id, task_name, command_payload, schedule_type,
  cron_expression, next_execution_time, dispatch_at,
  jitter_seconds, jitter_policy, status
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'scheduled')
RETURNING ...;
```

Publish the wakeup for `dispatch_at`, not `next_execution_time`:

```ts
await publishScheduleWakeup(task.id, task.dispatch_at);
```

### 6.5 Response

Return both fields so clients can see the difference:

```json
{
  "next_execution_time": "2026-10-30T10:00:00.000Z",
  "dispatch_at": "2026-10-30T10:00:17.000Z",
  "jitter_seconds": 30,
  "jitter_policy": "stable"
}
```

---

## 7. Dispatch path

### 7.1 Publisher

`publish_schedule_wakeup` already accepts a datetime. Rename the parameter to
`dispatch_at` and pass `task.dispatch_at` from all call sites:

- `api-gateway/src/routes/tasks.ts` create/update
- `coordinator/internal/queue/result_consumer.py` next recurring run
- `coordinator/internal/queue/reconciliation.py` if it republishes

The `_MAX_DELAY_MS` cap (24.8 days) remains. For longer delays, the due consumer
republishes another wakeup; no change needed.

### 7.2 Due consumer

Read `dispatch_at` with a fallback for rows created during rollout:

```sql
SELECT task_name, command_payload, schedule_type, status,
       next_execution_time, dispatch_at,
       COALESCE(dispatch_at, next_execution_time) AS effective_dispatch_at
FROM tasks
WHERE id = %s
FOR UPDATE;
```

Dispatch only if `effective_dispatch_at <= NOW()`. Use the database clock, not the
application clock, for this comparison:

```sql
SELECT ..., (COALESCE(dispatch_at, next_execution_time) <= NOW()) AS is_due
FROM tasks
WHERE id = %s
FOR UPDATE;
```

If not due, republish for `effective_dispatch_at` and ack.

If due, insert the execution with the base slot:

```sql
INSERT INTO task_executions (task_id, scheduled_for, started_at, status)
VALUES (%s, %s, NOW(), 'running')
ON CONFLICT (task_id, scheduled_for) DO NOTHING
RETURNING id;
```

If no row is returned, the slot was already dispatched. Ack and return. This is the
database-level duplicate guard; it works even if two coordinators process the same
wakeup at the same time.

Update the task conditionally:

```sql
UPDATE tasks
SET status = 'running', updated_at = NOW()
WHERE id = %s AND status = 'scheduled';
```

If `rowcount = 0`, roll back the execution insert (or leave it and let the state machine
handle it), acknowledge, and return. The existing code deletes the execution in this
case; keep that behavior until the state machine lands.

The task message should include both times:

```json
{
  "taskId": "...",
  "executionId": "...",
  "taskName": "...",
  "payload": "...",
  "scheduleType": "recurring",
  "scheduledFor": "2026-10-30T10:00:00Z",
  "dispatchAt": "2026-10-30T10:00:17Z",
  "retryCount": 0,
  "maxRetries": 3
}
```

### 7.3 Result consumer

For recurring tasks, compute the next base slot from the execution's `scheduled_for`,
not from `now`:

```python
base_next = croniter(cron_expr, scheduled_for).get_next(datetime)
```

During the expand/contract rollout, use `COALESCE(scheduled_for, started_at)` until every
execution has been backfilled.

This keeps cadence stable. Then:

```python
period_seconds = int(
    (croniter(cron_expr, base_next).get_next(datetime) - base_next).total_seconds()
)
dispatch_at = base_next + timedelta(
    seconds=stable_offset(
        task_id,
        max(0, min(jitter_seconds, period_seconds - 1)),
    )
)
```

Update both fields:

```sql
UPDATE tasks
SET next_execution_time = %s,
    dispatch_at = %s,
    status = 'scheduled',
    updated_at = NOW()
WHERE id = %s
  AND status <> 'cancelled';
```

The `status <> 'cancelled'` condition prevents a late result from undoing a cancellation.
Phase 2 replaces this with a full state machine.

**Missed slots:** if `base_next <= now()`, the scheduler has fallen behind. Default
policy: advance through the cron sequence until the next slot is in the future. This
avoids a catch-up storm after an outage:

```python
while base_next <= now:
    base_next = croniter(cron_expr, base_next).get_next(datetime)
```

A catch-up policy can be added later as a per-task option.

### 7.4 Reconciliation

Query by `dispatch_at`:

```sql
SELECT id
FROM tasks
WHERE status = 'scheduled'
  AND COALESCE(dispatch_at, next_execution_time) <= NOW()
ORDER BY COALESCE(dispatch_at, next_execution_time) ASC
LIMIT 100;
```

Add interval jitter:

```python
time.sleep(RECONCILE_INTERVAL_SEC * random.uniform(0.8, 1.2))
```

Pace the nudges if the batch is large; Phase 5 adds queue-depth backpressure so
reconciliation does not nudge while `scheduler.tasks` is above its high watermark.

### 7.5 Retry jitter

Replace deterministic worker backoff:

```python
ceiling_ms = min(_MAX_BACKOFF_MS, _BASE_BACKOFF_MS * (2 ** retry_count))
backoff_ms = int(random.uniform(0, ceiling_ms))
```

Use full jitter. If very early retries are undesirable, use equal jitter:

```python
half = ceiling_ms / 2
backoff_ms = int(half + random.uniform(0, half))
```

The retry message still uses `expiration=str(backoff_ms)` on
`scheduler.retry_delay`.

---

## 8. Edge cases

| Case | Behavior |
|------|----------|
| `jitter_seconds = 0` | `dispatch_at = next_execution_time`; exact current behavior. |
| Jitter window >= period | Cap at `period - 1` so two slots cannot collide. |
| Task created after `dispatch_at` | Fire immediately; record positive schedule lag. |
| Long-running task misses next slots | Skip missed slots by default (no catch-up storm). |
| Duplicate wakeup | `ON CONFLICT (task_id, scheduled_for)` makes it a no-op. |
| Duplicate result | Result consumer's `completed_at IS NULL` guard remains. |
| Task cancelled while running | Result must not reschedule it; use conditional updates. |
| Clock skew between API/coordinator/DB | Use `NOW()` in SQL for due checks; use app time only for computing offsets. |
| DST / timezone | All times remain UTC for now; timezone support is a separate design. |
| Manual “run now” | Use a unique `scheduled_for` (e.g., `NOW()` or a run UUID); not part of this phase. |
| `dispatch_at` beyond 24.8 days | Publisher clamps TTL; due consumer republishes. Existing behavior. |
| Task update while running | Recompute only the next slot; do not alter the in-flight execution. |

---

## 9. Configuration

```env
# Master switch. When false, API writes dispatch_at = next_execution_time.
SCHEDULER_JITTER_ENABLED=true

# Default jitter for recurring tasks when the client omits it.
# 0 keeps cron semantics exact; set a value to opt into spreading by default.
DEFAULT_RECURRING_JITTER_SEC=0

# Hard upper bound accepted by the API and schema.
MAX_JITTER_SEC=300

# full | equal | none
RETRY_JITTER_MODE=full

# Reconciliation interval jitter, as a fraction of the base interval.
RECONCILE_INTERVAL_JITTER_RATIO=0.2
```

Per-task fields:

| Field | Type | Notes |
|-------|------|-------|
| `jitter_seconds` | integer | `0..MAX_JITTER_SEC` |
| `jitter_policy` | `none` / `stable` | `none` forces `jitter_seconds = 0` |

---

## 10. Observability

Add to the API/coordinator/worker metrics modules:

| Metric | Type | Labels | Purpose |
|--------|------|--------|---------|
| `scheduler_task_jitter_seconds` | Histogram | `schedule_type` | Distribution of applied offsets |
| `scheduler_task_schedule_lag_seconds` | Histogram | `schedule_type` | `started_at - scheduled_for` |
| `scheduler_task_dispatch_delay_seconds` | Histogram | — | `actual_dispatch - dispatch_at` |
| `scheduler_task_next_run_drift_seconds` | Gauge/Histogram | — | Drift from the base cron sequence |
| `scheduler_worker_retry_backoff_seconds` | Histogram | `worker_id` | Applied retry delay |
| `scheduler_reconciliation_jitter_seconds` | Gauge | — | Last sleep interval |
| `scheduler_task_slot_conflicts_total` | Counter | — | Duplicate slot insert attempts |

Alerts:

- `p95(scheduler_task_schedule_lag_seconds) > 2 × jitter_window` for 10m.
- `scheduler_task_next_run_drift_seconds > 60` for 10m.
- `scheduler_task_slot_conflicts_total` rising unexpectedly (could indicate duplicate
  wakeups or clock issues).

---

## 11. Test plan

### Unit tests

- `stable_offset` returns the same value for the same task and a value within window.
- `compute_dispatch_at` caps at `period - 1`.
- `jitter_seconds = 0` returns the base time exactly.
- One-time task with no jitter is exact.
- Update recomputes `dispatch_at` when cron/time/jitter changes.
- Retry jitter stays within `[0, ceiling]` and `ceiling` respects the cap.
- TypeScript and Python implementations produce the same offset for the same task ID.

### Integration tests

- Create 10,000 recurring tasks with the same cron slot and a 30s window; assert the
  offsets are spread across the window and the dispatch rate is bounded.
- Publish the same wakeup twice; assert exactly one `task_executions` row per
  `(task_id, scheduled_for)`.
- Process two wakeups concurrently through two coordinators; assert one execution.
- Run a recurring task with `scheduled_for` in the past; assert missed slots are skipped
  and the next `dispatch_at` is in the future.
- Cancel a recurring task while an execution is running; assert the result does not
  reschedule it.
- Update `jitter_seconds`; assert the next wakeup uses the new offset.
- Backfill migration on a seeded database; assert no rows lost and constraints hold.

### Load tests

- 10k tasks at one cron boundary: measure dispatch P95, queue depth, DB writes, and
  duplicate-execution count.
- Mixed schedule periods (1m, 5m, 1h) to exercise the TTL queue head-of-line behavior.
- Worker outage and recovery: measure retry spread and queue drain time.

### Acceptance criteria

- [x] Existing tasks behave exactly as before when jitter is disabled.
- [x] With jitter enabled, 10k same-slot tasks are spread over the configured window.
- [x] Duplicate wakeups produce exactly one execution per `(task_id, scheduled_for)`.
- [x] Recurring cadence does not drift after jitter is enabled.
- [x] Cancellation is not undone by an in-flight result.
- [x] Migration is additive, backfilled, and rollback-safe.
- [x] Metrics show offset, schedule lag, dispatch delay, and slot conflicts.

---

## 12. Rollout

1. **Phase 0:** add migration runner and test harness.
2. Apply the expand migration (nullable columns + non-unique indexes, `0002`).
3. Backfill in batches (`0003`) and create the unique slot index (`0004`). The index is
   safe while old code runs because old code inserts `scheduled_for` as NULL.
4. Deploy API that writes `dispatch_at` and `scheduled_for`; deploy coordinator that reads
   `COALESCE(dispatch_at, next_execution_time)` and uses `ON CONFLICT`.
5. Apply the contract migration (`0005`: NOT NULL and constraints).
6. Deploy `SCHEDULER_JITTER_ENABLED=true` with `DEFAULT_RECURRING_JITTER_SEC=0`; opt in
   per task or per tenant first.
7. Measure schedule lag and queue behavior; increase defaults gradually.

Rollback:

- Set `SCHEDULER_JITTER_ENABLED=false`.
- Code writes `dispatch_at = next_execution_time`; behavior returns to baseline.
- Schema can remain in place; the columns are additive.

---

## 13. Open questions

1. **Default recurring jitter:** should new recurring tasks get a default 0–30s window, or
   remain exact unless the client opts in?
2. **Misfire policy:** should missed recurring slots be skipped (proposed), run once, or
   catch up with a bound?
3. **Timezone:** when should per-task timezones be added? Jitter interacts with DST.
4. **Manual runs:** what is the `scheduled_for` identity for an on-demand run?
5. **TTL scheduling:** do we keep the current TTL delay queue and rely on reconciliation,
   or move to a delayed exchange / DB scheduler next?

## 14. Phase 1 decisions

- Recurring jitter remains `0` by default; clients opt in with `jitter_seconds`.
- Missed recurring slots are skipped; there is no catch-up in Phase 1.
- Times remain UTC; per-task timezone support is deferred.
- The existing TTL delay queue remains; queue-depth backpressure and outbox/durable
  publication are later phases.
- One-time tasks remain exact unless the client explicitly requests jitter.
