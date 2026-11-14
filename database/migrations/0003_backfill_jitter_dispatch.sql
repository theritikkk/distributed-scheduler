-- 0003_backfill_jitter_dispatch.sql
--
-- Backfill existing rows in bounded batches. For a large production table,
-- run this during a low-traffic window; the loop avoids a single unbounded
-- UPDATE statement. The migration runner still wraps the file in one
-- transaction, so very large installations should run an equivalent batched
-- backfill as reviewed maintenance SQL before applying this migration.
--
-- Existing recurring tasks keep their current cadence: dispatch_at starts equal
-- to next_execution_time and jitter_seconds defaults to 0. Existing executions
-- use started_at as their historical slot.

DO $$
DECLARE
  affected INTEGER;
BEGIN
  LOOP
    WITH batch AS (
      SELECT id
      FROM tasks
      WHERE dispatch_at IS NULL
      ORDER BY id
      LIMIT 10000
    )
    UPDATE tasks AS t
    SET dispatch_at = t.next_execution_time
    FROM batch
    WHERE t.id = batch.id;

    GET DIAGNOSTICS affected = ROW_COUNT;
    EXIT WHEN affected = 0;
  END LOOP;
END $$;

DO $$
DECLARE
  affected INTEGER;
BEGIN
  LOOP
    WITH batch AS (
      SELECT id
      FROM task_executions
      WHERE scheduled_for IS NULL
      ORDER BY id
      LIMIT 10000
    )
    UPDATE task_executions AS e
    SET scheduled_for = e.started_at
    FROM batch
    WHERE e.id = batch.id;

    GET DIAGNOSTICS affected = ROW_COUNT;
    EXIT WHEN affected = 0;
  END LOOP;
END $$;

-- Fail before the unique index is created if historical data cannot satisfy
-- the slot invariant. Do not silently delete execution history.
DO $$
DECLARE
  duplicate_count INTEGER;
BEGIN
  SELECT count(*)
  INTO duplicate_count
  FROM (
    SELECT task_id, scheduled_for
    FROM task_executions
    GROUP BY task_id, scheduled_for
    HAVING count(*) > 1
  ) AS duplicates;

  IF duplicate_count > 0 THEN
    RAISE EXCEPTION
      'cannot create unique execution slots: % duplicate (task_id, scheduled_for) groups; review task_executions before applying 0004',
      duplicate_count;
  END IF;
END $$;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM tasks
    WHERE dispatch_at < next_execution_time
  ) THEN
    RAISE EXCEPTION
      'cannot contract schedule: dispatch_at is before next_execution_time for at least one task';
  END IF;
END $$;
