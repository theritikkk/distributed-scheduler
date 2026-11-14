import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import request from 'supertest';
import { app } from '../src/app';
import { pool } from '../src/db';
import { stableOffset } from '../src/scheduling';

const runDatabaseTests = Boolean(process.env.TEST_DATABASE_URL);
const describeWithDatabase = runDatabaseTests ? describe : describe.skip;

const email = `integration-${randomUUID()}@example.com`;
const password = 'TestPass1!';

describeWithDatabase('task API with PostgreSQL', () => {
  let token: string;
  let userId: string;

  beforeAll(async () => {
    await pool.query('DELETE FROM users WHERE email = $1', [email]);
    await pool.query('DELETE FROM outbox');
  });

  afterAll(async () => {
    await pool.query('DELETE FROM outbox');
    await pool.query('DELETE FROM users WHERE email = $1', [email]);
    await pool.end();
  });

  it('registers a user and returns a token', async () => {
    const response = await request(app)
      .post('/api/v1/auth/register')
      .send({ email, password });

    expect(response.status).toBe(201);
    expect(response.body.token).toEqual(expect.any(String));
    expect(response.body.user).toMatchObject({ email });

    token = response.body.token;
    userId = response.body.user.id;
  });

  it('creates a task for the authenticated user', async () => {
    const response = await request(app)
      .post('/api/v1/tasks')
      .set('Authorization', `Bearer ${token}`)
      .send({
        task_name: 'integration task',
        command_payload: { command: 'echo integration' },
        schedule_type: 'one-time',
        next_execution_time: new Date(Date.now() + 3_600_000).toISOString(),
      });

    expect(response.status).toBe(201);
    expect(response.body).toMatchObject({
      user_id: userId,
      task_name: 'integration task',
      status: 'scheduled',
      jitter_seconds: 0,
      jitter_policy: 'stable',
    });
    expect(response.body.dispatch_at).toBe(response.body.next_execution_time);

    const stored = await pool.query(
      'SELECT user_id, task_name, dispatch_at, jitter_seconds, jitter_policy FROM tasks WHERE id = $1',
      [response.body.id],
    );
    expect(stored.rows[0]).toMatchObject({
      user_id: userId,
      task_name: 'integration task',
      jitter_seconds: 0,
      jitter_policy: 'stable',
    });

    const outbox = await pool.query(
      `SELECT event_type, status, payload FROM outbox
       WHERE aggregate_id = $1 ORDER BY id`,
      [response.body.id],
    );
    expect(outbox.rows).toHaveLength(1);
    expect(outbox.rows[0]).toMatchObject({
      event_type: 'task.scheduled',
      status: 'pending',
      payload: { taskId: response.body.id },
    });
  });

  it('replays the stored response for a repeated Idempotency-Key', async () => {
    const key = `idem-${randomUUID()}`;
    const payload = {
      task_name: `idempotent ${randomUUID()}`,
      command_payload: { command: 'echo idempotent' },
      schedule_type: 'one-time',
      next_execution_time: new Date(Date.now() + 3_600_000).toISOString(),
    };

    const first = await request(app)
      .post('/api/v1/tasks')
      .set('Authorization', `Bearer ${token}`)
      .set('Idempotency-Key', key)
      .send(payload);
    const second = await request(app)
      .post('/api/v1/tasks')
      .set('Authorization', `Bearer ${token}`)
      .set('Idempotency-Key', key)
      .send(payload);

    expect(first.status).toBe(201);
    expect(second.status).toBe(201);
    expect(second.body.id).toBe(first.body.id);
    expect(second.headers['idempotent-replayed']).toBe('true');

    const stored = await pool.query(
      'SELECT count(*)::int AS count FROM tasks WHERE task_name = $1',
      [payload.task_name],
    );
    expect(stored.rows[0].count).toBe(1);

    const keyRow = await pool.query(
      `SELECT state, resource_id FROM idempotency_keys
       WHERE user_id = $1 AND key = $2`,
      [userId, key],
    );
    expect(keyRow.rows[0]).toMatchObject({
      state: 'completed',
      resource_id: first.body.id,
    });

    const outbox = await pool.query(
      `SELECT count(*)::int AS count FROM outbox WHERE aggregate_id = $1`,
      [first.body.id],
    );
    expect(outbox.rows[0].count).toBe(1);
  });

  it('rejects an Idempotency-Key reused with a different body', async () => {
    const key = `idem-${randomUUID()}`;
    const first = await request(app)
      .post('/api/v1/tasks')
      .set('Authorization', `Bearer ${token}`)
      .set('Idempotency-Key', key)
      .send({
        task_name: `idempotent conflict ${randomUUID()}`,
        command_payload: { command: 'echo first' },
        schedule_type: 'one-time',
        next_execution_time: new Date(Date.now() + 3_600_000).toISOString(),
      });
    expect(first.status).toBe(201);

    const second = await request(app)
      .post('/api/v1/tasks')
      .set('Authorization', `Bearer ${token}`)
      .set('Idempotency-Key', key)
      .send({
        task_name: `idempotent conflict ${randomUUID()}`,
        command_payload: { command: 'echo second' },
        schedule_type: 'one-time',
        next_execution_time: new Date(Date.now() + 3_600_000).toISOString(),
      });

    expect(second.status).toBe(409);
    expect(second.body.error).toMatch(/different request/i);
  });

  it('serializes concurrent requests with the same Idempotency-Key', async () => {
    const key = `idem-${randomUUID()}`;
    const payload = {
      task_name: `idempotent concurrent ${randomUUID()}`,
      command_payload: { command: 'echo concurrent' },
      schedule_type: 'one-time',
      next_execution_time: new Date(Date.now() + 3_600_000).toISOString(),
    };

    const [first, second] = await Promise.all([
      request(app)
        .post('/api/v1/tasks')
        .set('Authorization', `Bearer ${token}`)
        .set('Idempotency-Key', key)
        .send(payload),
      request(app)
        .post('/api/v1/tasks')
        .set('Authorization', `Bearer ${token}`)
        .set('Idempotency-Key', key)
        .send(payload),
    ]);

    expect([first.status, second.status]).toEqual([201, 201]);
    expect(first.body.id).toBe(second.body.id);

    const stored = await pool.query(
      'SELECT count(*)::int AS count FROM tasks WHERE task_name = $1',
      [payload.task_name],
    );
    expect(stored.rows[0].count).toBe(1);
  });

  it('rolls back the key claim when the task insert fails', async () => {
    const key = `idem-${randomUUID()}`;
    const payload = {
      task_name: `idempotent failure ${randomUUID()}`,
      command_payload: { command: 'echo retry-after-failure' },
      schedule_type: 'one-time',
      next_execution_time: new Date(Date.now() + 3_600_000).toISOString(),
    };

    const outboxBefore = await pool.query('SELECT count(*)::int AS count FROM outbox');

    await pool.query('DROP TRIGGER IF EXISTS fail_idempotent_insert ON tasks');
    await pool.query('DROP FUNCTION IF EXISTS fail_idempotent_insert()');
    await pool.query(`
      CREATE FUNCTION fail_idempotent_insert() RETURNS trigger AS $$
      BEGIN
        RAISE EXCEPTION 'simulated task insert failure';
      END;
      $$ LANGUAGE plpgsql
    `);
    await pool.query(`
      CREATE TRIGGER fail_idempotent_insert
      BEFORE INSERT ON tasks
      FOR EACH ROW EXECUTE PROCEDURE fail_idempotent_insert()
    `);

    try {
      const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => undefined);
      try {
        const failed = await request(app)
          .post('/api/v1/tasks')
          .set('Authorization', `Bearer ${token}`)
          .set('Idempotency-Key', key)
          .send(payload);
        expect(failed.status).toBe(500);
      } finally {
        errorSpy.mockRestore();
      }

      const missing = await pool.query(
        'SELECT 1 FROM idempotency_keys WHERE user_id = $1 AND key = $2',
        [userId, key],
      );
      expect(missing.rowCount).toBe(0);

      const outboxAfter = await pool.query(
        'SELECT count(*)::int AS count FROM outbox',
      );
      expect(outboxAfter.rows[0].count).toBe(outboxBefore.rows[0].count);
    } finally {
      await pool.query('DROP TRIGGER IF EXISTS fail_idempotent_insert ON tasks');
      await pool.query('DROP FUNCTION IF EXISTS fail_idempotent_insert()');
    }

    const retry = await request(app)
      .post('/api/v1/tasks')
      .set('Authorization', `Bearer ${token}`)
      .set('Idempotency-Key', key)
      .send(payload);
    expect(retry.status).toBe(201);

    const replay = await request(app)
      .post('/api/v1/tasks')
      .set('Authorization', `Bearer ${token}`)
      .set('Idempotency-Key', key)
      .send(payload);
    expect(replay.status).toBe(201);
    expect(replay.body.id).toBe(retry.body.id);
    expect(replay.headers['idempotent-replayed']).toBe('true');
  });

  it('rejects an overlong Idempotency-Key before creating a task', async () => {
    const response = await request(app)
      .post('/api/v1/tasks')
      .set('Authorization', `Bearer ${token}`)
      .set('Idempotency-Key', 'x'.repeat(256))
      .send({
        task_name: `idempotent too long ${randomUUID()}`,
        command_payload: { command: 'echo nope' },
        schedule_type: 'one-time',
        next_execution_time: new Date(Date.now() + 3_600_000).toISOString(),
      });

    expect(response.status).toBe(400);
    expect(response.body.error).toMatch(/Idempotency-Key/);
  });

  it('creates an opt-in jittered recurring task from the cron schedule', async () => {
    const response = await request(app)
      .post('/api/v1/tasks')
      .set('Authorization', `Bearer ${token}`)
      .send({
        task_name: 'jittered recurring',
        command_payload: { command: 'echo recurring' },
        schedule_type: 'recurring',
        cron_expression: '* * * * *',
        jitter_seconds: 30,
      });

    expect(response.status).toBe(201);
    expect(response.body).toMatchObject({
      user_id: userId,
      schedule_type: 'recurring',
      jitter_seconds: 30,
      jitter_policy: 'stable',
    });

    const base = new Date(response.body.next_execution_time).getTime();
    const dispatch = new Date(response.body.dispatch_at).getTime();
    expect(dispatch - base).toBe(stableOffset(response.body.id, 30) * 1_000);
    expect(dispatch).toBeGreaterThanOrEqual(base);
  });

  it('rejects an invalid cron expression', async () => {
    const response = await request(app)
      .post('/api/v1/tasks')
      .set('Authorization', `Bearer ${token}`)
      .send({
        task_name: 'bad cron',
        command_payload: { command: 'echo bad' },
        schedule_type: 'recurring',
        cron_expression: 'not a cron expression',
      });

    expect(response.status).toBe(400);
    expect(response.body.error).toMatch(/cron/i);
  });

  it('forces jitter_seconds to zero when policy is none', async () => {
    const response = await request(app)
      .post('/api/v1/tasks')
      .set('Authorization', `Bearer ${token}`)
      .send({
        task_name: 'no jitter',
        command_payload: { command: 'echo exact' },
        schedule_type: 'one-time',
        next_execution_time: new Date(Date.now() + 3_600_000).toISOString(),
        jitter_seconds: 30,
        jitter_policy: 'none',
      });

    expect(response.status).toBe(201);
    expect(response.body.jitter_seconds).toBe(0);
    expect(response.body.jitter_policy).toBe('none');
    expect(response.body.dispatch_at).toBe(response.body.next_execution_time);
  });

  it('recomputes dispatch_at when jitter is updated', async () => {
    const created = await request(app)
      .post('/api/v1/tasks')
      .set('Authorization', `Bearer ${token}`)
      .send({
        task_name: 'update jitter',
        command_payload: { command: 'echo update' },
        schedule_type: 'one-time',
        next_execution_time: new Date(Date.now() + 3_600_000).toISOString(),
      });

    expect(created.status).toBe(201);

    const response = await request(app)
      .put(`/api/v1/tasks/${created.body.id}`)
      .set('Authorization', `Bearer ${token}`)
      .send({ jitter_seconds: 20 });

    expect(response.status).toBe(200);
    expect(response.body.jitter_seconds).toBe(20);
    const base = new Date(response.body.next_execution_time).getTime();
    const dispatch = new Date(response.body.dispatch_at).getTime();
    expect(dispatch - base).toBe(stableOffset(response.body.id, 20) * 1_000);

    const events = await pool.query(
      `SELECT event_type FROM outbox WHERE aggregate_id = $1 ORDER BY id`,
      [created.body.id],
    );
    expect(events.rows.map((row) => row.event_type)).toEqual([
      'task.scheduled',
      'task.scheduled',
    ]);
  });

  it('queues a cancellation event instead of publishing a deleted task', async () => {
    const created = await request(app)
      .post('/api/v1/tasks')
      .set('Authorization', `Bearer ${token}`)
      .send({
        task_name: 'cancel event',
        command_payload: { command: 'echo cancel' },
        schedule_type: 'one-time',
        next_execution_time: new Date(Date.now() + 3_600_000).toISOString(),
      });
    expect(created.status).toBe(201);

    const cancelled = await request(app)
      .put(`/api/v1/tasks/${created.body.id}`)
      .set('Authorization', `Bearer ${token}`)
      .send({ status: 'cancelled' });

    expect(cancelled.status).toBe(200);
    expect(cancelled.body.status).toBe('cancelled');

    const events = await pool.query(
      `SELECT event_type, payload FROM outbox
       WHERE aggregate_id = $1 ORDER BY id`,
      [created.body.id],
    );
    expect(events.rows.map((row) => row.event_type)).toEqual([
      'task.scheduled',
      'task.cancelled',
    ]);
    expect(events.rows[1].payload).toEqual({ taskId: created.body.id });
  });

  it('clears cron_expression when changing recurring to one-time', async () => {
    const created = await request(app)
      .post('/api/v1/tasks')
      .set('Authorization', `Bearer ${token}`)
      .send({
        task_name: 'switch to one-time',
        command_payload: { command: 'echo once' },
        schedule_type: 'recurring',
        cron_expression: '* * * * *',
      });
    expect(created.status).toBe(201);

    const nextTime = new Date(Date.now() + 3_600_000).toISOString();
    const response = await request(app)
      .put(`/api/v1/tasks/${created.body.id}`)
      .set('Authorization', `Bearer ${token}`)
      .send({ schedule_type: 'one-time', next_execution_time: nextTime });

    expect(response.status).toBe(200);
    expect(response.body.schedule_type).toBe('one-time');
    expect(response.body.cron_expression).toBeNull();
    expect(response.body.next_execution_time).toBe(nextTime);
    expect(response.body.dispatch_at).toBe(nextTime);
  });

  it('recomputes dispatch_at when a one-time schedule time changes', async () => {
    const created = await request(app)
      .post('/api/v1/tasks')
      .set('Authorization', `Bearer ${token}`)
      .send({
        task_name: 'update time',
        command_payload: { command: 'echo update-time' },
        schedule_type: 'one-time',
        next_execution_time: new Date(Date.now() + 3_600_000).toISOString(),
      });
    expect(created.status).toBe(201);

    const newTime = new Date(Date.now() + 7_200_000).toISOString();
    const response = await request(app)
      .put(`/api/v1/tasks/${created.body.id}`)
      .set('Authorization', `Bearer ${token}`)
      .send({ next_execution_time: newTime });

    expect(response.status).toBe(200);
    expect(response.body.next_execution_time).toBe(newTime);
    expect(response.body.dispatch_at).toBe(newTime);
  });

  it('recomputes dispatch_at when a recurring cron expression changes', async () => {
    const created = await request(app)
      .post('/api/v1/tasks')
      .set('Authorization', `Bearer ${token}`)
      .send({
        task_name: 'update cron',
        command_payload: { command: 'echo update-cron' },
        schedule_type: 'recurring',
        cron_expression: '* * * * *',
        jitter_seconds: 10,
      });
    expect(created.status).toBe(201);

    const response = await request(app)
      .put(`/api/v1/tasks/${created.body.id}`)
      .set('Authorization', `Bearer ${token}`)
      .send({ cron_expression: '*/5 * * * *' });

    expect(response.status).toBe(200);
    const base = new Date(response.body.next_execution_time);
    expect(base.getUTCMinutes() % 5).toBe(0);
    const dispatch = new Date(response.body.dispatch_at).getTime();
    expect(dispatch - base.getTime()).toBe(
      stableOffset(response.body.id, 10) * 1_000,
    );
  });

  it('lists tasks for the authenticated user', async () => {
    const response = await request(app)
      .get('/api/v1/tasks')
      .set('Authorization', `Bearer ${token}`);

    expect(response.status).toBe(200);
    expect(response.body.tasks).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ task_name: 'integration task' }),
      ]),
    );
  });

  it('returns execution and per-attempt history', async () => {
    const created = await request(app)
      .post('/api/v1/tasks')
      .set('Authorization', `Bearer ${token}`)
      .send({
        task_name: `attempt history ${randomUUID()}`,
        command_payload: { command: 'echo history' },
        schedule_type: 'one-time',
        next_execution_time: new Date(Date.now() + 3_600_000).toISOString(),
      });
    expect(created.status).toBe(201);

    const execution = await pool.query(
      `INSERT INTO task_executions
         (task_id, scheduled_for, started_at, completed_at, status, attempt, worker_id)
       VALUES ($1, NOW(), NOW(), NOW(), 'completed', 1, 'worker-history')
       RETURNING id`,
      [created.body.id],
    );
    await pool.query(
      `INSERT INTO task_execution_attempts
         (execution_id, attempt, worker_id, started_at, finished_at, status)
       VALUES ($1, 1, 'worker-history', NOW(), NOW(), 'completed')`,
      [execution.rows[0].id],
    );

    const executions = await request(app)
      .get(`/api/v1/tasks/${created.body.id}/executions`)
      .set('Authorization', `Bearer ${token}`);
    expect(executions.status).toBe(200);
    expect(executions.body.executions[0]).toMatchObject({
      id: execution.rows[0].id,
      status: 'completed',
      attempt: 1,
      worker_id: 'worker-history',
    });

    const attempts = await request(app)
      .get(
        `/api/v1/tasks/${created.body.id}/executions/${execution.rows[0].id}/attempts`,
      )
      .set('Authorization', `Bearer ${token}`);
    expect(attempts.status).toBe(200);
    expect(attempts.body.attempts).toEqual([
      expect.objectContaining({
        attempt: 1,
        worker_id: 'worker-history',
        status: 'completed',
      }),
    ]);
  });
});
