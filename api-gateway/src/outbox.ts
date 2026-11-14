import type { PoolClient } from 'pg';
import { currentTraceHeaders } from './tracing';

export interface OutboxEvent {
  aggregateId: string;
  eventType: 'task.scheduled' | 'task.cancelled';
  payload: Record<string, unknown>;
  headers?: Record<string, unknown>;
}

/**
 * Insert an event using the caller's task/idempotency transaction. Keeping this
 * on the same client is what closes the database-to-RabbitMQ dual-write gap.
 *
 * The active W3C trace context is persisted with the row so the trace survives
 * the PostgreSQL and RabbitMQ hops, including failures and retries.
 */
export async function enqueueOutbox(
  client: PoolClient,
  event: OutboxEvent,
): Promise<void> {
  await client.query(
    `INSERT INTO outbox
       (aggregate_type, aggregate_id, event_type, payload, headers)
     VALUES ('task', $1, $2, $3, $4)`,
    [
      event.aggregateId,
      event.eventType,
      JSON.stringify(event.payload),
      JSON.stringify({
        ...currentTraceHeaders(),
        ...(event.headers ?? {}),
      }),
    ],
  );
}

export function taskScheduledEvent(
  taskId: string,
  dispatchAt: string | Date,
  nextExecutionTime: string | Date,
): OutboxEvent {
  return {
    aggregateId: taskId,
    eventType: 'task.scheduled',
    payload: {
      taskId,
      dispatchAt:
        dispatchAt instanceof Date ? dispatchAt.toISOString() : dispatchAt,
      nextExecutionTime:
        nextExecutionTime instanceof Date
          ? nextExecutionTime.toISOString()
          : nextExecutionTime,
    },
  };
}

export function taskCancelledEvent(taskId: string): OutboxEvent {
  return {
    aggregateId: taskId,
    eventType: 'task.cancelled',
    payload: { taskId },
  };
}
