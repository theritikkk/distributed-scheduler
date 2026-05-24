import * as amqplib from "amqplib";

const DELAY_QUEUE = 'scheduler.delay';
const DUE_QUEUE = 'scheduler.due';
const TASK_QUEUE = 'scheduler.tasks';
const RESULT_QUEUE = 'scheduler.results';
const RETRY_DELAY_QUEUE = 'scheduler.retry_delay';
const DLX_EXCHANGE = 'scheduler.dlx';
const TASK_DLQ = 'scheduler.tasks.dlq';

// Use a loose type for the underlying connection to accommodate differing
// amqplib/transport implementations (e.g. ChannelModel) that may not
// strictly satisfy the Connection interface used in types.
let _connection: any = null;
let channel: amqplib.Channel | null = null;

async function declareTopology(ch: amqplib.Channel): Promise<void> {
  await ch.assertExchange(DLX_EXCHANGE, 'direct', { durable: true });

  await ch.assertQueue(DELAY_QUEUE, {
    durable: true,
    arguments: {
      'x-dead-letter-exchange': '',
      'x-dead-letter-routing-key': DUE_QUEUE,
    },
  });

  await ch.assertQueue(DUE_QUEUE, { durable: true });

  await ch.assertQueue(TASK_QUEUE, {
    durable: true,
    arguments: {
      'x-dead-letter-exchange': DLX_EXCHANGE,
      'x-dead-letter-routing-key': TASK_DLQ,
    },
  });

  await ch.assertQueue(TASK_DLQ, { durable: true });
  await ch.bindQueue(TASK_DLQ, DLX_EXCHANGE, TASK_DLQ);

  await ch.assertQueue(RETRY_DELAY_QUEUE, {
    durable: true,
    arguments: {
      'x-dead-letter-exchange': '',
      'x-dead-letter-routing-key': TASK_QUEUE,
    },
  });

  await ch.assertQueue(RESULT_QUEUE, { durable: true });
}

const MAX_DELAY_MS = 2_147_483_647;

function delayMsUntil(nextExecutionTime: string | Date): number {
  const t =
    typeof nextExecutionTime === 'string'
      ? new Date(nextExecutionTime).getTime()
      : nextExecutionTime.getTime();
  const ms = Math.max(0, t - Date.now());
  return Math.min(ms, MAX_DELAY_MS);
}

export async function connectQueue(): Promise<void> {
  const url = process.env.RABBITMQ_URL || 'amqp://scheduler:scheduler_secret@localhost:5672';
  const conn = await amqplib.connect(url);
  _connection = conn;
  channel = await conn.createChannel();
  await declareTopology(channel);
}

/**
 * Schedule a DB wakeup: TTL in scheduler.delay -> dead-letter to scheduler.due, or immediate to due queue.
 */
export async function publishScheduleWakeup(
  taskId: string,
  nextExecutionTime: string | Date
): Promise<boolean> {
  if (!channel) {
    return false;
  }
  const body = Buffer.from(JSON.stringify({ taskId }));
  const delayMs = delayMsUntil(nextExecutionTime);

  if (delayMs <= 0) {
    return channel.sendToQueue(DUE_QUEUE, body, {
      persistent: true,
      contentType: 'application/json',
    });
  }

  return channel.sendToQueue(DELAY_QUEUE, body, {
    persistent: true,
    contentType: 'application/json',
    expiration: String(delayMs),
  });
}

/** @deprecated Use publishScheduleWakeup(taskId, nextExecutionTime) */
export async function publishTask(taskId: string): Promise<boolean> {
  if (!channel) {
    return false;
  }
  return channel.sendToQueue(DUE_QUEUE, Buffer.from(JSON.stringify({ taskId })), {
    persistent: true,
    contentType: 'application/json',
  });
}
