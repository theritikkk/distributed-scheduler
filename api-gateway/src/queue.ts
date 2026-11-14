import * as amqplib from "amqplib";
import { config } from './config';
import { currentTraceHeaders } from './tracing';

const DELAY_QUEUE = 'scheduler.delay';
const DUE_QUEUE = 'scheduler.due';
const TASK_QUEUE = 'scheduler.tasks';
const RESULT_QUEUE = 'scheduler.results';
const RETRY_DELAY_QUEUE = 'scheduler.retry_delay';
const DLX_EXCHANGE = 'scheduler.dlx';
const TASK_DLQ = 'scheduler.tasks.dlq';

// Keep these in sync with coordinator/internal/queue/topology.py and
// worker/rabbit_topology.py. Quorum queue arguments are immutable: changing
// them requires draining and recreating the queues.
const QUORUM_TASK_DELIVERY_LIMIT = 20;
const QUORUM_DUE_MESSAGE_TTL_MS = 3_600_000;
const QUORUM_DUE_DELIVERY_LIMIT = 10;
const QUORUM_RESULT_MESSAGE_TTL_MS = 604_800_000;
const QUORUM_RESULT_DELIVERY_LIMIT = 20;
const QUORUM_DLQ_MESSAGE_TTL_MS = 1_209_600_000;

// Use a loose type for the underlying connection to accommodate differing
// amqplib/transport implementations (e.g. ChannelModel) that may not
// strictly satisfy the Connection interface used in types.
let _connection: any = null;
let channel: amqplib.ConfirmChannel | null = null;
let _depthChannel: amqplib.Channel | null = null;
let _depthTimer: NodeJS.Timeout | null = null;
let _connectPromise: Promise<void> | null = null;
let _reconnectTimer: NodeJS.Timeout | null = null;
let _reconnectDelayMs = 1_000;
const MAX_RECONNECT_DELAY_MS = 30_000;
const PUBLISH_CONNECT_TIMEOUT_MS = 5_000;

export interface QueueDepthSnapshot {
  depth: number | null;
  observedAtMs: number;
}

let queueDepthSnapshot: QueueDepthSnapshot = {
  depth: null,
  observedAtMs: 0,
};

export function getTaskQueueDepth(): QueueDepthSnapshot {
  return queueDepthSnapshot;
}

async function pollTaskQueueDepth(): Promise<void> {
  if (!_depthChannel) return;
  try {
    const info = await _depthChannel.checkQueue(TASK_QUEUE);
    queueDepthSnapshot = {
      depth: info.messageCount,
      observedAtMs: Date.now(),
    };
  } catch {
    queueDepthSnapshot = { depth: null, observedAtMs: 0 };
  }
}

async function startQueueDepthMonitor(conn: any): Promise<void> {
  if (_depthChannel) return;
  try {
    const depthChannel = await conn.createChannel();
    _depthChannel = depthChannel;
    depthChannel.on('close', () => {
      if (_depthChannel === depthChannel) {
        queueDepthSnapshot = { depth: null, observedAtMs: 0 };
        _depthChannel = null;
      }
    });
    await pollTaskQueueDepth();
    if (!_depthTimer) {
      _depthTimer = setInterval(() => {
        void pollTaskQueueDepth();
      }, 5_000);
      _depthTimer.unref();
    }
  } catch (error) {
    queueDepthSnapshot = { depth: null, observedAtMs: 0 };
    _depthChannel = null;
    console.warn('Queue-depth monitor failed:', (error as Error).message);
  }
}

function scheduleReconnect(): void {
  if (_reconnectTimer) return;
  const delay = _reconnectDelayMs;
  _reconnectDelayMs = Math.min(
    MAX_RECONNECT_DELAY_MS,
    _reconnectDelayMs * 2,
  );
  console.warn(`RabbitMQ connection lost; reconnecting in ${delay}ms`);
  _reconnectTimer = setTimeout(() => {
    _reconnectTimer = null;
    void connectQueue().catch((error: Error) => {
      console.warn('RabbitMQ reconnect failed:', error.message);
    });
  }, delay);
  _reconnectTimer.unref();
}

async function declareTopology(ch: amqplib.Channel): Promise<void> {
  const queueType = config.rabbitmqQueueType;
  const queueArgs = (
    extra: Record<string, unknown> = {},
    quorumExtra: Record<string, unknown> = {},
  ) => ({
    durable: true,
    arguments:
      queueType === 'quorum'
        ? { ...extra, ...quorumExtra, 'x-queue-type': 'quorum' }
        : extra,
  });

  await ch.assertExchange(DLX_EXCHANGE, 'direct', { durable: true });

  if (queueType === 'classic') {
    await ch.assertQueue(DELAY_QUEUE, {
      durable: true,
      arguments: {
        'x-dead-letter-exchange': '',
        'x-dead-letter-routing-key': DUE_QUEUE,
      },
    });
  }

  await ch.assertQueue(
    DUE_QUEUE,
    queueArgs(
      {},
      {
        'x-message-ttl': QUORUM_DUE_MESSAGE_TTL_MS,
        'x-delivery-limit': QUORUM_DUE_DELIVERY_LIMIT,
      },
    ),
  );

  await ch.assertQueue(
    TASK_QUEUE,
    queueArgs(
      {
        'x-dead-letter-exchange': DLX_EXCHANGE,
        'x-dead-letter-routing-key': TASK_DLQ,
      },
      {
        'x-delivery-limit': QUORUM_TASK_DELIVERY_LIMIT,
      },
    ),
  );

  await ch.assertQueue(
    TASK_DLQ,
    queueArgs(
      {},
      {
        'x-message-ttl': QUORUM_DLQ_MESSAGE_TTL_MS,
      },
    ),
  );
  await ch.bindQueue(TASK_DLQ, DLX_EXCHANGE, TASK_DLQ);

  if (queueType === 'classic') {
    await ch.assertQueue(RETRY_DELAY_QUEUE, {
      durable: true,
      arguments: {
        'x-dead-letter-exchange': '',
        'x-dead-letter-routing-key': TASK_QUEUE,
      },
    });
  }

  await ch.assertQueue(
    RESULT_QUEUE,
    queueArgs(
      {},
      {
        'x-message-ttl': QUORUM_RESULT_MESSAGE_TTL_MS,
        'x-delivery-limit': QUORUM_RESULT_DELIVERY_LIMIT,
      },
    ),
  );
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

async function connectQueueOnce(): Promise<void> {
  let conn: any = null;
  try {
    conn = await amqplib.connect(config.rabbitmqUrl);
    const confirmChannel = await conn.createConfirmChannel();
    await declareTopology(confirmChannel);

    _connection = conn;
    channel = confirmChannel;

    confirmChannel.on('error', (error: Error) => {
      console.warn('RabbitMQ channel error:', error.message);
    });
    confirmChannel.on('close', () => {
      if (channel !== confirmChannel) return;
      channel = null;
      if (_connection === conn) {
        _connection = null;
        _depthChannel = null;
        queueDepthSnapshot = { depth: null, observedAtMs: 0 };
        try {
          conn.close();
        } catch {
          // Already closed.
        }
      }
      scheduleReconnect();
    });

    conn.on('error', (error: Error) => {
      console.warn('RabbitMQ connection error:', error.message);
    });
    conn.on('close', () => {
      if (_connection !== conn) return;
      _connection = null;
      channel = null;
      _depthChannel = null;
      queueDepthSnapshot = { depth: null, observedAtMs: 0 };
      scheduleReconnect();
    });

    await startQueueDepthMonitor(conn);
    _reconnectDelayMs = 1_000;
    if (_reconnectTimer) {
      clearTimeout(_reconnectTimer);
      _reconnectTimer = null;
    }
    console.log('RabbitMQ connected');
  } catch (error) {
    channel = null;
    _connection = null;
    if (conn) {
      try {
        conn.close();
      } catch {
        // Already closed.
      }
    }
    scheduleReconnect();
    throw error;
  }
}

export async function connectQueue(): Promise<void> {
  if (channel) return;
  if (_connectPromise) return _connectPromise;
  _connectPromise = connectQueueOnce().finally(() => {
    _connectPromise = null;
  });
  return _connectPromise;
}

async function ensureChannel(): Promise<amqplib.ConfirmChannel> {
  if (channel) return channel;

  let timeout: NodeJS.Timeout | null = null;
  try {
    await Promise.race([
      connectQueue(),
      new Promise<never>((_, reject) => {
        timeout = setTimeout(
          () => reject(new Error('RabbitMQ connection timed out')),
          PUBLISH_CONNECT_TIMEOUT_MS,
        );
        timeout.unref();
      }),
    ]);
  } finally {
    if (timeout) clearTimeout(timeout);
  }

  if (!channel) {
    throw new Error('RabbitMQ channel is not available');
  }
  return channel;
}

function sendWithConfirm(
  ch: amqplib.ConfirmChannel,
  queue: string,
  body: Buffer,
  options: amqplib.Options.Publish,
): Promise<boolean> {
  return new Promise((resolve, reject) => {
    ch.sendToQueue(queue, body, options, (error) => {
      if (error) {
        reject(error);
        return;
      }
      resolve(true);
    });
  });
}

/**
 * Schedule a DB wakeup: TTL in scheduler.delay -> dead-letter to scheduler.due,
 * or immediate to the due queue. The fallback path uses publisher confirms.
 */
export async function publishScheduleWakeup(
  taskId: string,
  nextExecutionTime: string | Date
): Promise<boolean> {
  const publishChannel = await ensureChannel();
  const body = Buffer.from(JSON.stringify({ taskId }));
  const delayMs = delayMsUntil(nextExecutionTime);
  const headers = currentTraceHeaders();

  if (delayMs <= 0) {
    return sendWithConfirm(publishChannel, DUE_QUEUE, body, {
      persistent: true,
      contentType: 'application/json',
      headers,
    });
  }

  return sendWithConfirm(publishChannel, DELAY_QUEUE, body, {
    persistent: true,
    contentType: 'application/json',
    expiration: String(delayMs),
    headers,
  });
}

/** @deprecated Use publishScheduleWakeup(taskId, nextExecutionTime) */
export async function publishTask(taskId: string): Promise<boolean> {
  const publishChannel = await ensureChannel();
  return sendWithConfirm(
    publishChannel,
    DUE_QUEUE,
    Buffer.from(JSON.stringify({ taskId })),
    {
      persistent: true,
      contentType: 'application/json',
      headers: currentTraceHeaders(),
    },
  );
}
