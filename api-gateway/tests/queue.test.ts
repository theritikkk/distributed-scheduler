import { beforeEach, describe, expect, it, vi } from 'vitest';

const amqp = vi.hoisted(() => ({ connect: vi.fn() }));

vi.mock('amqplib', () => ({ connect: amqp.connect }));

import {
  connectQueue,
  getTaskQueueDepth,
  publishScheduleWakeup,
} from '../src/queue';

function makeChannel() {
  return {
    assertExchange: vi.fn().mockResolvedValue(undefined),
    assertQueue: vi.fn().mockResolvedValue(undefined),
    bindQueue: vi.fn().mockResolvedValue(undefined),
    checkQueue: vi.fn().mockResolvedValue({ messageCount: 3 }),
    close: vi.fn().mockResolvedValue(undefined),
    on: vi.fn(),
    sendToQueue: vi.fn((_queue, _body, _options, callback) => callback(null)),
  };
}

function makeConnection() {
  const confirmChannel = makeChannel();
  const depthChannel = makeChannel();
  return {
    confirmChannel,
    createConfirmChannel: vi.fn(async () => confirmChannel),
    createChannel: vi.fn(async () => depthChannel),
    close: vi.fn(),
    on: vi.fn(),
  };
}

describe('API queue connection', () => {
  beforeEach(() => {
    amqp.connect.mockReset();
  });

  it('reconnects on demand and restores the depth monitor', async () => {
    const first = makeConnection();
    const second = makeConnection();
    const third = makeConnection();
    amqp.connect
      .mockResolvedValueOnce(first)
      .mockResolvedValueOnce(second)
      .mockResolvedValueOnce(third);

    await connectQueue();
    expect(amqp.connect).toHaveBeenCalledTimes(1);
    expect(getTaskQueueDepth().depth).toBe(3);

    const closeHandler = first.on.mock.calls.find(
      ([event]) => event === 'close',
    )?.[1] as () => void;
    closeHandler();
    expect(getTaskQueueDepth().depth).toBeNull();

    await expect(
      publishScheduleWakeup('task-1', new Date(Date.now() + 60_000)),
    ).resolves.toBe(true);
    expect(amqp.connect).toHaveBeenCalledTimes(2);

    const channelCloseHandler = second.confirmChannel.on.mock.calls.find(
      ([event]) => event === 'close',
    )?.[1] as () => void;
    channelCloseHandler();
    expect(getTaskQueueDepth().depth).toBeNull();

    await expect(
      publishScheduleWakeup('task-2', new Date(Date.now() + 60_000)),
    ).resolves.toBe(true);
    expect(amqp.connect).toHaveBeenCalledTimes(3);
    expect(getTaskQueueDepth().depth).toBe(3);
  });

  it('declares quorum queue TTLs and delivery limits', async () => {
    vi.resetModules();
    vi.stubEnv('RABBITMQ_QUEUE_TYPE', 'quorum');
    vi.stubEnv('OUTBOX_ENABLED', 'true');

    const connection = makeConnection();
    amqp.connect.mockResolvedValueOnce(connection);
    const queue = await import('../src/queue');

    try {
      await queue.connectQueue();
      const declared = Object.fromEntries(
        connection.confirmChannel.assertQueue.mock.calls.map(
          ([name, options]: [string, { arguments?: Record<string, unknown> }]) => [
            name,
            options.arguments ?? {},
          ],
        ),
      );

      expect(declared['scheduler.delay']).toBeUndefined();
      expect(declared['scheduler.retry_delay']).toBeUndefined();
      expect(declared['scheduler.due']).toMatchObject({
        'x-queue-type': 'quorum',
        'x-message-ttl': 3_600_000,
        'x-delivery-limit': 10,
      });
      expect(declared['scheduler.tasks']).toMatchObject({
        'x-queue-type': 'quorum',
        'x-delivery-limit': 20,
        'x-dead-letter-exchange': 'scheduler.dlx',
      });
      expect(declared['scheduler.tasks']).not.toHaveProperty('x-message-ttl');
      expect(declared['scheduler.tasks.dlq']).toMatchObject({
        'x-queue-type': 'quorum',
        'x-message-ttl': 1_209_600_000,
      });
      expect(declared['scheduler.results']).toMatchObject({
        'x-queue-type': 'quorum',
        'x-message-ttl': 604_800_000,
        'x-delivery-limit': 20,
      });
    } finally {
      vi.unstubAllEnvs();
      vi.resetModules();
    }
  });
});
