import { NextFunction, Request, Response } from 'express';
import { config } from '../config';
import { pool } from '../db';
import { getTaskQueueDepth } from '../queue';
import {
  admissionRejected,
  dbPoolUtilization,
  dbPoolWaiting,
} from './metrics';

const QUEUE_DEPTH_MAX_AGE_MS = 15_000;

// Latch queue congestion so admission does not flap between the high and low
// watermarks. Each API replica converges when it observes depth at/below low.
let taskQueueBackpressured = false;

/**
 * Keep requests out of the database and broker when either is already under
 * pressure. Rejections are explicit 503/429 responses with Retry-After rather
 * than allowing an overloaded dependency to time out every request.
 */
export function admissionMiddleware(
  _req: Request,
  res: Response,
  next: NextFunction,
): void {
  const utilization = pool.totalCount / Math.max(1, config.dbPoolMax);
  dbPoolUtilization.set(utilization);
  dbPoolWaiting.set(pool.waitingCount);

  if (!config.admissionEnabled) {
    next();
    return;
  }

  if (
    utilization >= config.dbPoolHighWatermark ||
    pool.waitingCount > config.dbPoolMaxWaiters
  ) {
    admissionRejected.inc({ reason: 'db_pool' });
    res.set('Retry-After', '1');
    res.status(503).json({
      error: 'Database capacity temporarily unavailable',
      reason: 'db_pool',
    });
    return;
  }

  const queue = getTaskQueueDepth();
  const queueSampleFresh =
    queue.depth !== null &&
    Date.now() - queue.observedAtMs <= QUEUE_DEPTH_MAX_AGE_MS;

  if (queueSampleFresh) {
    if (taskQueueBackpressured) {
      if (queue.depth! <= config.queueBacklogLow) {
        taskQueueBackpressured = false;
      }
    } else if (queue.depth! >= config.queueBacklogHigh) {
      taskQueueBackpressured = true;
    }
  }

  if (queueSampleFresh && taskQueueBackpressured) {
    admissionRejected.inc({ reason: 'queue_backlog' });
    res.set('Retry-After', '2');
    res.status(429).json({
      error: 'Task queue is backpressured',
      reason: 'queue_backlog',
    });
    return;
  }

  next();
}
