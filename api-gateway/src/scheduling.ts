import { createHash } from 'node:crypto';
import parser from 'cron-parser';

/**
 * Scheduling helpers shared by the API write path and the coordinator.
 *
 * The offset is derived from the first four bytes of SHA-256(task_id). This is
 * intentionally identical to the Python implementation in
 * coordinator/internal/scheduling.py; keep both implementations in sync.
 */

export function stableOffset(taskId: string, windowSec: number): number {
  if (windowSec <= 0) {
    return 0;
  }
  const digest = createHash('sha256').update(taskId).digest();
  return digest.readUInt32BE(0) % (windowSec + 1);
}

export function computeDispatchAt(
  base: Date,
  taskId: string,
  jitterSeconds: number,
  periodSeconds?: number | null,
): Date {
  if (!Number.isInteger(jitterSeconds) || jitterSeconds <= 0) {
    return new Date(base.getTime());
  }

  let window = jitterSeconds;
  if (periodSeconds !== undefined && periodSeconds !== null && periodSeconds > 1) {
    window = Math.min(jitterSeconds, periodSeconds - 1);
  }
  if (window <= 0) {
    return new Date(base.getTime());
  }

  return new Date(base.getTime() + stableOffset(taskId, window) * 1000);
}

export function parseCronNext(cronExpression: string, currentDate: Date = new Date()): Date {
  return parser
    .parseExpression(cronExpression, { currentDate, tz: 'UTC' })
    .next()
    .toDate();
}

export function isValidCronExpression(cronExpression: string): boolean {
  try {
    parseCronNext(cronExpression);
    return true;
  } catch {
    return false;
  }
}

export function cronPeriodSeconds(
  cronExpression: string,
  base: Date = new Date(),
): number {
  const first = parseCronNext(cronExpression, base);
  const second = parseCronNext(cronExpression, first);
  return Math.max(1, Math.floor((second.getTime() - first.getTime()) / 1000));
}
