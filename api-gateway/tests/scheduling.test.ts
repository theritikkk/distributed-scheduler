import { describe, expect, it } from 'vitest';
import {
  computeDispatchAt,
  cronPeriodSeconds,
  isValidCronExpression,
  parseCronNext,
  stableOffset,
} from '../src/scheduling';

const TASK_ID = '00000000-0000-4000-8000-000000000001';

describe('scheduling', () => {
  it('uses a stable SHA-256 offset within the window', () => {
    expect(stableOffset(TASK_ID, 0)).toBe(0);
    expect(stableOffset(TASK_ID, 10)).toBe(7);
    expect(stableOffset(TASK_ID, 30)).toBe(19);
    expect(stableOffset(TASK_ID, 59)).toBe(8);
  });

  it('adds the stable offset to the base schedule', () => {
    const base = new Date('2026-01-01T00:00:00.000Z');

    expect(computeDispatchAt(base, TASK_ID, 0).toISOString()).toBe(
      '2026-01-01T00:00:00.000Z',
    );
    expect(computeDispatchAt(base, TASK_ID, 30).toISOString()).toBe(
      '2026-01-01T00:00:19.000Z',
    );
  });

  it('caps jitter so a run cannot pass the next cron slot', () => {
    const base = new Date('2026-01-01T00:00:00.000Z');
    const dispatchAt = computeDispatchAt(base, TASK_ID, 300, 60);

    expect(dispatchAt.getTime() - base.getTime()).toBe(8_000);
  });

  it('validates cron expressions and computes their period', () => {
    const base = new Date('2026-01-01T00:00:30.000Z');

    expect(isValidCronExpression('* * * * *')).toBe(true);
    expect(isValidCronExpression('not a cron expression')).toBe(false);
    expect(parseCronNext('* * * * *', base).toISOString()).toBe(
      '2026-01-01T00:01:00.000Z',
    );
    expect(cronPeriodSeconds('*/5 * * * *', base)).toBe(300);
  });
});
