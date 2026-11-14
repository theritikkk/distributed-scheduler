import { describe, expect, it } from 'vitest';
import {
  buildTenantQuota,
  clearTenantQuotaLocalCache,
  defaultTenantQuota,
  resolveTenantQuota,
} from '../src/tenant-quota';

describe('buildTenantQuota', () => {
  it('multiplies the configured rate and burst by tenant weight', () => {
    const quota = buildTenantQuota(
      'user-1',
      { weight: '2.5', tasks_per_min: 40, burst: 8, enabled: true },
      'database',
    );

    expect(quota.weight).toBe(2.5);
    expect(quota.baseTasksPerMin).toBe(40);
    expect(quota.baseBurst).toBe(8);
    expect(quota.effectiveTasksPerMin).toBe(100);
    expect(quota.effectiveBurst).toBe(20);
    expect(quota.source).toBe('database');
  });

  it('rounds fractional weighted rates up to the minimum of one', () => {
    const quota = buildTenantQuota(
      'user-1',
      { weight: '0.1', tasks_per_min: 1, burst: 1, enabled: true },
      'database',
    );

    expect(quota.effectiveTasksPerMin).toBe(1);
    expect(quota.effectiveBurst).toBe(1);
  });

  it('uses configured defaults for a disabled tenant row', () => {
    const quota = buildTenantQuota(
      'user-1',
      { weight: '9', tasks_per_min: 999, burst: 99, enabled: false },
      'database',
    );

    // Defaults come from the shared application config (weight 1, 60/min, 10).
    expect(quota.weight).toBe(1);
    expect(quota.baseTasksPerMin).toBe(60);
    expect(quota.baseBurst).toBe(10);
    expect(quota.effectiveTasksPerMin).toBe(60);
    expect(quota.effectiveBurst).toBe(10);
    expect(quota.source).toBe('default');
  });

  it('clamps out-of-range stored weights', () => {
    const quota = buildTenantQuota(
      'user-1',
      { weight: '100000', tasks_per_min: 60, burst: 10, enabled: true },
      'database',
    );
    expect(quota.weight).toBe(100);
    expect(quota.effectiveTasksPerMin).toBe(6_000);
  });
});

describe('resolveTenantQuota', () => {
  it('reads the authoritative database row and serves later calls from the local cache', async () => {
    clearTenantQuotaLocalCache();
    let queries = 0;
    const query = {
      query: async () => {
        queries += 1;
        return {
          rows: [
            { weight: '2', tasks_per_min: 50, burst: 10, enabled: true },
          ],
        };
      },
    };

    const first = await resolveTenantQuota('user-db', { query });
    expect(first.source).toBe('database');
    expect(first.effectiveTasksPerMin).toBe(100);

    const second = await resolveTenantQuota('user-db', { query });
    expect(second.source).toBe('local');
    expect(second.effectiveTasksPerMin).toBe(100);
    expect(queries).toBe(1);
  });

  it('falls back to defaults when the database lookup fails', async () => {
    clearTenantQuotaLocalCache();
    const query = {
      query: async () => {
        throw new Error('database unavailable');
      },
    };

    const quota = await resolveTenantQuota('user-failure', { query });
    expect(quota.source).toBe('default');
    expect(quota.effectiveTasksPerMin).toBeGreaterThanOrEqual(1);
  });

  it('builds defaults without a row', () => {
    const quota = defaultTenantQuota('user-default');
    expect(quota.source).toBe('default');
    expect(quota.userId).toBe('user-default');
    expect(quota.effectiveBurst).toBeGreaterThanOrEqual(1);
  });
});
