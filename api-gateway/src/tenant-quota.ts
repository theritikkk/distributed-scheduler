import type { Pool } from 'pg';
import { cache } from './cache';
import { config } from './config';
import { tenantQuotaLookupErrors } from './rate-limit/metrics';

export type TenantQuotaSource = 'redis' | 'database' | 'local' | 'default';

export interface TenantQuotaRow {
  weight: number | string;
  tasks_per_min: number | string;
  burst: number | string;
  enabled: boolean;
}

export interface TenantQuota {
  userId: string;
  weight: number;
  baseTasksPerMin: number;
  baseBurst: number;
  /** tasks_per_min * weight, applied to the token bucket. */
  effectiveTasksPerMin: number;
  /** burst * weight, applied to the token bucket. */
  effectiveBurst: number;
  source: TenantQuotaSource;
}

interface LocalQuotaEntry {
  quota: TenantQuota;
  expiresAtMs: number;
}

interface CachedQuotaRow {
  row: TenantQuotaRow | null;
}

const LOCAL_CACHE_MAX_ENTRIES = 10_000;
const localCache = new Map<string, LocalQuotaEntry>();

function clamp(value: number, minimum: number, maximum: number): number {
  return Math.min(maximum, Math.max(minimum, value));
}

function numeric(value: unknown, fallback: number): number {
  const parsed = typeof value === 'number' ? value : Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

export function buildTenantQuota(
  userId: string,
  row: TenantQuotaRow | null,
  source: TenantQuotaSource,
): TenantQuota {
  const useRow = row !== null && row.enabled;
  const weight = clamp(
    useRow ? numeric(row.weight, config.tenantDefaultWeight) : config.tenantDefaultWeight,
    0.1,
    100,
  );
  const baseTasksPerMin = Math.max(
    1,
    Math.round(
      useRow
        ? numeric(row.tasks_per_min, config.tenantDefaultTasksPerMin)
        : config.tenantDefaultTasksPerMin,
    ),
  );
  const baseBurst = Math.max(
    1,
    Math.round(
      useRow ? numeric(row.burst, config.tenantDefaultBurst) : config.tenantDefaultBurst,
    ),
  );

  return {
    userId,
    weight: Math.round(weight * 1_000) / 1_000,
    baseTasksPerMin,
    baseBurst,
    effectiveTasksPerMin: Math.max(1, Math.round(baseTasksPerMin * weight)),
    effectiveBurst: Math.max(1, Math.round(baseBurst * weight)),
    source: row !== null && !row.enabled ? 'default' : source,
  };
}

export function defaultTenantQuota(userId: string): TenantQuota {
  return buildTenantQuota(userId, null, 'default');
}

function localTtlMs(): number {
  return Math.max(1, config.tenantQuotaCacheTtlSec) * 1_000;
}

function readLocal(userId: string, nowMs: number): TenantQuota | null {
  const entry = localCache.get(userId);
  if (!entry) {
    return null;
  }
  if (entry.expiresAtMs <= nowMs) {
    localCache.delete(userId);
    return null;
  }
  return entry.quota;
}

function writeLocal(userId: string, quota: TenantQuota, nowMs: number): void {
  if (localCache.size >= LOCAL_CACHE_MAX_ENTRIES && !localCache.has(userId)) {
    const oldest = localCache.keys().next().value;
    if (oldest !== undefined) {
      localCache.delete(oldest);
    }
  }
  localCache.set(userId, {
    quota,
    expiresAtMs: nowMs + localTtlMs(),
  });
}

export function clearTenantQuotaLocalCache(): void {
  localCache.clear();
}

export interface TenantQuotaResolverDeps {
  query?: Pick<Pool, 'query'>;
  now?: () => number;
}

async function queryQuotaRow(
  deps: TenantQuotaResolverDeps,
  userId: string,
): Promise<TenantQuotaRow | null> {
  if (deps.query) {
    const result = await deps.query.query(
      `SELECT weight, tasks_per_min, burst, enabled
       FROM tenant_quotas
       WHERE user_id = $1`,
      [userId],
    );
    return (result.rows[0] as TenantQuotaRow | undefined) ?? null;
  }

  const { pool } = await import('./db');
  const result = await pool.query(
    `SELECT weight, tasks_per_min, burst, enabled
     FROM tenant_quotas
     WHERE user_id = $1`,
    [userId],
  );
  return (result.rows[0] as TenantQuotaRow | undefined) ?? null;
}

/**
 * Resolve a tenant's admission quota. Redis and the process-local map are
 * short-lived read caches; PostgreSQL remains the authority. A lookup failure
 * falls back to the last known value or the configured defaults rather than
 * blocking task creation.
 */
export async function resolveTenantQuota(
  userId: string,
  deps: TenantQuotaResolverDeps = {},
): Promise<TenantQuota> {
  const nowMs = (deps.now ?? Date.now)();
  const local = readLocal(userId, nowMs);
  if (local) {
    return { ...local, source: 'local' };
  }

  if (cache !== null) {
    const cached = await cache.getJson<CachedQuotaRow>('quota', userId);
    if (cached) {
      const quota = buildTenantQuota(userId, cached.row, 'redis');
      writeLocal(userId, quota, nowMs);
      return quota;
    }
  }

  try {
    const row = await queryQuotaRow(deps, userId);
    const quota = buildTenantQuota(userId, row, 'database');
    writeLocal(userId, quota, nowMs);
    if (cache !== null) {
      await cache.setJson(
        'quota',
        userId,
        { row } satisfies CachedQuotaRow,
        config.tenantQuotaCacheTtlSec,
      );
    }
    return quota;
  } catch {
    tenantQuotaLookupErrors.inc();
    const stale = readLocal(userId, nowMs);
    if (stale) {
      return { ...stale, source: 'local' };
    }
    return defaultTenantQuota(userId);
  }
}
