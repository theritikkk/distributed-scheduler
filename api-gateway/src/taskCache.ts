import { cache } from './cache';
import { config } from './config';

export const TASK_VERSION_TTL_SEC = Math.max(300, config.cacheTtlSec * 4);

export function taskCacheKey(userId: string, taskId: string): string {
  return `${userId}:${taskId}`;
}

export function executionCacheKey(
  userId: string,
  taskId: string,
  version: string,
  page: number,
  limit: number,
): string {
  return `${userId}:${taskId}:${version}:${page}:${limit}`;
}

export async function getCachedTask<T>(
  userId: string,
  taskId: string,
): Promise<T | null> {
  if (!config.cacheEnabled || cache === null) {
    return null;
  }
  return cache.getJson<T>('task', taskCacheKey(userId, taskId));
}

export async function setCachedTask(
  userId: string,
  task: { id?: unknown },
): Promise<void> {
  if (!config.cacheEnabled || cache === null || task.id === undefined) {
    return;
  }
  await cache.setJson(
    'task',
    taskCacheKey(userId, String(task.id)),
    task,
    config.cacheTtlSec,
  );
}

/**
 * Invalidate a cached task and advance the execution-history generation so
 * every previously cached page key becomes unreachable immediately.
 */
export async function invalidateTaskCache(
  userId: string,
  taskId: string,
): Promise<void> {
  if (!config.cacheEnabled || cache === null) {
    return;
  }
  await Promise.all([
    cache.delete('task', taskCacheKey(userId, taskId)),
    cache.increment(
      'task-version',
      taskCacheKey(userId, taskId),
      TASK_VERSION_TTL_SEC,
    ),
  ]);
}

export async function getTaskCacheVersion(
  userId: string,
  taskId: string,
): Promise<string> {
  if (!config.cacheEnabled || cache === null) {
    return '0';
  }
  return (
    (await cache.getString('task-version', taskCacheKey(userId, taskId))) ??
    '0'
  );
}

export async function getCachedExecutions<T>(
  userId: string,
  taskId: string,
  version: string,
  page: number,
  limit: number,
): Promise<T | null> {
  if (!config.cacheEnabled || cache === null) {
    return null;
  }
  return cache.getJson<T>(
    'executions',
    executionCacheKey(userId, taskId, version, page, limit),
  );
}

export async function setCachedExecutions(
  userId: string,
  taskId: string,
  version: string,
  page: number,
  limit: number,
  value: unknown,
): Promise<void> {
  if (!config.cacheEnabled || cache === null) {
    return;
  }
  await cache.setJson(
    'executions',
    executionCacheKey(userId, taskId, version, page, limit),
    value,
    config.cacheTtlSec,
  );
}
