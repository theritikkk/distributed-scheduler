import { createHash } from 'node:crypto';
import { cache } from './cache';
import { config } from './config';

export interface CachedIdempotentResponse {
  requestHash: string;
  status: number;
  body: unknown;
}

export function idempotencyCacheKey(
  userId: string,
  idempotencyKey: string,
): string {
  const hashed = createHash('sha256')
    .update(idempotencyKey)
    .digest('hex')
    .slice(0, 32);
  return `${userId}:${hashed}`;
}

export async function getCachedIdempotentResponse(
  userId: string,
  idempotencyKey: string,
): Promise<CachedIdempotentResponse | null> {
  if (!config.idempotencyCacheEnabled || cache === null) {
    return null;
  }
  return cache.getJson<CachedIdempotentResponse>(
    'idempotency',
    idempotencyCacheKey(userId, idempotencyKey),
  );
}

export async function setCachedIdempotentResponse(
  userId: string,
  idempotencyKey: string,
  response: CachedIdempotentResponse,
): Promise<void> {
  if (!config.idempotencyCacheEnabled || cache === null) {
    return;
  }
  await cache.setJson(
    'idempotency',
    idempotencyCacheKey(userId, idempotencyKey),
    response,
    config.idempotencyCacheTtlSec,
  );
}
