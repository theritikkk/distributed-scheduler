import { createHash } from 'node:crypto';

export const IDEMPOTENCY_KEY_MAX_LENGTH = 255;

/**
 * Convert a JSON-compatible value into a canonical form: object keys sorted
 * recursively, array order preserved. This makes the request hash independent
 * of the order in which clients serialize JSON fields.
 */
export function canonicalizeJson(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(canonicalizeJson);
  }

  if (value !== null && typeof value === 'object') {
    return Object.keys(value as Record<string, unknown>)
      .sort()
      .reduce<Record<string, unknown>>((result, key) => {
        result[key] = canonicalizeJson((value as Record<string, unknown>)[key]);
        return result;
      }, {});
  }

  return value;
}

export function hashRequestBody(body: unknown): string {
  return createHash('sha256')
    .update(JSON.stringify(canonicalizeJson(body)))
    .digest('hex');
}

export function normalizeIdempotencyKey(
  raw: string | undefined,
): string | undefined {
  if (raw === undefined) {
    return undefined;
  }

  const key = raw.trim();
  if (key.length === 0) {
    throw new Error('Idempotency-Key must not be empty');
  }
  if (key.length > IDEMPOTENCY_KEY_MAX_LENGTH) {
    throw new Error(
      `Idempotency-Key must be at most ${IDEMPOTENCY_KEY_MAX_LENGTH} characters`,
    );
  }
  return key;
}
