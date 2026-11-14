import { describe, expect, it } from 'vitest';
import {
  canonicalizeJson,
  hashRequestBody,
  normalizeIdempotencyKey,
} from '../src/idempotency';

describe('idempotency helpers', () => {
  it('canonicalizes object keys recursively while preserving arrays', () => {
    expect(
      canonicalizeJson({
        z: 1,
        a: { y: 2, b: [3, { d: 4, c: 5 }] },
      }),
    ).toEqual({
      a: { b: [3, { c: 5, d: 4 }], y: 2 },
      z: 1,
    });
  });

  it('hashes equivalent request bodies identically', () => {
    const first = hashRequestBody({
      task_name: 'example',
      command_payload: { command: 'echo hello', args: ['a', 'b'] },
    });
    const second = hashRequestBody({
      command_payload: { args: ['a', 'b'], command: 'echo hello' },
      task_name: 'example',
    });

    expect(first).toBe(second);
    expect(first).toMatch(/^[a-f0-9]{64}$/);
  });

  it('normalizes and validates idempotency keys', () => {
    expect(normalizeIdempotencyKey('  key-1  ')).toBe('key-1');
    expect(normalizeIdempotencyKey(undefined)).toBeUndefined();
    expect(() => normalizeIdempotencyKey('   ')).toThrow(/empty/);
    expect(() => normalizeIdempotencyKey('x'.repeat(256))).toThrow(
      /at most 255/,
    );
  });
});
