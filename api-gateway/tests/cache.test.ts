import { describe, expect, it } from 'vitest';
import { RedisJsonCache } from '../src/cache';
import type { RedisCommandClient } from '../src/rate-limit/redisTokenBucket';

class FakeRedis {
  readonly store = new Map<string, string>();
  fail = false;
  expirations: Array<{ key: string; ttl: number }> = [];

  private maybeFail(): void {
    if (this.fail) {
      throw new Error('redis unavailable');
    }
  }

  async get(key: string): Promise<string | null> {
    this.maybeFail();
    return this.store.get(key) ?? null;
  }

  async set(
    key: string,
    value: string,
    _mode: string,
    ttl: number,
  ): Promise<'OK'> {
    this.maybeFail();
    this.store.set(key, value);
    this.expirations.push({ key, ttl });
    return 'OK';
  }

  async del(key: string): Promise<number> {
    this.maybeFail();
    return this.store.delete(key) ? 1 : 0;
  }

  async incr(key: string): Promise<number> {
    this.maybeFail();
    const next = Number(this.store.get(key) ?? '0') + 1;
    this.store.set(key, String(next));
    return next;
  }

  async expire(key: string, ttl: number): Promise<number> {
    this.maybeFail();
    this.expirations.push({ key, ttl });
    return 1;
  }
}

function cacheWith(redis: FakeRedis): RedisJsonCache {
  return new RedisJsonCache(
    redis as unknown as RedisCommandClient,
    50,
    'test:cache',
  );
}

describe('RedisJsonCache', () => {
  it('round-trips JSON values with a namespace prefix and TTL', async () => {
    const redis = new FakeRedis();
    const cache = cacheWith(redis);

    await cache.setJson('task', 'user:task', { id: 'task-1' }, 15);
    expect(redis.store.get('test:cache:task:user:task')).toBe(
      '{"id":"task-1"}',
    );
    expect(redis.expirations[0]?.ttl).toBe(15);

    await expect(cache.getJson('task', 'user:task')).resolves.toEqual({
      id: 'task-1',
    });
  });

  it('returns null on a miss', async () => {
    const cache = cacheWith(new FakeRedis());
    await expect(cache.getJson('task', 'missing')).resolves.toBeNull();
    await expect(cache.getString('task', 'missing')).resolves.toBeNull();
  });

  it('fails open when Redis errors', async () => {
    const redis = new FakeRedis();
    const cache = cacheWith(redis);
    redis.fail = true;

    await expect(cache.getJson('task', 'key')).resolves.toBeNull();
    await expect(cache.getString('task', 'key')).resolves.toBeNull();
    await expect(
      cache.setJson('task', 'key', { id: 1 }, 10),
    ).resolves.toBeUndefined();
    await expect(cache.delete('task', 'key')).resolves.toBeUndefined();
    await expect(cache.increment('task-version', 'key', 10)).resolves.toBeNull();
  });

  it('returns null for malformed JSON instead of throwing', async () => {
    const redis = new FakeRedis();
    const cache = cacheWith(redis);
    redis.store.set('test:cache:task:broken', '{not-json');

    await expect(cache.getJson('task', 'broken')).resolves.toBeNull();
  });

  it('increments a version key and refreshes its TTL', async () => {
    const redis = new FakeRedis();
    const cache = cacheWith(redis);

    await expect(cache.increment('task-version', 'user:task', 300)).resolves.toBe(1);
    await expect(cache.increment('task-version', 'user:task', 300)).resolves.toBe(2);
    expect(redis.store.get('test:cache:task-version:user:task')).toBe('2');
    expect(redis.expirations.at(-1)).toEqual({
      key: 'test:cache:task-version:user:task',
      ttl: 300,
    });
  });
});
