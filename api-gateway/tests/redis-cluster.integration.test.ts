import { Cluster, Redis } from 'ioredis';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import {
  createRedisClusterClient,
  RedisTokenBucket,
} from '../src/rate-limit/redisTokenBucket';

const rawNodes = process.env.REDIS_CLUSTER_NODES?.trim();
const enabled =
  process.env.REDIS_CLUSTER_TEST === '1' && Boolean(rawNodes);
const seedNodes = parseSeeds(rawNodes ?? '');
const suite = enabled ? describe : describe.skip;

function parseSeeds(raw: string): Array<{ host: string; port: number }> {
  return raw
    .split(',')
    .map((entry) => entry.trim())
    .filter(Boolean)
    .map((entry) => {
      const index = entry.lastIndexOf(':');
      return {
        host: entry.slice(0, index),
        port: Number(entry.slice(index + 1)),
      };
    });
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitFor(
  check: () => Promise<boolean>,
  timeoutMs: number,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  let lastError: unknown;
  while (Date.now() < deadline) {
    try {
      if (await check()) {
        return;
      }
    } catch (error) {
      lastError = error;
    }
    await sleep(250);
  }
  throw lastError ?? new Error('condition was not met before the timeout');
}

async function connectCluster(): Promise<Cluster> {
  const cluster = createRedisClusterClient({
    nodes: seedNodes,
    timeoutMs: 2_000,
  });
  await cluster.connect();
  await waitFor(async () => (await cluster.ping()) === 'PONG', 20_000);
  return cluster;
}

async function clusterCommand(
  cluster: Cluster,
  command: string,
  ...args: Array<string | number>
): Promise<any> {
  const node = cluster.nodes('all')[0];
  return (node as unknown as {
    cluster: (...values: Array<string | number>) => Promise<any>;
  }).cluster(command, ...args);
}

function nodeAddress(value: string): { host: string; port: number } {
  const address = value.split('@')[0];
  const index = address.lastIndexOf(':');
  return {
    host: address.slice(0, index),
    port: Number(address.slice(index + 1)),
  };
}

suite('Redis Cluster token bucket', () => {
  let cluster: Cluster;
  const prefix = `rl:cluster-test:${Date.now()}:`;
  const keys: string[] = [];

  beforeAll(async () => {
    cluster = await connectCluster();
  }, 40_000);

  afterAll(async () => {
    if (cluster) {
      await Promise.all(
        keys.map((key) => cluster.del(key).catch(() => undefined)),
      );
      cluster.disconnect();
    }
  });

  it('shares one bucket across independent cluster clients', async () => {
    const second = await connectCluster();
    const key = `${prefix}shared`;
    keys.push(key);
    const firstBucket = new RedisTokenBucket(cluster, 3, 0.001);
    const secondBucket = new RedisTokenBucket(second, 3, 0.001);

    try {
      expect((await firstBucket.consume(key)).allowed).toBe(true);
      expect((await firstBucket.consume(key)).allowed).toBe(true);
      expect((await secondBucket.consume(key)).allowed).toBe(true);

      const denied = await firstBucket.consume(key);
      expect(denied.allowed).toBe(false);
      expect(denied.remaining).toBe(0);
    } finally {
      second.disconnect();
    }
  });

  it('routes keys across slots and applies the Lua script on each master', async () => {
    const bucket = new RedisTokenBucket(cluster, 2, 0.001);
    const selected: string[] = [];
    const slots = new Set<number>();

    for (let index = 0; index < 64 && slots.size < 3; index += 1) {
      const key = `${prefix}slot-${index}`;
      const slot = Number(await clusterCommand(cluster, 'keyslot', key));
      if (!slots.has(slot)) {
        slots.add(slot);
        selected.push(key);
        keys.push(key);
      }
    }

    expect(selected).toHaveLength(3);
    for (const key of selected) {
      const decision = await bucket.consume(key);
      expect(decision.allowed).toBe(true);
      expect(decision.source).toBe('redis');
      expect(await cluster.exists(key)).toBe(1);
      expect(await cluster.ttl(key)).toBeGreaterThan(0);
    }
  });

  it(
    'keeps rate limiting available after a master failover',
    async () => {
      if (process.env.REDIS_CLUSTER_FAILOVER !== '1') {
        return;
      }

      const nodes = String(await clusterCommand(cluster, 'nodes'));
      const lines = nodes
        .split('\n')
        .map((line) => line.trim())
        .filter(Boolean);
      const masterLines = lines.filter((line) => {
        const flags = line.split(' ')[2] ?? '';
        return flags.split(',').includes('master');
      });
      const replicaLines = lines.filter((line) => {
        const flags = line.split(' ')[2] ?? '';
        return flags.split(',').includes('slave');
      });

      expect(masterLines.length).toBeGreaterThan(0);
      expect(replicaLines.length).toBeGreaterThan(0);

      const master = masterLines[0].split(' ');
      const masterId = master[0];
      const replica = replicaLines.find((line) => line.split(' ')[3] === masterId);
      expect(replica).toBeDefined();

      const replicaAddress = nodeAddress(replica!.split(' ')[1]);
      const slots = (await clusterCommand(cluster, 'slots')) as Array<
        [number, number, Array<string | number>]
      >;
      const range = slots.find(
        (row) => String(row[2]?.[2] ?? '') === masterId,
      );
      expect(range).toBeDefined();

      // Find a key owned by the master that is about to be demoted.
      let key = '';
      for (let index = 0; index < 10_000; index += 1) {
        const candidate = `${prefix}failover-${index}`;
        const slot = Number(await clusterCommand(cluster, 'keyslot', candidate));
        if (slot >= Number(range![0]) && slot <= Number(range![1])) {
          key = candidate;
          break;
        }
      }
      expect(key).not.toBe('');

      const direct = new Redis(replicaAddress.port, replicaAddress.host, {
        lazyConnect: true,
        maxRetriesPerRequest: 1,
      });
      try {
        await direct.connect();
        await (direct as unknown as {
          cluster: (...values: string[]) => Promise<string>;
        }).cluster('failover', 'FORCE');
      } finally {
        direct.disconnect();
      }

      const bucket = new RedisTokenBucket(cluster, 4, 0.001);
      keys.push(key);

      // The old master must actually be demoted before the limiter is called;
      // otherwise a fast PING could hide a no-op failover.
      await waitFor(async () => {
        try {
          const current = String(await clusterCommand(cluster, 'nodes'));
          const line = current
            .split('\n')
            .find((entry) => entry.startsWith(`${masterId} `));
          return Boolean(line && line.split(' ')[2]?.includes('slave'));
        } catch {
          return false;
        }
      }, 30_000);

      await waitFor(async () => {
        try {
          return (await bucket.consume(key)).allowed;
        } catch {
          return false;
        }
      }, 30_000);
    },
    60_000,
  );
});
