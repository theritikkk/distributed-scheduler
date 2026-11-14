# Redis Cluster integration harness

Starts a disposable six-node Redis Cluster (three masters, three replicas) and runs
`api-gateway/tests/redis-cluster.integration.test.ts` against it. The test uses the
same single-key token-bucket Lua script as production.

```bash
./tests/redis-cluster/run.sh
REDIS_CLUSTER_FAILOVER=1 ./tests/redis-cluster/run.sh   # adds a master-failover test
KEEP=1 ./tests/redis-cluster/run.sh                      # keep the cluster for poking
```

The harness publishes ports `7001`–`7006`. A single container runs all six Redis
processes on loopback so cluster nodes can meet each other while the host still reaches
every advertised address. `KEEP=1` prints the cleanup command.

## What is asserted

1. Two independent ioredis `Cluster` clients share one token bucket. The fourth request
   against a capacity-3 bucket is denied, proving the limit is cluster-wide rather than
   per-client/per-replica.
2. Keys are routed to at least three different slot owners, and each key executes the
   Lua script on its master with a positive TTL applied.
3. With `REDIS_CLUSTER_FAILOVER=1`, a replica is promoted with `CLUSTER FAILOVER FORCE`
   and the limiter keeps working after ioredis refreshes its slot map.

This is local evidence for cluster compatibility. It does not replace a managed-Redis
staging test under real network latency and failover.

## Requirements

- Docker with a running daemon
- `redis-cli` on the host
- `api-gateway/node_modules` installed (`npm install` in `api-gateway/`)
