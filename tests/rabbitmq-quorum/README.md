# RabbitMQ quorum-queue partition drill

Starts a disposable three-node RabbitMQ cluster, declares
`scheduler.quorum.partition-test` as a three-member quorum queue, and partitions the
Docker network to prove the majority rule and exactly-once backlog recovery.

```bash
./tests/rabbitmq-quorum/run.sh
KEEP=1 ./tests/rabbitmq-quorum/run.sh
```

## Phases

1. **One broker isolated.** Two of three nodes retain a Raft majority. The queue stays
   available; 20 additional messages are published and 40 are consumed.
2. **Two brokers isolated.** The remaining node has no majority. The drill waits for
   `pause_minority` to stop the RabbitMQ application, then asserts that a probe publish
   is not routed and a probe read does not return a message. The minority pauses instead
   of serving stale state.
3. **Cluster healed.** All three nodes and all three queue members return. The remaining
   backlog is consumed and every message ID is checked for exactly-once delivery (no
   loss, no duplicates).

The harness pre-creates the three named volumes and seeds the shared Erlang cookie as
uid/gid `100:101` before any broker starts. This removes the intermittent first-boot
`eacces` failure on fresh volumes; `ensure_node` also recreates a node a bounded number
of times if it still fails to become ready.

The drill uses only the Python standard library, the RabbitMQ management HTTP API, and
the Docker CLI. Management ports are published on `25672`–`25674` to avoid the host
port `15672` used by the local E2E stack. It ignores ID `9999`, which is the
minority-phase probe and is never counted as expected work.

## What this proves (and does not)

- Quorum queues stop accepting work without a majority; they do not split-brain.
- A confirmed backlog survives a single broker loss and a full partition.
- Recovery preserves every message exactly once at the queue level.

This is not an application-level exactly-once claim. The scheduler's exactly-once
execution slot and lease guards are validated separately in
[docs/HA.md](../../docs/HA.md) and the failover runbook.

## Requirements

- Docker with a running daemon
- `python3` (standard library only)

Broker configuration is mounted from
[`deploy/ha-lab/ha/rabbitmq.conf.tmpl`](../../deploy/ha-lab/ha/rabbitmq.conf.tmpl), so
resource alarms and `pause_minority` are exercised rather than defaults.
