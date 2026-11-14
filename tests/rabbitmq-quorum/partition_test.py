#!/usr/bin/env python3
"""Partition drill for RabbitMQ quorum queues.

Phase 1: isolate one broker. Two of three nodes retain a Raft majority, so the
queue stays available and accepted messages are durable.
Phase 2: isolate a second broker. The remaining node is a minority, so the
quorum queue stops serving writes/reads; the drill asserts that the minority
does not accept a probe publish.
Phase 3: reconnect both brokers, wait for all three nodes and all three queue
members, then consume the backlog and verify every message arrived exactly
once.

Only the Python standard library and the Docker CLI are used.
"""

from __future__ import annotations

import base64
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

QUEUE = "scheduler.quorum.partition-test"
USER = os.environ.get("RABBITMQ_USER", "scheduler")
PASSWORD = os.environ.get("RABBITMQ_PASSWORD", "scheduler_secret")
URLS = [
    value.strip().rstrip("/")
    for value in os.environ.get(
        "RABBITMQ_MANAGEMENT_URLS",
        "http://127.0.0.1:25672,http://127.0.0.1:25673,http://127.0.0.1:25674",
    ).split(",")
    if value.strip()
]
CONTAINERS = os.environ.get(
    "RABBITMQ_CONTAINERS",
    "scheduler-rmq-quorum-1 scheduler-rmq-quorum-2 scheduler-rmq-quorum-3",
).split()
NETWORK = os.environ.get("RABBITMQ_NETWORK", "scheduler-rabbitmq-quorum_default")
QUEUE_PATH = urllib.parse.quote(QUEUE, safe="")


class ApiError(RuntimeError):
    def __init__(self, status: int, body: str) -> None:
        super().__init__(f"HTTP {status}: {body[:500]}")
        self.status = status
        self.body = body


def log(message: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {message}", flush=True)


def request(
    base: str,
    path: str,
    method: str = "GET",
    payload: dict | None = None,
    timeout: float = 10.0,
):
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(base + path, data=data, method=method)
    credentials = base64.b64encode(f"{USER}:{PASSWORD}".encode()).decode()
    req.add_header("Authorization", f"Basic {credentials}")
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            body = response.read().decode()
            return json.loads(body) if body else None
    except urllib.error.HTTPError as exc:
        raise ApiError(exc.code, exc.read().decode(errors="replace")) from exc


def docker(*args: str, timeout: float = 30.0) -> str:
    completed = subprocess.run(
        ["docker", *args],
        check=False,
        text=True,
        capture_output=True,
        timeout=timeout,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"docker {' '.join(args)} failed: {completed.stderr.strip()}"
        )
    return completed.stdout


def cluster_status() -> dict:
    output = docker(
        "exec",
        CONTAINERS[0],
        "rabbitmqctl",
        "cluster_status",
        "--formatter",
        "json",
    )
    return json.loads(output)


def running_nodes() -> set[str]:
    try:
        return set(cluster_status().get("running_nodes", []))
    except Exception:
        return set()


def wait_cluster(expected: int, timeout: float = 150.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        nodes = running_nodes()
        if len(nodes) == expected:
            log(f"cluster running nodes = {sorted(nodes)}")
            return
        time.sleep(2)
    raise RuntimeError(
        f"cluster did not reach {expected} running nodes; saw {sorted(running_nodes())}"
    )


def app_running(container: str) -> bool:
    completed = subprocess.run(
        ["docker", "exec", container, "rabbitmq-diagnostics", "-q", "check_running"],
        check=False,
        text=True,
        capture_output=True,
        timeout=20,
    )
    return completed.returncode == 0


def wait_minority_paused(container: str, timeout: float = 180.0) -> None:
    """Wait until pause_minority stops the RabbitMQ app on the lone node."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if not app_running(container):
            log(f"{container} paused the RabbitMQ application (minority)")
            return
        time.sleep(2)
    raise RuntimeError(f"{container} stayed active without a cluster majority")


def queue_members() -> list[str]:
    output = docker(
        "exec",
        CONTAINERS[0],
        "rabbitmqctl",
        "list_queues",
        "name",
        "members",
        "--formatter",
        "json",
    )
    rows = json.loads(output)
    for row in rows:
        if row.get("name") == QUEUE:
            members = row.get("members") or []
            if isinstance(members, str):
                return [item.strip() for item in members.split(",") if item.strip()]
            return list(members)
    return []


def wait_queue_members(expected: int, timeout: float = 180.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            members = queue_members()
        except Exception:
            members = []
        if len(members) >= expected:
            log(f"queue members = {sorted(members)}")
            return
        time.sleep(2)
    raise RuntimeError(
        f"queue did not reach {expected} members; saw {queue_members()}"
    )


def declare_queue() -> None:
    request(
        URLS[0],
        f"/api/queues/%2F/{QUEUE_PATH}",
        method="PUT",
        payload={
            "durable": True,
            "auto_delete": False,
            "arguments": {
                "x-queue-type": "quorum",
                "x-quorum-initial-group-size": 3,
                "x-delivery-limit": 20,
            },
        },
    )
    log("declared quorum queue with three initial members")


def queue_info() -> dict:
    return request(URLS[0], f"/api/queues/%2F/{QUEUE_PATH}")


def wait_ready(expected: int, timeout: float = 90.0) -> None:
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            last = queue_info()
            ready = int(last.get("messages_ready", 0))
            if ready >= expected:
                log(f"messages_ready = {ready}")
                return
        except Exception as exc:
            last = str(exc)
        time.sleep(1)
    raise RuntimeError(f"queue did not reach {expected} ready messages: {last}")


def publish(ids: list[int], timeout: float = 15.0) -> None:
    for message_id in ids:
        payload = json.dumps(
            {
                "id": message_id,
                "producedAt": time.time(),
                "source": "rabbitmq-quorum-partition-drill",
            }
        )
        result = request(
            URLS[0],
            "/api/exchanges/%2F/amq.default/publish",
            method="POST",
            payload={
                "properties": {
                    "delivery_mode": 2,
                    "content_type": "application/json",
                },
                "routing_key": QUEUE,
                "payload": payload,
                "payload_encoding": "string",
            },
            timeout=timeout,
        )
        if not result or result.get("routed") is not True:
            raise RuntimeError(f"publish {message_id} was not routed: {result}")
    log(f"published {len(ids)} message(s) with confirms via management API")


def consume(count: int, timeout: float = 20.0) -> list[int]:
    response = request(
        URLS[0],
        f"/api/queues/%2F/{QUEUE_PATH}/get",
        method="POST",
        payload={
            "count": count,
            "ackmode": "ack_requeue_false",
            "encoding": "auto",
            "truncate": 100_000,
        },
        timeout=timeout,
    )
    ids = []
    for message in response or []:
        body = json.loads(message["payload"])
        ids.append(int(body["id"]))
    return ids


def consume_until(expected_total: int, consumed: list[int]) -> None:
    deadline = time.time() + 120
    while len(consumed) < expected_total and time.time() < deadline:
        batch = consume(min(100, expected_total - len(consumed)))
        if not batch:
            time.sleep(1)
            continue
        consumed.extend(batch)
        log(f"consumed {len(batch)} message(s); total={len(consumed)}")


def partition(container: str) -> None:
    docker("network", "disconnect", NETWORK, container)
    log(f"disconnected {container} from {NETWORK}")


def heal(container: str) -> None:
    completed = subprocess.run(
        ["docker", "network", "connect", NETWORK, container],
        check=False,
        text=True,
        capture_output=True,
    )
    if completed.returncode != 0 and "already exists" not in completed.stderr:
        raise RuntimeError(
            f"could not reconnect {container}: {completed.stderr.strip()}"
        )
    log(f"reconnected {container} to {NETWORK}")


def probe_minority_write() -> bool:
    """Return True only if the minority node accepted a queued message."""
    try:
        result = request(
            URLS[0],
            "/api/exchanges/%2F/amq.default/publish",
            method="POST",
            payload={
                "properties": {"delivery_mode": 2},
                "routing_key": QUEUE,
                "payload": json.dumps({"id": 9999, "probe": True}),
                "payload_encoding": "string",
            },
            timeout=6.0,
        )
        return bool(result and result.get("routed") is True)
    except Exception as exc:
        log(f"minority publish was rejected as expected: {exc}")
        return False


def probe_minority_read() -> list[int]:
    try:
        response = request(
            URLS[0],
            f"/api/queues/%2F/{QUEUE_PATH}/get",
            method="POST",
            payload={
                "count": 1,
                "ackmode": "ack_requeue_true",
                "encoding": "auto",
                "truncate": 100_000,
            },
            timeout=6.0,
        )
        return [int(json.loads(item["payload"])["id"]) for item in response or []]
    except Exception as exc:
        log(f"minority read was rejected as expected: {exc}")
        return []


def main() -> int:
    log("waiting for the initial three-node cluster")
    wait_cluster(3)
    declare_queue()
    wait_queue_members(3)

    expected = list(range(0, 120))
    publish(expected)
    wait_ready(len(expected))

    log("phase 1: isolating one broker (quorum remains available)")
    partition(CONTAINERS[1])
    wait_cluster(2)
    phase_one = list(range(120, 140))
    publish(phase_one)
    expected.extend(phase_one)

    consumed: list[int] = []
    consume_until(40, consumed)
    if len(consumed) != 40:
        raise RuntimeError(f"expected 40 messages during degraded mode, got {len(consumed)}")
    log("queue stayed available with two of three brokers")

    log("phase 2: isolating a second broker (no majority)")
    partition(CONTAINERS[2])
    wait_minority_paused(CONTAINERS[0])
    if probe_minority_write():
        raise RuntimeError("minority node accepted a publish; quorum safety is broken")
    if probe_minority_read():
        raise RuntimeError("minority node served a message; quorum safety is broken")
    log("minority correctly stopped serving the quorum queue")

    log("phase 3: healing the cluster")
    heal(CONTAINERS[1])
    heal(CONTAINERS[2])
    wait_cluster(3)
    wait_queue_members(3)
    wait_ready(max(0, len(expected) - len(consumed)))

    tail = list(range(140, 160))
    publish(tail)
    expected.extend(tail)
    consume_until(len(expected), consumed)

    # A probe publish during the minority phase must never be counted as an
    # expected message. If RabbitMQ did route it after healing, ignore id 9999.
    consumed = [message_id for message_id in consumed if message_id < 9999]

    if len(consumed) != len(expected):
        raise RuntimeError(
            f"consumed {len(consumed)} messages, expected {len(expected)}"
        )
    if len(consumed) != len(set(consumed)):
        duplicates = sorted(
            {message_id for message_id in consumed if consumed.count(message_id) > 1}
        )
        raise RuntimeError(f"duplicate deliveries: {duplicates}")
    if set(consumed) != set(expected):
        missing = sorted(set(expected) - set(consumed))
        extra = sorted(set(consumed) - set(expected))
        raise RuntimeError(f"message mismatch; missing={missing} extra={extra}")

    log(
        f"PASS: {len(expected)} messages consumed exactly once across "
        "single-broker loss, total partition, and recovery"
    )
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        raise
