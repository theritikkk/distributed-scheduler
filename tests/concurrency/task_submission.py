#!/usr/bin/env python3
"""Submit N task-creation requests in parallel and verify unique task IDs.

Usage:
    BASE_URL=http://localhost:3000 python3 tests/concurrency/task_submission.py
    python3 tests/concurrency/task_submission.py --base-url ... --requests 200 --concurrency 40
"""
from __future__ import annotations

import argparse
import json
import os
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from uuid import uuid4


def _request(method: str, url: str, payload: dict | None = None, token: str | None = None):
    body = json.dumps(payload).encode("utf-8") if payload is not None else None
    request = urllib.request.Request(url, data=body, method=method)
    request.add_header("Content-Type", "application/json")
    if token:
        request.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"{method} {url} -> {exc.code}: {detail}") from exc


def _create_task(base_url: str, token: str, index: int) -> str:
    scheduled_at = datetime.now(timezone.utc) + timedelta(hours=1)
    status, body = _request(
        "POST",
        f"{base_url}/api/v1/tasks",
        {
            "task_name": f"concurrency-{uuid4()}-{index}",
            "command_payload": {"command": f"echo concurrency-{index}"},
            "schedule_type": "one-time",
            "next_execution_time": scheduled_at.isoformat(),
        },
        token,
    )
    if status != 201:
        raise RuntimeError(f"expected 201, got {status}: {body}")
    return body["id"]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--base-url",
        default=os.environ.get("BASE_URL", "http://localhost:3000"),
    )
    parser.add_argument("--requests", type=int, default=100)
    parser.add_argument("--concurrency", type=int, default=20)
    args = parser.parse_args()

    if args.requests < 1 or args.concurrency < 1:
        parser.error("--requests and --concurrency must be positive")

    email = f"concurrency-{uuid4()}@example.com"
    password = "Concurrency1!"
    _, registered = _request(
        "POST",
        f"{args.base_url}/api/v1/auth/register",
        {"email": email, "password": password},
    )
    token = registered["token"]

    started = time.perf_counter()
    failures: list[str] = []
    task_ids: list[str] = []
    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        futures = [
            pool.submit(_create_task, args.base_url, token, index)
            for index in range(args.requests)
        ]
        for future in futures:
            try:
                task_ids.append(future.result())
            except Exception as exc:
                failures.append(str(exc))

    elapsed = time.perf_counter() - started
    unique = len(set(task_ids))
    print(
        f"requests={args.requests} concurrency={args.concurrency} "
        f"success={len(task_ids)} failed={len(failures)} unique_ids={unique} "
        f"elapsed={elapsed:.2f}s rate={len(task_ids) / elapsed:.1f}/s"
    )
    if failures:
        for failure in failures[:5]:
            print(f"failure: {failure}")
        return 1
    if unique != len(task_ids):
        print("duplicate task IDs detected")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
