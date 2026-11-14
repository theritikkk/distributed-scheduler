#!/usr/bin/env python3
"""
Bounded online archival for terminal scheduler history.

Copies terminal task executions/attempts and published/failed outbox rows into
the archive tables, then deletes them from the live tables in the same
transaction. Pending/running execution rows, pending outbox rows, and pending
timers are never touched.

Usage:
    DATABASE_URL=postgresql://... python scripts/archive_data.py --dry-run
    DATABASE_URL=postgresql://... python scripts/archive_data.py --retention-days 30

The coordinator runs the same logic automatically when ARCHIVE_ENABLED=true.
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

try:
    import psycopg2  # type: ignore[import-untyped]
except ImportError:  # pragma: no cover
    print(
        "psycopg2 is required. Install it with `pip install psycopg2-binary`.",
        file=sys.stderr,
    )
    raise SystemExit(2)

COORDINATOR_ROOT = Path(__file__).resolve().parents[1] / "coordinator"
if str(COORDINATOR_ROOT) not in sys.path:
    sys.path.insert(0, str(COORDINATOR_ROOT))

from internal.archive import archive_cycle  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", help="PostgreSQL URL; defaults to DATABASE_URL")
    parser.add_argument(
        "--retention-days",
        type=int,
        default=30,
        help="keep terminal rows newer than this many days (default: 30)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="maximum rows copied/deleted per batch (default: 500)",
    )
    parser.add_argument(
        "--max-batches",
        type=int,
        default=100,
        help="maximum batches per run (default: 100)",
    )
    parser.add_argument(
        "--sleep-ms",
        type=int,
        default=50,
        help="pause between batches in milliseconds (default: 50)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="count eligible rows without copying or deleting",
    )
    args = parser.parse_args(argv)

    url = args.url or os.environ.get("DATABASE_URL")
    if not url:
        print("DATABASE_URL is not set. Pass --url or export DATABASE_URL.", file=sys.stderr)
        return 2
    if args.retention_days < 1:
        print("--retention-days must be at least 1", file=sys.stderr)
        return 2
    if args.batch_size < 1 or args.max_batches < 1:
        print("--batch-size and --max-batches must be at least 1", file=sys.stderr)
        return 2

    def connect():
        return psycopg2.connect(url)

    try:
        result = archive_cycle(
            connect,
            retention_days=args.retention_days,
            batch_size=args.batch_size,
            max_batches=args.max_batches,
            sleep_ms=max(0, args.sleep_ms),
            dry_run=args.dry_run,
        )
    except Exception as exc:
        print(f"archive failed: {exc}", file=sys.stderr)
        return 1

    mode = "would archive" if args.dry_run else "archived"
    print(
        f"{mode}: executions={result.executions} "
        f"attempts={result.attempts} outbox={result.outbox} "
        f"total={result.total}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
