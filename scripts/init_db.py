#!/usr/bin/env python3
"""
Apply the bootstrap schema (database/init.sql) to a PostgreSQL database.

This is intended for a fresh database only. Existing databases should keep their
schema and use scripts/migrate.py baseline/up.
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


DEFAULT_SCHEMA = Path("database/init.sql")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", help="PostgreSQL URL; defaults to DATABASE_URL")
    parser.add_argument(
        "--file",
        type=Path,
        default=DEFAULT_SCHEMA,
        help=f"bootstrap SQL file (default: {DEFAULT_SCHEMA})",
    )
    args = parser.parse_args(argv)

    url = args.url or os.environ.get("DATABASE_URL")
    if not url:
        print("DATABASE_URL is not set. Pass --url or export DATABASE_URL.", file=sys.stderr)
        return 2
    if not args.file.is_file():
        print(f"schema file not found: {args.file}", file=sys.stderr)
        return 2

    sql = args.file.read_text(encoding="utf-8")
    conn = psycopg2.connect(url)
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(sql)
    finally:
        conn.close()

    print(f"applied {args.file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
