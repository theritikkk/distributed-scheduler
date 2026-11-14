#!/usr/bin/env python3
"""
Small, dependency-light PostgreSQL migration runner.

Usage:
    python scripts/migrate.py bootstrap [--to 0004]
    python scripts/migrate.py up [--to 0004]
    python scripts/migrate.py status
    python scripts/migrate.py baseline [--through 0001]
    python scripts/migrate.py rollback
    python scripts/migrate.py create add_outbox

The database URL is read from DATABASE_URL unless --url is provided.

Migration files are named:

    0001_initial.sql
    0002_add_outbox.sql

A matching rollback file is optional:

    0002_add_outbox.down.sql

To run a migration outside a transaction (for CREATE INDEX CONCURRENTLY), add this
line near the top of the file:

    -- migrate:no-transaction

Keep a no-transaction migration to a single SQL statement; psycopg2 sends multiple
statements as one implicit transaction, which PostgreSQL rejects for commands such as
CREATE INDEX CONCURRENTLY.

Existing databases that were bootstrapped with database/init.sql should be marked
once with `baseline`; future migrations are applied with `up`.
"""
from __future__ import annotations

import argparse
import hashlib
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

try:
    import psycopg2  # type: ignore[import-untyped]
except ImportError:  # pragma: no cover - exercised by a clear CLI error
    print(
        "psycopg2 is required. Install it with `pip install psycopg2-binary`.",
        file=sys.stderr,
    )
    raise SystemExit(2)


DEFAULT_MIGRATIONS_DIR = Path("database/migrations")
DEFAULT_BOOTSTRAP_FILE = Path("database/init.sql")
MIGRATION_RE = re.compile(r"^(?P<version>\d+)_(?P<name>.+)\.sql$")
DOWN_RE = re.compile(r"^(?P<version>\d+)_(?P<name>.+)\.down\.sql$")
NO_TRANSACTION_RE = re.compile(r"^\s*--\s*migrate:no-transaction\s*$", re.MULTILINE)
MIGRATION_LOCK_ID = 8_675_309


class MigrationError(RuntimeError):
    pass


def _database_url(args: argparse.Namespace) -> str:
    url = args.url or os.environ.get("DATABASE_URL")
    if not url:
        raise MigrationError(
            "DATABASE_URL is not set. Pass --url or export DATABASE_URL."
        )
    return url


def _read_sql(path: Path) -> str:
    text = path.read_text(encoding="utf-8")
    if not text.strip():
        raise MigrationError(f"migration is empty: {path}")
    return text


def _checksum(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _no_transaction(sql: str) -> bool:
    return bool(NO_TRANSACTION_RE.search(sql[:1000]))


def _migration_files(directory: Path) -> list[tuple[str, str, Path]]:
    if not directory.is_dir():
        raise MigrationError(f"migrations directory not found: {directory}")

    files: list[tuple[str, str, Path]] = []
    for path in directory.iterdir():
        if not path.is_file():
            continue
        if path.name.endswith(".down.sql"):
            continue
        match = MIGRATION_RE.match(path.name)
        if not match:
            if path.suffix == ".sql":
                raise MigrationError(
                    f"invalid migration filename: {path.name} "
                    "(expected <version>_<name>.sql)"
                )
            continue
        files.append((match.group("version"), match.group("name"), path))

    files.sort(key=lambda item: (int(item[0]), item[1]))
    versions = [item[0] for item in files]
    if len(versions) != len(set(versions)):
        raise MigrationError("duplicate migration versions found")
    return files


def _down_path(directory: Path, version: str, name: str) -> Path:
    return directory / f"{version}_{name}.down.sql"


def _connect(url: str):
    conn = psycopg2.connect(url)
    conn.autocommit = False
    return conn


def _ensure_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version    text PRIMARY KEY,
                name       text NOT NULL,
                checksum   text NOT NULL,
                applied_at timestamptz NOT NULL DEFAULT now()
            )
            """
        )
    conn.commit()


def _applied(conn) -> dict[str, dict[str, str]]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT version, name, checksum FROM schema_migrations ORDER BY version"
        )
        applied = {
            row[0]: {"name": row[1], "checksum": row[2]}
            for row in cur.fetchall()
        }
    # Leave the session out of a transaction so a following no-transaction
    # migration can switch autocommit safely.
    conn.commit()
    return applied


def _advisory_lock(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_lock(%s)", (MIGRATION_LOCK_ID,))
    conn.commit()


def _advisory_unlock(conn) -> None:
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_unlock(%s)", (MIGRATION_LOCK_ID,))
        conn.commit()
    except Exception:
        # The connection may already be broken; closing it releases the lock.
        pass


def cmd_up(args: argparse.Namespace) -> int:
    url = _database_url(args)
    conn = _connect(url)
    applied_count = 0
    try:
        _ensure_table(conn)
        _advisory_lock(conn)
        applied = _applied(conn)

        files = _migration_files(args.dir)
        target = getattr(args, "to", None)
        if target is not None:
            known = {version for version, _, _ in files}
            if target not in known:
                raise MigrationError(f"unknown migration target: {target}")
            files = [item for item in files if int(item[0]) <= int(target)]

        for version, name, path in files:
            sql = _read_sql(path)
            checksum = _checksum(sql)

            if version in applied:
                if applied[version]["checksum"] != checksum:
                    raise MigrationError(
                        f"checksum mismatch for applied migration {version}_{name}; "
                        "create a new migration instead of editing an applied one"
                    )
                continue

            print(f"applying {version}_{name}")
            if _no_transaction(sql):
                print(f"  running {version}_{name} outside a transaction")
                conn.autocommit = True
                try:
                    with conn.cursor() as cur:
                        cur.execute(sql)
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            INSERT INTO schema_migrations (version, name, checksum)
                            VALUES (%s, %s, %s)
                            """,
                            (version, name, checksum),
                        )
                finally:
                    conn.autocommit = False
            else:
                try:
                    with conn.cursor() as cur:
                        cur.execute(sql)
                        cur.execute(
                            """
                            INSERT INTO schema_migrations (version, name, checksum)
                            VALUES (%s, %s, %s)
                            """,
                            (version, name, checksum),
                        )
                    conn.commit()
                except Exception:
                    conn.rollback()
                    raise

            applied_count += 1

        print(f"applied {applied_count} migration(s)")
        return 0
    finally:
        _advisory_unlock(conn)
        conn.close()


def cmd_status(args: argparse.Namespace) -> int:
    url = _database_url(args)
    conn = _connect(url)
    try:
        _ensure_table(conn)
        applied = _applied(conn)
        files = _migration_files(args.dir)
        known_versions = {version for version, _, _ in files}

        print(f"{'VERSION':<12} {'STATUS':<12} NAME")
        for version, name, path in files:
            checksum = _checksum(_read_sql(path))
            if version not in applied:
                status = "pending"
            elif applied[version]["checksum"] != checksum:
                status = "checksum"
            else:
                status = "applied"
            print(f"{version:<12} {status:<12} {name}")

        for version in sorted(set(applied) - known_versions, key=int):
            print(f"{version:<12} {'missing':<12} {applied[version]['name']}")

        return 0
    finally:
        conn.close()


def cmd_baseline(args: argparse.Namespace) -> int:
    url = _database_url(args)
    conn = _connect(url)
    try:
        _ensure_table(conn)
        _advisory_lock(conn)
        applied = _applied(conn)
        files = _migration_files(args.dir)

        if args.through:
            through = args.through
            if through not in {version for version, _, _ in files}:
                raise MigrationError(f"unknown baseline version: {through}")
        else:
            through = None

        marked = 0
        for version, name, path in files:
            sql = _read_sql(path)
            checksum = _checksum(sql)
            if version in applied:
                continue
            if through is not None and int(version) > int(through):
                break
            print(f"marking baseline {version}_{name}")
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO schema_migrations (version, name, checksum)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (version) DO NOTHING
                    """,
                    (version, name, checksum),
                )
            conn.commit()
            marked += 1

        print(f"marked {marked} migration(s) as applied")
        return 0
    finally:
        _advisory_unlock(conn)
        conn.close()


def cmd_bootstrap(args: argparse.Namespace) -> int:
    """Bootstrap a fresh database, then baseline/adopt and apply migrations."""
    url = _database_url(args)
    conn = _connect(url)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('public.tasks')")
            has_schema = cur.fetchone()[0] is not None
        conn.commit()

        if not has_schema:
            path: Path = args.bootstrap_file
            if not path.is_file():
                raise MigrationError(f"bootstrap file not found: {path}")
            print(f"bootstrapping schema from {path}")
            conn.autocommit = True
            try:
                with conn.cursor() as cur:
                    cur.execute(_read_sql(path))
            finally:
                conn.autocommit = False
        else:
            print("existing schema detected; skipping bootstrap SQL")
    finally:
        conn.close()

    baseline_args = argparse.Namespace(
        url=args.url,
        dir=args.dir,
        through=args.baseline_version,
    )
    result = cmd_baseline(baseline_args)
    if result != 0:
        return result
    return cmd_up(args)


def cmd_rollback(args: argparse.Namespace) -> int:
    url = _database_url(args)
    conn = _connect(url)
    try:
        _ensure_table(conn)
        _advisory_lock(conn)
        applied = _applied(conn)
        if not applied:
            print("no applied migrations")
            return 0

        version = max(applied, key=int)
        name = applied[version]["name"]
        path = _down_path(args.dir, version, name)
        if not path.is_file():
            raise MigrationError(
                f"rollback file not found: {path}. "
                "Create a reviewed down migration before rolling back."
            )

        sql = _read_sql(path)
        print(f"rolling back {version}_{name}")
        if _no_transaction(sql):
            conn.autocommit = True
            try:
                with conn.cursor() as cur:
                    cur.execute(sql)
                with conn.cursor() as cur:
                    cur.execute(
                        "DELETE FROM schema_migrations WHERE version = %s", (version,)
                    )
            finally:
                conn.autocommit = False
        else:
            try:
                with conn.cursor() as cur:
                    cur.execute(sql)
                    cur.execute(
                        "DELETE FROM schema_migrations WHERE version = %s", (version,)
                    )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        print(f"rolled back {version}_{name}")
        return 0
    finally:
        _advisory_unlock(conn)
        conn.close()


def cmd_create(args: argparse.Namespace) -> int:
    directory: Path = args.dir
    directory.mkdir(parents=True, exist_ok=True)
    slug = re.sub(r"[^a-z0-9]+", "_", args.name.strip().lower()).strip("_")
    if not slug:
        raise MigrationError("migration name must contain letters or digits")
    version = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    path = directory / f"{version}_{slug}.sql"
    if path.exists():
        raise MigrationError(f"migration already exists: {path}")
    path.write_text(
        "-- Write the forward migration here.\n"
        "-- Add `-- migrate:no-transaction` near the top if it must run outside a transaction.\n",
        encoding="utf-8",
    )
    down = directory / f"{version}_{slug}.down.sql"
    down.write_text(
        "-- Write the reviewed rollback here. Delete this file if the migration is irreversible.\n",
        encoding="utf-8",
    )
    print(f"created {path}")
    print(f"created {down}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--url",
        help="PostgreSQL URL; defaults to DATABASE_URL",
    )
    parser.add_argument(
        "--dir",
        type=Path,
        default=DEFAULT_MIGRATIONS_DIR,
        help=f"migrations directory (default: {DEFAULT_MIGRATIONS_DIR})",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    up = subparsers.add_parser("up", help="apply pending migrations")
    up.add_argument(
        "--to",
        help="apply only through this migration version (for staged rollouts)",
    )
    up.set_defaults(func=cmd_up)

    status = subparsers.add_parser("status", help="show migration status")
    status.set_defaults(func=cmd_status)

    baseline = subparsers.add_parser(
        "baseline", help="mark existing migrations as applied without running them"
    )
    baseline.add_argument(
        "--through",
        help="mark only versions up to and including this version",
    )
    baseline.set_defaults(func=cmd_baseline)

    bootstrap = subparsers.add_parser(
        "bootstrap",
        help="create missing schema from database/init.sql, baseline, then apply migrations",
    )
    bootstrap.add_argument(
        "--bootstrap-file",
        type=Path,
        default=DEFAULT_BOOTSTRAP_FILE,
        help=f"bootstrap SQL file (default: {DEFAULT_BOOTSTRAP_FILE})",
    )
    bootstrap.add_argument(
        "--baseline-version",
        default="0001",
        help="mark migrations through this version as applied (default: 0001)",
    )
    bootstrap.add_argument(
        "--to",
        help="apply only through this migration version (for staged rollouts)",
    )
    bootstrap.set_defaults(func=cmd_bootstrap)

    rollback = subparsers.add_parser("rollback", help="roll back the newest applied migration")
    rollback.set_defaults(func=cmd_rollback)

    create = subparsers.add_parser("create", help="create a timestamped migration pair")
    create.add_argument("name", help="short migration name")
    create.set_defaults(func=cmd_create)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except MigrationError as exc:
        print(f"migration error: {exc}", file=sys.stderr)
        return 1
    except psycopg2.Error as exc:
        print(f"database error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
