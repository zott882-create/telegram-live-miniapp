#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""PostgreSQL-only persistent storage for Live ZOT.

SQLite is intentionally not supported here.  PostgreSQL is the source of truth;
Redis and in-process dictionaries may only be used as disposable read caches.
"""
from __future__ import annotations

import json
import os
import threading
import time
from datetime import date
from typing import Any, Callable

try:
    import psycopg
    from psycopg.types.json import Jsonb
except Exception as exc:  # pragma: no cover - startup validation reports this clearly
    psycopg = None  # type: ignore[assignment]
    Jsonb = None  # type: ignore[assignment]
    _IMPORT_ERROR: Exception | None = exc
else:
    _IMPORT_ERROR = None

ConnectionFactory = Callable[[], Any]

_lock = threading.RLock()
_ready = False
_database_url = ""
_connection_factory: ConnectionFactory | None = None
_last_ok_at = 0
_last_error = ""

SCHEMA_VERSION = "001_postgres_only_prediction_days"
TABLE = "livezot_prediction_days"
MIGRATIONS_TABLE = "livezot_schema_migrations"
STATE_TABLE = "livezot_service_state"


def _resolve_database_url(value: str = "") -> str:
    return (
        value
        or os.environ.get("DATABASE_URL", "")
        or os.environ.get("POSTGRES_URL", "")
        or os.environ.get("POSTGRESQL_URL", "")
    ).strip()


def configure(*, database_url: str = "", connection_factory: ConnectionFactory | None = None) -> None:
    """Configure the single PostgreSQL database used by the whole application."""
    global _database_url, _connection_factory, _ready
    resolved = _resolve_database_url(database_url)
    if not resolved:
        raise RuntimeError(
            "DATABASE_URL is required. Add a PostgreSQL service in Railway and expose DATABASE_URL. "
            "SQLite fallback has been removed."
        )
    if psycopg is None and connection_factory is None:
        raise RuntimeError(f"psycopg is not installed: {_IMPORT_ERROR}")
    with _lock:
        if _database_url and _database_url != resolved:
            raise RuntimeError("Live ZOT database was already configured with a different DATABASE_URL")
        _database_url = resolved
        _connection_factory = connection_factory
        _ready = False


def connection() -> Any:
    if not _database_url:
        configure()
    if _connection_factory is not None:
        return _connection_factory()
    if psycopg is None:
        raise RuntimeError(f"psycopg is not installed: {_IMPORT_ERROR}")
    return psycopg.connect(_database_url, connect_timeout=10)


def _json_value(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, (dict, list, int, float, bool)):
        return value
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8", "replace")
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return default
    return default


def _table_exists(cur: Any, name: str) -> bool:
    cur.execute("SELECT to_regclass(%s)", (f"public.{name}",))
    row = cur.fetchone()
    return bool(row and row[0])


def _migrate_old_postgres_tables(cur: Any) -> None:
    """Move data created by earlier versions into the one unified table."""
    cur.execute(f"SELECT 1 FROM {MIGRATIONS_TABLE} WHERE name=%s", (SCHEMA_VERSION,))
    if cur.fetchone():
        return

    now = int(time.time())
    if _table_exists(cur, "prediction_daily_history"):
        cur.execute(
            f"""
            INSERT INTO {TABLE}(source, stat_date, summary_json, feed_json, is_final, updated_at)
            SELECT
                'legacy_api',
                stat_date::date,
                COALESCE(NULLIF(summary_json, '')::jsonb, '{{}}'::jsonb),
                CASE WHEN feed_json IS NULL OR feed_json = '' THEN NULL ELSE feed_json::jsonb END,
                is_final,
                updated_at
            FROM prediction_daily_history
            ON CONFLICT(source, stat_date) DO UPDATE SET
                summary_json=EXCLUDED.summary_json,
                feed_json=COALESCE(EXCLUDED.feed_json, {TABLE}.feed_json),
                is_final=EXCLUDED.is_final,
                updated_at=GREATEST({TABLE}.updated_at, EXCLUDED.updated_at)
            """
        )
        cur.execute("DROP TABLE prediction_daily_history")

    if _table_exists(cur, "scores24_prediction_daily_history"):
        cur.execute(
            f"""
            INSERT INTO {TABLE}(source, stat_date, summary_json, feed_json, is_final, updated_at)
            SELECT
                'scores24',
                stat_date::date,
                COALESCE(NULLIF(payload_json, '')::jsonb, '{{}}'::jsonb),
                COALESCE(NULLIF(payload_json, '')::jsonb, '{{}}'::jsonb),
                is_final,
                updated_at
            FROM scores24_prediction_daily_history
            ON CONFLICT(source, stat_date) DO UPDATE SET
                summary_json=EXCLUDED.summary_json,
                feed_json=EXCLUDED.feed_json,
                is_final=EXCLUDED.is_final,
                updated_at=GREATEST({TABLE}.updated_at, EXCLUDED.updated_at)
            """
        )
        cur.execute("DROP TABLE scores24_prediction_daily_history")

    cur.execute(
        f"INSERT INTO {MIGRATIONS_TABLE}(name, applied_at) VALUES(%s,%s) ON CONFLICT(name) DO NOTHING",
        (SCHEMA_VERSION, now),
    )


def initialize() -> None:
    global _ready, _last_ok_at, _last_error
    with _lock:
        if _ready:
            return
        if not _database_url:
            configure()
        try:
            with connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        CREATE TABLE IF NOT EXISTS {MIGRATIONS_TABLE} (
                            name TEXT PRIMARY KEY,
                            applied_at BIGINT NOT NULL
                        )
                        """
                    )
                    cur.execute(
                        f"""
                        CREATE TABLE IF NOT EXISTS {TABLE} (
                            source TEXT NOT NULL,
                            stat_date DATE NOT NULL,
                            summary_json JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                            feed_json JSONB,
                            is_final BOOLEAN NOT NULL DEFAULT FALSE,
                            updated_at BIGINT NOT NULL,
                            PRIMARY KEY(source, stat_date)
                        )
                        """
                    )
                    cur.execute(
                        f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_updated_at ON {TABLE}(updated_at DESC)"
                    )
                    cur.execute(
                        f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_source_date ON {TABLE}(source, stat_date DESC)"
                    )
                    cur.execute(
                        f"""
                        CREATE TABLE IF NOT EXISTS {STATE_TABLE} (
                            state_key TEXT PRIMARY KEY,
                            payload_json JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                            updated_at BIGINT NOT NULL
                        )
                        """
                    )
                    cur.execute(
                        f"CREATE INDEX IF NOT EXISTS idx_{STATE_TABLE}_updated_at ON {STATE_TABLE}(updated_at DESC)"
                    )
                    _migrate_old_postgres_tables(cur)
                conn.commit()
            _ready = True
            _last_ok_at = int(time.time())
            _last_error = ""
            print(f"[postgres] ready table={TABLE} schema_version={SCHEMA_VERSION}")
        except Exception as exc:
            _ready = False
            _last_error = f"{type(exc).__name__}: {exc}"
            raise RuntimeError(f"PostgreSQL initialization failed; SQLite fallback is disabled: {_last_error}") from exc


def ping() -> bool:
    global _last_ok_at, _last_error
    try:
        initialize()
        with connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                row = cur.fetchone()
        ok = bool(row and row[0] == 1)
        if ok:
            _last_ok_at = int(time.time())
            _last_error = ""
        return ok
    except Exception as exc:
        _last_error = f"{type(exc).__name__}: {exc}"
        return False


def health() -> dict[str, Any]:
    ok = ping()
    return {
        "ok": ok,
        "storage": "postgres",
        "sqlite_fallback": False,
        "database_url": bool(_database_url),
        "table": TABLE,
        "state_table": STATE_TABLE,
        "schema_version": SCHEMA_VERSION,
        "last_ok_at": int(_last_ok_at or 0),
        "error": "" if ok else _last_error,
    }


def get_prediction_day(source: str, stat_date: str) -> dict[str, Any] | None:
    initialize()
    with connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT summary_json, feed_json, is_final, updated_at FROM {TABLE} WHERE source=%s AND stat_date=%s::date",
                (source, stat_date),
            )
            row = cur.fetchone()
    if not row:
        return None
    return {
        "summary": _json_value(row[0], {}),
        "feed": _json_value(row[1], None),
        "is_final": bool(row[2]),
        "updated_at": int(row[3] or 0),
    }


def upsert_prediction_day(
    source: str,
    stat_date: str,
    *,
    summary: dict[str, Any],
    feed: dict[str, Any] | list[Any] | None,
    is_final: bool,
    updated_at: int | None = None,
) -> None:
    initialize()
    now = int(updated_at or time.time())
    if Jsonb is None:
        raise RuntimeError("psycopg JSON adapter is unavailable")
    with connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {TABLE}(source, stat_date, summary_json, feed_json, is_final, updated_at)
                VALUES(%s,%s::date,%s,%s,%s,%s)
                ON CONFLICT(source, stat_date) DO UPDATE SET
                    summary_json=EXCLUDED.summary_json,
                    feed_json=EXCLUDED.feed_json,
                    is_final=EXCLUDED.is_final,
                    updated_at=EXCLUDED.updated_at
                """,
                (
                    source,
                    stat_date,
                    Jsonb(summary),
                    Jsonb(feed) if feed is not None else None,
                    bool(is_final),
                    now,
                ),
            )
        conn.commit()


def list_prediction_days(
    source: str,
    *,
    with_feed_only: bool = False,
    from_date: str | None = None,
    descending: bool = False,
) -> list[dict[str, Any]]:
    initialize()
    where = ["source=%s"]
    params: list[Any] = [source]
    if with_feed_only:
        where.append("feed_json IS NOT NULL")
    if from_date:
        where.append("stat_date >= %s::date")
        params.append(from_date)
    direction = "DESC" if descending else "ASC"
    sql = (
        f"SELECT stat_date, summary_json, feed_json, is_final, updated_at FROM {TABLE} "
        f"WHERE {' AND '.join(where)} ORDER BY stat_date {direction}"
    )
    with connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, tuple(params))
            rows = cur.fetchall() or []
    return [
        {
            "date": row[0].isoformat() if isinstance(row[0], date) else str(row[0]),
            "summary": _json_value(row[1], {}),
            "feed": _json_value(row[2], None),
            "is_final": bool(row[3]),
            "updated_at": int(row[4] or 0),
        }
        for row in rows
    ]


def delete_prediction_days_before(source: str, stat_date: str) -> int:
    initialize()
    with connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM {TABLE} WHERE source=%s AND stat_date < %s::date",
                (source, stat_date),
            )
            deleted = int(cur.rowcount or 0)
        conn.commit()
    return deleted

def get_service_state(state_key: str) -> dict[str, Any] | None:
    initialize()
    with connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT payload_json, updated_at FROM {STATE_TABLE} WHERE state_key=%s",
                (state_key,),
            )
            row = cur.fetchone()
    if not row:
        return None
    payload = _json_value(row[0], {})
    return {
        "payload": payload if isinstance(payload, dict) else {},
        "updated_at": int(row[1] or 0),
    }


def upsert_service_state(state_key: str, payload: dict[str, Any], *, updated_at: int | None = None) -> None:
    initialize()
    if Jsonb is None:
        raise RuntimeError("psycopg JSON adapter is unavailable")
    now = int(updated_at or time.time())
    with connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {STATE_TABLE}(state_key, payload_json, updated_at)
                VALUES(%s,%s,%s)
                ON CONFLICT(state_key) DO UPDATE SET
                    payload_json=EXCLUDED.payload_json,
                    updated_at=EXCLUDED.updated_at
                """,
                (state_key, Jsonb(payload), now),
            )
        conn.commit()


def upsert_service_states(items: dict[str, dict[str, Any]], *, updated_at: int | None = None) -> None:
    """Upsert several service-state rows in one PostgreSQL transaction."""
    if not items:
        return
    initialize()
    if Jsonb is None:
        raise RuntimeError("psycopg JSON adapter is unavailable")
    now = int(updated_at or time.time())
    rows = [(str(key), Jsonb(payload), now) for key, payload in items.items()]
    with connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                f"""
                INSERT INTO {STATE_TABLE}(state_key, payload_json, updated_at)
                VALUES(%s,%s,%s)
                ON CONFLICT(state_key) DO UPDATE SET
                    payload_json=EXCLUDED.payload_json,
                    updated_at=EXCLUDED.updated_at
                """,
                rows,
            )
        conn.commit()

