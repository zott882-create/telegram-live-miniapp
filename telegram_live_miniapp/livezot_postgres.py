#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""PostgreSQL-only persistent storage for Live ZOT.

SQLite is intentionally not supported here.  PostgreSQL is the source of truth;
Redis and in-process dictionaries may only be used as disposable read caches.
"""
from __future__ import annotations

import hashlib
import json
import os
import threading
import time
from datetime import date, datetime, timezone
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

SCHEMA_VERSION = "002_prediction_archive_guard"
TABLE = "livezot_prediction_days"
ARCHIVE_TABLE = "livezot_prediction_archive"
BACKUP_RUNS_TABLE = "livezot_prediction_backup_runs"
MIGRATIONS_TABLE = "livezot_schema_migrations"
STATE_TABLE = "livezot_service_state"
_DAILY_BACKUP_STATE_KEY = "prediction_archive:daily:v24"


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


def _deep_merge_archive(old: Any, new: Any) -> Any:
    """Merge newer prediction data without letting empty values erase old data."""
    if isinstance(old, dict) and isinstance(new, dict):
        merged = dict(old)
        for key, value in new.items():
            if value is None or value == "":
                continue
            if key in merged:
                merged[key] = _deep_merge_archive(merged[key], value)
            else:
                merged[key] = value
        return merged
    if isinstance(new, list):
        return new if new else old
    return new if new is not None else old


def _prediction_payload(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, list):
        return {"feed": list(value)}
    return {}


def _prediction_rows(payload: Any) -> list[dict[str, Any]]:
    data = _prediction_payload(payload)
    rows = data.get("feed")
    if not isinstance(rows, list):
        return []
    return [dict(row) for row in rows if isinstance(row, dict)]


def _prediction_archive_key(row: dict[str, Any], index: int) -> str:
    for field in ("prediction_key", "source_slug"):
        value = str(row.get(field) or "").strip()
        if value:
            return f"{field}:{value}"
    fixture_id = str(row.get("fixture_id") or "").strip()
    market = str(row.get("market") or row.get("market_name") or "").strip().lower()
    pick = str(
        row.get("pick_code")
        or row.get("pick")
        or row.get("prediction")
        or row.get("value")
        or ""
    ).strip().lower()
    if fixture_id and (market or pick):
        return f"fixture:{fixture_id}:{market}:{pick}"
    stable = {
        "match": row.get("match_slug") or row.get("fixture_id") or row.get("match_id") or "",
        "home": row.get("home_name") or row.get("home") or "",
        "away": row.get("away_name") or row.get("away") or "",
        "market": market,
        "pick": pick,
        "source": row.get("source_page") or "",
    }
    raw = json.dumps(stable, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    if not any(str(v or "").strip() for v in stable.values()):
        raw = json.dumps(row, ensure_ascii=False, sort_keys=True, default=str, separators=(",", ":"))
    return f"hash:{hashlib.sha256(raw.encode('utf-8')).hexdigest()}:{index if not raw else ''}"


def _merge_prediction_payloads(*payloads: Any) -> dict[str, Any]:
    """Build a monotonic archive: a missing row can never remove an old forecast."""
    merged_payload: dict[str, Any] = {}
    merged_rows: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    for payload in payloads:
        data = _prediction_payload(payload)
        if not data:
            continue
        body = dict(data)
        rows = body.pop("feed", None)
        merged_payload = _deep_merge_archive(merged_payload, body)
        if isinstance(rows, list):
            for index, row in enumerate(rows):
                if not isinstance(row, dict):
                    continue
                item = dict(row)
                key = _prediction_archive_key(item, index)
                if key not in merged_rows:
                    merged_rows[key] = item
                    order.append(key)
                else:
                    merged_rows[key] = _deep_merge_archive(merged_rows[key], item)
    if order:
        merged_payload["feed"] = [merged_rows[key] for key in order]
    elif "feed" not in merged_payload:
        merged_payload["feed"] = []
    return merged_payload


def _archive_prediction_day_locked(
    cur: Any,
    source: str,
    stat_date: str,
    *,
    summary: Any = None,
    feed: Any = None,
    is_final: bool = False,
    updated_at: int | None = None,
) -> int:
    """Copy/merge a prediction day into the permanent archive inside a transaction."""
    now = int(time.time())
    cur.execute(
        f"SELECT summary_json, feed_json, is_final, source_updated_at FROM {ARCHIVE_TABLE} WHERE source=%s AND stat_date=%s::date FOR UPDATE",
        (source, stat_date),
    )
    archived = cur.fetchone()
    old_summary = _json_value(archived[0], {}) if archived else {}
    old_feed = _json_value(archived[1], None) if archived else None
    old_final = bool(archived[2]) if archived else False
    old_updated = int(archived[3] or 0) if archived else 0

    merged_feed = _merge_prediction_payloads(old_feed, feed)
    merged_summary = _deep_merge_archive(old_summary, summary if isinstance(summary, dict) else {})
    row_count = len(_prediction_rows(merged_feed))
    source_updated_at = max(old_updated, int(updated_at or 0))
    if Jsonb is None:
        raise RuntimeError("psycopg JSON adapter is unavailable")
    cur.execute(
        f"""
        INSERT INTO {ARCHIVE_TABLE}(
            source, stat_date, summary_json, feed_json, is_final,
            prediction_rows, first_archived_at, last_archived_at, source_updated_at
        )
        VALUES(%s,%s::date,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT(source, stat_date) DO UPDATE SET
            summary_json=EXCLUDED.summary_json,
            feed_json=EXCLUDED.feed_json,
            is_final=({ARCHIVE_TABLE}.is_final OR EXCLUDED.is_final),
            prediction_rows=GREATEST({ARCHIVE_TABLE}.prediction_rows, EXCLUDED.prediction_rows),
            last_archived_at=EXCLUDED.last_archived_at,
            source_updated_at=GREATEST({ARCHIVE_TABLE}.source_updated_at, EXCLUDED.source_updated_at)
        """,
        (
            source,
            stat_date,
            Jsonb(merged_summary),
            Jsonb(merged_feed),
            bool(old_final or is_final),
            row_count,
            int(archived[4] if archived and len(archived) > 4 else now) if False else now,
            now,
            source_updated_at,
        ),
    )
    return row_count


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
                        CREATE TABLE IF NOT EXISTS {ARCHIVE_TABLE} (
                            source TEXT NOT NULL,
                            stat_date DATE NOT NULL,
                            summary_json JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                            feed_json JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                            is_final BOOLEAN NOT NULL DEFAULT FALSE,
                            prediction_rows INTEGER NOT NULL DEFAULT 0,
                            first_archived_at BIGINT NOT NULL,
                            last_archived_at BIGINT NOT NULL,
                            source_updated_at BIGINT NOT NULL DEFAULT 0,
                            PRIMARY KEY(source, stat_date)
                        )
                        """
                    )
                    cur.execute(
                        f"CREATE INDEX IF NOT EXISTS idx_{ARCHIVE_TABLE}_source_date ON {ARCHIVE_TABLE}(source, stat_date DESC)"
                    )
                    cur.execute(
                        f"""
                        CREATE TABLE IF NOT EXISTS {BACKUP_RUNS_TABLE} (
                            backup_day DATE PRIMARY KEY,
                            backed_up_at BIGINT NOT NULL,
                            source_days INTEGER NOT NULL DEFAULT 0,
                            prediction_rows INTEGER NOT NULL DEFAULT 0,
                            reason TEXT NOT NULL DEFAULT 'daily'
                        )
                        """
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
        "archive_table": ARCHIVE_TABLE,
        "backup_runs_table": BACKUP_RUNS_TABLE,
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
    """Save a prediction day while first protecting the old and new rows in archive.

    The archive is monotonic: if a later filter/prune writes fewer forecasts,
    the missing forecasts remain recoverable from ``livezot_prediction_archive``.
    """
    initialize()
    now = int(updated_at or time.time())
    if Jsonb is None:
        raise RuntimeError("psycopg JSON adapter is unavailable")
    with connection() as conn:
        with conn.cursor() as cur:
            # Lock the current live row and archive it before any overwrite.
            cur.execute(
                f"SELECT summary_json, feed_json, is_final, updated_at FROM {TABLE} WHERE source=%s AND stat_date=%s::date FOR UPDATE",
                (source, stat_date),
            )
            previous = cur.fetchone()
            previous_summary = _json_value(previous[0], {}) if previous else {}
            previous_feed = _json_value(previous[1], None) if previous else None
            previous_final = bool(previous[2]) if previous else False
            previous_updated = int(previous[3] or 0) if previous else 0
            archive_feed = _merge_prediction_payloads(previous_feed, feed)
            archive_summary = _deep_merge_archive(previous_summary, summary)
            _archive_prediction_day_locked(
                cur,
                source,
                stat_date,
                summary=archive_summary,
                feed=archive_feed,
                is_final=bool(previous_final or is_final),
                updated_at=max(previous_updated, now),
            )

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
    """Delete old live rows only after copying them to the permanent archive."""
    initialize()
    with connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT stat_date, summary_json, feed_json, is_final, updated_at FROM {TABLE} WHERE source=%s AND stat_date < %s::date FOR UPDATE",
                (source, stat_date),
            )
            rows = cur.fetchall() or []
            for row in rows:
                day = row[0].isoformat() if isinstance(row[0], date) else str(row[0])
                _archive_prediction_day_locked(
                    cur,
                    source,
                    day,
                    summary=_json_value(row[1], {}),
                    feed=_json_value(row[2], None),
                    is_final=bool(row[3]),
                    updated_at=int(row[4] or 0),
                )
            cur.execute(
                f"DELETE FROM {TABLE} WHERE source=%s AND stat_date < %s::date",
                (source, stat_date),
            )
            deleted = int(cur.rowcount or 0)
        conn.commit()
    if deleted:
        print(f"[prediction-backup] protected-before-delete source={source} days={deleted}")
    return deleted


def get_archived_prediction_day(source: str, stat_date: str) -> dict[str, Any] | None:
    initialize()
    with connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT summary_json, feed_json, is_final, prediction_rows, first_archived_at, last_archived_at, source_updated_at FROM {ARCHIVE_TABLE} WHERE source=%s AND stat_date=%s::date",
                (source, stat_date),
            )
            row = cur.fetchone()
    if not row:
        return None
    return {
        "summary": _json_value(row[0], {}),
        "feed": _json_value(row[1], {}),
        "is_final": bool(row[2]),
        "prediction_rows": int(row[3] or 0),
        "first_archived_at": int(row[4] or 0),
        "last_archived_at": int(row[5] or 0),
        "source_updated_at": int(row[6] or 0),
    }


def restore_prediction_day_from_archive(source: str, stat_date: str, *, overwrite: bool = False) -> bool:
    """Manual recovery helper. Nothing restores automatically, so filters stay intact."""
    archived = get_archived_prediction_day(source, stat_date)
    if not archived:
        return False
    if not overwrite and get_prediction_day(source, stat_date):
        return False
    payload = archived.get("feed")
    summary = archived.get("summary") if isinstance(archived.get("summary"), dict) else {}
    upsert_prediction_day(
        source,
        stat_date,
        summary=summary,
        feed=payload if isinstance(payload, (dict, list)) else None,
        is_final=bool(archived.get("is_final")),
        updated_at=int(time.time()),
    )
    return True


def ensure_daily_prediction_archive_backup(*, reason: str = "daily") -> dict[str, Any]:
    """Once per UTC day, resync every live prediction day into the permanent archive.

    This is intentionally an in-PostgreSQL backup: Railway redeploys replace the
    container filesystem, while the PostgreSQL volume survives deployments.
    """
    initialize()
    today = datetime.now(timezone.utc).date().isoformat()
    now = int(time.time())
    with connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT backed_up_at, source_days, prediction_rows FROM {BACKUP_RUNS_TABLE} WHERE backup_day=%s::date",
                (today,),
            )
            existing = cur.fetchone()
            if existing:
                return {
                    "ok": True,
                    "skipped": True,
                    "backup_day": today,
                    "backed_up_at": int(existing[0] or 0),
                    "source_days": int(existing[1] or 0),
                    "prediction_rows": int(existing[2] or 0),
                }

            cur.execute(
                f"SELECT source, stat_date, summary_json, feed_json, is_final, updated_at FROM {TABLE} ORDER BY source, stat_date"
            )
            rows = cur.fetchall() or []
            total_prediction_rows = 0
            for row in rows:
                source = str(row[0] or "")
                day = row[1].isoformat() if isinstance(row[1], date) else str(row[1])
                total_prediction_rows += _archive_prediction_day_locked(
                    cur,
                    source,
                    day,
                    summary=_json_value(row[2], {}),
                    feed=_json_value(row[3], None),
                    is_final=bool(row[4]),
                    updated_at=int(row[5] or 0),
                )
            cur.execute(
                f"""
                INSERT INTO {BACKUP_RUNS_TABLE}(backup_day, backed_up_at, source_days, prediction_rows, reason)
                VALUES(%s::date,%s,%s,%s,%s)
                ON CONFLICT(backup_day) DO NOTHING
                """,
                (today, now, len(rows), total_prediction_rows, reason),
            )
            cur.execute(
                f"""
                INSERT INTO {STATE_TABLE}(state_key, payload_json, updated_at)
                VALUES(%s,%s,%s)
                ON CONFLICT(state_key) DO UPDATE SET payload_json=EXCLUDED.payload_json, updated_at=EXCLUDED.updated_at
                """,
                (
                    _DAILY_BACKUP_STATE_KEY,
                    Jsonb({
                        "backup_day": today,
                        "source_days": len(rows),
                        "prediction_rows": total_prediction_rows,
                        "reason": reason,
                    }),
                    now,
                ),
            )
        conn.commit()
    result = {
        "ok": True,
        "skipped": False,
        "backup_day": today,
        "backed_up_at": now,
        "source_days": len(rows),
        "prediction_rows": total_prediction_rows,
    }
    print(
        f"[prediction-backup] daily archive ok day={today} "
        f"source_days={len(rows)} predictions={total_prediction_rows}"
    )
    return result


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

