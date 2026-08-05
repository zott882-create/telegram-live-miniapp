#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Scores24 editorial prediction collector.

Public Scores24 pages are read without bypassing access controls. The collector
stores every discovered prediction permanently and refreshes unsettled matches.
"""
from __future__ import annotations

import hashlib
import html as html_lib
import json
import os
import re
import sqlite3
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable
from zoneinfo import ZoneInfo

BASE_URL = os.environ.get("SCORES24_BASE_URL", "https://scores24.live").rstrip("/")
LANG = (os.environ.get("SCORES24_LANG", "ru") or "ru").strip().lower()
TIMEZONE_NAME = (os.environ.get("APP_TIMEZONE", "Europe/Moscow") or "Europe/Moscow").strip()
REQUEST_TIMEOUT = max(8, int(os.environ.get("SCORES24_TIMEOUT", "25")))
REFRESH_SECONDS = max(300, int(os.environ.get("SCORES24_REFRESH_SECONDS", "300")))
MIN_ODD = max(1.0, float(os.environ.get("SCORES24_MIN_ODD", "1.50")))
MAX_ODD = max(MIN_ODD, float(os.environ.get("SCORES24_MAX_ODD", "3.50")))
MAX_PAGES_PER_SCAN = max(1, min(40, int(os.environ.get("SCORES24_MAX_PAGES_PER_SCAN", "8"))))
REQUEST_GAP_SECONDS = max(0.25, min(5.0, float(os.environ.get("SCORES24_REQUEST_GAP_SECONDS", "1.25"))))
RATE_LIMIT_BACKOFF_SECONDS = max(300, int(os.environ.get("SCORES24_RATE_LIMIT_BACKOFF_SECONDS", "900")))
INDEX_REFRESH_SECONDS = max(900, int(os.environ.get("SCORES24_INDEX_REFRESH_SECONDS", "1800")))
MAX_DISCOVERED_URLS = max(50, min(1000, int(os.environ.get("SCORES24_MAX_DISCOVERED_URLS", "500"))))
CATEGORY_REFRESH_SECONDS = max(600, int(os.environ.get("SCORES24_CATEGORY_REFRESH_SECONDS", "900")))
CATEGORY_BATCH_SIZE = max(1, min(10, int(os.environ.get("SCORES24_CATEGORY_BATCH_SIZE", "3"))))
LEAGUE_REFRESH_SECONDS = max(900, int(os.environ.get("SCORES24_LEAGUE_REFRESH_SECONDS", "1800")))
LEAGUE_BATCH_SIZE = max(1, min(4, int(os.environ.get("SCORES24_LEAGUE_BATCH_SIZE", "1"))))
MAX_DISCOVERED_LEAGUES = max(20, min(500, int(os.environ.get("SCORES24_MAX_DISCOVERED_LEAGUES", "250"))))
HISTORY_LOOKBACK_DAYS = max(1, min(31, int(os.environ.get("SCORES24_HISTORY_LOOKBACK_DAYS", "7"))))
MAX_RESULT_PAGES_PER_SCAN = max(1, min(12, int(os.environ.get("SCORES24_MAX_RESULT_PAGES_PER_SCAN", "3"))))
MAX_API_HISTORY_DATES_PER_SCAN = max(1, min(12, int(os.environ.get("SCORES24_API_HISTORY_DATES_PER_SCAN", "1"))))
HISTORY_START_DATE = (os.environ.get("SCORES24_HISTORY_START_DATE", "2026-08-05") or "2026-08-05").strip()
try:
    datetime.strptime(HISTORY_START_DATE, "%Y-%m-%d")
except Exception:
    HISTORY_START_DATE = "2026-08-05"
USER_AGENT = os.environ.get(
    "SCORES24_USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
).strip()

_REACT_STATE_RE = re.compile(r'window\.__REACT_QUERY_STATE__\s*=\s*JSON\.parse\("(.*?)"\);', re.S)
_URQL_DATA_RE = re.compile(r'window\.URQL_DATA\s*=\s*JSON\.parse\("(.*?)"\);', re.S)
_PREDICTION_LINK_RE = re.compile(
    r'(?:https?://scores24\.live)?/(?P<lang>[a-z]{2})/soccer/m-(?P<slug>[^"\'<>?#]+?)-prediction',
    re.I,
)
_LEAGUE_PREDICTION_LINK_RE = re.compile(
    r'(?:https?://scores24\.live)?/(?P<lang>[a-z]{2})/soccer/l-(?P<slug>[^"\'<>?#/]+?)/predictions',
    re.I,
)
_SCORE_RE = re.compile(r'(-?\d+)\s*[:\-]\s*(-?\d+)')
_SLUG_DATE_RE = re.compile(r'^(?:lm-)?(?P<day>\d{2})-(?P<month>\d{2})-(?P<year>\d{4})-', re.I)
_TAG_RE = re.compile(r'<[^>]+>')

_lock = threading.RLock()
_request_lock = threading.Lock()
_fetch_cache: dict[str, tuple[float, str]] = {}
_next_request_at = 0.0
_blocked_until = 0.0
_category_cursor = 0
_league_cursor = 0
_discovered_league_urls: list[str] = []
_started = False
_stop = threading.Event()
_data_dir = Path(os.environ.get("SCORES24_DATA_DIR", "")).expanduser() if os.environ.get("SCORES24_DATA_DIR") else None
_db_path: Path | None = None
_pg_conn_factory: Callable[[], Any] | None = None
_database_url = ""
_api_fixture_resolver: Callable[[dict[str, Any], bool], dict[str, Any] | None] | None = None
_api_statistics_loader: Callable[[int, bool], list[dict[str, Any]]] | None = None
_date_migration_done = False


def configure(*, data_dir: Path, pg_conn_factory: Callable[[], Any] | None = None, database_url: str = "") -> None:
    global _data_dir, _db_path, _pg_conn_factory, _database_url
    _data_dir = Path(data_dir)
    _data_dir.mkdir(parents=True, exist_ok=True)
    _db_path = _data_dir / "scores24_prediction_history.sqlite3"
    _pg_conn_factory = pg_conn_factory
    _database_url = str(database_url or "")
    _storage_init()


def configure_api_football(
    *,
    fixture_resolver: Callable[[dict[str, Any], bool], dict[str, Any] | None],
    statistics_loader: Callable[[int, bool], list[dict[str, Any]]] | None = None,
) -> None:
    """Attach API-Football as the match-data provider.

    Scores24 remains the only source of the editorial pick and published odd.
    The callbacks are injected by ``combined_app`` so API calls share its cache,
    quota tracking and rate limiter.
    """
    global _api_fixture_resolver, _api_statistics_loader
    _api_fixture_resolver = fixture_resolver
    _api_statistics_loader = statistics_loader


def _api_football_enabled() -> bool:
    return _api_fixture_resolver is not None


def _storage_mode() -> str:
    return "postgres" if _pg_conn_factory is not None and _database_url else "sqlite"


def _storage_init() -> None:
    if _data_dir is None:
        return
    if _storage_mode() == "postgres":
        try:
            with _pg_conn_factory() as conn:  # type: ignore[misc]
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS scores24_prediction_daily_history (
                            stat_date TEXT PRIMARY KEY,
                            payload_json TEXT NOT NULL,
                            is_final BOOLEAN NOT NULL DEFAULT FALSE,
                            updated_at BIGINT NOT NULL
                        )
                        """
                    )
                conn.commit()
            return
        except Exception as exc:
            print(f"[scores24] postgres init failed, sqlite fallback: {exc}")
    if _db_path is None:
        return
    with sqlite3.connect(str(_db_path), timeout=30) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS scores24_prediction_daily_history (
                stat_date TEXT PRIMARY KEY,
                payload_json TEXT NOT NULL,
                is_final INTEGER NOT NULL DEFAULT 0,
                updated_at INTEGER NOT NULL
            )
            """
        )
        conn.commit()


def _storage_get(date_text: str) -> dict[str, Any] | None:
    if str(date_text or "") < HISTORY_START_DATE:
        return None
    _storage_init()
    try:
        if _storage_mode() == "postgres":
            with _pg_conn_factory() as conn:  # type: ignore[misc]
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT payload_json, is_final, updated_at FROM scores24_prediction_daily_history WHERE stat_date=%s",
                        (date_text,),
                    )
                    row = cur.fetchone()
        else:
            if _db_path is None:
                return None
            with sqlite3.connect(str(_db_path), timeout=30) as conn:
                row = conn.execute(
                    "SELECT payload_json, is_final, updated_at FROM scores24_prediction_daily_history WHERE stat_date=?",
                    (date_text,),
                ).fetchone()
        if not row:
            return None
        payload = json.loads(row[0] or "{}")
        payload["is_final"] = bool(row[1])
        payload["updated_at"] = int(row[2] or payload.get("updated_at") or 0)
        return payload
    except Exception as exc:
        print(f"[scores24] storage get failed date={date_text}: {exc}")
        return None


def _storage_records_full() -> list[dict[str, Any]]:
    """Return saved payloads from the configured prediction history start."""
    _storage_init()
    rows: list[Any] = []
    try:
        if _storage_mode() == "postgres":
            with _pg_conn_factory() as conn:  # type: ignore[misc]
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT stat_date,payload_json,is_final,updated_at FROM scores24_prediction_daily_history WHERE stat_date >= %s ORDER BY stat_date",
                        (HISTORY_START_DATE,),
                    )
                    rows = cur.fetchall() or []
        elif _db_path is not None:
            with sqlite3.connect(str(_db_path), timeout=30) as conn:
                rows = conn.execute(
                    "SELECT stat_date,payload_json,is_final,updated_at FROM scores24_prediction_daily_history WHERE stat_date >= ? ORDER BY stat_date",
                    (HISTORY_START_DATE,),
                ).fetchall()
    except Exception as exc:
        print(f"[scores24] storage records failed: {exc}")
        return []
    out: list[dict[str, Any]] = []
    for date_text, raw, is_final, updated_at in rows:
        try:
            payload = json.loads(raw or "{}")
        except Exception:
            payload = {}
        payload["date"] = str(date_text)
        payload["is_final"] = bool(is_final)
        payload["updated_at"] = int(updated_at or payload.get("updated_at") or 0)
        out.append(payload)
    return out


def period_summary(date_from: str, date_to: str) -> dict[str, Any]:
    """Aggregate saved Scores24 results for an inclusive date range."""
    if not (_valid_date(date_from) and _valid_date(date_to)):
        raise ValueError("invalid date range")
    start = datetime.strptime(date_from, "%Y-%m-%d").date()
    history_start = datetime.strptime(HISTORY_START_DATE, "%Y-%m-%d").date()
    if start < history_start:
        start = history_start
        date_from = HISTORY_START_DATE
    finish = datetime.strptime(date_to, "%Y-%m-%d").date()
    if finish < start:
        raise ValueError("invalid date range")
    if (finish - start).days > 62:
        raise ValueError("date range is too large")

    by_date: list[dict[str, Any]] = []
    won = lost = push = pending = feed_total = settled = 0
    stake = returned = 0.0
    final_days = 0
    current = start
    while current <= finish:
        date_text = current.isoformat()
        payload = _storage_get(date_text) or {}
        day_settled = int(payload.get("predicted_total") or 0)
        day_won = int(payload.get("won_total") or 0)
        day_lost = int(payload.get("lost_total") or 0)
        day_push = int(payload.get("push_total") or 0)
        day_pending = int(payload.get("pending_result_total") or 0)
        day_feed = int(payload.get("feed_total") or len(payload.get("feed") or []))
        day_stake = float(payload.get("stake_total") or 0)
        day_returned = float(payload.get("returned_total") or 0)
        won += day_won
        lost += day_lost
        push += day_push
        pending += day_pending
        feed_total += day_feed
        settled += day_settled
        stake += day_stake
        returned += day_returned
        if payload.get("is_final"):
            final_days += 1
        by_date.append({
            "date": date_text,
            "feed_total": day_feed,
            "predicted_total": day_settled,
            "won_total": day_won,
            "lost_total": day_lost,
            "push_total": day_push,
            "pending_result_total": day_pending,
            "profit_total": round(float(payload.get("profit_total") or 0), 2),
            "roi_percent": round(float(payload.get("roi_percent") or 0), 2),
            "is_final": bool(payload.get("is_final")),
        })
        current += timedelta(days=1)

    profit = returned - stake
    roi = (profit / stake) * 100 if stake else 0.0
    hit_rate = (won / max(1, won + lost)) * 100 if won + lost else 0.0
    total_days = (finish - start).days + 1
    return {
        "source": "Scores24",
        "date_from": date_from,
        "date_to": date_to,
        "days_total": total_days,
        "days_with_predictions": sum(1 for day in by_date if day["feed_total"]),
        "final_days": final_days,
        "feed_total": feed_total,
        "predicted_total": settled,
        "won_total": won,
        "lost_total": lost,
        "push_total": push,
        "pending_result_total": pending,
        "hit_rate": round(hit_rate, 2),
        "stake_total": round(stake, 2),
        "returned_total": round(returned, 2),
        "profit_total": round(profit, 2),
        "roi_percent": round(roi, 2),
        "by_date": by_date,
        "updated_at": int(time.time()),
        "note": "Only predictions still exposed by Scores24 or previously saved by the bot are included.",
    }


def _storage_save(date_text: str, payload: dict[str, Any], *, is_final: bool = False) -> None:
    if str(date_text or "") < HISTORY_START_DATE:
        return
    _storage_init()
    now = int(time.time())
    saved = dict(payload)
    saved["date"] = date_text
    saved["updated_at"] = now
    saved["is_final"] = bool(is_final)
    raw = json.dumps(saved, ensure_ascii=False, separators=(",", ":"))
    try:
        if _storage_mode() == "postgres":
            with _pg_conn_factory() as conn:  # type: ignore[misc]
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO scores24_prediction_daily_history(stat_date,payload_json,is_final,updated_at)
                        VALUES(%s,%s,%s,%s)
                        ON CONFLICT(stat_date) DO UPDATE SET
                            payload_json=EXCLUDED.payload_json,
                            is_final=EXCLUDED.is_final,
                            updated_at=EXCLUDED.updated_at
                        """,
                        (date_text, raw, bool(is_final), now),
                    )
                conn.commit()
        else:
            if _db_path is None:
                return
            with sqlite3.connect(str(_db_path), timeout=30) as conn:
                conn.execute(
                    """
                    INSERT INTO scores24_prediction_daily_history(stat_date,payload_json,is_final,updated_at)
                    VALUES(?,?,?,?)
                    ON CONFLICT(stat_date) DO UPDATE SET
                        payload_json=excluded.payload_json,
                        is_final=excluded.is_final,
                        updated_at=excluded.updated_at
                    """,
                    (date_text, raw, 1 if is_final else 0, now),
                )
                conn.commit()
    except Exception as exc:
        print(f"[scores24] storage save failed date={date_text}: {exc}")


def _storage_delete_before(date_text: str = HISTORY_START_DATE) -> int:
    """Remove legacy prediction days. The new database starts on 2026-08-05."""
    _storage_init()
    deleted = 0
    try:
        if _storage_mode() == "postgres":
            with _pg_conn_factory() as conn:  # type: ignore[misc]
                with conn.cursor() as cur:
                    cur.execute(
                        "DELETE FROM scores24_prediction_daily_history WHERE stat_date < %s",
                        (date_text,),
                    )
                    deleted = int(cur.rowcount or 0)
                conn.commit()
        elif _db_path is not None:
            with sqlite3.connect(str(_db_path), timeout=30) as conn:
                cur = conn.execute(
                    "DELETE FROM scores24_prediction_daily_history WHERE stat_date < ?",
                    (date_text,),
                )
                deleted = int(cur.rowcount or 0)
                conn.commit()
    except Exception as exc:
        print(f"[scores24] legacy cleanup failed: {exc}")
    return deleted


def history() -> list[dict[str, Any]]:
    _storage_init()
    rows: list[Any] = []
    try:
        if _storage_mode() == "postgres":
            with _pg_conn_factory() as conn:  # type: ignore[misc]
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT stat_date,payload_json,is_final,updated_at FROM scores24_prediction_daily_history WHERE stat_date >= %s ORDER BY stat_date DESC",
                        (HISTORY_START_DATE,),
                    )
                    rows = cur.fetchall() or []
        elif _db_path is not None:
            with sqlite3.connect(str(_db_path), timeout=30) as conn:
                rows = conn.execute(
                    "SELECT stat_date,payload_json,is_final,updated_at FROM scores24_prediction_daily_history WHERE stat_date >= ? ORDER BY stat_date DESC",
                    (HISTORY_START_DATE,),
                ).fetchall()
    except Exception as exc:
        print(f"[scores24] history failed: {exc}")
    out: list[dict[str, Any]] = []
    for date_text, raw, is_final, updated_at in rows:
        try:
            payload = json.loads(raw or "{}")
        except Exception:
            payload = {}
        out.append({
            "date": str(date_text),
            "predicted_total": int(payload.get("predicted_total") or 0),
            "won_total": int(payload.get("won_total") or 0),
            "lost_total": int(payload.get("lost_total") or 0),
            "hit_rate": float(payload.get("hit_rate") or 0),
            "profit_total": float(payload.get("profit_total") or 0),
            "roi_percent": float(payload.get("roi_percent") or 0),
            "is_final": bool(is_final),
            "updated_at": int(updated_at or 0),
            "source": "Scores24",
        })
    return out


def _tz() -> ZoneInfo:
    try:
        return ZoneInfo(TIMEZONE_NAME)
    except Exception:
        return ZoneInfo("UTC")


def _today() -> str:
    return datetime.now(_tz()).strftime("%Y-%m-%d")


def _valid_date(value: str) -> bool:
    return bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}", value or ""))


class Scores24RateLimited(RuntimeError):
    def __init__(self, retry_after: int) -> None:
        self.retry_after = max(1, int(retry_after))
        super().__init__(f"Scores24 временно ограничил запросы. Повтор через {max(1, (self.retry_after + 59) // 60)} мин.")


def _headers() -> dict[str, str]:
    return {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8",
        "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.7",
        "Referer": f"{BASE_URL}/{LANG}/predictions/soccer",
        "DNT": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
    }


def _compact_error(exc: Exception) -> str:
    if isinstance(exc, Scores24RateLimited):
        return str(exc)
    text = re.sub(r"\s+", " ", str(exc or "")).strip()
    text = re.sub(r"<!DOCTYPE.*", "", text, flags=re.I)
    if "HTTP 429" in text:
        return "Scores24 временно ограничил запросы. Сохранённые прогнозы остаются доступны, повтор будет позже."
    return text[:220] or "Не удалось получить данные Scores24"


def _fetch_text(url: str, *, force: bool = False, ttl: int = REFRESH_SECONDS) -> str:
    global _next_request_at, _blocked_until
    now = time.time()
    with _lock:
        cached = _fetch_cache.get(url)
        if cached and not force and cached[0] > now:
            return cached[1]
        blocked_for = int(max(0, _blocked_until - now))
        if blocked_for > 0:
            if cached:
                return cached[1]
            raise Scores24RateLimited(blocked_for)

    # All outbound Scores24 requests are serialized and spaced out. This keeps
    # one Railway instance from opening dozens of pages at once.
    with _request_lock:
        now = time.time()
        wait_for = max(0.0, _next_request_at - now)
        if wait_for:
            time.sleep(wait_for)
        _next_request_at = time.time() + REQUEST_GAP_SECONDS
        request = urllib.request.Request(url, headers=_headers(), method="GET")
        try:
            with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT) as response:
                raw = response.read()
                encoding = response.headers.get_content_charset() or "utf-8"
                text = raw.decode(encoding, "replace")
        except urllib.error.HTTPError as exc:
            if exc.code == 429:
                retry_header = str(exc.headers.get("Retry-After") or "").strip()
                retry_after = int(retry_header) if retry_header.isdigit() else RATE_LIMIT_BACKOFF_SECONDS
                retry_after = max(RATE_LIMIT_BACKOFF_SECONDS, retry_after)
                with _lock:
                    _blocked_until = max(_blocked_until, time.time() + retry_after)
                if cached:
                    return cached[1]
                raise Scores24RateLimited(retry_after) from exc
            raise RuntimeError(f"Scores24 HTTP {exc.code}") from exc
        except Exception as exc:
            if cached:
                return cached[1]
            raise RuntimeError(f"Scores24 request failed: {exc}") from exc

    now = time.time()
    with _lock:
        _fetch_cache[url] = (now + max(30, int(ttl)), text)
        if len(_fetch_cache) > 800:
            for key in list(_fetch_cache)[:200]:
                _fetch_cache.pop(key, None)
    return text


def _absolute_prediction_url(slug: str) -> str:
    clean = str(slug or "").strip().strip("/")
    if clean.startswith("m-"):
        clean = clean[2:]
    if clean.endswith("-prediction"):
        clean = clean[:-11]
    return f"{BASE_URL}/{LANG}/soccer/m-{clean}-prediction"


def _react_queries(page_html: str) -> list[dict[str, Any]]:
    match = _REACT_STATE_RE.search(page_html or "")
    if not match:
        return []
    try:
        decoded = json.loads('"' + match.group(1) + '"')
        state = json.loads(decoded)
    except Exception as exc:
        raise RuntimeError(f"Scores24 state parse failed: {exc}") from exc
    queries = state.get("queries") if isinstance(state, dict) else []
    return queries if isinstance(queries, list) else []


def _query_data(queries: list[dict[str, Any]], query_id: str) -> Any:
    for query in queries:
        key_list = query.get("queryKey") or []
        key = key_list[0] if key_list and isinstance(key_list[0], dict) else {}
        if key.get("_id") != query_id:
            continue
        state = query.get("state") or {}
        wrapper = state.get("data") or {}
        return wrapper.get("data")
    return None


def _urql_payloads(page_html: str) -> list[dict[str, Any]]:
    match = _URQL_DATA_RE.search(page_html or "")
    if not match:
        return []
    try:
        decoded = json.loads('"' + match.group(1) + '"')
        cache = json.loads(decoded)
    except Exception as exc:
        raise RuntimeError(f"Scores24 URQL parse failed: {exc}") from exc
    out: list[dict[str, Any]] = []
    for entry in cache.values() if isinstance(cache, dict) else []:
        raw = entry.get("data") if isinstance(entry, dict) else None
        if not isinstance(raw, str):
            continue
        try:
            payload = json.loads(raw)
        except Exception:
            continue
        if isinstance(payload, dict):
            out.append(payload)
    return out


def _clean_text(value: Any) -> str:
    text = _TAG_RE.sub(" ", str(value or ""))
    text = html_lib.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def _float(value: Any) -> float | None:
    try:
        return float(str(value).strip().replace(",", "."))
    except Exception:
        return None


def _line_text(value: Any) -> str:
    text = str(value or "").strip().replace("_", ".").replace(",", ".")
    return re.sub(r"(?<=\d)\.0$", "", text)


def _parse_iso(value: Any, *, assume_utc: bool = False) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None
    if parsed.tzinfo is None and assume_utc:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _prediction_date_from_slug(slug: Any) -> str:
    """Return the publication/match day encoded by a Scores24 prediction slug."""
    match = _SLUG_DATE_RE.search(str(slug or "").strip())
    if not match:
        return ""
    try:
        return datetime(
            int(match.group("year")),
            int(match.group("month")),
            int(match.group("day")),
        ).strftime("%Y-%m-%d")
    except Exception:
        return ""


def _row_prediction_date(row: dict[str, Any]) -> str:
    for value in (
        row.get("scores24_prediction_date"),
        row.get("prediction_date"),
        ((row.get("fixture_item") or {}).get("scores24") or {}).get("prediction_date"),
        _prediction_date_from_slug(row.get("source_slug")),
        str((((row.get("fixture_item") or {}).get("fixture") or {}).get("date") or ""))[:10],
    ):
        text = str(value or "")[:10]
        if _valid_date(text):
            return text
    return ""


def _score_from_match(match_data: dict[str, Any]) -> tuple[int | None, int | None]:
    candidates = [match_data.get("resultScore")]
    for item in match_data.get("resultScores") or []:
        if str(item.get("type") or "").upper() in {"FT", "AET", "PEN"}:
            candidates.insert(0, item.get("value"))
    for value in candidates:
        found = _SCORE_RE.search(str(value or ""))
        if found:
            home, away = int(found.group(1)), int(found.group(2))
            if home >= 0 and away >= 0:
                return home, away
    return None, None


def _stable_id(slug: str) -> int:
    return int(hashlib.sha256(slug.encode("utf-8")).hexdigest()[:12], 16)


def _team_name(teams: list[dict[str, Any]], index: int, fallback: str) -> str:
    if index < len(teams) and isinstance(teams[index], dict):
        return str(teams[index].get("name") or fallback)
    return fallback


def _canonical_prediction(raw_market: str, raw_value: Any, teams: list[dict[str, Any]]) -> dict[str, Any]:
    raw = str(raw_market or "").strip().lower()
    value = str(raw_value or "").strip().lower()
    home = _team_name(teams, 0, "Хозяева")
    away = _team_name(teams, 1, "Гости")
    line_text = _line_text(raw_value)
    line = _float(line_text)
    market = raw or "other"
    market_label = "Другой рынок"
    pick_code = value.upper() or raw.upper()
    pick_label = (raw + " " + value).replace("_", " ").strip().upper()
    settlement_supported = False

    if raw == "one_x_two":
        market = "1x2"
        market_label = "Исход матча"
        pick_code = {"w1": "1", "x": "X", "w2": "2"}.get(value, value.upper())
        pick_label = {"w1": f"Победа {home}", "x": "Ничья", "w2": f"Победа {away}"}.get(value, value.upper())
        settlement_supported = pick_code in {"1", "X", "2"}
    elif raw in {"both_to_score", "both_teams_to_score", "btts"}:
        market = "btts"
        market_label = "Обе забьют"
        yes = value in {"yes", "y", "1", "true"}
        pick_code = "YES" if yes else "NO"
        pick_label = "Обе забьют — Да" if yes else "Обе забьют — Нет"
        settlement_supported = True
    elif raw in {"double_chance", "doublechance"}:
        market = "double_chance"
        market_label = "Двойной шанс"
        compact = value.replace("w", "").replace("_", "").upper()
        compact = {"X1": "1X", "2X": "X2", "21": "12"}.get(compact, compact)
        pick_code = compact
        pick_label = {"1X": f"{home} или ничья", "X2": f"Ничья или {away}", "12": "Без ничьей"}.get(compact, compact)
        settlement_supported = compact in {"1X", "X2", "12"}
    elif raw == "correct_score" or "correct_score" in raw:
        market = "correct_score"
        market_label = "Точный счёт"
        score = line_text.replace(".", ":")
        pick_code = score
        pick_label = f"Точный счёт {score}"
        settlement_supported = bool(re.fullmatch(r"\d+:\d+", score))
    elif raw.startswith("total_t1_") or raw.startswith("team1_total_"):
        direction = "over" if "over" in raw else "under"
        market = "team_total_home"
        market_label = f"Тотал {home}"
        pick_code = f"{direction.upper()} {line_text}"
        pick_label = f"ИТБ {home} {line_text}" if direction == "over" else f"ИТМ {home} {line_text}"
        settlement_supported = line is not None and abs(line * 2 - round(line * 2)) < 1e-9
    elif raw.startswith("total_t2_") or raw.startswith("team2_total_"):
        direction = "over" if "over" in raw else "under"
        market = "team_total_away"
        market_label = f"Тотал {away}"
        pick_code = f"{direction.upper()} {line_text}"
        pick_label = f"ИТБ {away} {line_text}" if direction == "over" else f"ИТМ {away} {line_text}"
        settlement_supported = line is not None and abs(line * 2 - round(line * 2)) < 1e-9
    elif "corner" in raw:
        direction = "over" if "over" in raw else "under" if "under" in raw else ""
        market = "corners"
        market_label = "Угловые"
        pick_code = f"{direction.upper()} {line_text}".strip()
        pick_label = ("Угловые ТБ " if direction == "over" else "Угловые ТМ " if direction == "under" else "Угловые ") + line_text
    elif "red" in raw and "card" in raw:
        direction = "over" if "over" in raw else "under" if "under" in raw else ""
        market = "red_cards"
        market_label = "Красные карточки"
        pick_code = f"{direction.upper()} {line_text}".strip()
        pick_label = ("Красные ТБ " if direction == "over" else "Красные ТМ " if direction == "under" else "Красные ") + line_text
    elif "yellow" in raw or "card" in raw:
        direction = "over" if "over" in raw else "under" if "under" in raw else ""
        market = "yellow_cards"
        market_label = "Жёлтые карточки"
        pick_code = f"{direction.upper()} {line_text}".strip()
        pick_label = ("Жёлтые ТБ " if direction == "over" else "Жёлтые ТМ " if direction == "under" else "Жёлтые ") + line_text
    elif raw in {"total_over", "total_under"} or (raw.startswith("total_") and ("over" in raw or "under" in raw)):
        direction = "over" if "over" in raw else "under"
        market = "total"
        market_label = "Тотал голов"
        pick_code = f"{direction.upper()} {line_text}"
        pick_label = ("ТБ " if direction == "over" else "ТМ ") + line_text
        settlement_supported = line is not None and abs(line * 2 - round(line * 2)) < 1e-9
    elif raw.startswith("handicap") or raw.startswith("fora"):
        market = "handicap"
        market_label = "Фора"
        side = "1" if raw.endswith("1") or value.startswith("1:") else "2" if raw.endswith("2") or value.startswith("2:") else ""
        if ":" in line_text and not side:
            side, line_text = line_text.split(":", 1)
            line = _float(line_text)
        team = home if side == "1" else away if side == "2" else "команды"
        pick_code = f"H{side} {line_text}".strip()
        pick_label = f"Фора {team} ({line_text})"
        settlement_supported = side in {"1", "2"} and line is not None and abs(line * 2 - round(line * 2)) < 1e-9
    elif raw in {"w1", "x", "w2"}:
        return _canonical_prediction("one_x_two", raw, teams)

    return {
        "market": market,
        "market_label": market_label,
        "pick_code": pick_code,
        "pick_label": pick_label.strip(),
        "line": line,
        "line_text": line_text,
        "raw_market": raw,
        "raw_value": value,
        "settlement_supported": settlement_supported,
    }


def _compare_total(actual: float, direction: str, line: float) -> tuple[str, bool | None]:
    if actual == line:
        return "push", None
    won = actual > line if direction == "over" else actual < line
    return ("won" if won else "lost"), won


def _settle_row(row: dict[str, Any], home: int | None, away: int | None) -> tuple[str, bool | None, str]:
    if home is None or away is None:
        return "", None, ""
    market = str(row.get("market") or "").lower()
    code = str(row.get("pick_code") or "").upper()
    raw_market = str(row.get("source_market") or "").lower()
    raw_value = row.get("source_pick_code")
    line = _float(row.get("line"))
    result_code = "1" if home > away else "2" if away > home else "X"

    if market == "1x2":
        won = code == result_code
        return ("won" if won else "lost"), won, result_code
    if market == "double_chance":
        won = result_code in ({"1", "X"} if code == "1X" else {"X", "2"} if code == "X2" else {"1", "2"})
        return ("won" if won else "lost"), won, result_code
    if market == "btts":
        yes = home > 0 and away > 0
        expected = code == "YES"
        won = yes == expected
        return ("won" if won else "lost"), won, result_code
    if market == "correct_score":
        won = code == f"{home}:{away}"
        return ("won" if won else "lost"), won, f"{home}:{away}"
    if market in {"total", "team_total_home", "team_total_away"} and line is not None:
        actual = float(home + away) if market == "total" else float(home if market == "team_total_home" else away)
        direction = "over" if "OVER" in code or "over" in raw_market else "under"
        settlement, won = _compare_total(actual, direction, line)
        return settlement, won, f"{home}:{away}"
    if market == "handicap" and line is not None:
        side = "1" if code.startswith("H1") or raw_market.endswith("1") else "2" if code.startswith("H2") or raw_market.endswith("2") else ""
        adjusted = (home + line - away) if side == "1" else (away + line - home) if side == "2" else None
        if adjusted is None:
            return "", None, f"{home}:{away}"
        if adjusted == 0:
            return "push", None, f"{home}:{away}"
        won = adjusted > 0
        return ("won" if won else "lost"), won, f"{home}:{away}"
    return "", None, f"{home}:{away}"


def _fixture_from_index(match_data: dict[str, Any], league_data: dict[str, Any]) -> dict[str, Any] | None:
    teams = match_data.get("teams") or []
    if not isinstance(teams, list) or len(teams) < 2:
        return None
    match_dt = _parse_iso(match_data.get("matchDate"), assume_utc=True)
    if match_dt is None:
        return None
    local_dt = match_dt.astimezone(_tz())
    slug = str(match_data.get("slug") or "").strip()
    if not slug:
        return None
    prediction_date = _prediction_date_from_slug(slug) or local_dt.strftime("%Y-%m-%d")
    country = match_data.get("country") or league_data.get("country") or {}
    tournament = match_data.get("uniqueTournament") or league_data or {}
    home_team = teams[0] if isinstance(teams[0], dict) else {}
    away_team = teams[1] if isinstance(teams[1], dict) else {}
    return {
        "fixture": {
            "id": _stable_id(slug),
            "date": local_dt.isoformat(),
            "timestamp": int(local_dt.timestamp()),
            "timezone": TIMEZONE_NAME,
            "status": {"short": "NS", "long": "Не начался", "elapsed": None},
        },
        "league": {
            "id": _stable_id(str(tournament.get("slug") or match_data.get("leagueSlug") or "scores24")),
            "name": tournament.get("name") or league_data.get("name") or "Scores24",
            "country": country.get("name") or "Мир",
            "logo": tournament.get("logo") or league_data.get("logo") or "",
            "flag": country.get("logo") or "",
            "round": "Редакционные прогнозы Scores24",
        },
        "teams": {
            "home": {"id": _stable_id(str(home_team.get("slug") or home_team.get("name") or "home")), "name": home_team.get("name"), "logo": home_team.get("logo") or ""},
            "away": {"id": _stable_id(str(away_team.get("slug") or away_team.get("name") or "away")), "name": away_team.get("name"), "logo": away_team.get("logo") or ""},
        },
        "goals": {"home": None, "away": None},
        "score": {"fulltime": {"home": None, "away": None}},
        "scores24": {
            "source_url": _absolute_prediction_url(slug),
            "slug": slug,
            "prediction_date": prediction_date,
            "listed_match_date": local_dt.isoformat(),
        },
    }


def _row_from_prediction_node(
    node: dict[str, Any],
    source_page: str,
    league_data: dict[str, Any] | None = None,
    sport_context: str = "",
) -> dict[str, Any] | None:
    if sport_context and sport_context.lower() != "soccer":
        return None
    prediction = node.get("prediction") or []
    match_data = node.get("match") or {}
    if not isinstance(prediction, list) or len(prediction) < 2 or not isinstance(match_data, dict):
        return None
    odd = _float(node.get("predictionValue"))
    if odd is None or odd < MIN_ODD or odd > MAX_ODD:
        return None
    league_data = match_data.get("uniqueTournament") or league_data or {}
    if not isinstance(league_data, dict):
        league_data = {}
    league_sport = str(league_data.get("sportSlug") or "").strip().lower()
    if league_sport and league_sport != "soccer":
        return None
    fixture_item = _fixture_from_index(match_data, league_data)
    if not fixture_item:
        return None
    teams = match_data.get("teams") or []
    canonical = _canonical_prediction(str(prediction[0] or ""), prediction[1], teams)
    slug = str(match_data.get("slug") or "")
    raw_market = str(prediction[0] or "").lower()
    raw_value = str(prediction[1] or "").lower()
    source_url = _absolute_prediction_url(slug)
    return {
        "source": "Scores24",
        "source_url": source_url,
        "source_page": source_page,
        "source_slug": slug,
        "scores24_prediction_date": _prediction_date_from_slug(slug) or str((fixture_item.get("fixture") or {}).get("date") or "")[:10],
        "prediction_date": _prediction_date_from_slug(slug) or str((fixture_item.get("fixture") or {}).get("date") or "")[:10],
        "match_date": str((fixture_item.get("fixture") or {}).get("date") or "")[:10],
        "fixture_id": int((fixture_item.get("fixture") or {}).get("id") or _stable_id(slug)),
        "prediction_key": f"scores24:{slug}:{raw_market}:{raw_value}",
        "market": canonical["market"],
        "market_label": canonical["market_label"],
        "source_market": raw_market,
        "pick_code": canonical["pick_code"],
        "source_pick_code": raw_value,
        "pick_label": canonical["pick_label"],
        "line": canonical["line"],
        "line_text": canonical["line_text"],
        "settlement_supported": bool(canonical["settlement_supported"]),
        "odd": round(odd, 3),
        "probability": None,
        "community_percent": int(node.get("agreedVotesPercent") or 0),
        "votes_total": int(node.get("allVotesCount") or 0),
        "bookmaker": "Scores24",
        "bookmaker_logo": "",
        "author": "Редакция Scores24",
        "author_avatar": "",
        "published_at": None,
        "modified_at": None,
        "editorial_text": "",
        "handwritten": True,
        "settlement": "",
        "won": None,
        "result_code": "",
        "result_pending": False,
        "fixture_item": fixture_item,
    }


def _rows_from_index_page(page_html: str, source_page: str) -> list[dict[str, Any]]:
    """Read prediction nodes from category, league and general Scores24 pages.

    Scores24 uses different GraphQL root names on different pages. Searching
    recursively for the stable prediction node shape is more reliable than
    depending only on ``LeaguesPrediction``.
    """
    rows: list[dict[str, Any]] = []

    def walk(
        value: Any,
        league_context: dict[str, Any] | None = None,
        sport_context: str = "",
    ) -> None:
        if isinstance(value, list):
            for item in value:
                walk(item, league_context, sport_context)
            return
        if not isinstance(value, dict):
            return
        current_league = league_context
        current_sport = sport_context
        if str(value.get("__typename") or "") == "SportPredictionListType":
            current_sport = str(value.get("slug") or current_sport).strip().lower()
        own_league = value.get("league")
        if isinstance(own_league, dict):
            current_league = own_league
        if isinstance(value.get("prediction"), list) and isinstance(value.get("match"), dict):
            row = _row_from_prediction_node(value, source_page, current_league, current_sport)
            if row:
                rows.append(row)
        for key, child in value.items():
            next_league = current_league
            if key in {"league", "uniqueTournament"} and isinstance(child, dict):
                next_league = child
            walk(child, next_league, current_sport)

    for payload in _urql_payloads(page_html):
        walk(payload)
    dedup: dict[str, dict[str, Any]] = {}
    for row in rows:
        dedup[str(row["prediction_key"])] = row
    return list(dedup.values())


def _parse_match_status_page(page_html: str, source_url: str) -> dict[str, Any] | None:
    queries = _react_queries(page_html)
    match_data = _query_data(queries, "matchShow") or {}
    if not isinstance(match_data, dict) or not match_data.get("slug"):
        return None
    match_dt = _parse_iso(match_data.get("matchDate"), assume_utc=True)
    if match_dt is None:
        return None
    local_dt = match_dt.astimezone(_tz())
    finished = bool(match_data.get("isFinished"))
    live = bool(match_data.get("isLive"))
    home_score, away_score = _score_from_match(match_data) if (finished or live) else (None, None)
    status = match_data.get("status") or {}
    return {
        "source_url": source_url,
        "source_slug": str(match_data.get("slug") or ""),
        "date": local_dt.isoformat(),
        "timestamp": int(local_dt.timestamp()),
        "finished": finished,
        "live": live,
        "status_short": "FT" if finished else "LIVE" if live else "NS",
        "status_long": "Матч завершён" if finished else str(status.get("name") or "LIVE") if live else "Не начался",
        "home_score": home_score,
        "away_score": away_score,
    }


def _apply_match_status(row: dict[str, Any], status: dict[str, Any]) -> dict[str, Any]:
    updated = dict(row)
    fixture_item = dict(updated.get("fixture_item") or {})
    fixture = dict(fixture_item.get("fixture") or {})
    fixture["date"] = status.get("date") or fixture.get("date")
    fixture["timestamp"] = status.get("timestamp") or fixture.get("timestamp")
    fixture["status"] = {
        "short": status.get("status_short") or "NS",
        "long": status.get("status_long") or "",
        "elapsed": 90 if status.get("finished") else None,
    }
    fixture_item["fixture"] = fixture
    home_score = status.get("home_score")
    away_score = status.get("away_score")
    fixture_item["goals"] = {"home": home_score, "away": away_score}
    fixture_item["score"] = {"fulltime": {"home": home_score, "away": away_score}}
    updated["fixture_item"] = fixture_item
    if status.get("finished"):
        settlement, won, result_code = _settle_row(updated, home_score, away_score)
        updated["settlement"] = settlement
        updated["won"] = won
        updated["result_code"] = result_code
        updated["result_pending"] = not bool(settlement)
    return updated


def _stat_number(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(str(value).replace("%", "").strip())
    except Exception:
        return None


def _statistics_total(blocks: list[dict[str, Any]], accepted_types: set[str]) -> float | None:
    total = 0.0
    found = False
    accepted = {x.lower() for x in accepted_types}
    for block in blocks or []:
        for stat in block.get("statistics") or []:
            if str(stat.get("type") or "").strip().lower() not in accepted:
                continue
            number = _stat_number(stat.get("value"))
            if number is None:
                continue
            total += number
            found = True
    return total if found else None


def _settle_stat_market(row: dict[str, Any], statistics: list[dict[str, Any]]) -> tuple[str, bool | None, str]:
    market = str(row.get("market") or "").lower()
    line = _float(row.get("line"))
    if line is None:
        return "", None, ""
    if market == "corners":
        actual = _statistics_total(statistics, {"Corner Kicks", "Corners"})
        label = "Угловые"
    elif market == "yellow_cards":
        actual = _statistics_total(statistics, {"Yellow Cards", "Yellow Card"})
        label = "Жёлтые карточки"
    elif market == "red_cards":
        actual = _statistics_total(statistics, {"Red Cards", "Red Card"})
        label = "Красные карточки"
    else:
        return "", None, ""
    if actual is None:
        return "", None, ""
    code = str(row.get("pick_code") or "").upper()
    raw_market = str(row.get("source_market") or "").lower()
    direction = "over" if "OVER" in code or "over" in raw_market else "under"
    settlement, won = _compare_total(float(actual), direction, float(line))
    actual_text = str(int(actual)) if float(actual).is_integer() else str(round(float(actual), 2))
    return settlement, won, f"{label}: {actual_text}"


def _apply_api_fixture(
    row: dict[str, Any],
    api_item: dict[str, Any],
    *,
    statistics: list[dict[str, Any]] | None = None,
    confidence: float | None = None,
) -> dict[str, Any]:
    """Replace synthetic Scores24 match metadata with API-Football data."""
    updated = dict(row)
    # API responses are JSON-compatible. A round-trip prevents one prediction
    # row from mutating the shared fixture object used by another row.
    try:
        fixture_item = json.loads(json.dumps(api_item, ensure_ascii=False))
    except Exception:
        fixture_item = dict(api_item)
    fixture = fixture_item.get("fixture") or {}
    fixture_id = int(fixture.get("id") or 0)
    if not fixture_id:
        return updated
    prediction_date = _row_prediction_date(updated)
    fixture_item["scores24"] = {
        "source_url": str(updated.get("source_url") or ""),
        "slug": str(updated.get("source_slug") or ""),
        "prediction_key": str(updated.get("prediction_key") or ""),
        "prediction_date": prediction_date,
        "api_fixture_date": str(fixture.get("date") or "")[:10],
    }
    if prediction_date:
        updated["scores24_prediction_date"] = prediction_date
        updated["prediction_date"] = prediction_date
    fixture_item["data_source"] = "API-Football"
    updated["fixture_item"] = fixture_item
    updated["fixture_id"] = fixture_id
    updated["api_football_fixture_id"] = fixture_id
    updated["api_football_match_confidence"] = round(float(confidence or 0.0), 4)
    updated["api_football_matched_at"] = int(time.time())
    updated["api_football_match_status"] = "matched"
    updated["match_data_source"] = "API-Football"

    status = fixture.get("status") or {}
    status_short = str(status.get("short") or "")
    goals = fixture_item.get("goals") or {}
    home_score = goals.get("home")
    away_score = goals.get("away")
    if status_short in {"FT", "AET", "PEN"}:
        settlement, won, result_code = _settle_row(updated, home_score, away_score)
        if not settlement and statistics:
            settlement, won, result_code = _settle_stat_market(updated, statistics)
        updated["settlement"] = settlement
        updated["won"] = won
        updated["result_code"] = result_code or (f"{home_score}:{away_score}" if home_score is not None and away_score is not None else "")
        updated["result_pending"] = not bool(settlement)
    else:
        # A previous pending flag must not make an upcoming match look as if its
        # result data is already missing.
        updated["result_pending"] = False
    return updated


def _enrich_rows_with_api(
    rows: list[dict[str, Any]],
    *,
    force: bool = False,
) -> tuple[list[dict[str, Any]], int, list[str]]:
    if not rows or _api_fixture_resolver is None:
        return rows, 0, []
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        key = str(row.get("source_slug") or row.get("prediction_key") or "")
        if key:
            grouped.setdefault(key, []).append(row)
    matched = 0
    warnings: list[str] = []
    out: list[dict[str, Any]] = []
    for key, group in grouped.items():
        sample = group[0]
        try:
            api_item = _api_fixture_resolver(sample, force)
        except Exception as exc:
            warnings.append(f"API-Football: {_compact_error(exc)}")
            api_item = None
        if not api_item:
            for row in group:
                copy = dict(row)
                if copy.get("api_football_fixture_id"):
                    copy["api_football_match_status"] = "stale"
                    copy["match_data_source"] = "API-Football"
                else:
                    copy["api_football_match_status"] = "unmatched"
                    copy["match_data_source"] = "Scores24 metadata"
                out.append(copy)
            continue
        confidence = _float(api_item.get("_scores24_match_confidence")) or 0.0
        api_item = dict(api_item)
        api_item.pop("_scores24_match_confidence", None)
        fixture_id = int(((api_item.get("fixture") or {}).get("id") or 0))
        status_short = str((((api_item.get("fixture") or {}).get("status") or {}).get("short") or ""))
        needs_stats = status_short in {"FT", "AET", "PEN"} and any(
            str(row.get("market") or "").lower() in {"corners", "yellow_cards", "red_cards"} for row in group
        )
        stats: list[dict[str, Any]] = []
        if needs_stats and fixture_id and _api_statistics_loader is not None:
            try:
                stats = _api_statistics_loader(fixture_id, force) or []
            except Exception as exc:
                warnings.append(f"API-Football статистика: {_compact_error(exc)}")
        for row in group:
            out.append(_apply_api_fixture(row, api_item, statistics=stats, confidence=confidence))
        matched += 1
    # Rows without a stable grouping key are kept unchanged.
    grouped_ids = {id(row) for group in grouped.values() for row in group}
    out.extend(row for row in rows if id(row) not in grouped_ids)
    return out, matched, list(dict.fromkeys(warnings))[-4:]


_CATEGORY_PATHS = [
    # General pages
    "/predictions/soccer",
    "/predictions/soccer/today",
    "/predictions/soccer/1x2",
    # Separate 1X2 pages keep recently played picks visible longer than the
    # compact general page and are therefore important for history recovery.
    "/predictions/soccer/1x2/home-win",
    "/predictions/soccer/1x2/away-win",
    "/predictions/soccer/1x2/draw",
    # Totals
    "/predictions/soccer/over-under",
    "/predictions/soccer/over-under/over",
    "/predictions/soccer/over-under/under",
    "/predictions/soccer/over-1-5-goals",
    "/predictions/soccer/under-1-5-goals",
    "/predictions/soccer/over-2-5-goals",
    "/predictions/soccer/under-2-5-goals",
    "/predictions/soccer/over-3-5-goals",
    "/predictions/soccer/under-3-5-goals",
    # Other published football markets
    "/predictions/soccer/handicap",
    "/predictions/soccer/double-chance",
    "/predictions/soccer/both-teams-to-score",
    "/predictions/soccer/corners",
    "/predictions/soccer/yellow-card",
    "/predictions/soccer/correct-score",
    "/predictions/soccer/live-in-play",
]



def _category_urls() -> list[str]:
    configured = [x.strip() for x in str(os.environ.get("SCORES24_CATEGORY_PATHS") or "").split(",") if x.strip()]
    paths = configured or _CATEGORY_PATHS
    out: list[str] = []
    for path in paths:
        if path.startswith("http://") or path.startswith("https://"):
            out.append(path)
        else:
            out.append(f"{BASE_URL}/{LANG}{path if path.startswith('/') else '/' + path}")
    return list(dict.fromkeys(out))


def _next_category_batch() -> list[str]:
    global _category_cursor
    urls = _category_urls()
    if not urls:
        return []
    with _lock:
        start = _category_cursor % len(urls)
        batch = [urls[(start + i) % len(urls)] for i in range(min(CATEGORY_BATCH_SIZE, len(urls)))]
        _category_cursor = (start + len(batch)) % len(urls)
    base = f"{BASE_URL}/{LANG}/predictions/soccer"
    if base not in batch:
        batch.insert(0, base)
    return list(dict.fromkeys(batch))


def _existing_rows(payload: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for row in (payload or {}).get("feed") or []:
        if isinstance(row, dict) and row.get("prediction_key"):
            rows[str(row["prediction_key"])] = dict(row)
    return rows


def _merge_new_row(rows: dict[str, dict[str, Any]], row: dict[str, Any]) -> None:
    key = str(row.get("prediction_key") or "")
    if not key:
        return
    previous = rows.get(key)
    if previous:
        row["odd"] = previous.get("odd", row.get("odd"))
        row["bookmaker"] = previous.get("bookmaker", row.get("bookmaker"))
        row["bookmaker_logo"] = previous.get("bookmaker_logo", row.get("bookmaker_logo"))
        row["published_at"] = previous.get("published_at", row.get("published_at"))
        for field in ("scores24_prediction_date", "prediction_date"):
            if previous.get(field):
                row[field] = previous.get(field)
        old_status = ((((previous.get("fixture_item") or {}).get("fixture") or {}).get("status") or {}).get("short"))
        if previous.get("api_football_fixture_id"):
            row["fixture_item"] = previous.get("fixture_item")
            for field in (
                "fixture_id", "api_football_fixture_id", "api_football_match_confidence",
                "api_football_matched_at", "match_data_source", "api_football_match_status",
            ):
                if field in previous:
                    row[field] = previous.get(field)
        if old_status in {"FT", "AET", "PEN"}:
            row["fixture_item"] = previous.get("fixture_item")
            row["settlement"] = previous.get("settlement")
            row["won"] = previous.get("won")
            row["result_code"] = previous.get("result_code")
            row["result_pending"] = previous.get("result_pending", False)
    rows[key] = row


def _row_match_date(row: dict[str, Any]) -> str:
    """Return the real match day, not the date embedded in a Scores24 URL.

    Scores24 URL slugs can be one day behind/ahead of ``matchDate``. Tabs and
    ROI must always use the actual fixture date supplied by API-Football (or
    Scores24 match metadata before a fixture is matched).
    """
    fixture_item = row.get("fixture_item") or {}
    fixture = fixture_item.get("fixture") or {}
    candidates = [
        fixture.get("date"),
        (fixture_item.get("scores24") or {}).get("listed_match_date"),
        row.get("match_date"),
    ]
    for value in candidates:
        text = str(value or "").strip()
        if not text:
            continue
        parsed = _parse_iso(text, assume_utc=False)
        if parsed is not None:
            if parsed.tzinfo is not None:
                parsed = parsed.astimezone(_tz())
            return parsed.strftime("%Y-%m-%d")
        if _valid_date(text[:10]):
            return text[:10]
    return _row_prediction_date(row)


def _row_date(row: dict[str, Any]) -> str:
    return _row_match_date(row)


def _league_prediction_urls_from_page(page_html: str) -> list[str]:
    urls: list[str] = []
    for found in _LEAGUE_PREDICTION_LINK_RE.finditer(page_html or ""):
        urls.append(f"{BASE_URL}/{LANG}/soccer/l-{found.group('slug')}/predictions")
    for payload in _urql_payloads(page_html):
        stack = [payload]
        while stack:
            value = stack.pop()
            if isinstance(value, list):
                stack.extend(value)
                continue
            if not isinstance(value, dict):
                continue
            slug = str(value.get("slug") or "").strip()
            sport_slug = str(value.get("sportSlug") or "").strip().lower()
            if slug and sport_slug == "soccer":
                urls.append(f"{BASE_URL}/{LANG}/soccer/l-{slug}/predictions")
            stack.extend(value.values())
    return list(dict.fromkeys(urls))[:MAX_DISCOVERED_LEAGUES]


def _remember_league_urls(urls: list[str]) -> None:
    global _discovered_league_urls
    if not urls:
        return
    with _lock:
        merged = list(dict.fromkeys(_discovered_league_urls + urls))
        _discovered_league_urls = merged[-MAX_DISCOVERED_LEAGUES:]


def _next_league_batch() -> list[str]:
    global _league_cursor
    with _lock:
        urls = list(_discovered_league_urls)
        if not urls:
            return []
        start = _league_cursor % len(urls)
        batch = [urls[(start + i) % len(urls)] for i in range(min(LEAGUE_BATCH_SIZE, len(urls)))]
        _league_cursor = (start + len(batch)) % len(urls)
    return batch


def _discover_category_rows(*, force: bool = False) -> tuple[list[dict[str, Any]], int, int, list[str], bool]:
    discovered: list[dict[str, Any]] = []
    warnings: list[str] = []
    checked_categories = 0
    checked_leagues = 0
    rate_limited = False
    for category_url in _next_category_batch():
        try:
            page_html = _fetch_text(category_url, force=bool(force and checked_categories == 0), ttl=CATEGORY_REFRESH_SECONDS)
            checked_categories += 1
            discovered.extend(_rows_from_index_page(page_html, category_url))
            _remember_league_urls(_league_prediction_urls_from_page(page_html))
        except Scores24RateLimited as exc:
            warnings.append(str(exc))
            rate_limited = True
            break
        except Exception as exc:
            warnings.append(_compact_error(exc))
    if not rate_limited:
        for league_url in _next_league_batch():
            try:
                page_html = _fetch_text(league_url, force=False, ttl=LEAGUE_REFRESH_SECONDS)
                checked_leagues += 1
                discovered.extend(_rows_from_index_page(page_html, league_url))
            except Scores24RateLimited as exc:
                warnings.append(str(exc))
                rate_limited = True
                break
            except Exception as exc:
                warnings.append(_compact_error(exc))
    dedup: dict[str, dict[str, Any]] = {}
    for row in discovered:
        key = str(row.get("prediction_key") or "")
        if key:
            dedup[key] = row
    return list(dedup.values()), checked_categories, checked_leagues, warnings, rate_limited


def _store_discovered_rows(rows: list[dict[str, Any]], warnings: list[str] | None = None) -> dict[str, int]:
    """Distribute one category scan across every discovered match date."""
    grouped: dict[str, list[dict[str, Any]]] = {}
    today = datetime.now(_tz()).date()
    history_start = datetime.strptime(HISTORY_START_DATE, "%Y-%m-%d").date()
    oldest = max(today - timedelta(days=62), history_start)
    newest = today + timedelta(days=7)
    for row in rows:
        date_text = _row_date(row)
        if not _valid_date(date_text):
            continue
        try:
            row_date = datetime.strptime(date_text, "%Y-%m-%d").date()
        except Exception:
            continue
        if row_date < oldest or row_date > newest:
            continue
        grouped.setdefault(date_text, []).append(row)

    saved_dates = 0
    saved_rows = 0
    for date_text, new_rows in grouped.items():
        old = _storage_get(date_text) or {}
        merged = _existing_rows(old)
        before = len(merged)
        for row in new_rows:
            _merge_new_row(merged, row)
        if len(merged) == before and old:
            continue
        old_warnings = list(old.get("warnings") or [])
        payload = _recalculate(date_text, list(merged.values()), old_warnings + list(warnings or []))
        payload["historical_recovery"] = date_text < _today()
        _storage_save(date_text, payload, is_final=False)
        saved_dates += 1
        saved_rows += max(0, len(merged) - before)
    return {"dates": saved_dates, "rows": saved_rows}


def _migrate_saved_prediction_dates() -> dict[str, int]:
    """Move saved rows to their real fixture day.

    A previous build grouped by the date embedded in the Scores24 URL. That
    date is not always the match date (for example a ``04-08`` slug can contain
    a match scheduled on 05 August). The migration is idempotent.
    """
    global _date_migration_done
    with _lock:
        if _date_migration_done:
            return {"moved": 0, "dates": 0}
        _date_migration_done = True

    records = _storage_records_full()
    if not records:
        return {"moved": 0, "dates": 0}

    rows_by_date: dict[str, dict[str, dict[str, Any]]] = {}
    warnings_by_date: dict[str, list[str]] = {}
    final_by_date: dict[str, bool] = {}
    moved = 0
    touched: set[str] = set()

    for payload in records:
        source_date = str(payload.get("date") or "")
        warnings_by_date.setdefault(source_date, []).extend(payload.get("warnings") or [])
        final_by_date[source_date] = bool(payload.get("is_final"))
        for raw_row in payload.get("feed") or []:
            if not isinstance(raw_row, dict):
                continue
            row = dict(raw_row)
            target_date = _row_match_date(row) or source_date
            if not _valid_date(target_date):
                target_date = source_date
            if target_date != source_date:
                moved += 1
                touched.update({source_date, target_date})
            row["match_date"] = target_date
            fixture_item = dict(row.get("fixture_item") or {})
            scores24_meta = dict(fixture_item.get("scores24") or {})
            scores24_meta["match_date"] = target_date
            fixture_item["scores24"] = scores24_meta
            row["fixture_item"] = fixture_item
            key = str(row.get("prediction_key") or f"row:{len(rows_by_date.get(target_date, {}))}")
            rows_by_date.setdefault(target_date, {})[key] = row
            warnings_by_date.setdefault(target_date, []).extend(payload.get("warnings") or [])

    if not moved:
        return {"moved": 0, "dates": 0}

    all_dates = {str(payload.get("date") or "") for payload in records if _valid_date(str(payload.get("date") or ""))} | set(rows_by_date)
    for date_text in sorted(all_dates):
        rows = list((rows_by_date.get(date_text) or {}).values())
        rebuilt = _recalculate(date_text, rows, warnings_by_date.get(date_text) or [])
        is_past = date_text < _today()
        all_finished = bool(rows) and all(
            str(((((row.get("fixture_item") or {}).get("fixture") or {}).get("status") or {}).get("short") or "")) in {"FT", "AET", "PEN"}
            for row in rows
        )
        _storage_save(
            date_text,
            rebuilt,
            is_final=bool(is_past and all_finished and not rebuilt.get("pending_result_total")),
        )
    print(f"[scores24] date migration moved={moved} dates={len(touched)}")
    return {"moved": moved, "dates": len(touched)}


def _refresh_unsettled_history(max_pages: int = MAX_RESULT_PAGES_PER_SCAN) -> dict[str, int]:
    """Refresh a few oldest unfinished saved matches across all dates."""
    now_ts = int(time.time())
    candidates: dict[str, tuple[int, list[str]]] = {}
    records = _storage_records_full()
    for payload in records:
        date_text = str(payload.get("date") or "")
        for row in payload.get("feed") or []:
            if not isinstance(row, dict):
                continue
            fixture = ((row.get("fixture_item") or {}).get("fixture") or {})
            status = str(((fixture.get("status") or {}).get("short") or ""))
            source_url = str(row.get("source_url") or "")
            kickoff = int(fixture.get("timestamp") or 0)
            if not source_url or status in {"FT", "AET", "PEN"}:
                continue
            if kickoff and kickoff <= now_ts + 15 * 60:
                current = candidates.get(source_url)
                dates = list(current[1]) if current else []
                if date_text not in dates:
                    dates.append(date_text)
                candidates[source_url] = (min(kickoff, current[0]) if current else kickoff, dates)

    checked = updated_dates = 0
    for source_url, (_, dates) in sorted(candidates.items(), key=lambda item: item[1][0])[:max(1, int(max_pages))]:
        try:
            page_html = _fetch_text(source_url, force=False, ttl=REFRESH_SECONDS)
            checked += 1
            status = _parse_match_status_page(page_html, source_url)
            if not status:
                continue
            slug = str(status.get("source_slug") or "")
            for date_text in dates:
                payload = _storage_get(date_text) or {}
                changed = False
                rows: list[dict[str, Any]] = []
                for row in payload.get("feed") or []:
                    if isinstance(row, dict) and str(row.get("source_slug") or "") == slug:
                        row = _apply_match_status(row, status)
                        changed = True
                    if isinstance(row, dict):
                        rows.append(row)
                if changed:
                    rebuilt = _recalculate(date_text, rows, list(payload.get("warnings") or []))
                    is_past = date_text < _today()
                    all_finished = bool(rows) and all(
                        str(((((item.get("fixture_item") or {}).get("fixture") or {}).get("status") or {}).get("short") or "")) in {"FT", "AET", "PEN"}
                        for item in rows
                    )
                    _storage_save(date_text, rebuilt, is_final=bool(is_past and all_finished and not rebuilt.get("pending_result_total")))
                    updated_dates += 1
        except Scores24RateLimited:
            break
        except Exception as exc:
            print(f"[scores24] history result refresh failed: {_compact_error(exc)}")
    return {"checked_pages": checked, "updated_dates": updated_dates}


def _refresh_unsettled_history_api(max_dates: int = MAX_API_HISTORY_DATES_PER_SCAN) -> dict[str, int]:
    """Refresh saved unfinished matches from API-Football, grouped by date."""
    if _api_fixture_resolver is None:
        return {"checked_dates": 0, "updated_dates": 0}
    now_ts = int(time.time())
    candidates: list[tuple[int, str, dict[str, Any]]] = []
    seen_dates: set[str] = set()
    for payload in _storage_records_full():
        date_text = str(payload.get("date") or "")
        if not _valid_date(date_text) or date_text in seen_dates:
            continue
        rows = [row for row in payload.get("feed") or [] if isinstance(row, dict)]
        due = []
        for row in rows:
            fixture = ((row.get("fixture_item") or {}).get("fixture") or {})
            status = str(((fixture.get("status") or {}).get("short") or ""))
            kickoff = int(fixture.get("timestamp") or 0)
            if status not in {"FT", "AET", "PEN"} and (not kickoff or kickoff <= now_ts + 15 * 60):
                due.append(kickoff or 0)
        if due:
            candidates.append((min(due), date_text, payload))
            seen_dates.add(date_text)
    checked = updated_dates = 0
    for _, date_text, payload in sorted(candidates, key=lambda item: item[0])[:max(1, int(max_dates))]:
        rows = [dict(row) for row in payload.get("feed") or [] if isinstance(row, dict)]
        enriched, matched, warnings = _enrich_rows_with_api(rows, force=False)
        checked += 1
        if not matched:
            continue
        rebuilt = _recalculate(date_text, enriched, list(payload.get("warnings") or []) + warnings)
        is_past = date_text < _today()
        all_finished = bool(enriched) and all(
            str(((((item.get("fixture_item") or {}).get("fixture") or {}).get("status") or {}).get("short") or "")) in {"FT", "AET", "PEN"}
            for item in enriched
        )
        _storage_save(date_text, rebuilt, is_final=bool(is_past and all_finished and not rebuilt.get("pending_result_total")))
        updated_dates += 1
    return {"checked_dates": checked, "updated_dates": updated_dates}


def _recalculate(date_text: str, rows: list[dict[str, Any]], warnings: list[str] | None = None) -> dict[str, Any]:
    rows.sort(key=lambda row: int((((row.get("fixture_item") or {}).get("fixture") or {}).get("timestamp") or 0)))
    settled = [row for row in rows if row.get("settlement") in {"won", "lost", "push"} and row.get("odd") is not None]
    won = sum(1 for row in settled if row.get("settlement") == "won")
    lost = sum(1 for row in settled if row.get("settlement") == "lost")
    push = sum(1 for row in settled if row.get("settlement") == "push")
    pending_result = sum(1 for row in rows if row.get("result_pending"))
    stake = float(len(settled))
    returned = sum(float(row.get("odd") or 0) if row.get("settlement") == "won" else 1.0 if row.get("settlement") == "push" else 0.0 for row in settled)
    profit = returned - stake
    hit_rate = (won / max(1, won + lost)) * 100 if won + lost else 0.0
    roi = (profit / stake) * 100 if stake else 0.0
    market_counts: dict[str, int] = {}
    for row in rows:
        label = str(row.get("market_label") or row.get("market") or "Другой рынок")
        market_counts[label] = market_counts.get(label, 0) + 1
    return {
        "date": date_text,
        "source": "Scores24",
        "source_mode": "Scores24 editorial picks + API-Football match data",
        "market_policy": "All published Scores24 football markets",
        "odds_policy": f"Scores24 published odd {MIN_ODD:.2f}-{MAX_ODD:.2f}",
        "match_data_source": "API-Football",
        "fixtures_total": len({str(row.get("source_slug") or row.get("fixture_id") or "") for row in rows}),
        "finished_total": len(settled),
        "predicted_total": len(settled),
        "feed_total": len(rows),
        "won_total": won,
        "lost_total": lost,
        "push_total": push,
        "pending_result_total": pending_result,
        "hit_rate": round(hit_rate, 2),
        "stake_total": round(stake, 2),
        "returned_total": round(returned, 2),
        "profit_total": round(profit, 2),
        "roi_percent": round(roi, 2),
        "odds_total": len(settled),
        "market_counts": market_counts,
        "feed": rows,
        "errors": {},
        "warnings": list(dict.fromkeys(warnings or []))[-4:],
        "updated_at": int(time.time()),
    }


def feed(date_text: str, *, force: bool = False) -> dict[str, Any]:
    if not _valid_date(date_text):
        raise ValueError("invalid date")
    if date_text < HISTORY_START_DATE:
        return _recalculate(date_text, [], [])
    old = _storage_get(date_text)
    rows = _existing_rows(old)
    warnings: list[str] = []
    checked_category_pages = 0
    checked_league_pages = 0
    rate_limited = False

    # Category pages contain current picks; league prediction pages retain
    # recently played picks and are scanned slowly for historical recovery.
    discovered, checked_category_pages, checked_league_pages, discover_warnings, rate_limited = _discover_category_rows(force=force)
    warnings.extend(discover_warnings)
    discovered = [row for row in discovered if (_row_date(row) or "") >= HISTORY_START_DATE]
    discovered, discovered_matched, api_warnings = _enrich_rows_with_api(discovered, force=force)
    warnings.extend(api_warnings)
    recovery = _store_discovered_rows(discovered, discover_warnings + api_warnings)

    old = _storage_get(date_text) or old
    rows = _existing_rows(old)
    for row in discovered:
        if _row_date(row) == date_text:
            _merge_new_row(rows, row)

    # Refresh all match metadata and results from API-Football. Its date request
    # is cached, so many predictions on the same day still consume one request.
    enriched, matched, current_api_warnings = _enrich_rows_with_api(list(rows.values()), force=force)
    warnings.extend(current_api_warnings)
    rows = {str(row.get("prediction_key") or index): row for index, row in enumerate(enriched)}

    payload = _recalculate(date_text, list(rows.values()), warnings)
    payload["scan_status"] = {
        "checked_category_pages": checked_category_pages,
        "checked_league_pages": checked_league_pages,
        "known_league_pages": len(_discovered_league_urls),
        "category_pages_total": len(_category_urls()),
        "category_batch_size": CATEGORY_BATCH_SIZE,
        "checked_scores24_result_pages": 0,
        "api_football_matched_matches": matched,
        "api_football_discovered_matches": discovered_matched,
        "rate_limited": rate_limited,
        "recovered_dates": int(recovery.get("dates") or 0),
        "recovered_predictions": int(recovery.get("rows") or 0),
    }
    is_past = date_text < _today()
    all_finished = bool(payload["feed"]) and all(
        str(((((row.get("fixture_item") or {}).get("fixture") or {}).get("status") or {}).get("short") or "")) in {"FT", "AET", "PEN"}
        for row in payload["feed"]
    )
    _storage_save(date_text, payload, is_final=bool(is_past and all_finished and not payload.get("pending_result_total")))
    return payload


def cached_feed(date_text: str) -> dict[str, Any]:
    """Return the last saved prediction snapshot without network scanning.

    The background worker remains responsible for refreshing Scores24 and
    API-Football data. This function is used by the UI endpoint so reopening
    the Mini App reads the database immediately instead of repeating the scan.
    """
    if not _valid_date(date_text):
        raise ValueError("invalid date")
    if date_text < HISTORY_START_DATE:
        return _recalculate(date_text, [], [])
    payload = _storage_get(date_text)
    if payload is None:
        return feed(date_text, force=False)
    result = dict(payload)
    updated_at = int(result.get("updated_at") or 0)
    result["served_from_cache"] = True
    result["cache_age_seconds"] = max(0, int(time.time()) - updated_at) if updated_at else None
    return result


def _worker() -> None:
    _stop.wait(8)
    cycle = 0
    while not _stop.is_set():
        started = time.time()
        try:
            today = datetime.now(_tz()).date()
            offsets = [0]
            if cycle % 3 == 0:
                offsets.append(1 + ((cycle // 3) % 3))
            for offset in offsets:
                date_text = (today + timedelta(days=offset)).isoformat()
                try:
                    result = feed(date_text, force=False)
                    print(
                        f"[scores24] refresh date={date_text} feed={result.get('feed_total', 0)} "
                        f"settled={result.get('predicted_total', 0)} warnings={len(result.get('warnings') or [])}"
                    )
                except Scores24RateLimited as exc:
                    print(f"[scores24] refresh paused: {exc}")
                    break
                except Exception as exc:
                    print(f"[scores24] refresh failed date={date_text}: {_compact_error(exc)}")
            history_refresh = _refresh_unsettled_history_api(MAX_API_HISTORY_DATES_PER_SCAN)
            if history_refresh.get("checked_dates"):
                print(
                    f"[scores24] API-Football history dates={history_refresh.get('checked_dates')} "
                    f"updated_dates={history_refresh.get('updated_dates')}"
                )
        except Exception as exc:
            print(f"[scores24] worker error: {_compact_error(exc)}")
        cycle += 1
        elapsed = time.time() - started
        _stop.wait(max(5, REFRESH_SECONDS - elapsed))


def start_worker() -> None:
    global _started
    with _lock:
        if _started:
            return
        _started = True
    deleted = _storage_delete_before(HISTORY_START_DATE)
    if deleted:
        print(f"[scores24] removed legacy dates before {HISTORY_START_DATE}: {deleted}")
    threading.Thread(target=_worker, name="scores24-predictions", daemon=True).start()


def health() -> dict[str, Any]:
    return {
        "source": "Scores24",
        "base_url": BASE_URL,
        "refresh_seconds": REFRESH_SECONDS,
        "category_refresh_seconds": CATEGORY_REFRESH_SECONDS,
        "category_batch_size": CATEGORY_BATCH_SIZE,
        "category_pages": len(_category_urls()),
        "league_batch_size": LEAGUE_BATCH_SIZE,
        "known_league_pages": len(_discovered_league_urls),
        "history_lookback_days": HISTORY_LOOKBACK_DAYS,
        "history_start_date": HISTORY_START_DATE,
        "date_source": "actual fixture date (API-Football / Scores24 matchDate)",
        "max_result_pages_per_scan": 0,
        "api_history_dates_per_scan": MAX_API_HISTORY_DATES_PER_SCAN,
        "prediction_source": "Scores24",
        "odds_source": "Scores24",
        "match_data_source": "API-Football",
        "api_football_provider_attached": _api_football_enabled(),
        "min_odd": MIN_ODD,
        "max_odd": MAX_ODD,
        "request_gap_seconds": REQUEST_GAP_SECONDS,
        "rate_limit_backoff_seconds": RATE_LIMIT_BACKOFF_SECONDS,
        "storage": _storage_mode(),
        "markets": "all",
    }


# Test helpers kept private from WSGI routes.
def _parse_file(path: str) -> dict[str, Any] | None:
    text = Path(path).read_text("utf-8", errors="ignore")
    status = _parse_match_status_page(text, "https://scores24.live/ru/soccer/m-test-prediction")
    return status


def _parse_index_file(path: str) -> list[dict[str, Any]]:
    text = Path(path).read_text("utf-8", errors="ignore")
    # Chrome's saved view-source page wraps the original source in a table.
    if 'class="line-content"' in text:
        try:
            from bs4 import BeautifulSoup  # type: ignore
            source = BeautifulSoup(text, "html.parser")
            text = "\n".join(td.get_text("", strip=False) for td in source.select("td.line-content"))
        except Exception:
            pass
    return _rows_from_index_page(text, "file://scores24-index")
