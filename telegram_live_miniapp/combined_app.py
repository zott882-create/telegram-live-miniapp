#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""telegram_live_miniapp combined Telegram Mini App.

- /                    -> API-Football match centre
- /notify/             -> frozen notification bot UI
- /api-football/*      -> API-Football match data with Redis/memory cache
- /api/*                -> frozen notification bot API

The notification bot source is imported from notification_bot/ and is not
modified by this wrapper.
"""
from __future__ import annotations

import collections
from concurrent.futures import ThreadPoolExecutor
import difflib
import hashlib
import importlib.util
import io
import json
import math
import mimetypes
import os
import re
import signal
import sqlite3
import threading
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
import unicodedata
from datetime import datetime, timezone, timedelta
from email.message import Message
from http import HTTPStatus
from pathlib import Path
from typing import Any, Callable
from zoneinfo import ZoneInfo

import scores24_predictions

BASE_DIR = Path(__file__).resolve().parent
MATCHES_DIR = BASE_DIR / "matches_static"
FROZEN_APP_PATH = BASE_DIR / "notification_bot" / "telegram_live_miniapp" / "app.py"
HOST = os.environ.get("HOST", "0.0.0.0")
PORT = int(os.environ.get("PORT", "8080"))
SERVER_THREADS = max(4, int(os.environ.get("SERVER_THREADS", "16")))
SERVER_CHANNEL_TIMEOUT = max(15, int(os.environ.get("SERVER_CHANNEL_TIMEOUT", "60")))

API_FOOTBALL_KEY = os.environ.get("API_FOOTBALL_KEY", "").strip()
API_FOOTBALL_BASE_URL = os.environ.get("API_FOOTBALL_BASE_URL", "https://v3.football.api-sports.io").rstrip("/")
API_FOOTBALL_TIMEOUT = max(5, int(os.environ.get("API_FOOTBALL_TIMEOUT", "20")))
API_FOOTBALL_MAX_RPS = max(1.0, min(5.0, float(os.environ.get("API_FOOTBALL_MAX_RPS", "4"))))
API_FOOTBALL_DEMO = os.environ.get("API_FOOTBALL_DEMO", "0").strip().lower() in {"1", "true", "yes", "on"}
PREDICTION_LIVE_REFRESH_SECONDS = max(300, int(os.environ.get("PREDICTION_LIVE_REFRESH_SECONDS", "300")))
# The worker wakes up every five minutes. It always checks today's fixture list
# once, but it does not request predictions/odds for every match on every cycle.
# New fixtures are checked immediately; unavailable data is retried with a
# backoff, which keeps API usage predictable even on days with many matches.
PREDICTION_NO_DATA_RECHECK_SECONDS = max(900, int(os.environ.get("PREDICTION_NO_DATA_RECHECK_SECONDS", "1800")))
PREDICTION_WEAK_MODEL_RECHECK_SECONDS = max(1800, int(os.environ.get("PREDICTION_WEAK_MODEL_RECHECK_SECONDS", "10800")))
PREDICTION_ODDS_RECHECK_SECONDS = max(900, int(os.environ.get("PREDICTION_ODDS_RECHECK_SECONDS", "1800")))
PREDICTION_SCAN_MAX_FIXTURES_PER_CYCLE = max(25, int(os.environ.get("PREDICTION_SCAN_MAX_FIXTURES_PER_CYCLE", "400")))
PREDICTION_SCAN_STATE_VERSION = "five-minute-outcomes-only-v5"
# Restore old summary-only days once after upgrading. The full match cards are
# then kept in persistent storage forever and no longer consume API requests.
PREDICTION_HISTORY_BACKFILL_DAYS = max(0, min(3650, int(os.environ.get("PREDICTION_HISTORY_BACKFILL_DAYS", "31"))))
PREDICTION_HISTORY_RETENTION_POLICY = "full-feed-forever-v1"
PREDICTION_MIN_MODEL_PERCENT = 50.0
PREDICTION_MIN_ODD = 1.50
PREDICTION_MAX_ODD = 3.50
PREDICTION_TOTAL_MIN_MODEL_PERCENT = 50.0
PREDICTION_TOTAL_MIN_ODD = 1.50
PREDICTION_TOTAL_MAX_ODD = 3.00
PREDICTION_TOTAL_LINE = 2.5
PREDICTION_ODDS_POLICY = "lowest-bookmaker-outcomes-only-model50-odd150to350-v8"
APP_TIMEZONE = os.environ.get("APP_TIMEZONE", "Europe/Moscow").strip() or "Europe/Moscow"
REDIS_URL = (
    os.environ.get("REDIS_URL")
    or os.environ.get("REDIS_INTERNAL_URL")
    or os.environ.get("REDIS_PUBLIC_URL")
    or ""
).strip()
CACHE_PREFIX = os.environ.get("API_FOOTBALL_CACHE_PREFIX", "telegram_live_miniapp:football:v1").strip().strip(":")
GOAL_NOTIFY_INTERVAL_SECONDS = max(20, int(os.environ.get("GOAL_NOTIFY_INTERVAL_SECONDS", "45")))

# Load the frozen notification bot as a module without changing its files.
_spec = importlib.util.spec_from_file_location("telegram_live_miniapp_frozen_notify", FROZEN_APP_PATH)
if _spec is None or _spec.loader is None:
    raise RuntimeError(f"Cannot import frozen notification bot: {FROZEN_APP_PATH}")
notify_app = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(notify_app)

scores24_predictions.configure(
    data_dir=Path(getattr(notify_app, "DATA_DIR", BASE_DIR / "data")),
    pg_conn_factory=getattr(notify_app, "_notify_pg_conn", None),
    database_url=str(getattr(notify_app, "DATABASE_URL", "") or ""),
)

try:
    import redis  # type: ignore
except Exception:
    redis = None

_redis_client = None
if redis is not None and REDIS_URL:
    try:
        _redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True, socket_timeout=2, socket_connect_timeout=2)
        _redis_client.ping()
        print("[api-football] Redis cache connected")
    except Exception as exc:
        print(f"[api-football] Redis unavailable, memory cache used: {exc}")
        _redis_client = None

_mem_cache: dict[str, tuple[float, Any]] = {}
_mem_cache_lock = threading.RLock()
_key_locks: dict[str, threading.Lock] = {}
_key_locks_guard = threading.Lock()
_rate_lock = threading.Lock()
_rate_times: collections.deque[float] = collections.deque()
_quota_lock = threading.Lock()
_quota_state: dict[str, Any] = {"limit": None, "remaining": None, "updated_at": None}
_runtime_started = False
_runtime_lock = threading.Lock()
_goal_notify_lock = threading.RLock()
_goal_notify_subs: dict[str, dict[str, Any]] = {}
_goal_notify_started = False
_goal_notify_file = Path(getattr(notify_app, "DATA_DIR", BASE_DIR / "data")) / "api_football_goal_subscriptions.json"
_goal_notify_send_pool = ThreadPoolExecutor(max_workers=4, thread_name_prefix="api-goal-send")
_prediction_history_lock = threading.RLock()
_prediction_history_ready = False
_prediction_history_worker_started = False
_prediction_history_db = Path(getattr(notify_app, "DATA_DIR", BASE_DIR / "data")) / "prediction_history.sqlite3"


def _cache_key(endpoint: str, params: dict[str, Any]) -> str:
    normalized = urllib.parse.urlencode(sorted((str(k), str(v)) for k, v in params.items() if v not in (None, "")))
    digest = hashlib.sha256(f"{endpoint}?{normalized}".encode()).hexdigest()[:32]
    return f"{CACHE_PREFIX}:{endpoint.strip('/').replace('/', ':')}:{digest}"


def _cache_get(key: str) -> Any | None:
    if _redis_client is not None:
        try:
            raw = _redis_client.get(key)
            if raw:
                return json.loads(raw)
        except Exception:
            pass
    now = time.time()
    with _mem_cache_lock:
        item = _mem_cache.get(key)
        if not item:
            return None
        expires, value = item
        if expires <= now:
            _mem_cache.pop(key, None)
            return None
        return value


def _cache_set(key: str, value: Any, ttl: int) -> None:
    ttl = max(1, int(ttl))
    if _redis_client is not None:
        try:
            _redis_client.setex(key, ttl, json.dumps(value, ensure_ascii=False, separators=(",", ":")))
        except Exception:
            pass
    with _mem_cache_lock:
        _mem_cache[key] = (time.time() + ttl, value)
        if len(_mem_cache) > 2000:
            now = time.time()
            for old_key, (expires, _) in list(_mem_cache.items()):
                if expires <= now:
                    _mem_cache.pop(old_key, None)
            if len(_mem_cache) > 2000:
                for old_key in list(_mem_cache.keys())[:500]:
                    _mem_cache.pop(old_key, None)


def _lock_for(key: str) -> threading.Lock:
    with _key_locks_guard:
        lock = _key_locks.get(key)
        if lock is None:
            lock = threading.Lock()
            _key_locks[key] = lock
        return lock


def _rate_wait() -> None:
    interval = 1.0
    max_calls = max(1, int(API_FOOTBALL_MAX_RPS))
    while True:
        with _rate_lock:
            now = time.monotonic()
            while _rate_times and now - _rate_times[0] >= interval:
                _rate_times.popleft()
            if len(_rate_times) < max_calls:
                _rate_times.append(now)
                return
            wait = max(0.01, interval - (now - _rate_times[0]) + 0.01)
        time.sleep(wait)


def _update_quota(headers: Any) -> None:
    def get_header(*names: str) -> str | None:
        for name in names:
            value = headers.get(name)
            if value is not None:
                return str(value)
        return None

    limit = get_header("x-ratelimit-requests-limit", "X-RateLimit-Requests-Limit")
    remaining = get_header("x-ratelimit-requests-remaining", "X-RateLimit-Requests-Remaining")
    with _quota_lock:
        if limit is not None:
            try:
                _quota_state["limit"] = int(limit)
            except Exception:
                pass
        if remaining is not None:
            try:
                _quota_state["remaining"] = int(remaining)
            except Exception:
                pass
        _quota_state["updated_at"] = int(time.time())


def _quota_snapshot() -> dict[str, Any]:
    with _quota_lock:
        return dict(_quota_state)


def _api_call(
    endpoint: str,
    params: dict[str, Any],
    ttl: int,
    *,
    allow_demo: bool = True,
    bypass_cache: bool = False,
) -> dict[str, Any]:
    key = _cache_key(endpoint, params)
    if not bypass_cache:
        cached = _cache_get(key)
        if cached is not None:
            result = dict(cached)
            result["_cache"] = "hit"
            result["_quota"] = _quota_snapshot()
            return result

    with _lock_for(key):
        if not bypass_cache:
            cached = _cache_get(key)
            if cached is not None:
                result = dict(cached)
                result["_cache"] = "hit-after-wait"
                result["_quota"] = _quota_snapshot()
                return result

        if API_FOOTBALL_DEMO or not API_FOOTBALL_KEY:
            if allow_demo:
                return {"get": endpoint.strip("/"), "parameters": params, "errors": {"key": "API_FOOTBALL_KEY is not configured"}, "results": 0, "paging": {"current": 1, "total": 1}, "response": [], "_cache": "demo", "_quota": _quota_snapshot()}
            raise RuntimeError("API_FOOTBALL_KEY is not configured")

        _rate_wait()
        query = urllib.parse.urlencode({k: v for k, v in params.items() if v not in (None, "") and k != "refresh"})
        url = f"{API_FOOTBALL_BASE_URL}/{endpoint.lstrip('/')}" + (f"?{query}" if query else "")
        request = urllib.request.Request(
            url,
            headers={
                "x-apisports-key": API_FOOTBALL_KEY,
                "Accept": "application/json",
                "User-Agent": "telegram_live_miniapp/1.0",
            },
            method="GET",
        )
        last_exc: Exception | None = None
        for attempt in range(2):
            try:
                with urllib.request.urlopen(request, timeout=API_FOOTBALL_TIMEOUT) as response:
                    raw = response.read()
                    _update_quota(response.headers)
                    data = json.loads(raw.decode("utf-8"))
                    if not isinstance(data, dict):
                        raise RuntimeError("API-Football returned a non-object response")
                    errors = data.get("errors")
                    if errors and errors not in ({}, []):
                        # Do not cache auth/rate errors for long.
                        data["_cache"] = "upstream-error"
                        data["_quota"] = _quota_snapshot()
                        return data
                    store = dict(data)
                    store.pop("_cache", None)
                    store.pop("_quota", None)
                    _cache_set(key, store, ttl)
                    data["_cache"] = "miss"
                    data["_quota"] = _quota_snapshot()
                    return data
            except urllib.error.HTTPError as exc:
                last_exc = exc
                try:
                    payload = json.loads(exc.read().decode("utf-8", "replace"))
                except Exception:
                    payload = {"errors": {"http": f"HTTP {exc.code}"}, "response": []}
                _update_quota(exc.headers)
                if exc.code not in {429, 500, 502, 503, 504} or attempt == 1:
                    payload["_cache"] = "http-error"
                    payload["_quota"] = _quota_snapshot()
                    return payload
                time.sleep(0.6 * (attempt + 1))
            except Exception as exc:
                last_exc = exc
                if attempt == 0:
                    time.sleep(0.5)
                    continue
        raise RuntimeError(f"API-Football request failed: {last_exc}")


def _fixture_ttl(date_text: str) -> int:
    today = datetime.now().strftime("%Y-%m-%d")
    if date_text == today:
        return max(30, int(os.environ.get("API_FOOTBALL_TODAY_TTL", "60")))
    return max(300, int(os.environ.get("API_FOOTBALL_OTHER_DATE_TTL", "1800")))


_CYRILLIC_TRANSLIT = str.maketrans({
    "а":"a","б":"b","в":"v","г":"g","д":"d","е":"e","ё":"e","ж":"zh","з":"z","и":"i","й":"y",
    "к":"k","л":"l","м":"m","н":"n","о":"o","п":"p","р":"r","с":"s","т":"t","у":"u","ф":"f",
    "х":"kh","ц":"ts","ч":"ch","ш":"sh","щ":"sch","ъ":"","ы":"y","ь":"","э":"e","ю":"yu","я":"ya",
})
_scores24_force_lock = threading.Lock()
_scores24_force_dates: dict[str, float] = {}
_MATCH_STOPWORDS = {
    "fc", "cf", "sc", "afc", "fk", "ac", "club", "football", "futbol", "the", "de", "cd", "sd", "bk", "if",
    "women", "woman", "w", "u19", "u20", "u21", "u23", "reserves", "reserve",
}


def _match_name_normalize(value: Any) -> str:
    text = unicodedata.normalize("NFKD", str(value or "").lower())
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.translate(_CYRILLIC_TRANSLIT)
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return " ".join(text.split())


def _match_tokens(value: Any, *, keep_stopwords: bool = False) -> list[str]:
    tokens = _match_name_normalize(value).split()
    if keep_stopwords:
        return tokens
    useful = [token for token in tokens if token not in _MATCH_STOPWORDS]
    return useful or tokens


def _token_similarity(left: Any, right: Any) -> float:
    a = _match_tokens(left)
    b = _match_tokens(right)
    if not a or not b:
        return 0.0
    sa, sb = set(a), set(b)
    overlap = len(sa & sb)
    precision = overlap / max(1, len(sa))
    recall = overlap / max(1, len(sb))
    f1 = 2 * precision * recall / max(1e-9, precision + recall)
    seq = difflib.SequenceMatcher(None, " ".join(a), " ".join(b)).ratio()
    # A single distinctive shared token is often enough for names such as
    # "Sabah Baku" and "Sabah FA".
    contains = 1.0 if " ".join(a) in " ".join(b) or " ".join(b) in " ".join(a) else 0.0
    return max(f1, seq * 0.92, contains * 0.94)


def _scores24_slug_body(slug: str) -> str:
    return re.sub(r"^\d{2}-\d{2}-\d{4}-", "", str(slug or "").strip().lower())


def _ordered_slug_score(slug: str, home_name: str, away_name: str) -> float:
    tokens = _match_tokens(_scores24_slug_body(slug), keep_stopwords=True)
    if len(tokens) < 2:
        return 0.0
    best = 0.0
    for split in range(1, len(tokens)):
        left = " ".join(tokens[:split])
        right = " ".join(tokens[split:])
        home_score = _token_similarity(left, home_name)
        away_score = _token_similarity(right, away_name)
        # Both sides must have at least a plausible match; this prevents a very
        # famous home club from matching an unrelated fixture on the same date.
        if min(home_score, away_score) < 0.30:
            continue
        best = max(best, (home_score + away_score) / 2)
    return best


def _scores24_fixture_match_score(row: dict[str, Any], candidate: dict[str, Any]) -> float:
    source_slug = str(row.get("source_slug") or "")
    source_teams = ((row.get("fixture_item") or {}).get("teams") or {})
    api_teams = candidate.get("teams") or {}
    api_home = str((api_teams.get("home") or {}).get("name") or "")
    api_away = str((api_teams.get("away") or {}).get("name") or "")
    if not api_home or not api_away:
        return 0.0

    slug_score = _ordered_slug_score(source_slug, api_home, api_away)
    localized_home = _token_similarity((source_teams.get("home") or {}).get("name"), api_home)
    localized_away = _token_similarity((source_teams.get("away") or {}).get("name"), api_away)
    localized_score = (localized_home + localized_away) / 2

    source_ts = int((((row.get("fixture_item") or {}).get("fixture") or {}).get("timestamp") or 0))
    api_ts = int(((candidate.get("fixture") or {}).get("timestamp") or 0))
    if source_ts and api_ts:
        delta_hours = abs(source_ts - api_ts) / 3600
        time_score = max(0.0, 1.0 - delta_hours / 12.0)
    else:
        time_score = 0.5

    # Slugs are normally Latin and therefore the most reliable bridge between
    # localized Scores24 names and API-Football names.
    score = slug_score * 0.76 + localized_score * 0.16 + time_score * 0.08
    return round(score, 6)


def _scores24_resolve_api_fixture(row: dict[str, Any], force: bool = False) -> dict[str, Any] | None:
    if API_FOOTBALL_DEMO or not API_FOOTBALL_KEY:
        return None
    source_fixture = ((row.get("fixture_item") or {}).get("fixture") or {})
    date_text = str(source_fixture.get("date") or "")[:10]
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", date_text):
        return None
    bypass = False
    if force:
        with _scores24_force_lock:
            now = time.monotonic()
            previous = float(_scores24_force_dates.get(date_text) or 0.0)
            if now - previous >= 20.0:
                _scores24_force_dates[date_text] = now
                bypass = True
    data = _api_call(
        "fixtures",
        {"date": date_text, "timezone": APP_TIMEZONE},
        _fixture_ttl(date_text),
        allow_demo=False,
        bypass_cache=bypass,
    )
    candidates = [item for item in data.get("response") or [] if isinstance(item, dict)]
    if not candidates:
        return None

    known_id = int(row.get("api_football_fixture_id") or 0)
    if known_id:
        for item in candidates:
            if int(((item.get("fixture") or {}).get("id") or 0)) == known_id:
                result = dict(item)
                result["_scores24_match_confidence"] = 1.0
                return result

    ranked = sorted(
        ((_scores24_fixture_match_score(row, item), item) for item in candidates),
        key=lambda pair: pair[0],
        reverse=True,
    )
    best_score, best_item = ranked[0]
    second_score = ranked[1][0] if len(ranked) > 1 else 0.0
    # Require a solid match and a small separation from the next candidate.
    if best_score < 0.58 or (second_score >= 0.50 and best_score - second_score < 0.035):
        return None
    result = dict(best_item)
    result["_scores24_match_confidence"] = best_score
    return result


def _scores24_api_statistics(fixture_id: int, force: bool = False) -> list[dict[str, Any]]:
    if API_FOOTBALL_DEMO or not API_FOOTBALL_KEY or not int(fixture_id or 0):
        return []
    bypass = False
    if force:
        guard_key = f"stats:{int(fixture_id)}"
        with _scores24_force_lock:
            now = time.monotonic()
            previous = float(_scores24_force_dates.get(guard_key) or 0.0)
            if now - previous >= 20.0:
                _scores24_force_dates[guard_key] = now
                bypass = True
    data = _api_call(
        "fixtures/statistics",
        {"fixture": str(int(fixture_id))},
        6 * 3600,
        allow_demo=False,
        bypass_cache=bypass,
    )
    return [item for item in data.get("response") or [] if isinstance(item, dict)]


scores24_predictions.configure_api_football(
    fixture_resolver=_scores24_resolve_api_fixture,
    statistics_loader=_scores24_api_statistics,
)


def _demo_fixtures(date_text: str) -> dict[str, Any]:
    now_ts = int(time.time())
    fixtures = [
        {
            "fixture": {"id": 900001, "date": f"{date_text}T18:30:00+03:00", "timestamp": now_ts - 3600, "timezone": APP_TIMEZONE, "status": {"long": "Second Half", "short": "2H", "elapsed": 68}},
            "league": {"id": 39, "name": "Premier League", "country": "England", "logo": "", "flag": "", "season": 2026, "round": "Regular Season - 1"},
            "teams": {"home": {"id": 42, "name": "Arsenal", "logo": "", "winner": True}, "away": {"id": 49, "name": "Chelsea", "logo": "", "winner": False}},
            "goals": {"home": 2, "away": 1}, "score": {"halftime": {"home": 1, "away": 1}, "fulltime": {"home": None, "away": None}},
        },
        {
            "fixture": {"id": 900002, "date": f"{date_text}T21:00:00+03:00", "timestamp": now_ts + 5400, "timezone": APP_TIMEZONE, "status": {"long": "Not Started", "short": "NS", "elapsed": None}},
            "league": {"id": 140, "name": "La Liga", "country": "Spain", "logo": "", "flag": "", "season": 2026, "round": "Regular Season - 1"},
            "teams": {"home": {"id": 529, "name": "Barcelona", "logo": "", "winner": None}, "away": {"id": 541, "name": "Real Madrid", "logo": "", "winner": None}},
            "goals": {"home": None, "away": None}, "score": {"halftime": {"home": None, "away": None}, "fulltime": {"home": None, "away": None}},
        },
        {
            "fixture": {"id": 900003, "date": f"{date_text}T16:00:00+03:00", "timestamp": now_ts - 10800, "timezone": APP_TIMEZONE, "status": {"long": "Match Finished", "short": "FT", "elapsed": 90}},
            "league": {"id": 78, "name": "Bundesliga", "country": "Germany", "logo": "", "flag": "", "season": 2026, "round": "Regular Season - 1"},
            "teams": {"home": {"id": 157, "name": "Bayern Munich", "logo": "", "winner": True}, "away": {"id": 165, "name": "Borussia Dortmund", "logo": "", "winner": False}},
            "goals": {"home": 3, "away": 2}, "score": {"halftime": {"home": 2, "away": 1}, "fulltime": {"home": 3, "away": 2}},
        },
    ]
    return {"get": "fixtures", "parameters": {"date": date_text}, "errors": {}, "results": len(fixtures), "paging": {"current": 1, "total": 1}, "response": fixtures, "_cache": "demo", "_quota": _quota_snapshot(), "demo": True}


def _demo_stats() -> list[dict[str, Any]]:
    values = [
        ("Shots on Goal", 7, 3), ("Shots off Goal", 6, 4), ("Total Shots", 16, 9),
        ("Blocked Shots", 3, 2), ("Shots insidebox", 11, 5), ("Shots outsidebox", 5, 4),
        ("Fouls", 9, 12), ("Corner Kicks", 8, 3), ("Offsides", 2, 1),
        ("Ball Possession", "61%", "39%"), ("Yellow Cards", 1, 3), ("Goalkeeper Saves", 2, 5),
    ]
    return [
        {"team": {"id": 42, "name": "Arsenal", "logo": ""}, "statistics": [{"type": t, "value": h} for t, h, _ in values]},
        {"team": {"id": 49, "name": "Chelsea", "logo": ""}, "statistics": [{"type": t, "value": a} for t, _, a in values]},
    ]


def _number(value: Any) -> float:
    if value in (None, ""):
        return 0.0
    try:
        return float(str(value).replace("%", "").strip())
    except Exception:
        return 0.0


def _statistics_from_fixture_players(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Build a partial team-statistics response from fixtures/players.

    This is only a fallback. Official fixtures/statistics values always win.
    """
    blocks: list[dict[str, Any]] = []
    for team_block in items:
        shots_total = shots_on = fouls = yellow = red = saves = passes = 0.0
        found = False
        for player_item in team_block.get("players") or []:
            stat_list = player_item.get("statistics") or []
            stats = stat_list[0] if stat_list else {}
            if not stats:
                continue
            found = True
            shots = stats.get("shots") or {}
            foul_stats = stats.get("fouls") or {}
            cards = stats.get("cards") or {}
            goals = stats.get("goals") or {}
            pass_stats = stats.get("passes") or {}
            shots_total += _number(shots.get("total"))
            shots_on += _number(shots.get("on"))
            fouls += _number(foul_stats.get("committed"))
            yellow += _number(cards.get("yellow"))
            red += _number(cards.get("red"))
            saves += _number(goals.get("saves"))
            passes += _number(pass_stats.get("total"))
        if not found:
            continue
        def clean(value: float) -> int | float:
            return int(value) if value.is_integer() else round(value, 1)
        statistics = [
            {"type": "Shots on Goal", "value": clean(shots_on)},
            {"type": "Shots off Goal", "value": clean(max(0.0, shots_total - shots_on))},
            {"type": "Total Shots", "value": clean(shots_total)},
            {"type": "Fouls", "value": clean(fouls)},
            {"type": "Yellow Cards", "value": clean(yellow)},
            {"type": "Red Cards", "value": clean(red)},
            {"type": "Goalkeeper Saves", "value": clean(saves)},
            {"type": "Total passes", "value": clean(passes)},
        ]
        blocks.append({"team": team_block.get("team") or {}, "statistics": statistics})
    return blocks


def _merge_fixture_statistics(official: list[dict[str, Any]], fallback: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[int, dict[str, Any]] = {}
    order: list[int] = []
    for block in [*official, *fallback]:
        team = block.get("team") or {}
        team_id = int(team.get("id") or 0)
        if team_id not in merged:
            merged[team_id] = {"team": team, "statistics": []}
            order.append(team_id)
        elif not merged[team_id].get("team"):
            merged[team_id]["team"] = team
        existing = {str(x.get("type")): x.get("value") for x in merged[team_id].get("statistics") or []}
        for item in block.get("statistics") or []:
            stat_type = str(item.get("type") or "")
            value = item.get("value")
            if stat_type not in existing or existing.get(stat_type) in (None, ""):
                existing[stat_type] = value
        merged[team_id]["statistics"] = [{"type": key, "value": value} for key, value in existing.items()]
    return [merged[team_id] for team_id in order if team_id in merged]


def _last_team_fixtures(team_id: str, last: int = 20) -> list[dict[str, Any]]:
    data = _api_call("fixtures", {"team": team_id, "last": max(5, min(30, last)), "timezone": APP_TIMEZONE}, 6 * 3600)
    return data.get("response") or []


def _team_trend_stats(fixtures: list[dict[str, Any]], team_id: str) -> dict[str, int]:
    out = {
        "played": 0,
        "unbeaten": 0,
        "scored": 0,
        "conceded": 0,
        "btts": 0,
        "wins": 0,
        "cover_plus1": 0,
        "win_by2": 0,
        "over15": 0,
        "over25": 0,
        "under35": 0,
        "under45": 0,
    }
    team_id_int = int(team_id)
    for item in fixtures:
        teams = item.get("teams") or {}
        goals = item.get("goals") or {}
        home = teams.get("home") or {}
        away = teams.get("away") or {}
        is_home = int(home.get("id") or 0) == team_id_int
        is_away = int(away.get("id") or 0) == team_id_int
        if not (is_home or is_away):
            continue
        gf = goals.get("home") if is_home else goals.get("away")
        ga = goals.get("away") if is_home else goals.get("home")
        if gf is None or ga is None:
            continue
        gf, ga = int(gf), int(ga)
        total = gf + ga
        margin = gf - ga
        out["played"] += 1
        out["unbeaten"] += int(gf >= ga)
        out["wins"] += int(gf > ga)
        out["scored"] += int(gf > 0)
        out["conceded"] += int(ga > 0)
        out["btts"] += int(gf > 0 and ga > 0)
        out["cover_plus1"] += int(margin >= -1)
        out["win_by2"] += int(margin >= 2)
        out["over15"] += int(total >= 2)
        out["over25"] += int(total >= 3)
        out["under35"] += int(total <= 3)
        out["under45"] += int(total <= 4)
    return out


def _build_trends(home_id: str, away_id: str, home_name: str, away_name: str) -> dict[str, Any]:
    if API_FOOTBALL_DEMO or not API_FOOTBALL_KEY:
        home = {"played": 20, "unbeaten": 19, "scored": 18, "conceded": 11, "btts": 10, "wins": 15, "cover_plus1": 20, "win_by2": 8, "over15": 16, "over25": 12, "under35": 14, "under45": 18}
        away = {"played": 20, "unbeaten": 14, "scored": 17, "conceded": 16, "btts": 13, "wins": 9, "cover_plus1": 17, "win_by2": 4, "over15": 15, "over25": 11, "under35": 13, "under45": 17}
        demo = True
    else:
        home_fixtures = _last_team_fixtures(home_id, 20)
        away_fixtures = _last_team_fixtures(away_id, 20)
        home = _team_trend_stats(home_fixtures, home_id)
        away = _team_trend_stats(away_fixtures, away_id)
        demo = False

    def phrase(value: int, played: int) -> str:
        return f"в {value} из {played} последних матчей" if played else "данных пока недостаточно"

    groups = [
        {
            "title": "Прогнозы на исход",
            "items": [
                {
                    "label": f"{home_name} не проиграет",
                    "notes": [
                        {"team": "home", "text": f"{home_name} не проигрывает", "stat": phrase(home["unbeaten"], home["played"])},
                        {"team": "away", "text": f"{away_name} выигрывает", "stat": phrase(away["wins"], away["played"])},
                    ],
                    "pick": f"{home_name} не проиграет",
                }
            ],
        },
        {
            "title": "Прогнозы на Тотал",
            "items": [
                {
                    "label": f"{home_name} забивает",
                    "notes": [
                        {"team": "home", "text": f"{home_name} забивает", "stat": phrase(home["scored"], home["played"])},
                        {"team": "away", "text": f"{away_name} пропускает", "stat": phrase(away["conceded"], away["played"])},
                    ],
                    "pick": f"{home_name} забьёт гол",
                },
                {
                    "label": f"{away_name} забивает",
                    "notes": [
                        {"team": "away", "text": f"{away_name} забивает", "stat": phrase(away["scored"], away["played"])},
                        {"team": "home", "text": f"{home_name} пропускает", "stat": phrase(home["conceded"], home["played"])},
                    ],
                    "pick": f"{away_name} забьёт гол",
                },
                {
                    "label": "ТБ 1.5",
                    "notes": [
                        {"team": "home", "text": f"Матчи {home_name}: ТБ 1.5", "stat": phrase(home["over15"], home["played"])},
                        {"team": "away", "text": f"Матчи {away_name}: ТБ 1.5", "stat": phrase(away["over15"], away["played"])},
                    ],
                    "pick": "ТБ 1.5",
                },
                {
                    "label": "ТБ 2.5",
                    "notes": [
                        {"team": "home", "text": f"Матчи {home_name}: ТБ 2.5", "stat": phrase(home["over25"], home["played"])},
                        {"team": "away", "text": f"Матчи {away_name}: ТБ 2.5", "stat": phrase(away["over25"], away["played"])},
                    ],
                    "pick": "ТБ 2.5",
                },
                {
                    "label": "ТМ 3.5",
                    "notes": [
                        {"team": "home", "text": f"Матчи {home_name}: ТМ 3.5", "stat": phrase(home["under35"], home["played"])},
                        {"team": "away", "text": f"Матчи {away_name}: ТМ 3.5", "stat": phrase(away["under35"], away["played"])},
                    ],
                    "pick": "ТМ 3.5",
                },
                {
                    "label": "ТМ 4.5",
                    "notes": [
                        {"team": "home", "text": f"Матчи {home_name}: ТМ 4.5", "stat": phrase(home["under45"], home["played"])},
                        {"team": "away", "text": f"Матчи {away_name}: ТМ 4.5", "stat": phrase(away["under45"], away["played"])},
                    ],
                    "pick": "ТМ 4.5",
                },
            ],
        },
        {
            "title": "Прогнозы на фору",
            "items": [
                {
                    "label": f"{home_name} с форой (+1)",
                    "notes": [
                        {"team": "home", "text": f"{home_name} проходит фору (+1)", "stat": phrase(home["cover_plus1"], home["played"])},
                        {"team": "away", "text": f"{away_name} выигрывает в 2+ мяча", "stat": phrase(away["win_by2"], away["played"])},
                    ],
                    "pick": f"{home_name} Фора (+1)",
                },
                {
                    "label": f"{away_name} с форой (+1)",
                    "notes": [
                        {"team": "away", "text": f"{away_name} проходит фору (+1)", "stat": phrase(away["cover_plus1"], away["played"])},
                        {"team": "home", "text": f"{home_name} выигрывает в 2+ мяча", "stat": phrase(home["win_by2"], home["played"])},
                    ],
                    "pick": f"{away_name} Фора (+1)",
                },
                {
                    "label": f"{home_name} с форой (-1)",
                    "notes": [
                        {"team": "home", "text": f"{home_name} выигрывает в 2+ мяча", "stat": phrase(home["win_by2"], home["played"])},
                    ],
                    "pick": f"{home_name} Фора (-1)",
                },
            ],
        },
        {
            "title": "Прогнозы на Обе забьют",
            "items": [
                {
                    "label": "Команды забивают и пропускают",
                    "notes": [
                        {"team": "home", "text": f"{home_name}: обе забьют", "stat": phrase(home["btts"], home["played"])},
                        {"team": "away", "text": f"{away_name}: обе забьют", "stat": phrase(away["btts"], away["played"])},
                    ],
                    "pick": "Обе команды забьют — Да",
                }
            ],
        },
    ]
    return {"ok": True, "home": home, "away": away, "groups": groups, "demo": demo, "_quota": _quota_snapshot()}



def _json_response(start_response: Callable[..., Any], payload: Any, status: int = 200, headers: list[tuple[str, str]] | None = None) -> list[bytes]:
    body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    try:
        phrase = HTTPStatus(status).phrase
    except Exception:
        phrase = "Unknown"
    response_headers = [
        ("Content-Type", "application/json; charset=utf-8"),
        ("Content-Length", str(len(body))),
        ("Cache-Control", "no-store"),
        ("X-Content-Type-Options", "nosniff"),
    ]
    if headers:
        response_headers.extend(headers)
    start_response(f"{status} {phrase}", response_headers)
    return [body]


def _file_response(start_response: Callable[..., Any], path: Path) -> list[bytes]:
    if not path.exists() or not path.is_file():
        return _json_response(start_response, {"ok": False, "error": "not found"}, 404)
    body = path.read_bytes()
    ctype = mimetypes.guess_type(str(path))[0] or "application/octet-stream"
    if path.suffix == ".html":
        ctype = "text/html; charset=utf-8"
    elif path.suffix == ".css":
        ctype = "text/css; charset=utf-8"
    elif path.suffix == ".js":
        ctype = "application/javascript; charset=utf-8"
    start_response("200 OK", [
        ("Content-Type", ctype),
        ("Content-Length", str(len(body))),
        ("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0"),
        ("Pragma", "no-cache"),
        ("Expires", "0"),
        ("X-Content-Type-Options", "nosniff"),
    ])
    return [body]


def _query(environ: dict[str, Any]) -> dict[str, str]:
    parsed = urllib.parse.parse_qs(str(environ.get("QUERY_STRING") or ""), keep_blank_values=False)
    return {key: str(values[0]) for key, values in parsed.items() if values}



def _demo_teams(search: str) -> dict[str, Any]:
    items = [
        {"team": {"id": 42, "name": "Arsenal", "code": "ARS", "country": "England", "founded": 1886, "national": False, "logo": "https://media.api-sports.io/football/teams/42.png"}, "venue": {"name": "Emirates Stadium", "city": "London", "capacity": 60383}},
        {"team": {"id": 529, "name": "Barcelona", "code": "BAR", "country": "Spain", "founded": 1899, "national": False, "logo": "https://media.api-sports.io/football/teams/529.png"}, "venue": {"name": "Camp Nou", "city": "Barcelona", "capacity": 99354}},
        {"team": {"id": 541, "name": "Real Madrid", "code": "REA", "country": "Spain", "founded": 1902, "national": False, "logo": "https://media.api-sports.io/football/teams/541.png"}, "venue": {"name": "Santiago Bernabéu", "city": "Madrid", "capacity": 81044}},
        {"team": {"id": 50, "name": "Manchester City", "code": "MCI", "country": "England", "founded": 1880, "national": False, "logo": "https://media.api-sports.io/football/teams/50.png"}, "venue": {"name": "Etihad Stadium", "city": "Manchester", "capacity": 55097}},
        {"team": {"id": 33, "name": "Manchester United", "code": "MUN", "country": "England", "founded": 1878, "national": False, "logo": "https://media.api-sports.io/football/teams/33.png"}, "venue": {"name": "Old Trafford", "city": "Manchester", "capacity": 76212}},
    ]
    q = search.casefold().strip()
    result = [x for x in items if q in str(x["team"]["name"]).casefold()] if q else items
    return {"get": "teams", "parameters": {"search": search}, "errors": {}, "results": len(result), "paging": {"current": 1, "total": 1}, "response": result, "demo": True, "_quota": _quota_snapshot()}


def _demo_leagues(search: str = "") -> dict[str, Any]:
    items = [
        {"league": {"id": 39, "name": "Premier League", "type": "League", "logo": "https://media.api-sports.io/football/leagues/39.png"}, "country": {"name": "England", "code": "GB", "flag": "https://media.api-sports.io/flags/gb.svg"}, "seasons": [{"year": 2026, "start": "2026-08-01", "end": "2027-05-31", "current": True, "coverage": {"standings": True, "fixtures": {"statistics_fixtures": True, "events": True, "lineups": True}}}]},
        {"league": {"id": 140, "name": "La Liga", "type": "League", "logo": "https://media.api-sports.io/football/leagues/140.png"}, "country": {"name": "Spain", "code": "ES", "flag": "https://media.api-sports.io/flags/es.svg"}, "seasons": [{"year": 2026, "start": "2026-08-01", "end": "2027-05-31", "current": True, "coverage": {"standings": True, "fixtures": {"statistics_fixtures": True, "events": True, "lineups": True}}}]},
        {"league": {"id": 135, "name": "Serie A", "type": "League", "logo": "https://media.api-sports.io/football/leagues/135.png"}, "country": {"name": "Italy", "code": "IT", "flag": "https://media.api-sports.io/flags/it.svg"}, "seasons": [{"year": 2026, "start": "2026-08-01", "end": "2027-05-31", "current": True, "coverage": {"standings": True, "fixtures": {"statistics_fixtures": True, "events": True, "lineups": True}}}]},
        {"league": {"id": 78, "name": "Bundesliga", "type": "League", "logo": "https://media.api-sports.io/football/leagues/78.png"}, "country": {"name": "Germany", "code": "DE", "flag": "https://media.api-sports.io/flags/de.svg"}, "seasons": [{"year": 2026, "start": "2026-08-01", "end": "2027-05-31", "current": True, "coverage": {"standings": True, "fixtures": {"statistics_fixtures": True, "events": True, "lineups": True}}}]},
        {"league": {"id": 61, "name": "Ligue 1", "type": "League", "logo": "https://media.api-sports.io/football/leagues/61.png"}, "country": {"name": "France", "code": "FR", "flag": "https://media.api-sports.io/flags/fr.svg"}, "seasons": [{"year": 2026, "start": "2026-08-01", "end": "2027-05-31", "current": True, "coverage": {"standings": True, "fixtures": {"statistics_fixtures": True, "events": True, "lineups": True}}}]},
    ]
    q = search.casefold().strip()
    result = [x for x in items if q in (str(x["league"]["name"]) + " " + str(x["country"]["name"])).casefold()] if q else items
    return {"get": "leagues", "parameters": {"search": search}, "errors": {}, "results": len(result), "paging": {"current": 1, "total": 1}, "response": result, "demo": True, "_quota": _quota_snapshot()}

def _demo_player_search(search: str) -> dict[str, Any]:
    q = (search or "").strip()
    response = []
    if q:
        for i in range(1, 4):
            response.append({
                "player": {
                    "id": 9000 + i,
                    "name": f"{q.title()} {i}",
                    "firstname": q.title(),
                    "lastname": str(i),
                    "age": 20 + i,
                    "nationality": "Demo",
                    "photo": f"https://media.api-sports.io/football/players/{9000 + i}.png",
                },
                "statistics": [{
                    "team": {"id": 500 + i, "name": f"{q.title()} FC {i}", "logo": ""},
                    "league": {"id": 300 + i, "name": "Demo League", "logo": ""},
                }],
            })
    return {
        "get": "players/search",
        "parameters": {"search": search},
        "errors": {},
        "results": len(response),
        "paging": {"current": 1, "total": 1},
        "response": response,
        "demo": True,
        "_quota": _quota_snapshot(),
    }


def _demo_team_profile(team_id: str, league_id: str = "39", season: int = 2026) -> dict[str, Any]:
    lookup = {str(x["team"]["id"]): x for x in _demo_teams("")["response"]}
    item = lookup.get(team_id) or next(iter(lookup.values()))
    fixtures = _demo_fixtures(datetime.now().strftime("%Y-%m-%d"))["response"]
    team = item["team"]
    statistics = {
        "league": {"id": int(league_id or 39), "name": "Premier League", "country": team.get("country") or "England", "logo": "https://media.api-sports.io/football/leagues/39.png", "season": season},
        "team": team,
        "form": "WWDLW",
        "fixtures": {
            "played": {"home": 8, "away": 7, "total": 15},
            "wins": {"home": 6, "away": 4, "total": 10},
            "draws": {"home": 1, "away": 2, "total": 3},
            "loses": {"home": 1, "away": 1, "total": 2},
        },
        "goals": {
            "for": {
                "total": {"home": 19, "away": 13, "total": 32},
                "average": {"home": "2.4", "away": "1.9", "total": "2.1"},
                "minute": {
                    "0-15": {"total": 4, "percentage": "12.50%"},
                    "16-30": {"total": 6, "percentage": "18.75%"},
                    "31-45": {"total": 8, "percentage": "25.00%"},
                    "46-60": {"total": 5, "percentage": "15.63%"},
                    "61-75": {"total": 4, "percentage": "12.50%"},
                    "76-90": {"total": 5, "percentage": "15.63%"},
                },
            },
            "against": {
                "total": {"home": 7, "away": 8, "total": 15},
                "average": {"home": "0.9", "away": "1.1", "total": "1.0"},
                "minute": {
                    "0-15": {"total": 1, "percentage": "6.67%"},
                    "16-30": {"total": 3, "percentage": "20.00%"},
                    "31-45": {"total": 2, "percentage": "13.33%"},
                    "46-60": {"total": 4, "percentage": "26.67%"},
                    "61-75": {"total": 2, "percentage": "13.33%"},
                    "76-90": {"total": 3, "percentage": "20.00%"},
                },
            },
        },
        "clean_sheet": {"home": 4, "away": 3, "total": 7},
        "failed_to_score": {"home": 1, "away": 1, "total": 2},
        "biggest": {
            "streak": {"wins": 4, "draws": 1, "loses": 1},
            "wins": {"home": "5-0", "away": "0-4"},
            "loses": {"home": "0-2", "away": "3-1"},
            "goals": {"for": {"home": 5, "away": 4}, "against": {"home": 2, "away": 3}},
        },
    }
    standing = {"rank": 2, "team": team, "points": 33, "goalsDiff": 17, "form": "WWDLW", "all": {"played": 15, "win": 10, "draw": 3, "lose": 2, "goals": {"for": 32, "against": 15}}}
    coach = {
        "id": 101,
        "name": "M. Arteta",
        "firstname": "Mikel",
        "lastname": "Arteta",
        "age": 44,
        "photo": "https://media.api-sports.io/football/coachs/101.png",
        "nationality": "Spain",
        "team": team,
        "career": [
            {"team": team, "start": "2019-12-22", "end": None},
            {"team": {"id": 50, "name": "Manchester City", "logo": "https://media.api-sports.io/football/teams/50.png"}, "start": "2016-07-03", "end": "2019-12-20"},
        ],
    }
    transfers = [
        {
            "player": {"id": 9001, "name": "Игрок A"},
            "update": f"{season}-07-12",
            "transfers": [{"date": f"{season}-07-01", "type": "€45M", "teams": {"in": team, "out": {"id": 49, "name": "Chelsea", "logo": "https://media.api-sports.io/football/teams/49.png"}}}],
        },
        {
            "player": {"id": 9002, "name": "Игрок B"},
            "update": f"{season}-06-28",
            "transfers": [{"date": f"{season}-06-20", "type": "Loan", "teams": {"in": {"id": 40, "name": "Liverpool", "logo": "https://media.api-sports.io/football/teams/40.png"}, "out": team}}],
        },
    ]
    return {
        "team": item,
        "last": fixtures[:2],
        "next": fixtures[1:],
        "statistics": statistics,
        "standing": standing,
        "coach": coach,
        "transfers": transfers,
        "club_trophies": [],
        "club_trophies_supported": False,
        "context": {"league": int(league_id or 39), "season": season},
        "demo": True,
        "_quota": _quota_snapshot(),
    }



def _demo_player_profile(player_id: str, season: int = 2026, fixture_id: str = "", team_id: str = "", league_id: str = "") -> dict[str, Any]:
    pid = int(player_id)
    player = {
        "id": pid,
        "name": "B. Saka" if pid in {7, 8} else f"Игрок {pid}",
        "firstname": "Bukayo",
        "lastname": "Saka",
        "age": 24,
        "birth": {"date": "2001-09-05", "place": "London", "country": "England"},
        "nationality": "England",
        "height": "178 cm",
        "weight": "72 kg",
        "injured": False,
        "photo": f"https://media.api-sports.io/football/players/{pid}.png",
    }
    team = {"id": int(team_id or 42), "name": "Arsenal", "logo": "https://media.api-sports.io/football/teams/42.png"}
    league = {"id": int(league_id or 39), "name": "Premier League", "country": "England", "logo": "https://media.api-sports.io/football/leagues/39.png", "season": season}
    season_stats = {
        "team": team,
        "league": league,
        "games": {"appearences": 28, "lineups": 26, "minutes": 2314, "number": 7, "position": "Attacker", "rating": "7.42", "captain": False},
        "substitutes": {"in": 2, "out": 18, "bench": 2},
        "shots": {"total": 66, "on": 31},
        "goals": {"total": 14, "conceded": 0, "assists": 9, "saves": None},
        "passes": {"total": 923, "key": 58, "accuracy": 84},
        "tackles": {"total": 31, "blocks": 4, "interceptions": 12},
        "duels": {"total": 231, "won": 126},
        "dribbles": {"attempts": 92, "success": 48, "past": None},
        "fouls": {"drawn": 44, "committed": 19},
        "cards": {"yellow": 4, "yellowred": 0, "red": 0},
        "penalty": {"won": 2, "commited": 0, "scored": 3, "missed": 0, "saved": 0},
    }
    cup_stats = {
        **season_stats,
        "league": {"id": 2, "name": "Champions League", "country": "World", "logo": "https://media.api-sports.io/football/leagues/2.png", "season": season},
        "games": {**season_stats["games"], "appearences": 8, "lineups": 7, "minutes": 648, "rating": "7.18"},
        "goals": {**season_stats["goals"], "total": 3, "assists": 2},
    }
    match_stats = {
        "team": team,
        "player": {"id": pid, "name": player["name"], "photo": player["photo"]},
        "statistics": {
            "games": {"minutes": 90, "number": 7, "position": "F", "rating": "8.1", "captain": False, "substitute": False},
            "offsides": 1,
            "shots": {"total": 4, "on": 3},
            "goals": {"total": 1, "conceded": 0, "assists": 1, "saves": 0},
            "passes": {"total": 43, "key": 4, "accuracy": "86%"},
            "tackles": {"total": 2, "blocks": 0, "interceptions": 1},
            "duels": {"total": 11, "won": 7},
            "dribbles": {"attempts": 5, "success": 3, "past": 0},
            "fouls": {"drawn": 2, "committed": 1},
            "cards": {"yellow": 0, "red": 0},
            "penalty": {"won": 0, "commited": 0, "scored": 0, "missed": 0, "saved": 0},
        },
    }
    career_teams = [
        {"team": team, "seasons": [2019, 2020, 2021, 2022, 2023, 2024, 2025, season]},
        {"team": {"id": 10, "name": "England", "logo": "https://media.api-sports.io/football/teams/10.png"}, "seasons": [2020, 2021, 2022, 2023, 2024, 2025, season]},
    ]
    injuries = [{"player": player, "team": team, "fixture": {"id": 1, "date": f"{season}-04-11"}, "league": league, "type": "Knock", "reason": "Minor injury"}]
    sidelined = [
        {"type": "Hamstring Injury", "start": "2025-11-02", "end": "2025-11-19"},
        {"type": "Ankle Injury", "start": "2024-03-08", "end": "2024-03-18"},
    ]
    transfers = [{
        "player": {"id": pid, "name": player["name"]},
        "update": "2019-07-01",
        "transfers": [{"date": "2019-07-01", "type": "Youth", "teams": {"in": team, "out": {"id": 0, "name": "Arsenal Academy", "logo": ""}}}],
    }]
    trophies = [
        {"league": "FA Cup", "country": "England", "season": "2019/2020", "place": "Winner"},
        {"league": "Community Shield", "country": "England", "season": "2023", "place": "Winner"},
        {"league": "European Championship", "country": "World", "season": "2024", "place": "Runner-up"},
    ]
    current_squad = {"team": team, "player": {"id": pid, "name": player["name"], "age": player["age"], "number": 7, "position": "Attacker", "photo": player["photo"]}}
    return {
        "profile": {"player": player, "statistics": [season_stats, cup_stats]},
        "match": match_stats if fixture_id else None,
        "season": season,
        "career_seasons": list(range(2019, season + 1)),
        "career_teams": career_teams,
        "injuries": injuries,
        "sidelined": sidelined,
        "transfers": transfers,
        "trophies": trophies,
        "current_squad": current_squad,
        "rankings": {"topscorers": 3, "topassists": 2, "topyellowcards": None, "topredcards": None},
        "demo": True,
        "_quota": _quota_snapshot(),
    }


def _demo_league_view(league_id: str, season: int) -> dict[str, Any]:
    league_items = _demo_leagues("")["response"]
    league_item = next((x for x in league_items if str(x["league"]["id"]) == league_id), league_items[0])
    teams = _demo_teams("")["response"]
    standings = []
    for i, item in enumerate(teams, start=1):
        played = 8
        home_played, away_played = 4, 4
        home_win = max(0, 4 - i // 3)
        away_win = max(0, 3 - i // 3)
        home_draw, away_draw = 1, 1
        home_lose = max(0, home_played - home_win - home_draw)
        away_lose = max(0, away_played - away_win - away_draw)
        gf, ga = max(4, 18 - i), max(2, 7 + i // 2)
        standings.append({
            "rank": i,
            "team": item["team"],
            "points": home_win * 3 + home_draw + away_win * 3 + away_draw,
            "goalsDiff": gf - ga,
            "form": "WWDLW",
            "all": {"played": played, "win": home_win + away_win, "draw": home_draw + away_draw, "lose": home_lose + away_lose, "goals": {"for": gf, "against": ga}},
            "home": {"played": home_played, "win": home_win, "draw": home_draw, "lose": home_lose, "goals": {"for": max(2, gf // 2 + 2), "against": max(1, ga // 2)}},
            "away": {"played": away_played, "win": away_win, "draw": away_draw, "lose": away_lose, "goals": {"for": max(2, gf // 2 - 1), "against": max(1, ga - max(1, ga // 2))}},
        })
    fixtures = _demo_fixtures(datetime.now().strftime("%Y-%m-%d"))["response"]
    return {
        "league": league_item,
        "season": season,
        "standings": standings,
        "standings_groups": [{"name": "Общая таблица", "rows": standings}],
        "next": fixtures,
        "current_round": "Regular Season - 1",
        "demo": True,
        "_quota": _quota_snapshot(),
    }


def _demo_league_leaders(league_id: str, season: int) -> dict[str, Any]:
    teams = _demo_teams("")["response"]
    def make(index: int, metric: str) -> dict[str, Any]:
        team = teams[index % len(teams)]["team"]
        goals = max(1, 12 - index)
        return {
            "player": {"id": 7000 + index, "name": f"Игрок {index + 1}", "photo": f"https://media.api-sports.io/football/players/{7000 + index}.png"},
            "statistics": [{
                "team": team,
                "league": {"id": int(league_id), "season": season},
                "games": {"appearences": 12 + index, "minutes": 900 + index * 45},
                "goals": {"total": goals, "assists": max(0, 8 - index)},
                "cards": {"yellow": 7 + index, "red": index % 3},
            }],
        }
    items = [make(i, "") for i in range(10)]
    return {"leaders": {"topscorers": items, "topassists": list(reversed(items)), "topyellowcards": items, "topredcards": items[:6]}, "season": season, "demo": True, "_quota": _quota_snapshot()}


def _demo_league_calendar(league_id: str, season: int, selected_round: str = "") -> dict[str, Any]:
    rounds = [f"Regular Season - {i}" for i in range(1, 6)]
    selected = selected_round if selected_round in rounds else rounds[0]
    fixtures = _demo_fixtures(datetime.now().strftime("%Y-%m-%d"))["response"]
    for item in fixtures:
        item.setdefault("league", {})["round"] = selected
    return {"rounds": rounds, "round": selected, "fixtures": fixtures, "season": season, "demo": True, "_quota": _quota_snapshot()}


def _demo_league_teams(league_id: str, season: int) -> dict[str, Any]:
    return {"teams": _demo_teams("")["response"], "season": season, "demo": True, "_quota": _quota_snapshot()}


def _demo_league_seasons(league_id: str, season: int) -> dict[str, Any]:
    item = next((x for x in _demo_leagues("")["response"] if str((x.get("league") or {}).get("id")) == league_id), _demo_leagues("")["response"][0])
    base = item.get("seasons") or []
    seasons = []
    for year in range(season, max(1999, season - 8), -1):
        seasons.append({"year": year, "start": f"{year}-08-01", "end": f"{year + 1}-05-31", "current": year == season, "coverage": (base[0].get("coverage") if base else {})})
    teams = _demo_teams("")["response"]
    winners = [{"season": year, "team": teams[(season - year) % len(teams)]["team"]} for year in range(season - 1, max(1999, season - 6), -1)]
    return {"seasons": seasons, "winners": winners, "season": season, "demo": True, "_quota": _quota_snapshot()}



def _read_json_body(environ: dict[str, Any], max_bytes: int = 64_000) -> dict[str, Any]:
    try:
        length = int(environ.get("CONTENT_LENGTH") or 0)
    except Exception:
        length = 0
    if length <= 0 or length > max_bytes:
        return {}
    try:
        raw = environ.get("wsgi.input").read(length)
        data = json.loads(raw.decode("utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _goal_notify_key(chat_id: Any, fixture_id: Any) -> str:
    return f"{str(chat_id).strip()}:{str(fixture_id).strip()}"


def _goal_notify_load() -> None:
    global _goal_notify_subs
    loaded: dict[str, dict[str, Any]] = {}
    if _redis_client is not None:
        try:
            raw = _redis_client.get(f"{CACHE_PREFIX}:goal-notify:subscriptions")
            if raw:
                value = json.loads(raw)
                if isinstance(value, dict):
                    loaded = {str(k): v for k, v in value.items() if isinstance(v, dict)}
        except Exception as exc:
            print(f"[goal-notify] Redis load failed: {exc}")
    if not loaded:
        try:
            if _goal_notify_file.exists():
                value = json.loads(_goal_notify_file.read_text(encoding="utf-8"))
                if isinstance(value, dict):
                    loaded = {str(k): v for k, v in value.items() if isinstance(v, dict)}
        except Exception as exc:
            print(f"[goal-notify] file load failed: {exc}")
    with _goal_notify_lock:
        _goal_notify_subs = loaded
    print(f"[goal-notify] loaded {len(loaded)} subscriptions")


def _goal_notify_save() -> None:
    with _goal_notify_lock:
        snapshot = dict(_goal_notify_subs)
    raw = json.dumps(snapshot, ensure_ascii=False, separators=(",", ":"))
    if _redis_client is not None:
        try:
            _redis_client.set(f"{CACHE_PREFIX}:goal-notify:subscriptions", raw)
        except Exception as exc:
            print(f"[goal-notify] Redis save failed: {exc}")
    try:
        _goal_notify_file.parent.mkdir(parents=True, exist_ok=True)
        tmp = _goal_notify_file.with_suffix(".tmp")
        tmp.write_text(raw, encoding="utf-8")
        tmp.replace(_goal_notify_file)
    except Exception as exc:
        print(f"[goal-notify] file save failed: {exc}")


def _goal_notify_fixture(fixture_id: str) -> dict[str, Any] | None:
    if API_FOOTBALL_DEMO or not API_FOOTBALL_KEY:
        return None
    data = _api_call("fixtures", {"id": fixture_id, "timezone": APP_TIMEZONE}, 10, allow_demo=False)
    items = data.get("response") or []
    return items[0] if items else None


def _goal_notify_user(init_data: str) -> dict[str, Any] | None:
    verifier = getattr(notify_app, "verify_init_data", None)
    if not callable(verifier):
        return None
    try:
        user = verifier(init_data)
        return user if isinstance(user, dict) else None
    except Exception:
        return None


def _goal_notify_escape(value: Any) -> str:
    try:
        return notify_app.html.escape(str(value or ""))
    except Exception:
        return str(value or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _goal_notify_text(item: dict[str, Any], old_home: int, old_away: int) -> str:
    fixture = item.get("fixture") or {}
    status = fixture.get("status") or {}
    teams = item.get("teams") or {}
    goals = item.get("goals") or {}
    league = item.get("league") or {}
    home_name = _goal_notify_escape((teams.get("home") or {}).get("name") or "Хозяева")
    away_name = _goal_notify_escape((teams.get("away") or {}).get("name") or "Гости")
    league_name = _goal_notify_escape(league.get("name") or "Футбол")
    home_score = int(goals.get("home") or 0)
    away_score = int(goals.get("away") or 0)
    elapsed = status.get("elapsed")
    scorer_side = ""
    if home_score > old_home and away_score == old_away:
        scorer_side = f"\nЗабил: <b>{home_name}</b>"
    elif away_score > old_away and home_score == old_home:
        scorer_side = f"\nЗабил: <b>{away_name}</b>"
    minute = f"{int(elapsed)}′" if elapsed is not None else "LIVE"
    return (
        "⚽ <b>ГОЛ!</b>\n"
        f"🏆 {league_name}\n"
        f"{home_name}  <b>{home_score} : {away_score}</b>  {away_name}\n"
        f"⏱ {minute}{scorer_side}"
    )


def _goal_notify_send(chat_id: str, text: str) -> None:
    try:
        notify_app.send_telegram_message(chat_id, text, link="", button_text="")
    except Exception as exc:
        print(f"[goal-notify] send failed chat={chat_id}: {exc}")


def _goal_notify_worker() -> None:
    print(f"[goal-notify] worker started, interval={GOAL_NOTIFY_INTERVAL_SECONDS}s")
    while True:
        try:
            with _goal_notify_lock:
                subs = dict(_goal_notify_subs)
            if not subs or API_FOOTBALL_DEMO or not API_FOOTBALL_KEY:
                time.sleep(GOAL_NOTIFY_INTERVAL_SECONDS)
                continue

            data = _api_call("fixtures", {"live": "all", "timezone": APP_TIMEZONE}, 12, allow_demo=False)
            live_items = data.get("response") or []
            live_by_id = {str((x.get("fixture") or {}).get("id") or ""): x for x in live_items}
            now_ts = int(time.time())
            changed = False

            with _goal_notify_lock:
                for key, sub in list(_goal_notify_subs.items()):
                    fixture_id = str(sub.get("fixture_id") or "")
                    item = live_by_id.get(fixture_id)
                    if not item:
                        kickoff_ts = int(sub.get("kickoff_ts") or 0)
                        if kickoff_ts and now_ts > kickoff_ts + 8 * 3600:
                            _goal_notify_subs.pop(key, None)
                            changed = True
                        continue

                    goals = item.get("goals") or {}
                    new_home = int(goals.get("home") or 0)
                    new_away = int(goals.get("away") or 0)
                    old_home = int(sub.get("last_home") or 0)
                    old_away = int(sub.get("last_away") or 0)

                    if new_home > old_home or new_away > old_away:
                        text = _goal_notify_text(item, old_home, old_away)
                        _goal_notify_send_pool.submit(_goal_notify_send, str(sub.get("chat_id") or ""), text)
                    if new_home != old_home or new_away != old_away:
                        sub["last_home"] = new_home
                        sub["last_away"] = new_away
                        sub["updated_at"] = now_ts
                        _goal_notify_subs[key] = sub
                        changed = True

                    status_short = str(((item.get("fixture") or {}).get("status") or {}).get("short") or "")
                    if status_short in {"FT", "AET", "PEN", "CANC", "ABD", "AWD", "WO"}:
                        _goal_notify_subs.pop(key, None)
                        changed = True

            if changed:
                _goal_notify_save()
        except Exception as exc:
            print(f"[goal-notify] worker error: {type(exc).__name__}: {exc}")
        time.sleep(GOAL_NOTIFY_INTERVAL_SECONDS)


def start_goal_notify_worker() -> None:
    global _goal_notify_started
    with _goal_notify_lock:
        if _goal_notify_started:
            return
        _goal_notify_started = True
    _goal_notify_load()
    threading.Thread(target=_goal_notify_worker, name="api-football-goal-notify", daemon=True).start()


def _prediction_today() -> str:
    try:
        return datetime.now(ZoneInfo(APP_TIMEZONE)).strftime('%Y-%m-%d')
    except Exception:
        return datetime.now().strftime('%Y-%m-%d')


def _prediction_history_key(date_text: str) -> str:
    return f"{CACHE_PREFIX}:prediction-history-v1:{date_text}"


def _prediction_storage_mode() -> str:
    mode = str(getattr(notify_app, 'NOTIFY_STORAGE', 'sqlite') or 'sqlite').lower()
    if mode == 'postgres' and getattr(notify_app, 'DATABASE_URL', ''):
        return 'postgres'
    return 'sqlite'


def _prediction_storage_init() -> None:
    global _prediction_history_ready
    with _prediction_history_lock:
        if _prediction_history_ready:
            return
        mode = _prediction_storage_mode()
        try:
            if mode == 'postgres':
                with notify_app._notify_pg_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            CREATE TABLE IF NOT EXISTS prediction_daily_history (
                                stat_date TEXT PRIMARY KEY,
                                summary_json TEXT NOT NULL,
                                feed_json TEXT,
                                is_final BOOLEAN NOT NULL DEFAULT FALSE,
                                updated_at BIGINT NOT NULL
                            )
                            """
                        )
                    conn.commit()
            else:
                _prediction_history_db.parent.mkdir(parents=True, exist_ok=True)
                with sqlite3.connect(str(_prediction_history_db), timeout=30) as conn:
                    conn.execute(
                        """
                        CREATE TABLE IF NOT EXISTS prediction_daily_history (
                            stat_date TEXT PRIMARY KEY,
                            summary_json TEXT NOT NULL,
                            feed_json TEXT,
                            is_final INTEGER NOT NULL DEFAULT 0,
                            updated_at INTEGER NOT NULL
                        )
                        """
                    )
                    conn.commit()
            _prediction_history_ready = True
            print(f"[prediction-history] storage={mode} ready")
        except Exception as exc:
            print(f"[prediction-history] storage init failed, Redis fallback: {exc}")
            _prediction_history_ready = True


def _prediction_record_from_payload(payload: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    summary = payload.get('summary') if isinstance(payload.get('summary'), dict) else None
    if not summary:
        return None
    feed = payload.get('feed') if isinstance(payload.get('feed'), dict) else None
    return {
        'summary': summary,
        'feed': feed,
        'is_final': bool(payload.get('is_final')),
        'updated_at': int(payload.get('updated_at') or 0),
    }


def _prediction_storage_get(date_text: str) -> dict[str, Any] | None:
    _prediction_storage_init()
    key = _prediction_history_key(date_text)
    if _redis_client is not None:
        try:
            raw = _redis_client.get(key)
            if raw:
                record = _prediction_record_from_payload(json.loads(raw))
                if record:
                    return record
        except Exception:
            pass
    try:
        mode = _prediction_storage_mode()
        row = None
        if mode == 'postgres':
            with notify_app._notify_pg_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT summary_json, feed_json, is_final, updated_at FROM prediction_daily_history WHERE stat_date=%s",
                        (date_text,),
                    )
                    row = cur.fetchone()
        else:
            with sqlite3.connect(str(_prediction_history_db), timeout=30) as conn:
                row = conn.execute(
                    "SELECT summary_json, feed_json, is_final, updated_at FROM prediction_daily_history WHERE stat_date=?",
                    (date_text,),
                ).fetchone()
        if not row:
            return None
        record = {
            'summary': json.loads(row[0] or '{}'),
            'feed': json.loads(row[1]) if row[1] else None,
            'is_final': bool(row[2]),
            'updated_at': int(row[3] or 0),
        }
        if _redis_client is not None:
            try:
                _redis_client.set(key, json.dumps(record, ensure_ascii=False, separators=(',', ':')))
            except Exception:
                pass
        return record
    except Exception as exc:
        print(f"[prediction-history] load failed date={date_text}: {exc}")
        return None


def _prediction_summary_only(result: dict[str, Any]) -> dict[str, Any]:
    summary = dict(result or {})
    summary.pop('feed', None)
    summary.pop('response', None)
    summary.pop('_quota', None)
    summary.pop('_build_seconds', None)
    summary.pop('_shared_cache_ttl', None)
    # The incremental scanner state can contain hundreds of fixture ids. It is
    # persisted only inside feed_json and is intentionally excluded from the
    # lightweight date-summary endpoint used by the calendar strip.
    summary.pop('_prediction_scan_state', None)
    summary.pop('_prediction_scan_state_version', None)
    summary.pop('_prediction_scan_checked_at', None)
    summary.pop('_prediction_scan_cycle', None)
    summary['archived'] = True
    return summary


def _prediction_storage_save(date_text: str, result: dict[str, Any], *, keep_feed: bool, is_final: bool = False) -> None:
    _prediction_storage_init()
    now_ts = int(time.time())
    archived = bool(is_final or not keep_feed)
    summary = _prediction_summary_only(result)
    summary['date'] = date_text
    summary['archived'] = archived
    summary['history_retention'] = PREDICTION_HISTORY_RETENTION_POLICY
    summary['updated_at'] = now_ts
    feed_payload = dict(result) if keep_feed else None
    if feed_payload is not None:
        feed_payload['date'] = date_text
        feed_payload['archived'] = archived
        feed_payload['history_retention'] = PREDICTION_HISTORY_RETENTION_POLICY
        feed_payload['updated_at'] = now_ts
    record = {'summary': summary, 'feed': feed_payload, 'is_final': bool(is_final), 'updated_at': now_ts}
    key = _prediction_history_key(date_text)
    if _redis_client is not None:
        try:
            _redis_client.set(key, json.dumps(record, ensure_ascii=False, separators=(',', ':')))
        except Exception as exc:
            print(f"[prediction-history] Redis save failed date={date_text}: {exc}")
    try:
        summary_json = json.dumps(summary, ensure_ascii=False, separators=(',', ':'))
        feed_json = json.dumps(feed_payload, ensure_ascii=False, separators=(',', ':')) if feed_payload is not None else None
        mode = _prediction_storage_mode()
        if mode == 'postgres':
            with notify_app._notify_pg_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO prediction_daily_history(stat_date, summary_json, feed_json, is_final, updated_at)
                        VALUES(%s,%s,%s,%s,%s)
                        ON CONFLICT(stat_date) DO UPDATE SET
                            summary_json=EXCLUDED.summary_json,
                            feed_json=EXCLUDED.feed_json,
                            is_final=EXCLUDED.is_final,
                            updated_at=EXCLUDED.updated_at
                        """,
                        (date_text, summary_json, feed_json, bool(is_final), now_ts),
                    )
                conn.commit()
        else:
            with sqlite3.connect(str(_prediction_history_db), timeout=30) as conn:
                conn.execute(
                    """
                    INSERT INTO prediction_daily_history(stat_date, summary_json, feed_json, is_final, updated_at)
                    VALUES(?,?,?,?,?)
                    ON CONFLICT(stat_date) DO UPDATE SET
                        summary_json=excluded.summary_json,
                        feed_json=excluded.feed_json,
                        is_final=excluded.is_final,
                        updated_at=excluded.updated_at
                    """,
                    (date_text, summary_json, feed_json, 1 if is_final else 0, now_ts),
                )
                conn.commit()
    except Exception as exc:
        print(f"[prediction-history] DB save failed date={date_text}: {exc}")


def _prediction_storage_records(*, with_feed_only: bool = False) -> list[dict[str, Any]]:
    _prediction_storage_init()
    rows: list[Any] = []
    try:
        mode = _prediction_storage_mode()
        where = " WHERE feed_json IS NOT NULL" if with_feed_only else ""
        if mode == 'postgres':
            with notify_app._notify_pg_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"SELECT stat_date, summary_json, feed_json, is_final, updated_at FROM prediction_daily_history{where} ORDER BY stat_date")
                    rows = cur.fetchall() or []
        else:
            with sqlite3.connect(str(_prediction_history_db), timeout=30) as conn:
                rows = conn.execute(
                    f"SELECT stat_date, summary_json, feed_json, is_final, updated_at FROM prediction_daily_history{where} ORDER BY stat_date"
                ).fetchall()
    except Exception as exc:
        print(f"[prediction-history] list failed: {exc}")
    records: list[dict[str, Any]] = []
    for row in rows:
        try:
            records.append({
                'date': str(row[0]),
                'summary': json.loads(row[1] or '{}'),
                'feed': json.loads(row[2]) if row[2] else None,
                'is_final': bool(row[3]),
                'updated_at': int(row[4] or 0),
            })
        except Exception:
            continue
    if not records and _redis_client is not None:
        try:
            pattern = f"{CACHE_PREFIX}:prediction-history-v1:*"
            for key in _redis_client.scan_iter(match=pattern, count=200):
                raw = _redis_client.get(key)
                record = _prediction_record_from_payload(json.loads(raw)) if raw else None
                if not record:
                    continue
                date_text = str(key).rsplit(':', 1)[-1]
                if with_feed_only and not record.get('feed'):
                    continue
                records.append({'date': date_text, **record})
        except Exception:
            pass
    records.sort(key=lambda x: str(x.get('date') or ''))
    return records


def _prediction_history_summaries() -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for record in _prediction_storage_records():
        summary = dict(record.get('summary') or {})
        summary['date'] = record.get('date')
        summary['is_final'] = bool(record.get('is_final'))
        summary['updated_at'] = int(record.get('updated_at') or 0)
        out.append(summary)
    out.sort(key=lambda x: str(x.get('date') or ''), reverse=True)
    return out

def _prediction_percent(value: Any) -> float:
    try:
        text = str(value or '').replace(',', '.').replace('%', '').strip()
        num = float(text)
    except Exception:
        return 0.0
    return max(0.0, min(100.0, num))


def _prediction_norm(value: Any) -> str:
    try:
        import unicodedata
        base = unicodedata.normalize('NFD', str(value or '').lower())
        base = ''.join(ch for ch in base if unicodedata.category(ch) != 'Mn')
    except Exception:
        base = str(value or '').lower()
    out = []
    prev_space = False
    for ch in base.replace('ё', 'е'):
        ok = ('a' <= ch <= 'z') or ('а' <= ch <= 'я') or ('0' <= ch <= '9') or ch == '.'
        if ok:
            out.append(ch)
            prev_space = False
        else:
            if not prev_space:
                out.append(' ')
                prev_space = True
    return ''.join(out).strip()


def _prediction_team_side(winner: dict[str, Any] | None, teams: dict[str, Any] | None, percent: dict[str, Any] | None) -> str:
    winner = winner or {}
    teams = teams or {}
    percent = percent or {}
    try:
        wid = int(winner.get('id') or 0)
    except Exception:
        wid = 0
    try:
        home_id = int(((teams.get('home') or {}).get('id')) or 0)
    except Exception:
        home_id = 0
    try:
        away_id = int(((teams.get('away') or {}).get('id')) or 0)
    except Exception:
        away_id = 0
    if wid and wid == home_id:
        return 'home'
    if wid and wid == away_id:
        return 'away'
    winner_name = _prediction_norm(winner.get('name'))
    home_name = _prediction_norm((teams.get('home') or {}).get('name'))
    away_name = _prediction_norm((teams.get('away') or {}).get('name'))
    if winner_name and winner_name == home_name:
        return 'home'
    if winner_name and winner_name == away_name:
        return 'away'
    rows = [
        ('home', _prediction_percent(percent.get('home'))),
        ('draw', _prediction_percent(percent.get('draw'))),
        ('away', _prediction_percent(percent.get('away'))),
    ]
    rows.sort(key=lambda item: item[1], reverse=True)
    return rows[0][0] if rows else 'draw'


def _prediction_primary_outcome_code(item: dict[str, Any] | None, teams: dict[str, Any] | None) -> str:
    predictions = (item or {}).get('predictions') or {}
    side = _prediction_team_side(predictions.get('winner'), teams or {}, predictions.get('percent') or {})
    if side == 'home':
        return '1'
    if side == 'away':
        return '2'
    return 'X'


def _prediction_market_kind(name: Any) -> str:
    n = _prediction_norm(name)
    if 'double chance' in n or 'double result' in n or 'двойной шанс' in n:
        return 'double'
    if (
        'over under' in n
        or 'total goals' in n
        or 'goals over under' in n
        or 'goal line' in n
        or 'тотал' in n
    ):
        return 'total'
    if 'match winner' in n or n == '1x2' or 'full time result' in n or 'fulltime result' in n or n == 'winner' or 'исход матча' in n:
        return '1x2'
    return ''


def _prediction_outcome_code(raw_value: Any, kind: str, teams: dict[str, Any] | None) -> str:
    raw = str(raw_value or '').strip()
    n = _prediction_norm(raw)
    teams = teams or {}
    home = _prediction_norm((teams.get('home') or {}).get('name'))
    away = _prediction_norm((teams.get('away') or {}).get('name'))
    if kind == '1x2':
        if n in {'home', '1'} or (home and n == home):
            return '1'
        if n in {'draw', 'x', 'tie', 'ничья'}:
            return 'X'
        if n in {'away', '2'} or (away and n == away):
            return '2'
    if kind == 'double':
        compact = n.replace(' ', '')
        if n in {'home draw', 'draw home', 'home or draw', 'draw or home'} or compact in {'1x', 'x1'}:
            return '1X'
        if n in {'draw away', 'away draw', 'draw or away', 'away or draw'} or compact in {'x2', '2x'}:
            return 'X2'
        if n in {'home away', 'away home', 'home or away', 'away or home'} or compact in {'12', '21'}:
            return '12'
    if kind == 'total':
        import re
        m = re.search(r'(\d+(?:[.,]\d+)?)', raw)
        number = (m.group(1).replace(',', '.') if m else '')
        lower = raw.lower()
        if any(k in lower for k in ['over', 'больше', 'тб']):
            return f'OVER {number}'.strip()
        if any(k in lower for k in ['under', 'меньше', 'тм']):
            return f'UNDER {number}'.strip()
    return ''


def _prediction_total_market_code(raw_value: Any, market_name: Any = '') -> str:
    """Normalize a bookmaker total selection.

    Some bookmakers return ``Over 2.5`` as the value, while others return only
    ``Over`` and keep the line in the market name (for example
    ``Over/Under 2.5 Goals``). Supporting both formats prevents valid totals
    from disappearing from the feed.
    """
    import re

    raw = str(raw_value or '').strip()
    market = str(market_name or '').strip()
    direction = ''
    if re.search(r'\b(over|больше|тб)\b', raw, flags=re.I):
        direction = 'OVER'
    elif re.search(r'\b(under|меньше|тм)\b', raw, flags=re.I):
        direction = 'UNDER'
    if not direction:
        return ''
    number_match = re.search(r'(\d+(?:[.,]\d+)?)', raw)
    if not number_match:
        number_match = re.search(r'(\d+(?:[.,]\d+)?)', market)
    if not number_match:
        return ''
    try:
        line = float(number_match.group(1).replace(',', '.'))
    except Exception:
        return ''
    # The API prediction normally uses .5 lines. Whole-goal lines are also
    # supported with a stake refund on an exact tie. Quarter lines are skipped
    # because settling them correctly requires half-win/half-loss accounting.
    if line <= 0 or abs(line * 2 - round(line * 2)) > 1e-6:
        return ''
    return f'{direction} {line:g}'


def _prediction_total_code(item: dict[str, Any] | None) -> str:
    """Select only the fixed 2.5-goal total with the stronger model side.

    API-Football bookmaker odds can contain many total lines. For the prediction
    feed we intentionally allow only OVER 2.5 or UNDER 2.5. The direction is
    chosen solely from our model probabilities; bookmaker prices are used only
    afterwards as the 1.50-3.00 eligibility filter.
    """
    candidates = [f'OVER {PREDICTION_TOTAL_LINE:g}', f'UNDER {PREDICTION_TOTAL_LINE:g}']
    scored: list[tuple[float, str]] = []
    for code in candidates:
        probability = _prediction_total_probability(item, code)
        if probability is not None:
            scored.append((float(probability), code))
    if not scored:
        return ''
    scored.sort(key=lambda value: (value[0], value[1].startswith('UNDER')), reverse=True)
    return scored[0][1]


def _prediction_total_parts(code: Any) -> tuple[str, float] | None:
    import re

    match = re.fullmatch(r'\s*(OVER|UNDER)\s+(\d+(?:\.\d+)?)\s*', str(code or ''), flags=re.I)
    if not match:
        return None
    try:
        line = float(match.group(2))
    except Exception:
        return None
    if line <= 0:
        return None
    return match.group(1).upper(), line


def _prediction_total_label(code: Any) -> str:
    parts = _prediction_total_parts(code)
    if not parts:
        return 'Тотал матча'
    direction, line = parts
    return f"Тотал {'больше' if direction == 'OVER' else 'меньше'} {line:g}"



def _prediction_numeric(value: Any) -> float | None:
    """Return a usable non-negative model number from an API field."""
    import re

    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        number = float(value)
    else:
        raw = str(value).strip().replace(',', '.')
        if not raw:
            return None
        match = re.search(r'-?\d+(?:\.\d+)?', raw)
        if not match:
            return None
        try:
            number = float(match.group(0))
        except Exception:
            return None
    if not math.isfinite(number) or number < 0:
        return None
    return number


def _prediction_nested_number(payload: dict[str, Any] | None, *path: str) -> float | None:
    value: Any = payload or {}
    for key in path:
        if not isinstance(value, dict):
            return None
        value = value.get(key)
    return _prediction_numeric(value)


def _prediction_mean(values: list[float | None]) -> float | None:
    cleaned = [float(value) for value in values if value is not None and math.isfinite(float(value)) and 0 <= float(value) <= 8]
    if not cleaned:
        return None
    return sum(cleaned) / len(cleaned)


def _prediction_side_expected_goals(pred_item: dict[str, Any] | None, side: str) -> float | None:
    """Estimate one team's goals only from API-Football model/stat fields.

    The predictions endpoint exposes a total pick but no direct percentage for
    that pick. We therefore combine the same response's recent attack,
    opponent defence and season home/away goal averages, then use Poisson for
    the total probability. No bookmaker price is used in the model percent.
    """
    teams = (pred_item or {}).get('teams') or {}
    own = teams.get(side) or {}
    opponent_side = 'away' if side == 'home' else 'home'
    opponent = teams.get(opponent_side) or {}
    venue_key = 'home' if side == 'home' else 'away'
    opponent_venue_key = 'away' if side == 'home' else 'home'

    candidates = [
        _prediction_nested_number(own, 'last_5', 'goals', 'for', 'average'),
        _prediction_nested_number(opponent, 'last_5', 'goals', 'against', 'average'),
        _prediction_nested_number(own, 'league', 'goals', 'for', 'average', venue_key),
        _prediction_nested_number(opponent, 'league', 'goals', 'against', 'average', opponent_venue_key),
    ]
    estimate = _prediction_mean(candidates)
    if estimate is not None:
        return max(0.05, min(6.0, estimate))

    # Fallback to season-wide averages if venue splits are unavailable.
    fallback = _prediction_mean([
        _prediction_nested_number(own, 'league', 'goals', 'for', 'average', 'total'),
        _prediction_nested_number(opponent, 'league', 'goals', 'against', 'average', 'total'),
    ])
    if fallback is not None:
        return max(0.05, min(6.0, fallback))
    return None


def _prediction_h2h_expected_total(pred_item: dict[str, Any] | None) -> float | None:
    totals: list[float] = []
    for fixture_item in ((pred_item or {}).get('h2h') or [])[:10]:
        goals = fixture_item.get('goals') or {}
        home = _prediction_numeric(goals.get('home'))
        away = _prediction_numeric(goals.get('away'))
        if home is not None and away is not None:
            totals.append(home + away)
    if len(totals) < 2:
        return None
    return max(0.1, min(8.0, sum(totals) / len(totals)))


def _prediction_expected_total_goals(pred_item: dict[str, Any] | None) -> float | None:
    home = _prediction_side_expected_goals(pred_item, 'home')
    away = _prediction_side_expected_goals(pred_item, 'away')
    if home is not None and away is not None:
        return max(0.1, min(8.0, home + away))
    return _prediction_h2h_expected_total(pred_item)


def _prediction_poisson_cdf(max_goals: int, expected_goals: float) -> float:
    if max_goals < 0:
        return 0.0
    expected_goals = max(0.0, min(12.0, float(expected_goals)))
    term = math.exp(-expected_goals)
    total = term
    for goals in range(1, max_goals + 1):
        term *= expected_goals / goals
        total += term
    return max(0.0, min(1.0, total))


def _prediction_total_probability(pred_item: dict[str, Any] | None, code: Any) -> float | None:
    parts = _prediction_total_parts(code)
    expected_total = _prediction_expected_total_goals(pred_item)
    if not parts or expected_total is None:
        return None
    direction, line = parts
    rounded = round(line)
    is_whole = abs(line - rounded) < 1e-9
    if direction == 'OVER':
        # Over 2.5 wins on 3+, while Over 2.0 wins on 3+ and pushes on 2.
        cutoff = int(rounded if is_whole else math.floor(line))
        probability = 1.0 - _prediction_poisson_cdf(cutoff, expected_total)
    else:
        # Under 2.5 wins on 0..2, while Under 2.0 wins on 0..1 and pushes on 2.
        cutoff = int(rounded - 1 if is_whole else math.floor(line))
        probability = _prediction_poisson_cdf(cutoff, expected_total)
    return round(max(0.0, min(100.0, probability * 100.0)), 2)


def _prediction_lowest_1x2_odd(payload: dict[str, Any] | None, teams: dict[str, Any] | None, code: str) -> float | None:
    best: float | None = None
    for fixture in (payload or {}).get('response') or []:
        for bookmaker in fixture.get('bookmakers') or []:
            for bet in bookmaker.get('bets') or []:
                kind = _prediction_market_kind(bet.get('name'))
                if kind != '1x2':
                    continue
                for value in bet.get('values') or []:
                    outcome = _prediction_outcome_code(value.get('value'), kind, teams or {})
                    if str(outcome).upper() != str(code).upper():
                        continue
                    try:
                        odd = float(str(value.get('odd') or '').replace(',', '.'))
                    except Exception:
                        continue
                    if odd <= 1:
                        continue
                    if best is None or odd < best:
                        best = odd
    return best


def _prediction_lowest_total_odd(payload: dict[str, Any] | None, code: str) -> float | None:
    best: float | None = None
    wanted = str(code or '').upper()
    for fixture in (payload or {}).get('response') or []:
        for bookmaker in fixture.get('bookmakers') or []:
            for bet in bookmaker.get('bets') or []:
                if _prediction_market_kind(bet.get('name')) != 'total':
                    continue
                for value in bet.get('values') or []:
                    outcome = _prediction_total_market_code(value.get('value'), bet.get('name'))
                    if outcome.upper() != wanted:
                        continue
                    try:
                        odd = float(str(value.get('odd') or '').replace(',', '.'))
                    except Exception:
                        continue
                    if odd <= 1:
                        continue
                    if best is None or odd < best:
                        best = odd
    return best


def _fixture_is_finished(fixture_item: dict[str, Any] | None) -> bool:
    status = str((((fixture_item or {}).get('fixture') or {}).get('status') or {}).get('short') or '')
    return status in {'FT', 'AET', 'PEN'}


def _fixture_result_code(fixture_item: dict[str, Any] | None) -> str | None:
    if not _fixture_is_finished(fixture_item):
        return None
    goals = (fixture_item or {}).get('goals') or {}
    try:
        home = int(goals.get('home'))
        away = int(goals.get('away'))
    except Exception:
        return None
    if home > away:
        return '1'
    if away > home:
        return '2'
    return 'X'


def _fixture_total_goals(fixture_item: dict[str, Any] | None) -> int | None:
    if not _fixture_is_finished(fixture_item):
        return None
    goals = (fixture_item or {}).get('goals') or {}
    try:
        return int(goals.get('home')) + int(goals.get('away'))
    except Exception:
        return None


def _prediction_row_key(row: dict[str, Any] | None) -> str:
    row = row or {}
    explicit = str(row.get('prediction_key') or '').strip()
    if explicit:
        return explicit
    try:
        fixture_id = int(row.get('fixture_id') or 0)
    except Exception:
        fixture_id = 0
    market = str(row.get('market') or '1x2').strip().lower() or '1x2'
    code = str(row.get('pick_code') or '').strip().upper()
    return f'{fixture_id}:{market}:{code}'


def _prediction_row_allowed(row: dict[str, Any] | None) -> bool:
    """Allow only saved 1X2 forecasts under the current policy.

    This deliberately rejects legacy total rows, so a deployment upgrade
    removes ТБ/ТМ from old saved days as soon as their payload is recalculated.
    """
    row = row or {}
    if str(row.get('market') or '1x2').strip().lower() != '1x2':
        return False
    if str(row.get('pick_code') or '').strip().upper() not in {'1', 'X', '2'}:
        return False
    try:
        probability = float(row.get('probability') or 0.0)
        odd = float(row.get('odd') or 0.0)
    except Exception:
        return False
    return probability >= PREDICTION_MIN_MODEL_PERCENT and PREDICTION_MIN_ODD <= odd <= PREDICTION_MAX_ODD


def _prediction_row_is_settled(row: dict[str, Any] | None) -> bool:
    row = row or {}
    if str(row.get('settlement') or '') in {'won', 'lost', 'push'}:
        return True
    # Compatibility with rows saved by earlier versions.
    return str(row.get('result_code') or '') in {'1', 'X', '2'} and _fixture_is_finished(row.get('fixture_item') or {})


def _prediction_apply_result(original: dict[str, Any], fixture_item: dict[str, Any] | None) -> dict[str, Any]:
    row = dict(original or {})
    if fixture_item:
        row['fixture_item'] = fixture_item
    row['stake'] = 1.0
    if not fixture_item or not _fixture_is_finished(fixture_item):
        row['result_code'] = ''
        row['settlement'] = 'pending'
        row['won'] = False
        row['returned'] = 0.0
        return row

    market = str(row.get('market') or '1x2').lower()
    if market == 'total':
        parts = _prediction_total_parts(row.get('pick_code'))
        total_goals = _fixture_total_goals(fixture_item)
        if not parts or total_goals is None:
            row['result_code'] = ''
            row['settlement'] = 'pending'
            row['won'] = False
            row['returned'] = 0.0
            return row
        direction, line = parts
        row['actual_total'] = total_goals
        if abs(total_goals - line) < 1e-9:
            row['result_code'] = 'PUSH'
            row['settlement'] = 'push'
            row['won'] = False
            row['returned'] = 1.0
            return row
        won = total_goals > line if direction == 'OVER' else total_goals < line
        row['result_code'] = f'TOTAL {total_goals}'
        row['settlement'] = 'won' if won else 'lost'
        row['won'] = bool(won)
        row['returned'] = float(row.get('odd') or 0.0) if won else 0.0
        return row

    result_code = _fixture_result_code(fixture_item)
    won = bool(result_code and str(row.get('pick_code') or '') == result_code)
    row['result_code'] = result_code or ''
    row['settlement'] = 'won' if won else 'lost'
    row['won'] = won
    row['returned'] = float(row.get('odd') or 0.0) if won else 0.0
    return row


def _prediction_evaluate_fixture(
    item: dict[str, Any],
    *,
    pred_ttl: int,
    odds_ttl: int,
    bypass_prediction_cache: bool = False,
    bypass_odds_cache: bool = False,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Evaluate only the main 1X2 outcome for one fixture."""
    now_ts = int(time.time())
    fixture = item.get('fixture') or {}
    fixture_id = str(fixture.get('id') or '').strip()
    base_state: dict[str, Any] = {
        'checked_at': now_ts,
        'next_check': 0,
        'status': 'invalid',
        'markets': {},
        'included_markets': [],
        'complete': False,
    }
    if not fixture_id.isdigit():
        return [], base_state

    teams = item.get('teams') or {}
    try:
        pred_data = _api_call(
            'predictions',
            {'fixture': fixture_id},
            pred_ttl,
            allow_demo=False,
            bypass_cache=bypass_prediction_cache,
        )
    except Exception:
        base_state.update({'status': 'api_error', 'next_check': now_ts + 15 * 60})
        return [], base_state

    pred_item = ((pred_data.get('response') or [None])[0])
    if not pred_item:
        next_check = now_ts + PREDICTION_NO_DATA_RECHECK_SECONDS
        base_state.update({
            'status': 'no_prediction',
            'next_check': next_check,
            'markets': {'1x2': {'status': 'no_prediction', 'next_check': next_check}},
        })
        return [], base_state

    predictions = pred_item.get('predictions') or {}
    percent = predictions.get('percent') or {}
    pick_code = _prediction_primary_outcome_code(pred_item, teams)
    side = 'home' if pick_code == '1' else ('away' if pick_code == '2' else 'draw')
    probability = _prediction_percent(percent.get('home' if side == 'home' else 'away' if side == 'away' else 'draw'))

    if pick_code not in {'1', 'X', '2'}:
        next_check = now_ts + PREDICTION_WEAK_MODEL_RECHECK_SECONDS
        base_state.update({
            'status': 'unsupported_pick',
            'next_check': next_check,
            'markets': {'1x2': {'status': 'unsupported_pick', 'next_check': next_check}},
        })
        return [], base_state
    if probability < PREDICTION_MIN_MODEL_PERCENT:
        next_check = now_ts + PREDICTION_WEAK_MODEL_RECHECK_SECONDS
        market_state = {
            'status': 'weak_model',
            'probability': round(probability, 2),
            'pick_code': pick_code,
            'next_check': next_check,
        }
        base_state.update({'status': 'weak_model', 'next_check': next_check, 'markets': {'1x2': market_state}})
        return [], base_state

    try:
        odds_data = _api_call(
            'odds',
            {'fixture': fixture_id},
            odds_ttl,
            allow_demo=False,
            bypass_cache=bypass_odds_cache,
        )
    except Exception:
        next_check = now_ts + 15 * 60
        market_state = {
            'status': 'odds_api_error',
            'probability': round(probability, 2),
            'pick_code': pick_code,
            'next_check': next_check,
        }
        base_state.update({'status': 'odds_api_error', 'next_check': next_check, 'markets': {'1x2': market_state}})
        return [], base_state

    odd = _prediction_lowest_1x2_odd(odds_data, teams, pick_code)
    if odd is None:
        next_check = now_ts + PREDICTION_ODDS_RECHECK_SECONDS
        market_state = {
            'status': 'no_odd',
            'probability': round(probability, 2),
            'pick_code': pick_code,
            'next_check': next_check,
        }
        base_state.update({'status': 'no_odd', 'next_check': next_check, 'markets': {'1x2': market_state}})
        return [], base_state
    if float(odd) < PREDICTION_MIN_ODD:
        next_check = now_ts + PREDICTION_ODDS_RECHECK_SECONDS
        market_state = {
            'status': 'below_odd',
            'probability': round(probability, 2),
            'pick_code': pick_code,
            'odd': round(float(odd), 3),
            'next_check': next_check,
        }
        base_state.update({'status': 'below_odd', 'next_check': next_check, 'markets': {'1x2': market_state}})
        return [], base_state
    if float(odd) > PREDICTION_MAX_ODD:
        next_check = now_ts + PREDICTION_ODDS_RECHECK_SECONDS
        market_state = {
            'status': 'above_odd',
            'probability': round(probability, 2),
            'pick_code': pick_code,
            'odd': round(float(odd), 3),
            'next_check': next_check,
        }
        base_state.update({'status': 'above_odd', 'next_check': next_check, 'markets': {'1x2': market_state}})
        return [], base_state

    if side == 'home':
        label = f"Победа {((teams.get('home') or {}).get('name') or 'хозяев')}"
    elif side == 'away':
        label = f"Победа {((teams.get('away') or {}).get('name') or 'гостей')}"
    else:
        label = 'Ничья'

    row = {
        'fixture_id': int(fixture_id),
        'fixture_item': item,
        'league': (item.get('league') or {}).get('name') or '',
        'country': (item.get('league') or {}).get('country') or '',
        'home': (teams.get('home') or {}).get('name') or 'Хозяева',
        'away': (teams.get('away') or {}).get('name') or 'Гости',
        'market': '1x2',
        'pick_code': pick_code,
        'pick_label': label,
        'probability': round(probability, 2),
        'odd': float(odd),
        'stake': 1.0,
        'prediction_key': f'{fixture_id}:1x2:{pick_code}',
    }
    row = _prediction_apply_result(row, item)
    market_state = {
        'status': 'included',
        'probability': round(probability, 2),
        'pick_code': pick_code,
        'odd': round(float(odd), 3),
        'next_check': 0,
    }
    base_state.update({
        'status': 'included',
        'next_check': 0,
        'markets': {'1x2': market_state},
        'included_markets': ['1x2'],
        'complete': True,
    })
    return [row], base_state


def _prediction_audit_for_date(date_text: str) -> dict[str, Any]:
    fixtures_data = _api_call('fixtures', {'date': date_text, 'timezone': APP_TIMEZONE}, _fixture_ttl(date_text), allow_demo=False)
    fixtures = fixtures_data.get('response') or []
    finished_items = [item for item in fixtures if _fixture_is_finished(item)]
    is_past = date_text < _prediction_today()
    pred_ttl = 12 * 3600 if is_past else PREDICTION_NO_DATA_RECHECK_SECONDS
    odds_ttl = 12 * 3600 if is_past else PREDICTION_ODDS_RECHECK_SECONDS

    rows: list[dict[str, Any]] = []
    if finished_items:
        def evaluate(item: dict[str, Any]) -> list[dict[str, Any]]:
            result, _ = _prediction_evaluate_fixture(item, pred_ttl=pred_ttl, odds_ttl=odds_ttl)
            return result
        with ThreadPoolExecutor(max_workers=min(8, max(1, len(finished_items)))) as pool:
            for result_rows in pool.map(evaluate, finished_items):
                rows.extend(result_rows)

    payload = _prediction_recalculate_payload({
        'date': date_text,
        'fixtures_total': len(fixtures),
        'finished_total': len(finished_items),
        '_quota': _quota_snapshot(),
    }, rows)
    payload['response'] = list(payload.get('feed') or [])
    return payload


def _prediction_feed_for_date(date_text: str) -> dict[str, Any]:
    fixtures_data = _api_call('fixtures', {'date': date_text, 'timezone': APP_TIMEZONE}, _fixture_ttl(date_text), allow_demo=False)
    fixtures = fixtures_data.get('response') or []
    is_past = date_text < _prediction_today()
    pred_ttl = 12 * 3600 if is_past else PREDICTION_NO_DATA_RECHECK_SECONDS
    odds_ttl = 12 * 3600 if is_past else PREDICTION_ODDS_RECHECK_SECONDS

    def evaluate(item: dict[str, Any]) -> tuple[int, list[dict[str, Any]], dict[str, Any]]:
        fixture_id = int(((item.get('fixture') or {}).get('id')) or 0)
        rows, scan = _prediction_evaluate_fixture(item, pred_ttl=pred_ttl, odds_ttl=odds_ttl)
        return fixture_id, rows, scan

    rows: list[dict[str, Any]] = []
    scan_state: dict[str, dict[str, Any]] = {}
    if fixtures:
        with ThreadPoolExecutor(max_workers=min(8, max(1, len(fixtures)))) as pool:
            for fixture_id, result_rows, scan in pool.map(evaluate, fixtures):
                if fixture_id:
                    scan_state[str(fixture_id)] = scan
                rows.extend(result_rows)

    payload = _prediction_recalculate_payload({
        'date': date_text,
        'fixtures_total': len(fixtures),
        'finished_total': sum(1 for item in fixtures if _fixture_is_finished(item)),
        '_quota': _quota_snapshot(),
        '_prediction_scan_state': scan_state,
        '_prediction_scan_state_version': PREDICTION_SCAN_STATE_VERSION,
        '_prediction_scan_checked_at': int(time.time()),
    }, rows)
    return payload


def _prediction_recalculate_payload(result: dict[str, Any], rows: list[dict[str, Any]]) -> dict[str, Any]:
    payload = dict(result or {})
    feed_rows: list[dict[str, Any]] = []
    for original in rows or []:
        row = dict(original)
        row['market'] = str(row.get('market') or '1x2').lower()
        row['prediction_key'] = _prediction_row_key(row)
        fixture_item = row.get('fixture_item') if isinstance(row.get('fixture_item'), dict) else None
        if fixture_item:
            row = _prediction_apply_result(row, fixture_item)
        elif not row.get('settlement'):
            row['settlement'] = 'won' if row.get('won') else ('lost' if row.get('result_code') else 'pending')
        if _prediction_row_allowed(row):
            feed_rows.append(row)

    def sort_key(row: dict[str, Any]) -> tuple[int, str, str]:
        fixture = (row.get('fixture_item') or {}).get('fixture') or {}
        dt = str(fixture.get('date') or '')
        status = str(((fixture.get('status') or {}).get('short')) or '')
        pri = 0 if status in {'NS', 'TBD'} else (1 if status in {'1H','HT','2H','ET','BT','P','SUSP','INT','LIVE'} else 2)
        return (pri, dt, str(row.get('pick_code') or ''))
    feed_rows.sort(key=sort_key)

    settled = [row for row in feed_rows if _prediction_row_is_settled(row) and row.get('odd') is not None]
    decided = [row for row in settled if str(row.get('settlement') or '') in {'won', 'lost'}]
    checked = len(settled)
    won = sum(1 for row in decided if str(row.get('settlement') or '') == 'won')
    lost = sum(1 for row in decided if str(row.get('settlement') or '') == 'lost')
    pushes = sum(1 for row in settled if str(row.get('settlement') or '') == 'push')
    stake_total = round(sum(float(row.get('stake') or 1.0) for row in settled), 2)
    returned_total = round(sum(float(row.get('returned') or 0.0) for row in settled), 2)
    profit = round(returned_total - stake_total, 2)
    payload.update({
        'predicted_total': checked,
        'won_total': won,
        'lost_total': lost,
        'push_total': pushes,
        'hit_rate': round((won / len(decided)) * 100, 2) if decided else 0.0,
        'stake_total': stake_total,
        'returned_total': returned_total,
        'profit_total': profit,
        'roi_percent': round((profit / stake_total) * 100, 2) if stake_total > 0 else 0.0,
        'odds_total': checked,
        'response': settled,
        'feed': feed_rows,
        'market_counts': {'1x2': len(feed_rows)},
        'odds_policy': PREDICTION_ODDS_POLICY,
        'min_model_percent': PREDICTION_MIN_MODEL_PERCENT,
        'min_odd': PREDICTION_MIN_ODD,
        'max_odd': PREDICTION_MAX_ODD,
    })
    for obsolete in ('total_min_odd', 'total_max_odd', 'total_line'):
        payload.pop(obsolete, None)
    return payload


def _prediction_merge_day_payload(old_payload: dict[str, Any] | None, new_payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(old_payload, dict):
        return new_payload
    merged: dict[str, dict[str, Any]] = {}
    for row in old_payload.get('feed') or []:
        key = _prediction_row_key(row)
        if key and not key.startswith('0:'):
            merged[key] = dict(row)
    for row in new_payload.get('feed') or []:
        key = _prediction_row_key(row)
        if not key or key.startswith('0:'):
            continue
        previous = merged.get(key) or {}
        combined = dict(previous)
        combined.update(row)
        merged[key] = combined
    result = dict(new_payload)
    result['fixtures_total'] = max(int(old_payload.get('fixtures_total') or 0), int(new_payload.get('fixtures_total') or 0))
    return _prediction_recalculate_payload(result, list(merged.values()))


def _prediction_update_archived_payload(payload: dict[str, Any], fixtures: list[dict[str, Any]]) -> tuple[dict[str, Any], bool]:
    by_id: dict[int, dict[str, Any]] = {}
    for item in fixtures or []:
        try:
            fixture_id = int(((item.get('fixture') or {}).get('id')) or 0)
        except Exception:
            fixture_id = 0
        if fixture_id:
            by_id[fixture_id] = item
    rows: list[dict[str, Any]] = []
    all_settled = True
    for original in payload.get('feed') or []:
        row = dict(original)
        fixture_id = int(row.get('fixture_id') or 0)
        current = by_id.get(fixture_id)
        if current:
            row = _prediction_apply_result(row, current)
        if not _prediction_row_is_settled(row):
            all_settled = False
        rows.append(row)
    updated = dict(payload)
    updated['finished_total'] = sum(1 for item in fixtures or [] if _fixture_is_finished(item))
    return _prediction_recalculate_payload(updated, rows), all_settled


def _prediction_refresh_today_scores() -> None:
    """Every five minutes check new 1X2 forecasts and settle finished matches."""
    today = _prediction_today()
    now_ts = int(time.time())
    record = _prediction_storage_get(today)
    payload = record.get('feed') if record and isinstance(record.get('feed'), dict) else None
    if not isinstance(payload, dict):
        payload = {
            'date': today,
            'fixtures_total': 0,
            'finished_total': 0,
            'predicted_total': 0,
            'won_total': 0,
            'lost_total': 0,
            'push_total': 0,
            'hit_rate': 0.0,
            'stake_total': 0.0,
            'returned_total': 0.0,
            'profit_total': 0.0,
            'roi_percent': 0.0,
            'odds_total': 0,
            'response': [],
            'feed': [],
            'odds_policy': PREDICTION_ODDS_POLICY,
            'min_model_percent': PREDICTION_MIN_MODEL_PERCENT,
            'min_odd': PREDICTION_MIN_ODD,
            'max_odd': PREDICTION_MAX_ODD,
        }

    fixtures_ttl = max(60, min(240, PREDICTION_LIVE_REFRESH_SECONDS - 30))
    fixtures_data = _api_call(
        'fixtures',
        {'date': today, 'timezone': APP_TIMEZONE},
        fixtures_ttl,
        allow_demo=False,
    )
    fixtures = fixtures_data.get('response') or []

    updated, _ = _prediction_update_archived_payload(payload, fixtures)
    existing_rows: dict[str, dict[str, Any]] = {}
    included_fixture_ids: set[int] = set()
    for row in updated.get('feed') or []:
        if not _prediction_row_allowed(row):
            continue
        try:
            fixture_id = int(row.get('fixture_id') or 0)
        except Exception:
            fixture_id = 0
        if not fixture_id:
            continue
        key = _prediction_row_key(row)
        existing_rows[key] = dict(row)
        included_fixture_ids.add(fixture_id)

    raw_scan_state = payload.get('_prediction_scan_state')
    if payload.get('_prediction_scan_state_version') == PREDICTION_SCAN_STATE_VERSION and isinstance(raw_scan_state, dict):
        scan_state: dict[str, dict[str, Any]] = dict(raw_scan_state)
    else:
        scan_state = {}

    for fixture_id in included_fixture_ids:
        previous = scan_state.get(str(fixture_id)) if isinstance(scan_state.get(str(fixture_id)), dict) else {}
        scan_state[str(fixture_id)] = {
            'checked_at': int((previous or {}).get('checked_at') or now_ts),
            'next_check': 0,
            'status': 'included',
            'included_markets': ['1x2'],
            'complete': True,
        }

    candidates: list[tuple[int, str, dict[str, Any], dict[str, Any] | None]] = []
    for item in fixtures:
        fixture = item.get('fixture') or {}
        try:
            fixture_id = int(fixture.get('id') or 0)
        except Exception:
            fixture_id = 0
        if not fixture_id or fixture_id in included_fixture_ids:
            continue
        key = str(fixture_id)
        status = str((fixture.get('status') or {}).get('short') or '')
        entry = scan_state.get(key) if isinstance(scan_state.get(key), dict) else None

        # Never create a new forecast after kickoff. Existing forecasts above
        # continue to be settled from the shared score request.
        if status not in {'NS', 'TBD'}:
            scan_state[key] = {
                'checked_at': now_ts,
                'next_check': 0,
                'status': 'started_without_prediction',
                'included_markets': [],
                'complete': False,
            }
            continue

        if entry and bool(entry.get('complete')):
            continue
        next_check = int((entry or {}).get('next_check') or 0)
        if entry and next_check > now_ts:
            continue
        kickoff = str(fixture.get('date') or '')
        priority = 0 if not entry else 1
        candidates.append((priority, kickoff, item, dict(entry) if entry else None))

    candidates.sort(key=lambda value: (value[0], value[1]))
    candidates = candidates[:PREDICTION_SCAN_MAX_FIXTURES_PER_CYCLE]

    def evaluate_candidate(candidate: tuple[int, str, dict[str, Any], dict[str, Any] | None]) -> tuple[int, list[dict[str, Any]], dict[str, Any]]:
        _, _, item, previous = candidate
        fixture_id = int(((item.get('fixture') or {}).get('id')) or 0)
        is_retry = bool(previous)
        rows, state = _prediction_evaluate_fixture(
            item,
            pred_ttl=PREDICTION_NO_DATA_RECHECK_SECONDS,
            odds_ttl=PREDICTION_ODDS_RECHECK_SECONDS,
            bypass_prediction_cache=is_retry,
            bypass_odds_cache=is_retry,
        )
        attempts = int((previous or {}).get('attempts') or 0) + 1
        state['attempts'] = attempts
        status = str(state.get('status') or '')
        if status == 'no_prediction':
            delay = min(6 * 3600, PREDICTION_NO_DATA_RECHECK_SECONDS * (2 ** max(0, attempts - 1)))
            state['next_check'] = int(time.time()) + delay
        elif status in {'api_error', 'odds_api_error'}:
            state['next_check'] = int(time.time()) + min(3600, 15 * 60 * attempts)
        elif not rows and int(state.get('next_check') or 0) > 0 and attempts > 1:
            remaining = max(300, int(state.get('next_check') or 0) - int(time.time()))
            state['next_check'] = int(time.time()) + min(6 * 3600, remaining * min(4, attempts))
        return fixture_id, rows, state

    added = 0
    if candidates:
        with ThreadPoolExecutor(max_workers=min(8, max(1, len(candidates)))) as pool:
            for fixture_id, result_rows, state in pool.map(evaluate_candidate, candidates):
                if not fixture_id:
                    continue
                for row in result_rows:
                    if not _prediction_row_allowed(row):
                        continue
                    key = _prediction_row_key(row)
                    if key not in existing_rows:
                        existing_rows[key] = row
                        included_fixture_ids.add(fixture_id)
                        added += 1
                included = fixture_id in included_fixture_ids or '1x2' in set(state.get('included_markets') or [])
                if included:
                    state['included_markets'] = ['1x2']
                    state['complete'] = True
                    state['status'] = 'included'
                    state['next_check'] = 0
                scan_state[str(fixture_id)] = state

    updated = _prediction_recalculate_payload(updated, list(existing_rows.values()))
    updated.update({
        'date': today,
        'fixtures_total': len(fixtures),
        'finished_total': sum(1 for item in fixtures if _fixture_is_finished(item)),
        'archived': False,
        'updated_at': now_ts,
        '_prediction_scan_state': scan_state,
        '_prediction_scan_state_version': PREDICTION_SCAN_STATE_VERSION,
        '_prediction_scan_checked_at': now_ts,
        '_prediction_scan_cycle': {
            'fixtures_request': 1,
            'prediction_checks': len(candidates),
            'new_predictions': added,
            'pending_candidates': len(candidates),
        },
    })
    _prediction_storage_save(today, updated, keep_feed=True, is_final=False)
    key = _cache_key('prediction-feed-result-v11-outcomes', {'date': today})
    _cache_set(key, updated, _prediction_feed_cache_ttl(today))
    print(
        f"[prediction-history] 5m outcomes refresh date={today} fixtures={len(fixtures)} "
        f"prediction_checks={len(candidates)} added={added} "
        f"finished={updated.get('finished_total', 0)} settled={updated.get('predicted_total', 0)} "
        f"hit={updated.get('hit_rate', 0)}%"
    )


def _prediction_finalize_old_days() -> None:
    today = _prediction_today()
    for record in _prediction_storage_records(with_feed_only=True):
        date_text = str(record.get('date') or '')
        if not date_text or date_text >= today or bool(record.get('is_final')):
            continue
        payload = record.get('feed') or {}
        try:
            fixtures_data = _api_call('fixtures', {'date': date_text, 'timezone': APP_TIMEZONE}, 10 * 60, allow_demo=False)
            updated, all_settled = _prediction_update_archived_payload(payload, fixtures_data.get('response') or [])
            # A day older than yesterday is archived even if the provider left a match abandoned.
            try:
                age_days = (datetime.strptime(today, '%Y-%m-%d') - datetime.strptime(date_text, '%Y-%m-%d')).days
            except Exception:
                age_days = 1
            final_now = all_settled or age_days >= 2
            # Keep every match card forever. Final days are no longer refreshed, but
            # their complete feed remains available when the user selects the date.
            _prediction_storage_save(date_text, updated, keep_feed=True, is_final=final_now)
            if final_now:
                print(f"[prediction-history] archived date={date_text} matches={updated.get('predicted_total', 0)}")
        except Exception as exc:
            print(f"[prediction-history] finalize failed date={date_text}: {exc}")


def _prediction_recalculate_saved_month_filtered() -> None:
    """Remove legacy total rows from every saved day without extra API calls."""
    records = _prediction_storage_records()
    migrated = 0
    for record in records:
        date_text = str(record.get('date') or '')
        payload = record.get('feed') if isinstance(record.get('feed'), dict) else None
        if not date_text or not isinstance(payload, dict):
            continue
        rows = [dict(row) for row in (payload.get('feed') or [])]
        has_legacy_total = any(str(row.get('market') or '1x2').lower() != '1x2' for row in rows)
        if payload.get('odds_policy') == PREDICTION_ODDS_POLICY and not has_legacy_total:
            continue
        rebuilt = _prediction_recalculate_payload(payload, rows)
        rebuilt.update({
            'date': date_text,
            'odds_policy': PREDICTION_ODDS_POLICY,
            'min_model_percent': PREDICTION_MIN_MODEL_PERCENT,
            'min_odd': PREDICTION_MIN_ODD,
            'max_odd': PREDICTION_MAX_ODD,
            'updated_at': int(time.time()),
            'archived': date_text < _prediction_today(),
            'history_retention': PREDICTION_HISTORY_RETENTION_POLICY,
            '_prediction_scan_state_version': PREDICTION_SCAN_STATE_VERSION,
        })
        _prediction_storage_save(date_text, rebuilt, keep_feed=True, is_final=bool(record.get('is_final')) or date_text < _prediction_today())
        cache_key = _cache_key('prediction-feed-result-v11-outcomes', {'date': date_text})
        _cache_set(cache_key, rebuilt, _prediction_feed_cache_ttl(date_text))
        migrated += 1
    if migrated:
        print(f"[prediction-history] outcomes-only migration dates={migrated}")


def _prediction_restore_saved_feeds() -> None:
    """Rebuild old summary-only dates once and then retain full cards forever.

    Older versions intentionally deleted ``feed_json`` after settlement. This
    migration restores recent saved dates in the background. Any older missing
    date is also restored on demand when it is opened in the date strip.
    """
    if API_FOOTBALL_DEMO or not API_FOOTBALL_KEY or PREDICTION_HISTORY_BACKFILL_DAYS <= 0:
        return
    today_text = _prediction_today()
    try:
        today_dt = datetime.strptime(today_text, '%Y-%m-%d')
    except Exception:
        return
    pending: list[dict[str, Any]] = []
    for record in _prediction_storage_records():
        date_text = str(record.get('date') or '')
        if not date_text or date_text > today_text or isinstance(record.get('feed'), dict):
            continue
        try:
            age_days = (today_dt - datetime.strptime(date_text, '%Y-%m-%d')).days
        except Exception:
            continue
        if 0 <= age_days <= PREDICTION_HISTORY_BACKFILL_DAYS:
            pending.append(record)
    pending.sort(key=lambda row: str(row.get('date') or ''))
    if not pending:
        return
    print(f"[prediction-history] restoring full match cards dates={len(pending)}")
    for record in pending:
        date_text = str(record.get('date') or '')
        try:
            latest = _prediction_storage_get(date_text)
            if latest and isinstance(latest.get('feed'), dict):
                continue
            rebuilt = _prediction_feed_for_date(date_text)
            rebuilt['archived'] = date_text < today_text
            rebuilt['history_retention'] = PREDICTION_HISTORY_RETENTION_POLICY
            rebuilt['updated_at'] = int(time.time())
            _prediction_storage_save(date_text, rebuilt, keep_feed=True, is_final=date_text < today_text)
            cache_key = _cache_key('prediction-feed-result-v11-outcomes', {'date': date_text})
            _cache_set(cache_key, rebuilt, _prediction_feed_cache_ttl(date_text))
            print(f"[prediction-history] full cards restored date={date_text} matches={len(rebuilt.get('feed') or [])}")
        except Exception as exc:
            print(f"[prediction-history] full cards restore failed date={date_text}: {exc}")
        time.sleep(1.0)


def _prediction_history_worker() -> None:
    _prediction_recalculate_saved_month_filtered()
    _prediction_restore_saved_feeds()
    last_today_refresh = 0.0
    last_archive_check = 0.0
    while True:
        now = time.time()
        try:
            if API_FOOTBALL_KEY and not API_FOOTBALL_DEMO:
                if now - last_today_refresh >= PREDICTION_LIVE_REFRESH_SECONDS:
                    _prediction_refresh_today_scores()
                    last_today_refresh = now
                if now - last_archive_check >= 15 * 60:
                    _prediction_finalize_old_days()
                    last_archive_check = now
        except Exception as exc:
            print(f"[prediction-history] worker error: {exc}")
        time.sleep(60)


def start_prediction_history_worker() -> None:
    global _prediction_history_worker_started
    with _prediction_history_lock:
        if _prediction_history_worker_started:
            return
        _prediction_history_worker_started = True
    _prediction_storage_init()
    threading.Thread(target=_prediction_history_worker, name='prediction-history', daemon=True).start()

def _prediction_feed_cache_ttl(date_text: str) -> int:
    today = _prediction_today()
    if date_text < today:
        return 24 * 3600
    if date_text > today:
        return 30 * 60
    return 15 * 60


def _prediction_feed_cached(date_text: str, *, force: bool = False) -> dict[str, Any]:
    """Build a date once, share it with everyone and retain every match forever.

    Historical match cards are served directly from PostgreSQL/SQLite/Redis and
    never rebuilt after they have been saved. Dates created by an older version
    with summary-only storage are rebuilt once on first open and then retained.
    """
    today = _prediction_today()
    # Manual refresh of today's page uses the same incremental path as the
    # background worker. It never re-downloads predictions for every fixture.
    if date_text == today and force:
        _prediction_refresh_today_scores()
        force = False
    record = _prediction_storage_get(date_text)
    key = _cache_key('prediction-feed-result-v11-outcomes', {'date': date_text})

    # A complete historical feed is authoritative, even when refresh is pressed.
    # This keeps API usage at zero for dates that have already been saved.
    if date_text < today and record and isinstance(record.get('feed'), dict) and record['feed'].get('odds_policy') == PREDICTION_ODDS_POLICY:
        result = dict(record['feed'])
        result.update({
            'date': date_text,
            'archived': True,
            'history_retention': PREDICTION_HISTORY_RETENTION_POLICY,
            '_persistent_store': 'full-history',
            '_shared_cache': 'persistent-hit',
            '_shared_cache_ttl': _prediction_feed_cache_ttl(date_text),
        })
        _cache_set(key, result, _prediction_feed_cache_ttl(date_text))
        return result

    if not force:
        cached = _cache_get(key)
        if isinstance(cached, dict) and cached.get('odds_policy') == PREDICTION_ODDS_POLICY:
            result = dict(cached)
            result['_shared_cache'] = 'hit'
            result['_shared_cache_ttl'] = _prediction_feed_cache_ttl(date_text)
            return result

    # A container restart must not trigger the current day to be rebuilt while
    # its persistent payload is still fresh.
    if date_text == today and not force and record and isinstance(record.get('feed'), dict) and record['feed'].get('odds_policy') == PREDICTION_ODDS_POLICY:
        age = max(0, int(time.time()) - int(record.get('updated_at') or 0))
        if age < _prediction_feed_cache_ttl(date_text):
            result = dict(record['feed'])
            result.update({
                'archived': False,
                'history_retention': PREDICTION_HISTORY_RETENTION_POLICY,
                '_persistent_store': 'today',
                '_shared_cache': 'persistent-hit',
            })
            _cache_set(key, result, max(1, _prediction_feed_cache_ttl(date_text) - age))
            return result

    lock = _lock_for(key)
    with lock:
        record = _prediction_storage_get(date_text)

        # Re-check persistent history after waiting for another request to build it.
        if date_text < today and record and isinstance(record.get('feed'), dict) and record['feed'].get('odds_policy') == PREDICTION_ODDS_POLICY:
            result = dict(record['feed'])
            result.update({
                'date': date_text,
                'archived': True,
                'history_retention': PREDICTION_HISTORY_RETENTION_POLICY,
                '_persistent_store': 'full-history',
                '_shared_cache': 'persistent-hit-after-wait',
            })
            _cache_set(key, result, _prediction_feed_cache_ttl(date_text))
            return result

        if not force:
            cached = _cache_get(key)
            if isinstance(cached, dict) and cached.get('odds_policy') == PREDICTION_ODDS_POLICY:
                result = dict(cached)
                result['_shared_cache'] = 'hit-after-wait'
                result['_shared_cache_ttl'] = _prediction_feed_cache_ttl(date_text)
                return result

        started = time.time()
        result = _prediction_feed_for_date(date_text)
        old_payload = record.get('feed') if record and isinstance(record.get('feed'), dict) else None
        if date_text == today:
            result = _prediction_merge_day_payload(old_payload, result)
        result['archived'] = date_text < today
        result['history_retention'] = PREDICTION_HISTORY_RETENTION_POLICY
        result['updated_at'] = int(time.time())

        # Full feed_json is always retained. Historical dates are marked final so
        # the worker does not refresh them again, but their cards stay selectable.
        _prediction_storage_save(date_text, result, keep_feed=True, is_final=date_text < today)
        result['_shared_cache'] = 'miss'
        result['_shared_cache_ttl'] = _prediction_feed_cache_ttl(date_text)
        result['_build_seconds'] = round(time.time() - started, 2)
        _cache_set(key, result, _prediction_feed_cache_ttl(date_text))
        return result


def _api_football_wsgi(environ: dict[str, Any], start_response: Callable[..., Any]) -> list[bytes]:
    path = str(environ.get("PATH_INFO") or "")
    params = _query(environ)
    try:
        if path in {
            "/api-football/predictions",
            "/api-football/prediction-history",
            "/api-football/prediction-feed",
            "/api-football/prediction-audit",
            "/api-football/odds",
        }:
            return _json_response(start_response, {
                "ok": False,
                "error": "Прогноз и коэффициент берутся только из Scores24. Этот API-Football prediction/odds endpoint не используется.",
                "replacement": "/scores24/prediction-feed",
            }, 410)
        if path == "/api-football/goal-notify":
            if str(environ.get("REQUEST_METHOD") or "GET").upper() != "POST":
                return _json_response(start_response, {"ok": False, "error": "POST required"}, 405)
            body = _read_json_body(environ)
            init_data = str(body.get("init_data") or "")
            fixture_id = str(body.get("fixture_id") or "").strip()
            action = str(body.get("action") or "status").strip().lower()
            if not fixture_id.isdigit():
                return _json_response(start_response, {"ok": False, "error": "Некорректный матч"}, 400)
            user = _goal_notify_user(init_data)
            if not user or not str(user.get("id") or "").isdigit():
                return _json_response(start_response, {"ok": False, "error": "Откройте приложение через Telegram-бота"}, 401)
            chat_id = str(user.get("id"))
            key = _goal_notify_key(chat_id, fixture_id)
            with _goal_notify_lock:
                enabled_now = key in _goal_notify_subs
            if action == "status":
                return _json_response(start_response, {"ok": True, "enabled": enabled_now})
            if action != "toggle":
                return _json_response(start_response, {"ok": False, "error": "Неизвестное действие"}, 400)
            if enabled_now:
                with _goal_notify_lock:
                    _goal_notify_subs.pop(key, None)
                _goal_notify_save()
                return _json_response(start_response, {"ok": True, "enabled": False})
            if not getattr(notify_app, "BOT_TOKEN", ""):
                return _json_response(start_response, {"ok": False, "error": "BOT_TOKEN не настроен"}, 503)
            item = _goal_notify_fixture(fixture_id)
            if not item:
                return _json_response(start_response, {"ok": False, "error": "Не удалось получить матч"}, 404)
            fixture = item.get("fixture") or {}
            status = fixture.get("status") or {}
            if str(status.get("short") or "") in {"FT", "AET", "PEN", "CANC", "ABD", "AWD", "WO"}:
                return _json_response(start_response, {"ok": False, "error": "Матч уже завершён"}, 400)
            goals = item.get("goals") or {}
            teams = item.get("teams") or {}
            league = item.get("league") or {}
            try:
                kickoff_ts = int(datetime.fromisoformat(str(fixture.get("date") or "").replace("Z", "+00:00")).timestamp())
            except Exception:
                kickoff_ts = int(time.time())
            sub = {
                "chat_id": chat_id,
                "fixture_id": int(fixture_id),
                "home": str((teams.get("home") or {}).get("name") or "Хозяева"),
                "away": str((teams.get("away") or {}).get("name") or "Гости"),
                "league": str(league.get("name") or ""),
                "last_home": int(goals.get("home") or 0),
                "last_away": int(goals.get("away") or 0),
                "kickoff_ts": kickoff_ts,
                "created_at": int(time.time()),
                "updated_at": int(time.time()),
            }
            home_safe = _goal_notify_escape(sub["home"])
            away_safe = _goal_notify_escape(sub["away"])
            confirm = f"🔔 Уведомления о голах включены\n<b>{home_safe} — {away_safe}</b>"
            if not notify_app.send_telegram_message(chat_id, confirm, link="", button_text=""):
                return _json_response(start_response, {"ok": False, "error": "Бот не смог отправить сообщение. Откройте бота и нажмите /start"}, 400)
            with _goal_notify_lock:
                _goal_notify_subs[key] = sub
            _goal_notify_save()
            return _json_response(start_response, {"ok": True, "enabled": True})

        if path == "/api-football/status":
            if API_FOOTBALL_DEMO or not API_FOOTBALL_KEY:
                return _json_response(start_response, {"ok": bool(API_FOOTBALL_DEMO), "configured": bool(API_FOOTBALL_KEY), "demo": True, "timezone": APP_TIMEZONE, "quota": _quota_snapshot()})
            data = _api_call("status", {}, 60, allow_demo=False)
            return _json_response(start_response, {"ok": not bool(data.get("errors")), "configured": True, "demo": False, "timezone": APP_TIMEZONE, "quota": data.get("_quota"), "response": data.get("response"), "errors": data.get("errors")})


        if path == "/api-football/teams":
            search = params.get("search", "").strip()
            if len(search) < 3 or len(search) > 60:
                return _json_response(start_response, {"ok": False, "error": "Введите не менее 3 символов для поиска клуба"}, 400)
            if API_FOOTBALL_DEMO or not API_FOOTBALL_KEY:
                return _json_response(start_response, _demo_teams(search))
            return _json_response(start_response, _api_call("teams", {"search": search}, 24 * 3600, allow_demo=False))

        if path == "/api-football/leagues":
            search = params.get("search", "").strip()
            if search and len(search) < 3:
                return _json_response(start_response, {"ok": False, "error": "Введите не менее 3 символов для поиска лиги"}, 400)
            if API_FOOTBALL_DEMO or not API_FOOTBALL_KEY:
                return _json_response(start_response, _demo_leagues(search))
            api_params: dict[str, Any] = {"current": "true"}
            if search:
                api_params = {"search": search, "current": "true"}
            return _json_response(start_response, _api_call("leagues", api_params, 24 * 3600, allow_demo=False))

        if path == "/api-football/players-search":
            search = params.get("search", "").strip()
            if len(search) < 3 or len(search) > 60:
                return _json_response(start_response, {"ok": False, "error": "Введите не менее 3 символов для поиска игрока"}, 400)
            if API_FOOTBALL_DEMO or not API_FOOTBALL_KEY:
                return _json_response(start_response, _demo_player_search(search))
            errors: list[str] = []
            for endpoint in ("players/profiles", "players"):
                try:
                    data = _api_call(endpoint, {"search": search}, 24 * 3600, allow_demo=False)
                    items = data.get("response") or []
                    normalized: list[dict[str, Any]] = []
                    for raw in items:
                        if not isinstance(raw, dict):
                            continue
                        player = raw.get("player") if isinstance(raw.get("player"), dict) else raw
                        pid = str((player or {}).get("id") or raw.get("player_id") or "").strip()
                        if not pid.isdigit():
                            continue
                        normalized.append({
                            "player": dict(player or {}),
                            "statistics": raw.get("statistics") if isinstance(raw.get("statistics"), list) else [],
                            "team": raw.get("team") if isinstance(raw.get("team"), dict) else {},
                            "league": raw.get("league") if isinstance(raw.get("league"), dict) else {},
                        })
                    if normalized:
                        data = dict(data)
                        data["response"] = normalized
                        data["results"] = len(normalized)
                        data["errors"] = {}
                        return _json_response(start_response, data)
                except Exception as exc:
                    errors.append(str(exc))
            return _json_response(start_response, {"get": "players/search", "parameters": {"search": search}, "errors": {"request": "; ".join(errors)} if errors else {}, "results": 0, "paging": {"current": 1, "total": 1}, "response": [], "_quota": _quota_snapshot()})

        if path == "/api-football/team-profile":
            team_id = params.get("team", "").strip()
            league_id = params.get("league", "").strip()
            season_text = params.get("season", "").strip()
            if not team_id.isdigit():
                return _json_response(start_response, {"ok": False, "error": "invalid team id"}, 400)
            requested_season = int(season_text) if season_text.isdigit() and 2000 <= int(season_text) <= 2100 else None
            if API_FOOTBALL_DEMO or not API_FOOTBALL_KEY:
                return _json_response(start_response, _demo_team_profile(team_id, league_id or "39", requested_season or 2026))

            team_data = _api_call("teams", {"id": team_id}, 24 * 3600, allow_demo=False)
            last_data = _api_call("fixtures", {"team": team_id, "last": 10, "timezone": APP_TIMEZONE}, 6 * 3600, allow_demo=False)
            next_data = _api_call("fixtures", {"team": team_id, "next": 10, "timezone": APP_TIMEZONE}, 30 * 60, allow_demo=False)
            coach_data = _api_call("coachs", {"team": team_id}, 24 * 3600, allow_demo=False)
            transfers_data = _api_call("transfers", {"team": team_id}, 12 * 3600, allow_demo=False)
            last_items = last_data.get("response") or []
            next_items = next_data.get("response") or []
            coach_items = coach_data.get("response") or []
            transfers_items = transfers_data.get("response") or []

            # Prefer the league/season of the match from which the user opened the team.
            # For a team opened from global search, infer context from its latest fixture.
            context_fixture = next((x for x in last_items if (x.get("league") or {}).get("id")), None) or next((x for x in next_items if (x.get("league") or {}).get("id")), None)
            context_league = league_id if league_id.isdigit() else str(((context_fixture or {}).get("league") or {}).get("id") or "")
            context_season = requested_season or int(((context_fixture or {}).get("league") or {}).get("season") or 0) or None

            statistics: dict[str, Any] | None = None
            standing: dict[str, Any] | None = None
            statistics_errors: Any = {}
            standings_errors: Any = {}
            if context_league.isdigit() and context_season:
                stats_data = _api_call(
                    "teams/statistics",
                    {"league": context_league, "season": context_season, "team": team_id},
                    12 * 3600,
                    allow_demo=False,
                )
                statistics = stats_data.get("response") or None
                statistics_errors = stats_data.get("errors") or {}

                standings_data = _api_call(
                    "standings",
                    {"league": context_league, "season": context_season, "team": team_id},
                    6 * 3600,
                    allow_demo=False,
                )
                standings_errors = standings_data.get("errors") or {}
                standings_response = standings_data.get("response") or []
                if standings_response:
                    groups = ((standings_response[0].get("league") or {}).get("standings") or [])
                    rows = [row for group in groups for row in (group or [])]
                    standing = next((row for row in rows if str(((row.get("team") or {}).get("id") or "")) == team_id), rows[0] if rows else None)

            return _json_response(start_response, {
                "team": (team_data.get("response") or [None])[0],
                "last": last_items,
                "next": next_items,
                "statistics": statistics,
                "standing": standing,
                "coach": coach_items[0] if coach_items else None,
                "transfers": transfers_items,
                "club_trophies": [],
                "club_trophies_supported": False,
                "context": {"league": int(context_league) if context_league.isdigit() else None, "season": context_season},
                "errors": (
                    team_data.get("errors")
                    or last_data.get("errors")
                    or next_data.get("errors")
                    or coach_data.get("errors")
                    or transfers_data.get("errors")
                    or statistics_errors
                    or standings_errors
                    or {}
                ),
                "_quota": _quota_snapshot(),
            })

        if path == "/api-football/league-view":
            league_id = params.get("league", "").strip()
            season_text = params.get("season", "").strip()
            if not (league_id.isdigit() and season_text.isdigit()):
                return _json_response(start_response, {"ok": False, "error": "invalid league or season"}, 400)
            season = int(season_text)
            if season < 2000 or season > 2100:
                return _json_response(start_response, {"ok": False, "error": "invalid season"}, 400)
            if API_FOOTBALL_DEMO or not API_FOOTBALL_KEY:
                return _json_response(start_response, _demo_league_view(league_id, season))
            league_data = _api_call("leagues", {"id": league_id, "season": season}, 24 * 3600, allow_demo=False)
            standings_data = _api_call("standings", {"league": league_id, "season": season}, 6 * 3600, allow_demo=False)
            next_data = _api_call("fixtures", {"league": league_id, "season": season, "next": 10, "timezone": APP_TIMEZONE}, 30 * 60, allow_demo=False)
            standings_response = standings_data.get("response") or []
            table: list[dict[str, Any]] = []
            standings_groups: list[dict[str, Any]] = []
            if standings_response:
                league_block = standings_response[0].get("league") or {}
                groups = league_block.get("standings") or []
                for index, group in enumerate(groups):
                    rows = group or []
                    if rows:
                        standings_groups.append({"name": f"Группа {index + 1}" if len(groups) > 1 else "Общая таблица", "rows": rows})
                if standings_groups:
                    table = standings_groups[0]["rows"]
            next_items = next_data.get("response") or []
            current_round = str((((next_items[0].get("league") or {}).get("round")) if next_items else "") or "")
            return _json_response(start_response, {
                "league": (league_data.get("response") or [None])[0],
                "season": season,
                "standings": table,
                "standings_groups": standings_groups,
                "next": next_items,
                "current_round": current_round,
                "errors": league_data.get("errors") or standings_data.get("errors") or next_data.get("errors") or {},
                "_quota": _quota_snapshot(),
            })

        if path == "/api-football/league-leaders":
            league_id = params.get("league", "").strip()
            season_text = params.get("season", "").strip()
            if not (league_id.isdigit() and season_text.isdigit()):
                return _json_response(start_response, {"ok": False, "error": "invalid league or season"}, 400)
            season = int(season_text)
            if API_FOOTBALL_DEMO or not API_FOOTBALL_KEY:
                return _json_response(start_response, _demo_league_leaders(league_id, season))
            tasks = {
                "topscorers": ("players/topscorers", {"league": league_id, "season": season}, 24 * 3600),
                "topassists": ("players/topassists", {"league": league_id, "season": season}, 24 * 3600),
                "topyellowcards": ("players/topyellowcards", {"league": league_id, "season": season}, 24 * 3600),
                "topredcards": ("players/topredcards", {"league": league_id, "season": season}, 24 * 3600),
            }
            leaders: dict[str, list[dict[str, Any]]] = {}
            errors: dict[str, Any] = {}
            with ThreadPoolExecutor(max_workers=4) as pool:
                futures = {pool.submit(_api_call, endpoint, query, ttl, allow_demo=False): key for key, (endpoint, query, ttl) in tasks.items()}
                for future, key in [(future, futures[future]) for future in futures]:
                    try:
                        data = future.result()
                        leaders[key] = data.get("response") or []
                        if data.get("errors"):
                            errors[key] = data.get("errors")
                    except Exception as exc:
                        leaders[key] = []
                        errors[key] = str(exc)
            return _json_response(start_response, {"leaders": leaders, "season": season, "errors": errors, "_quota": _quota_snapshot()})

        if path == "/api-football/league-calendar":
            league_id = params.get("league", "").strip()
            season_text = params.get("season", "").strip()
            requested_round = params.get("round", "").strip()
            if not (league_id.isdigit() and season_text.isdigit()):
                return _json_response(start_response, {"ok": False, "error": "invalid league or season"}, 400)
            season = int(season_text)
            if API_FOOTBALL_DEMO or not API_FOOTBALL_KEY:
                return _json_response(start_response, _demo_league_calendar(league_id, season, requested_round))
            rounds_data = _api_call("fixtures/rounds", {"league": league_id, "season": season}, 24 * 3600, allow_demo=False)
            rounds = [str(value) for value in (rounds_data.get("response") or []) if value]
            selected_round = requested_round if requested_round in rounds else ""
            current_errors: Any = {}
            if not selected_round:
                current_data = _api_call("fixtures/rounds", {"league": league_id, "season": season, "current": "true"}, 30 * 60, allow_demo=False)
                current_values = [str(value) for value in (current_data.get("response") or []) if value]
                current_errors = current_data.get("errors") or {}
                selected_round = current_values[0] if current_values else (rounds[-1] if rounds else "")
            fixtures: list[dict[str, Any]] = []
            fixture_errors: Any = {}
            if selected_round:
                fixtures_data = _api_call("fixtures", {"league": league_id, "season": season, "round": selected_round, "timezone": APP_TIMEZONE}, 15 * 60, allow_demo=False)
                fixtures = fixtures_data.get("response") or []
                fixture_errors = fixtures_data.get("errors") or {}
            return _json_response(start_response, {
                "rounds": rounds,
                "round": selected_round,
                "fixtures": fixtures,
                "season": season,
                "errors": rounds_data.get("errors") or current_errors or fixture_errors or {},
                "_quota": _quota_snapshot(),
            })

        if path == "/api-football/league-teams":
            league_id = params.get("league", "").strip()
            season_text = params.get("season", "").strip()
            if not (league_id.isdigit() and season_text.isdigit()):
                return _json_response(start_response, {"ok": False, "error": "invalid league or season"}, 400)
            season = int(season_text)
            if API_FOOTBALL_DEMO or not API_FOOTBALL_KEY:
                return _json_response(start_response, _demo_league_teams(league_id, season))
            data = _api_call("teams", {"league": league_id, "season": season}, 24 * 3600, allow_demo=False)
            return _json_response(start_response, {"teams": data.get("response") or [], "season": season, "errors": data.get("errors") or {}, "_quota": _quota_snapshot()})

        if path == "/api-football/league-seasons":
            league_id = params.get("league", "").strip()
            season_text = params.get("season", "").strip()
            if not (league_id.isdigit() and season_text.isdigit()):
                return _json_response(start_response, {"ok": False, "error": "invalid league or season"}, 400)
            season = int(season_text)
            if API_FOOTBALL_DEMO or not API_FOOTBALL_KEY:
                return _json_response(start_response, _demo_league_seasons(league_id, season))
            league_data = _api_call("leagues", {"id": league_id}, 7 * 24 * 3600, allow_demo=False)
            league_items = league_data.get("response") or []
            seasons = (league_items[0].get("seasons") or []) if league_items else []
            previous_years = sorted([int(item.get("year")) for item in seasons if str(item.get("year", "")).isdigit() and int(item.get("year")) < season], reverse=True)[:5]
            winners: list[dict[str, Any]] = []
            winner_errors: dict[str, Any] = {}
            def winner_for(year: int) -> tuple[int, dict[str, Any] | None, Any]:
                try:
                    data = _api_call("standings", {"league": league_id, "season": year}, 7 * 24 * 3600, allow_demo=False)
                    response = data.get("response") or []
                    groups = (((response[0].get("league") or {}).get("standings") or []) if response else [])
                    rows = [row for group in groups for row in (group or [])]
                    winner = next((row.get("team") for row in rows if int(row.get("rank") or 0) == 1), None)
                    return year, winner, data.get("errors") or {}
                except Exception as exc:
                    return year, None, str(exc)
            if previous_years:
                with ThreadPoolExecutor(max_workers=min(5, len(previous_years))) as pool:
                    results = list(pool.map(winner_for, previous_years))
                for year, team, error in results:
                    winners.append({"season": year, "team": team})
                    if error:
                        winner_errors[str(year)] = error
            return _json_response(start_response, {
                "seasons": seasons,
                "winners": winners,
                "season": season,
                "errors": league_data.get("errors") or winner_errors or {},
                "_quota": _quota_snapshot(),
            })

        if path == "/api-football/fixtures":
            date_text = params.get("date") or datetime.now().strftime("%Y-%m-%d")
            if not __import__("re").fullmatch(r"\d{4}-\d{2}-\d{2}", date_text):
                return _json_response(start_response, {"ok": False, "error": "invalid date"}, 400)
            if API_FOOTBALL_DEMO or not API_FOOTBALL_KEY:
                return _json_response(start_response, _demo_fixtures(date_text))
            data = _api_call("fixtures", {"date": date_text, "timezone": params.get("timezone") or APP_TIMEZONE}, _fixture_ttl(date_text), allow_demo=False)
            return _json_response(start_response, data)

        if path == "/api-football/fixture":
            fixture_id = params.get("id", "").strip()
            if not fixture_id.isdigit():
                return _json_response(start_response, {"ok": False, "error": "missing or invalid fixture id"}, 400)
            if API_FOOTBALL_DEMO or not API_FOOTBALL_KEY:
                data = _demo_fixtures(datetime.now().strftime("%Y-%m-%d"))
                fixture = next((x for x in data["response"] if str(x["fixture"]["id"]) == fixture_id), data["response"][0])
                return _json_response(start_response, {"get": "fixtures", "errors": {}, "results": 1, "response": [fixture], "demo": True, "_quota": _quota_snapshot()})
            return _json_response(start_response, _api_call("fixtures", {"id": fixture_id, "timezone": APP_TIMEZONE}, 60, allow_demo=False))

        if path == "/api-football/statistics":
            fixture_id = params.get("fixture", "").strip()
            force = params.get("force", "").strip().lower() in {"1", "true", "yes"}
            if not fixture_id.isdigit():
                return _json_response(start_response, {"ok": False, "error": "invalid fixture id"}, 400)
            if API_FOOTBALL_DEMO or not API_FOOTBALL_KEY:
                return _json_response(start_response, {"get": "fixtures/statistics", "errors": {}, "results": 2, "response": _demo_stats(), "source": "official", "demo": True, "_quota": _quota_snapshot()})

            def usable(blocks: list[dict[str, Any]]) -> bool:
                return sum(1 for block in blocks if block.get("statistics")) >= 2

            query: dict[str, Any] = {"fixture": fixture_id}
            if force:
                query["refresh"] = int(time.time() // 20)
            official_data = _api_call("fixtures/statistics", query, 45, allow_demo=False)
            official = official_data.get("response") or []

            # Empty live statistics can be cached before the provider publishes them.
            # Retry with a shared 20-second refresh key, so all users reuse one request.
            if not usable(official):
                fresh_query = {"fixture": fixture_id, "refresh": int(time.time() // 20)}
                fresh_data = _api_call("fixtures/statistics", fresh_query, 30, allow_demo=False)
                fresh_blocks = fresh_data.get("response") or []
                if len(fresh_blocks) >= len(official):
                    official_data, official = fresh_data, fresh_blocks

            # Some competitions publish player statistics earlier than the team block.
            # Use them only to fill missing fields; official values always take priority.
            players_query: dict[str, Any] = {"fixture": fixture_id}
            if force or not usable(official):
                players_query["refresh"] = int(time.time() // 30)
            players_data = _api_call("fixtures/players", players_query, 60, allow_demo=False)
            fallback = _statistics_from_fixture_players(players_data.get("response") or [])
            merged = _merge_fixture_statistics(official, fallback)
            source = "official+players" if official and fallback else ("official" if official else ("players" if fallback else "unavailable"))
            return _json_response(start_response, {
                "get": "fixtures/statistics",
                "errors": official_data.get("errors") or players_data.get("errors") or {},
                "results": len(merged),
                "response": merged,
                "source": source,
                "_quota": _quota_snapshot(),
            })


        if path == "/api-football/player-profile":
            player_id = params.get("player", "").strip()
            season_text = params.get("season", "").strip()
            fixture_id = params.get("fixture", "").strip()
            team_id = params.get("team", "").strip()
            league_id = params.get("league", "").strip()
            if not player_id.isdigit():
                return _json_response(start_response, {"ok": False, "error": "invalid player id"}, 400)
            season = int(season_text) if season_text.isdigit() and 2000 <= int(season_text) <= 2100 else datetime.now().year
            if API_FOOTBALL_DEMO or not API_FOOTBALL_KEY:
                return _json_response(start_response, _demo_player_profile(player_id, season, fixture_id, team_id, league_id))

            def safe_call(endpoint: str, query: dict[str, Any], ttl: int) -> dict[str, Any]:
                try:
                    return _api_call(endpoint, query, ttl, allow_demo=False)
                except Exception as exc:
                    return {"response": [], "errors": {"request": str(exc)}}

            player_data = safe_call("players", {"id": player_id, "season": season}, 12 * 3600)
            player_items = player_data.get("response") or []
            # A player opened from global search may have no statistics in the
            # calendar year yet. Try the two previous seasons so the profile
            # card still opens with real career/team data.
            if not player_items:
                for fallback_season in (season - 1, season - 2):
                    if fallback_season < 2000:
                        continue
                    fallback_data = safe_call("players", {"id": player_id, "season": fallback_season}, 12 * 3600)
                    fallback_items = fallback_data.get("response") or []
                    if fallback_items:
                        season = fallback_season
                        player_data = fallback_data
                        player_items = fallback_items
                        break
            profile = player_items[0] if player_items else None
            derived_team_id = team_id
            if not derived_team_id and profile:
                stats = profile.get("statistics") or []
                if stats:
                    derived_team_id = str(((stats[0].get("team") or {}).get("id") or ""))

            tasks: dict[str, tuple[str, dict[str, Any], int]] = {
                "career_seasons": ("players/seasons", {"player": player_id}, 7 * 24 * 3600),
                "career_teams": ("players/teams", {"player": player_id}, 7 * 24 * 3600),
                "injuries": ("injuries", {"player": player_id}, 6 * 3600),
                "sidelined": ("sidelined", {"player": player_id}, 24 * 3600),
                "transfers": ("transfers", {"player": player_id}, 24 * 3600),
                "trophies": ("trophies", {"player": player_id}, 7 * 24 * 3600),
            }
            if derived_team_id.isdigit():
                tasks["squad"] = ("players/squads", {"team": derived_team_id}, 24 * 3600)
            if league_id.isdigit():
                tasks.update({
                    "topscorers": ("players/topscorers", {"league": league_id, "season": season}, 24 * 3600),
                    "topassists": ("players/topassists", {"league": league_id, "season": season}, 24 * 3600),
                    "topyellowcards": ("players/topyellowcards", {"league": league_id, "season": season}, 24 * 3600),
                    "topredcards": ("players/topredcards", {"league": league_id, "season": season}, 24 * 3600),
                })

            results: dict[str, dict[str, Any]] = {}
            with ThreadPoolExecutor(max_workers=min(6, max(1, len(tasks)))) as pool:
                future_map = {pool.submit(safe_call, endpoint, query, ttl): key for key, (endpoint, query, ttl) in tasks.items()}
                for future, key in [(f, future_map[f]) for f in future_map]:
                    try:
                        results[key] = future.result()
                    except Exception as exc:
                        results[key] = {"response": [], "errors": {"request": str(exc)}}

            match_player = None
            match_errors: Any = {}
            if fixture_id.isdigit():
                fixture_players = safe_call("fixtures/players", {"fixture": fixture_id}, 5 * 60)
                match_errors = fixture_players.get("errors") or {}
                for team_block in fixture_players.get("response") or []:
                    for item in team_block.get("players") or []:
                        if str(((item.get("player") or {}).get("id") or "")) == player_id:
                            stat_list = item.get("statistics") or []
                            match_player = {
                                "team": team_block.get("team") or {},
                                "player": item.get("player") or {},
                                "statistics": stat_list[0] if stat_list else {},
                            }
                            break
                    if match_player is not None:
                        break

            current_squad = None
            squad_response = (results.get("squad") or {}).get("response") or []
            for team_block in squad_response:
                for squad_player in team_block.get("players") or []:
                    if str(squad_player.get("id") or "") == player_id:
                        current_squad = {"team": team_block.get("team") or {}, "player": squad_player}
                        break
                if current_squad:
                    break

            rankings: dict[str, int | None] = {}
            for ranking_key in ("topscorers", "topassists", "topyellowcards", "topredcards"):
                ranking_items = (results.get(ranking_key) or {}).get("response") or []
                rank = next((index + 1 for index, item in enumerate(ranking_items) if str(((item.get("player") or {}).get("id") or "")) == player_id), None)
                rankings[ranking_key] = rank

            all_errors: dict[str, Any] = {}
            for key, data in results.items():
                if data.get("errors"):
                    all_errors[key] = data.get("errors")

            return _json_response(start_response, {
                "profile": profile,
                "match": match_player,
                "season": season,
                "career_seasons": (results.get("career_seasons") or {}).get("response") or [],
                "career_teams": (results.get("career_teams") or {}).get("response") or [],
                "injuries": (results.get("injuries") or {}).get("response") or [],
                "sidelined": (results.get("sidelined") or {}).get("response") or [],
                "transfers": (results.get("transfers") or {}).get("response") or [],
                "trophies": (results.get("trophies") or {}).get("response") or [],
                "current_squad": current_squad,
                "rankings": rankings,
                # Optional career/injury/ranking endpoints may be unavailable for
                # some players. They must not block opening the player card.
                "errors": player_data.get("errors") if not (profile or current_squad or match_player) else {},
                "warnings": {"match": match_errors, "optional": all_errors},
                "_quota": _quota_snapshot(),
            })

        if path == "/api-football/events":
            fixture_id = params.get("fixture", "").strip()
            if not fixture_id.isdigit():
                return _json_response(start_response, {"ok": False, "error": "invalid fixture id"}, 400)
            if API_FOOTBALL_DEMO or not API_FOOTBALL_KEY:
                events = [
                    {"time": {"elapsed": 58, "extra": None}, "team": {"id": 42, "name": "Arsenal", "logo": ""}, "player": {"name": "B. Saka"}, "assist": {"name": "M. Ødegaard"}, "type": "Goal", "detail": "Normal Goal", "comments": None},
                    {"time": {"elapsed": 43, "extra": None}, "team": {"id": 49, "name": "Chelsea", "logo": ""}, "player": {"name": "M. Cucurella"}, "assist": {"name": None}, "type": "Card", "detail": "Yellow Card", "comments": None},
                ]
                return _json_response(start_response, {"get": "fixtures/events", "errors": {}, "results": len(events), "response": events, "demo": True, "_quota": _quota_snapshot()})
            return _json_response(start_response, _api_call("fixtures/events", {"fixture": fixture_id}, 60, allow_demo=False))

        if path == "/api-football/lineups":
            fixture_id = params.get("fixture", "").strip()
            force = params.get("force", "").strip() in {"1", "true", "yes"}
            if not fixture_id.isdigit():
                return _json_response(start_response, {"ok": False, "error": "invalid fixture id"}, 400)
            if API_FOOTBALL_DEMO or not API_FOOTBALL_KEY:
                names1 = ["Raya", "Saliba", "Gabriel", "White", "Rice", "Ødegaard", "Partey", "Saka", "Martinelli", "Trossard", "Havertz"]
                names2 = ["Sánchez", "James", "Colwill", "Fofana", "Cucurella", "Caicedo", "Enzo", "Palmer", "Madueke", "Nkunku", "Jackson"]
                lineups = [
                    {"team": {"id": 42, "name": "Arsenal", "logo": ""}, "formation": "4-3-3", "startXI": [{"player": {"id": i + 1, "name": name, "number": i + 1, "pos": "G" if i == 0 else "M"}} for i, name in enumerate(names1)], "substitutes": []},
                    {"team": {"id": 49, "name": "Chelsea", "logo": ""}, "formation": "4-2-3-1", "startXI": [{"player": {"id": i + 20, "name": name, "number": i + 1, "pos": "G" if i == 0 else "M"}} for i, name in enumerate(names2)], "substitutes": []},
                ]
                return _json_response(start_response, {"get": "fixtures/lineups", "errors": {}, "results": 2, "response": lineups, "source": "lineups", "demo": True, "_quota": _quota_snapshot()})

            # Official match lineups. Keep the empty result only briefly because
            # providers often publish lineups shortly before kickoff or during live.
            lineup_params: dict[str, Any] = {"fixture": fixture_id}
            if force:
                lineup_params["refresh"] = int(time.time() // 60)
            lineup_data = _api_call("fixtures/lineups", lineup_params, 90, allow_demo=False)
            lineups = lineup_data.get("response") or []
            if lineups:
                lineup_data["source"] = "lineups"
                return _json_response(start_response, lineup_data)

            # Fallback: fixtures/players is often available even when the dedicated
            # lineups endpoint is empty. Convert it into the same UI structure.
            player_params: dict[str, Any] = {"fixture": fixture_id}
            if force:
                player_params["refresh"] = int(time.time() // 60)
            players_data = _api_call("fixtures/players", player_params, 90, allow_demo=False)
            converted: list[dict[str, Any]] = []
            for team_block in players_data.get("response") or []:
                start_xi: list[dict[str, Any]] = []
                substitutes: list[dict[str, Any]] = []
                for item in team_block.get("players") or []:
                    player = dict(item.get("player") or {})
                    stats_list = item.get("statistics") or []
                    stats = stats_list[0] if stats_list else {}
                    games = stats.get("games") or {}
                    if not player.get("pos"):
                        player["pos"] = games.get("position") or ""
                    if player.get("number") is None:
                        player["number"] = games.get("number")
                    entry = {"player": player}
                    if games.get("substitute") is True:
                        substitutes.append(entry)
                    else:
                        start_xi.append(entry)
                if start_xi or substitutes:
                    converted.append({
                        "team": team_block.get("team") or {},
                        "formation": "",
                        "coach": {},
                        "startXI": start_xi,
                        "substitutes": substitutes,
                    })

            errors = lineup_data.get("errors") or players_data.get("errors") or {}
            return _json_response(start_response, {
                "get": "fixtures/lineups",
                "errors": errors,
                "results": len(converted),
                "response": converted,
                "source": "players" if converted else "unavailable",
                "_quota": _quota_snapshot(),
            })

        if path == "/api-football/h2h":
            home = params.get("home", "").strip()
            away = params.get("away", "").strip()
            if not (home.isdigit() and away.isdigit()):
                return _json_response(start_response, {"ok": False, "error": "invalid team ids"}, 400)
            if API_FOOTBALL_DEMO or not API_FOOTBALL_KEY:
                return _json_response(start_response, {"get": "fixtures/headtohead", "errors": {}, "results": 0, "response": [], "demo": True, "_quota": _quota_snapshot()})
            return _json_response(start_response, _api_call("fixtures/headtohead", {"h2h": f"{home}-{away}", "last": max(1, min(20, int(params.get("last", "5") or 5))), "timezone": APP_TIMEZONE}, 12 * 3600, allow_demo=False))

        if path == "/api-football/predictions":
            fixture_id = params.get("fixture", "").strip()
            if not fixture_id.isdigit():
                return _json_response(start_response, {"ok": False, "error": "invalid fixture id"}, 400)
            if API_FOOTBALL_DEMO or not API_FOOTBALL_KEY:
                return _json_response(start_response, {"get": "predictions", "errors": {}, "results": 0, "response": [], "demo": True, "_quota": _quota_snapshot()})
            return _json_response(start_response, _api_call("predictions", {"fixture": fixture_id}, 3600, allow_demo=False))

        if path == "/api-football/prediction-history":
            return _json_response(start_response, {"response": _prediction_history_summaries(), "_quota": _quota_snapshot()})

        if path == "/api-football/prediction-feed":
            date_text = params.get("date", "").strip() or _prediction_today()
            if not __import__("re").fullmatch(r"\d{4}-\d{2}-\d{2}", date_text):
                return _json_response(start_response, {"ok": False, "error": "invalid date"}, 400)
            if API_FOOTBALL_DEMO or not API_FOOTBALL_KEY:
                return _json_response(start_response, {"date": date_text, "fixtures_total": 0, "finished_total": 0, "predicted_total": 0, "won_total": 0, "lost_total": 0, "hit_rate": 0, "stake_total": 0, "returned_total": 0, "profit_total": 0, "roi_percent": 0, "odds_total": 0, "response": [], "feed": [], "demo": True, "_quota": _quota_snapshot(), "_shared_cache": "demo"})
            force = params.get("refresh", "").strip().lower() in {"1", "true", "yes"}
            return _json_response(start_response, _prediction_feed_cached(date_text, force=force))

        if path == "/api-football/prediction-audit":
            date_text = params.get("date", "").strip()
            if not __import__("re").fullmatch(r"\d{4}-\d{2}-\d{2}", date_text):
                return _json_response(start_response, {"ok": False, "error": "invalid date"}, 400)
            if API_FOOTBALL_DEMO or not API_FOOTBALL_KEY:
                return _json_response(start_response, {"date": date_text, "fixtures_total": 0, "finished_total": 0, "predicted_total": 0, "won_total": 0, "lost_total": 0, "hit_rate": 0, "stake_total": 0, "returned_total": 0, "profit_total": 0, "roi_percent": 0, "odds_total": 0, "response": [], "demo": True, "_quota": _quota_snapshot(), "_shared_cache": "demo"})
            force = params.get("refresh", "").strip().lower() in {"1", "true", "yes"}
            data = _prediction_feed_cached(date_text, force=force)
            audit = dict(data)
            audit.pop("feed", None)
            return _json_response(start_response, audit)


        if path == "/api-football/trends":
            home = params.get("home", "").strip()
            away = params.get("away", "").strip()
            home_name = params.get("home_name", "Хозяева")[:80]
            away_name = params.get("away_name", "Гости")[:80]
            if not (home.isdigit() and away.isdigit()):
                return _json_response(start_response, {"ok": False, "error": "invalid team ids"}, 400)
            return _json_response(start_response, _build_trends(home, away, home_name, away_name))

        if path == "/api-football/odds":
            fixture_id = params.get("fixture", "").strip()
            if not fixture_id.isdigit():
                return _json_response(start_response, {"ok": False, "error": "invalid fixture id"}, 400)
            if API_FOOTBALL_DEMO or not API_FOOTBALL_KEY:
                return _json_response(start_response, {"get": "odds", "errors": {}, "results": 0, "response": [], "demo": True, "_quota": _quota_snapshot()})
            return _json_response(start_response, _api_call("odds", {"fixture": fixture_id}, 1800, allow_demo=False))

        return _json_response(start_response, {"ok": False, "error": "unknown API-Football endpoint"}, 404)
    except Exception as exc:
        traceback.print_exc()
        return _json_response(start_response, {"ok": False, "error": f"{type(exc).__name__}: {exc}", "quota": _quota_snapshot()}, 502)


def _delegate_notify(environ: dict[str, Any], start_response: Callable[..., Any], rewritten_path: str | None = None) -> list[bytes]:
    delegated = dict(environ)
    if rewritten_path is not None:
        delegated["PATH_INFO"] = rewritten_path
    return notify_app.application(delegated, start_response)


def _scores24_wsgi(environ: dict[str, Any], start_response: Callable[..., Any]) -> list[bytes]:
    path = str(environ.get("PATH_INFO") or "")
    params = _query(environ)
    try:
        if path == "/scores24/health":
            return _json_response(start_response, {"ok": True, **scores24_predictions.health()})
        if path == "/scores24/prediction-history":
            return _json_response(start_response, {"response": scores24_predictions.history(), "source": "Scores24"})
        if path == "/scores24/prediction-summary":
            today = __import__("datetime").datetime.now(scores24_predictions._tz()).date()
            default_from = today.replace(day=1).isoformat()
            default_to = (today - __import__("datetime").timedelta(days=1)).isoformat()
            date_from = params.get("from", "").strip() or default_from
            date_to = params.get("to", "").strip() or default_to
            if not (__import__("re").fullmatch(r"\d{4}-\d{2}-\d{2}", date_from) and __import__("re").fullmatch(r"\d{4}-\d{2}-\d{2}", date_to)):
                return _json_response(start_response, {"ok": False, "error": "invalid date range"}, 400)
            return _json_response(start_response, scores24_predictions.period_summary(date_from, date_to))
        if path == "/scores24/prediction-feed":
            date_text = params.get("date", "").strip() or scores24_predictions._today()
            if not __import__("re").fullmatch(r"\d{4}-\d{2}-\d{2}", date_text):
                return _json_response(start_response, {"ok": False, "error": "invalid date"}, 400)
            force = params.get("refresh", "").strip().lower() in {"1", "true", "yes"}
            return _json_response(start_response, scores24_predictions.feed(date_text, force=force))
        return _json_response(start_response, {"ok": False, "error": "unknown Scores24 endpoint"}, 404)
    except ValueError as exc:
        return _json_response(start_response, {"ok": False, "error": str(exc)}, 400)
    except Exception as exc:
        traceback.print_exc()
        return _json_response(start_response, {"ok": False, "error": f"{type(exc).__name__}: {exc}"}, 502)


def application(environ: dict[str, Any], start_response: Callable[..., Any]) -> list[bytes]:
    method = str(environ.get("REQUEST_METHOD") or "GET").upper()
    path = str(environ.get("PATH_INFO") or "/")

    if method not in {"GET", "HEAD", "POST"}:
        return _json_response(start_response, {"ok": False, "error": "method not allowed"}, 405)

    if path in {"/health", "/healthz", "/ready", "/readyz"}:
        return _json_response(start_response, {
            "ok": True,
            "service": "telegram_live_miniapp Combined",
            "api_football_configured": bool(API_FOOTBALL_KEY),
            "api_football_demo": API_FOOTBALL_DEMO or not bool(API_FOOTBALL_KEY),
            "scores24_predictions": scores24_predictions.health(),
            "notification_bot": "frozen-loaded",
            "quota": _quota_snapshot(),
        })

    if path.startswith("/scores24/"):
        return _scores24_wsgi(environ, start_response)

    if path.startswith("/api-football/"):
        return _api_football_wsgi(environ, start_response)

    # Frozen notification bot routes. Its API and assets remain untouched.
    if path.startswith("/api/") or path.startswith("/team-logos/") or path == "/styles.css":
        return _delegate_notify(environ, start_response)
    if path in {"/notify", "/notify/", "/notify/index.html"}:
        return _delegate_notify(environ, start_response, "/")

    if path in {"/", "/index.html"}:
        return _file_response(start_response, MATCHES_DIR / "index.html")
    if path.startswith("/matches-static/"):
        rel = Path(path[len("/matches-static/"):])
        if ".." in rel.parts:
            return _json_response(start_response, {"ok": False, "error": "forbidden"}, 403)
        return _file_response(start_response, MATCHES_DIR / rel)

    return _json_response(start_response, {"ok": False, "error": "not found"}, 404)


def _install_api_quota_admin_extension() -> None:
    """Append API-Football quota to the existing private Telegram admin stats.

    The frozen notification source files remain unchanged; only the loaded module
    function is wrapped at runtime by the combined application.
    """
    n = notify_app
    original = getattr(n, "_admin_stats_text", None)
    if not callable(original) or getattr(n, "_combined_api_quota_installed", False):
        return

    def _fmt_count(value: Any) -> str:
        try:
            return f"{int(value):,}".replace(",", " " )
        except Exception:
            return "—"

    def _admin_stats_with_api_quota() -> str:
        base = original()
        q = _quota_snapshot()
        limit = q.get("limit")
        remaining = q.get("remaining")
        updated_at = q.get("updated_at")
        if limit is None or remaining is None:
            api_block = (
                "\n\n<b>API-Football</b>\n"
                "Лимит ещё не получен. Он появится после первого успешного запроса к API."
            )
        else:
            used = max(0, int(limit) - int(remaining))
            if updated_at:
                try:
                    age_seconds = max(0, int(time.time()) - int(updated_at))
                    if age_seconds < 60:
                        age_text = "только что"
                    elif age_seconds < 3600:
                        age_text = f"{age_seconds // 60} мин. назад"
                    else:
                        age_text = f"{age_seconds // 3600} ч. назад"
                except Exception:
                    age_text = "—"
            else:
                age_text = "—"
            api_block = (
                "\n\n<b>API-Football</b>\n"
                f"Использовано сегодня: <b>{_fmt_count(used)}</b> / <b>{_fmt_count(limit)}</b>\n"
                f"Осталось: <b>{_fmt_count(remaining)}</b>\n"
                f"Обновлено: <b>{age_text}</b>"
            )
        return base + api_block

    n._admin_stats_text = _admin_stats_with_api_quota
    n._combined_api_quota_installed = True


def start_notification_runtime() -> None:
    global _runtime_started
    with _runtime_lock:
        if _runtime_started:
            return
        _runtime_started = True
    n = notify_app
    n.DATA_DIR.mkdir(exist_ok=True)
    n.TEAM_LOGO_DIR.mkdir(exist_ok=True)
    n.LAST_GOOD_MATCH_DIR.mkdir(parents=True, exist_ok=True)
    n._init_logo_db()
    n._init_notify_storage()
    n._init_admin_storage()
    n._load_admin_state()
    _install_api_quota_admin_extension()
    n.start_collector_worker()
    n._load_notify_subs()
    n._load_notify_matches()
    n.cleanup_runtime_caches(force=True)
    n.start_telegram_send_workers()
    n.start_notify_worker()
    n.start_admin_panel_worker()
    start_goal_notify_worker()
    scores24_predictions.start_worker()
    print("[combined] frozen notification runtime + Scores24 predictions started")


def main() -> None:
    start_notification_runtime()
    print("=" * 72)
    print("telegram_live_miniapp Combined — Scores24 Predictions + Frozen Notifications")
    print(f"Local:      http://127.0.0.1:{PORT}")
    print(f"Matches:    / (API-Football {'configured' if API_FOOTBALL_KEY else 'DEMO — set API_FOOTBALL_KEY'})")
    print("Notify:     /notify/ (frozen V10_03 source)")
    print(f"Cache:      {'Redis + memory' if _redis_client is not None else 'memory'}")
    print(f"Rate limit: {API_FOOTBALL_MAX_RPS:.0f} req/s max to upstream")
    print("=" * 72)

    def _on_sigterm(signum: int, _frame: Any) -> None:
        print(f"[combined] signal {signum} received")
        raise KeyboardInterrupt

    try:
        signal.signal(signal.SIGTERM, _on_sigterm)
    except Exception:
        pass

    try:
        from waitress import serve
        runner = lambda: serve(application, host=HOST, port=PORT, threads=SERVER_THREADS, channel_timeout=SERVER_CHANNEL_TIMEOUT, clear_untrusted_proxy_headers=True)
        print("[combined] HTTP server: Waitress")
    except Exception:
        from wsgiref.simple_server import make_server
        runner = lambda: make_server(HOST, PORT, application).serve_forever()
        print("[combined] HTTP server: wsgiref fallback (install requirements for production)")
    try:
        runner()
    except KeyboardInterrupt:
        print("[combined] stopped")


if __name__ == "__main__":
    main()
