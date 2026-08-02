#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""telegram_live_miniapp combined Telegram Mini App.

- /                    -> API-Football match centre
- /notify/             -> frozen notification bot UI
- /api-football/*      -> API-Football proxy with Redis/memory cache
- /api/*                -> frozen notification bot API

The notification bot source is imported from notification_bot/ and is not
modified by this wrapper.
"""
from __future__ import annotations

import collections
import hashlib
import importlib.util
import io
import json
import mimetypes
import os
import signal
import threading
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from email.message import Message
from http import HTTPStatus
from pathlib import Path
from typing import Any, Callable

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
APP_TIMEZONE = os.environ.get("APP_TIMEZONE", "Europe/Moscow").strip() or "Europe/Moscow"
REDIS_URL = (
    os.environ.get("REDIS_URL")
    or os.environ.get("REDIS_INTERNAL_URL")
    or os.environ.get("REDIS_PUBLIC_URL")
    or ""
).strip()
CACHE_PREFIX = os.environ.get("API_FOOTBALL_CACHE_PREFIX", "telegram_live_miniapp:football:v1").strip().strip(":")

# Load the frozen notification bot as a module without changing its files.
_spec = importlib.util.spec_from_file_location("telegram_live_miniapp_frozen_notify", FROZEN_APP_PATH)
if _spec is None or _spec.loader is None:
    raise RuntimeError(f"Cannot import frozen notification bot: {FROZEN_APP_PATH}")
notify_app = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(notify_app)

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


def _api_call(endpoint: str, params: dict[str, Any], ttl: int, *, allow_demo: bool = True) -> dict[str, Any]:
    key = _cache_key(endpoint, params)
    cached = _cache_get(key)
    if cached is not None:
        result = dict(cached)
        result["_cache"] = "hit"
        result["_quota"] = _quota_snapshot()
        return result

    with _lock_for(key):
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
        query = urllib.parse.urlencode({k: v for k, v in params.items() if v not in (None, "")})
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


def _last_team_fixtures(team_id: str, last: int = 20) -> list[dict[str, Any]]:
    data = _api_call("fixtures", {"team": team_id, "last": max(5, min(30, last)), "timezone": APP_TIMEZONE}, 6 * 3600)
    return data.get("response") or []


def _team_trend_stats(fixtures: list[dict[str, Any]], team_id: str) -> dict[str, int]:
    out = {"played": 0, "unbeaten": 0, "scored": 0, "conceded": 0, "btts": 0, "wins": 0}
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
        out["played"] += 1
        out["unbeaten"] += int(gf >= ga)
        out["wins"] += int(gf > ga)
        out["scored"] += int(gf > 0)
        out["conceded"] += int(ga > 0)
        out["btts"] += int(gf > 0 and ga > 0)
    return out


def _build_trends(home_id: str, away_id: str, home_name: str, away_name: str) -> dict[str, Any]:
    if API_FOOTBALL_DEMO or not API_FOOTBALL_KEY:
        home = {"played": 20, "unbeaten": 19, "scored": 18, "conceded": 11, "btts": 10, "wins": 15}
        away = {"played": 20, "unbeaten": 14, "scored": 17, "conceded": 16, "btts": 13, "wins": 9}
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
        ("Cache-Control", "no-cache" if path.suffix == ".html" else "public, max-age=3600"),
        ("X-Content-Type-Options", "nosniff"),
    ])
    return [body]


def _query(environ: dict[str, Any]) -> dict[str, str]:
    parsed = urllib.parse.parse_qs(str(environ.get("QUERY_STRING") or ""), keep_blank_values=False)
    return {key: str(values[0]) for key, values in parsed.items() if values}


def _api_football_wsgi(environ: dict[str, Any], start_response: Callable[..., Any]) -> list[bytes]:
    path = str(environ.get("PATH_INFO") or "")
    params = _query(environ)
    try:
        if path == "/api-football/status":
            if API_FOOTBALL_DEMO or not API_FOOTBALL_KEY:
                return _json_response(start_response, {"ok": bool(API_FOOTBALL_DEMO), "configured": bool(API_FOOTBALL_KEY), "demo": True, "timezone": APP_TIMEZONE, "quota": _quota_snapshot()})
            data = _api_call("status", {}, 60, allow_demo=False)
            return _json_response(start_response, {"ok": not bool(data.get("errors")), "configured": True, "demo": False, "timezone": APP_TIMEZONE, "quota": data.get("_quota"), "response": data.get("response"), "errors": data.get("errors")})

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
            if not fixture_id.isdigit():
                return _json_response(start_response, {"ok": False, "error": "invalid fixture id"}, 400)
            if API_FOOTBALL_DEMO or not API_FOOTBALL_KEY:
                return _json_response(start_response, {"get": "fixtures/statistics", "errors": {}, "results": 2, "response": _demo_stats(), "demo": True, "_quota": _quota_snapshot()})
            return _json_response(start_response, _api_call("fixtures/statistics", {"fixture": fixture_id}, 90, allow_demo=False))

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
            if not fixture_id.isdigit():
                return _json_response(start_response, {"ok": False, "error": "invalid fixture id"}, 400)
            if API_FOOTBALL_DEMO or not API_FOOTBALL_KEY:
                names1 = ["Raya", "Saliba", "Gabriel", "White", "Rice", "Ødegaard", "Partey", "Saka", "Martinelli", "Trossard", "Havertz"]
                names2 = ["Sánchez", "James", "Colwill", "Fofana", "Cucurella", "Caicedo", "Enzo", "Palmer", "Madueke", "Nkunku", "Jackson"]
                lineups = [
                    {"team": {"id": 42, "name": "Arsenal", "logo": ""}, "formation": "4-3-3", "startXI": [{"player": {"id": i + 1, "name": name, "number": i + 1, "pos": "G" if i == 0 else "M"}} for i, name in enumerate(names1)], "substitutes": []},
                    {"team": {"id": 49, "name": "Chelsea", "logo": ""}, "formation": "4-2-3-1", "startXI": [{"player": {"id": i + 20, "name": name, "number": i + 1, "pos": "G" if i == 0 else "M"}} for i, name in enumerate(names2)], "substitutes": []},
                ]
                return _json_response(start_response, {"get": "fixtures/lineups", "errors": {}, "results": 2, "response": lineups, "demo": True, "_quota": _quota_snapshot()})
            return _json_response(start_response, _api_call("fixtures/lineups", {"fixture": fixture_id}, 1800, allow_demo=False))

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
            "notification_bot": "frozen-loaded",
            "quota": _quota_snapshot(),
        })

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
    n.start_collector_worker()
    n._load_notify_subs()
    n._load_notify_matches()
    n.cleanup_runtime_caches(force=True)
    n.start_telegram_send_workers()
    n.start_notify_worker()
    n.start_admin_panel_worker()
    print("[combined] frozen notification runtime started")


def main() -> None:
    start_notification_runtime()
    print("=" * 72)
    print("telegram_live_miniapp Combined V1 — API-Football Matches + Frozen Notifications")
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
