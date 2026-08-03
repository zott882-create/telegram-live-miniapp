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
from concurrent.futures import ThreadPoolExecutor
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
GOAL_NOTIFY_INTERVAL_SECONDS = max(20, int(os.environ.get("GOAL_NOTIFY_INTERVAL_SECONDS", "45")))

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
_goal_notify_lock = threading.RLock()
_goal_notify_subs: dict[str, dict[str, Any]] = {}
_goal_notify_started = False
_goal_notify_file = Path(getattr(notify_app, "DATA_DIR", BASE_DIR / "data")) / "api_football_goal_subscriptions.json"
_goal_notify_send_pool = ThreadPoolExecutor(max_workers=4, thread_name_prefix="api-goal-send")


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
        ("Cache-Control", "no-cache" if path.suffix == ".html" else "public, max-age=3600"),
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

def _api_football_wsgi(environ: dict[str, Any], start_response: Callable[..., Any]) -> list[bytes]:
    path = str(environ.get("PATH_INFO") or "")
    params = _query(environ)
    try:
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
                    if items:
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
                "errors": player_data.get("errors") or match_errors or all_errors or {},
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
