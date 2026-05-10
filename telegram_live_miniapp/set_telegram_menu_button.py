#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Telegram Live Matches Mini App
Standalone Python server + static frontend.

Run:
    python app.py

Environment:
    HOST=0.0.0.0
    PORT=8080
    DATA_MODE=auto          # auto | igscore | sqlite | demo
    LIVE_DB_PATH=...        # optional path to live_history_v1.sqlite3 from the old bot
    PUBLIC_BASE_URL=...     # optional, used only in docs/logs
"""

from __future__ import annotations

import datetime as _dt
import gzip
import hashlib
import json
import mimetypes
import os
import re
import sqlite3
import threading
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
DATA_DIR = BASE_DIR / "data"

HOST = os.environ.get("HOST", "0.0.0.0")
PORT = int(os.environ.get("PORT", "8080"))
DATA_MODE = os.environ.get("DATA_MODE", "auto").strip().lower()

API_BASE = os.environ.get("IGSCORE_API_BASE", "https://api.igscore.net:8080").rstrip("/")
WEB_ORIGIN = "https://www.igscore.net"
TIME_ZONE = os.environ.get("IGSCORE_TIME_ZONE", "+05:00")
LANG = os.environ.get("IGSCORE_LANG", "en")

LIVE_CACHE_SECONDS = float(os.environ.get("LIVE_CACHE_SECONDS", "7"))
STAT_CACHE_SECONDS = float(os.environ.get("STAT_CACHE_SECONDS", "15"))
SQLITE_RECENT_MINUTES = int(os.environ.get("SQLITE_RECENT_MINUTES", "240"))

LIVE_STATUSES = {2, 3, 4}
FINISHED_STATUSES = {8, 9, 10, 11, 12, 13}

DEFAULT_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "ru,en;q=0.9,en-US;q=0.8",
    "Content-Type": "application/json",
    "device_type": "web",
    "Origin": WEB_ORIGIN,
    "Referer": WEB_ORIGIN + "/",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/147.0.0.0 Safari/537.36"
    ),
}

STAT_TYPE_NAMES = {
    25: "possession",
    2: "corners",
    3: "yellow_cards",
    4: "red_cards",
    21: "shots_on_target",
    22: "shots_off_target",
    23: "attacks",
    24: "dangerous_attacks",
}

RUS_COUNTRY_TO_CODE = {
    "англия": "GB",
    "испания": "ES",
    "италия": "IT",
    "германия": "DE",
    "франция": "FR",
    "нидерланды": "NL",
    "португалия": "PT",
    "турция": "TR",
    "бразилия": "BR",
    "аргентина": "AR",
    "сша": "US",
}

COUNTRY_CODE_MAP = {
    "england": "GB",
    "scotland": "GB",
    "wales": "GB",
    "northern ireland": "GB",
    "spain": "ES",
    "italy": "IT",
    "germany": "DE",
    "france": "FR",
    "netherlands": "NL",
    "portugal": "PT",
    "turkey": "TR",
    "brazil": "BR",
    "argentina": "AR",
    "usa": "US",
    "united states": "US",
    "russia": "RU",
}

_cache_lock = threading.Lock()
_live_cache: dict[str, Any] = {"saved_at": 0.0, "payload": None, "raw": {}}
_stats_cache: dict[str, dict[str, Any]] = {}


def now_ts() -> int:
    return int(time.time())


def json_response(handler: BaseHTTPRequestHandler, payload: Any, status: int = 200) -> None:
    raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Cache-Control", "no-store")
    handler.send_header("Content-Length", str(len(raw)))
    handler.end_headers()
    handler.wfile.write(raw)


def text_response(handler: BaseHTTPRequestHandler, text: str, status: int = 200) -> None:
    raw = text.encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "text/plain; charset=utf-8")
    handler.send_header("Content-Length", str(len(raw)))
    handler.end_headers()
    handler.wfile.write(raw)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(str(value).strip()))
    except Exception:
        return int(default)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(str(value).strip())
    except Exception:
        return float(default)


def _base_payload() -> dict[str, Any]:
    return {
        "lang": LANG,
        "timeZone": TIME_ZONE,
        "platform": "web",
        "agentType": None,
        "appVersion": None,
        "sign": None,
    }


def date_filter_now() -> str:
    now = _dt.datetime.now()
    return now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{int(now.microsecond / 1000):03d}" + TIME_ZONE


def post_json(path: str, payload: dict[str, Any], timeout: float = 15.0) -> dict[str, Any]:
    url = API_BASE + path
    raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    req = urllib.request.Request(url, data=raw, method="POST", headers=DEFAULT_HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = resp.read()
            enc = (resp.headers.get("Content-Encoding") or "").lower()
            if enc == "gzip":
                data = gzip.decompress(data)
            text = data.decode("utf-8", "replace")
    except urllib.error.HTTPError as exc:
        body = ""
        try:
            body = exc.read().decode("utf-8", "replace")[:400]
        except Exception:
            pass
        raise RuntimeError(f"IGScore HTTP {exc.code} {path}: {body}") from exc
    except Exception as exc:
        raise RuntimeError(f"IGScore request failed {path}: {type(exc).__name__}: {exc}") from exc

    try:
        parsed = json.loads(text)
    except Exception as exc:
        raise RuntimeError(f"IGScore invalid JSON {path}: {text[:300]}") from exc

    if isinstance(parsed, dict) and parsed.get("code") not in (None, "A00000"):
        raise RuntimeError(f"IGScore API error {path}: {parsed.get('code')} {parsed.get('message')}")
    return parsed if isinstance(parsed, dict) else {"result": parsed}


def igscore_match_url(match_id: Any) -> str:
    mid = str(match_id or "").strip()
    return f"https://www.igscore.net/football-match/{mid}/MatchLive/" if mid else ""


def competition_list() -> dict[str, Any]:
    payload = {
        "listType": 0,
        "dateFilter": date_filter_now(),
        "skipOdds": True,
        **_base_payload(),
    }
    return post_json("/v1/football/competition/list", payload, timeout=15.0)


def match_statistics(match_id: str) -> dict[str, Any]:
    return post_json("/v1/football/match/statistics", {"matchId": str(match_id), **_base_payload()}, timeout=15.0)


def match_info(match_id: str) -> dict[str, Any]:
    return post_json("/v1/football/match/info", {"matchId": str(match_id), **_base_payload()}, timeout=15.0)


def team_recent(team_id: str, size: int = 10) -> dict[str, Any]:
    return post_json("/v1/football/match/analysis/recent", {"teamId": str(team_id), "size": int(size), **_base_payload()}, timeout=15.0)


def iter_competition_matches(response: dict[str, Any]) -> list[dict[str, Any]]:
    result = response.get("result") if isinstance(response, dict) else None
    comps = (result or {}).get("competitions") if isinstance(result, dict) else []
    out: list[dict[str, Any]] = []
    for comp in comps or []:
        if not isinstance(comp, dict):
            continue
        for match in comp.get("matches") or []:
            if not isinstance(match, dict):
                continue
            item = dict(match)
            item.setdefault("competition", {
                "id": comp.get("competitionId"),
                "name": comp.get("competitionName"),
                "logo": comp.get("logo"),
                "category": comp.get("category"),
                "additionalCompetitionName": comp.get("additionalCompetitionName"),
            })
            item.setdefault("competitionName", comp.get("competitionName"))
            item.setdefault("additionalCompetitionName", comp.get("additionalCompetitionName"))
            item.setdefault("category", comp.get("category"))
            out.append(item)
    return out


def iter_result_matches(response: dict[str, Any]) -> list[dict[str, Any]]:
    result = response.get("result") if isinstance(response, dict) else None
    matches = (result or {}).get("matches") if isinstance(result, dict) else []
    return [m for m in (matches or []) if isinstance(m, dict)]


def team_name(team: Any) -> str:
    if isinstance(team, dict):
        return str(team.get("name") or team.get("shortName") or "").strip()
    return ""


def team_id(team: Any) -> str:
    if isinstance(team, dict):
        return str(team.get("id") or team.get("teamId") or "").strip()
    return ""


def score_from_match(match: dict[str, Any]) -> tuple[int, int, str]:
    ch = str(match.get("calculatedHomeScore") or "").strip()
    ca = str(match.get("calculatedAwayScore") or "").strip()
    if ch != "" and ca != "":
        h, a = _safe_int(ch), _safe_int(ca)
        return h, a, f"{h}-{a}"

    home_scores = match.get("homeScores") if isinstance(match.get("homeScores"), list) else []
    away_scores = match.get("awayScores") if isinstance(match.get("awayScores"), list) else []
    if home_scores and away_scores:
        h, a = _safe_int(home_scores[0]), _safe_int(away_scores[0])
        return h, a, f"{h}-{a}"

    score = str(match.get("score") or "").strip()
    m = re.search(r"(\d+)\s*[-:]\s*(\d+)", score)
    if m:
        h, a = int(m.group(1)), int(m.group(2))
        return h, a, f"{h}-{a}"

    return 0, 0, "0-0"


def minute_from_match(match: dict[str, Any], server_time: int | None = None) -> tuple[int, str, str]:
    now = int(server_time or match.get("server_time") or time.time())
    status = _safe_int(match.get("matchStatus") or match.get("statusId"), 0)

    if status == 3:
        return 45, "HT", "HT"

    second = _safe_int(match.get("secondHalfKickOffTime"), 0)
    first = _safe_int(match.get("firstHalfKickOffTime"), 0)

    if second > 0:
        minute = 45 + max(0, (now - second) // 60)
        return int(min(max(minute, 46), 130)), f"{int(minute)}’", "2T"

    if first > 0:
        minute = max(1, (now - first) // 60)
        return int(min(max(minute, 1), 45)), f"{int(minute)}’", "1T"

    raw = str(match.get("minute_raw") or match.get("minute_source") or "").strip()
    if raw:
        mv = _safe_int(match.get("minute_value") or match.get("minute"), 0)
        return mv, raw, "2T" if mv > 45 else "1T"
    return _safe_int(match.get("minute") or match.get("minute_value"), 0), "LIVE", "LIVE"


def country_from_match(match: dict[str, Any]) -> str:
    cat = match.get("category") or (match.get("competition") or {}).get("category") or {}
    if isinstance(cat, dict):
        name = str(cat.get("name") or "").strip()
        if name:
            return name
    return str(match.get("country") or "Без страны").strip() or "Без страны"


def league_from_match(match: dict[str, Any]) -> str:
    return str(
        match.get("competitionName")
        or (match.get("competition") or {}).get("name")
        or match.get("league")
        or match.get("tournament")
        or "Без лиги"
    ).strip() or "Без лиги"


def country_code(country: str) -> str:
    text = str(country or "").strip()
    if len(text) == 2 and text.isalpha():
        return text.upper()
    low = text.lower()
    return RUS_COUNTRY_TO_CODE.get(low) or COUNTRY_CODE_MAP.get(low, "")


def public_match_id(raw_id: Any, link: str, home: str, away: str) -> str:
    mid = str(raw_id or "").strip()
    if mid:
        return mid
    m = re.search(r"football-match/([^/]+)/", str(link or ""))
    if m:
        return m.group(1)
    seed = f"{link}|{home}|{away}".encode("utf-8", "replace")
    return hashlib.sha1(seed).hexdigest()[:12]


def to_public_match(match: dict[str, Any], server_time: int | None = None, source: str = "igscore") -> dict[str, Any]:
    home = team_name(match.get("homeTeam")) or str(match.get("team1") or match.get("team1_ru") or "Home").strip()
    away = team_name(match.get("awayTeam")) or str(match.get("team2") or match.get("team2_ru") or "Away").strip()
    link = str(match.get("link") or igscore_match_url(match.get("matchId") or match.get("id"))).strip()
    mid = public_match_id(match.get("matchId") or match.get("id"), link, home, away)
    sh, sa, score = score_from_match(match)
    minute_value, minute_text, period = minute_from_match(match, server_time)
    country = country_from_match(match)
    league = league_from_match(match)
    return {
        "id": mid,
        "home": home,
        "away": away,
        "score_home": sh,
        "score_away": sa,
        "score": score,
        "minute": minute_value,
        "minute_text": minute_text,
        "period": period,
        "country": country,
        "country_code": country_code(country),
        "league": league,
        "link": link,
        "source": source,
    }


def group_matches(matches: list[dict[str, Any]]) -> list[dict[str, Any]]:
    countries: dict[str, dict[str, Any]] = {}
    for item in matches:
        ckey = item.get("country") or "Без страны"
        lkey = item.get("league") or "Без лиги"
        c = countries.setdefault(ckey, {
            "country": ckey,
            "country_code": item.get("country_code") or "",
            "match_count": 0,
            "leagues": {},
        })
        l = c["leagues"].setdefault(lkey, {"league": lkey, "match_count": 0, "matches": []})
        l["matches"].append(item)
        l["match_count"] += 1
        c["match_count"] += 1

    out = []
    for c in countries.values():
        leagues = list(c["leagues"].values())
        leagues.sort(key=lambda x: (-int(x.get("match_count") or 0), str(x.get("league") or "")))
        out.append({
            "country": c["country"],
            "country_code": c["country_code"],
            "match_count": c["match_count"],
            "leagues": leagues,
        })
    out.sort(key=lambda x: (-int(x.get("match_count") or 0), str(x.get("country") or "")))
    return out


def build_payload(matches: list[dict[str, Any]], source: str, error: str | None = None) -> dict[str, Any]:
    return {
        "ok": True,
        "source": source,
        "updated_at": now_ts(),
        "total": len(matches),
        "countries": group_matches(matches),
        "error": error or "",
    }


def fetch_live_igscore() -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    response = competition_list()
    server_time = int(response.get("server_time") or time.time())
    raw_matches: dict[str, dict[str, Any]] = {}
    public: list[dict[str, Any]] = []

    for match in iter_competition_matches(response):
        status = _safe_int(match.get("matchStatus") or match.get("statusId"), 0)
        if status not in LIVE_STATUSES:
            continue
        match["server_time"] = server_time
        item = to_public_match(match, server_time=server_time, source="igscore")
        public.append(item)
        raw_matches[item["id"]] = match

    public.sort(key=lambda x: (str(x.get("country") or ""), str(x.get("league") or ""), -int(x.get("minute") or 0)))
    return public, raw_matches


def sqlite_db_path() -> Path:
    env = os.environ.get("LIVE_DB_PATH", "").strip()
    if env:
        return Path(env).expanduser()
    local = DATA_DIR / "live_history_v1.sqlite3"
    if local.exists():
        return local
    return BASE_DIR / "live_history_v1.sqlite3"


def row_to_match(row: sqlite3.Row) -> dict[str, Any]:
    link = str(row["link"] or "")
    score = str(row["score"] or "0-0")
    m = re.search(r"(\d+)\s*[-:]\s*(\d+)", score)
    sh, sa = (int(m.group(1)), int(m.group(2))) if m else (0, 0)
    home = str(row["team1_ru"] or row["team1"] or "Home")
    away = str(row["team2_ru"] or row["team2"] or "Away")
    minute = _safe_int(row["minute_value"], 0)
    minute_raw = str(row["minute_raw"] or (f"{minute}’" if minute else "LIVE"))
    country = str(row["country"] or "Без страны")
    league = str(row["league"] or row["tournament"] or "Без лиги")
    mid = public_match_id("", link, home, away)
    return {
        "id": mid,
        "home": home,
        "away": away,
        "score_home": sh,
        "score_away": sa,
        "score": f"{sh}-{sa}",
        "minute": minute,
        "minute_text": minute_raw,
        "period": "2T" if minute > 45 else "1T",
        "country": country,
        "country_code": country_code(country),
        "league": league,
        "link": link,
        "source": "sqlite",
    }


def fetch_live_sqlite() -> list[dict[str, Any]]:
    path = sqlite_db_path()
    if not path.exists():
        raise FileNotFoundError(f"SQLite DB not found: {path}")
    cutoff = now_ts() - SQLITE_RECENT_MINUTES * 60
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.execute(
            """
            SELECT * FROM snapshots
            WHERE collected_at >= ?
            ORDER BY collected_at DESC
            LIMIT 2000
            """,
            (cutoff,),
        )
        seen: set[str] = set()
        out: list[dict[str, Any]] = []
        for row in cur.fetchall():
            key = str(row["link"] or f"{row['team1']}|{row['team2']}")
            if key in seen:
                continue
            seen.add(key)
            out.append(row_to_match(row))
        return out
    finally:
        conn.close()


DEMO_MATCHES = [
    {
        "id": "demo-liv-ars",
        "home": "Liverpool",
        "away": "Arsenal",
        "score_home": 2,
        "score_away": 1,
        "score": "2-1",
        "minute": 67,
        "minute_text": "67’",
        "period": "2T",
        "country": "Англия",
        "country_code": "GB",
        "league": "Премьер-лига",
        "link": "https://www.igscore.net/",
        "source": "demo",
    },
    {
        "id": "demo-mci-che",
        "home": "Man City",
        "away": "Chelsea",
        "score_home": 0,
        "score_away": 0,
        "score": "0-0",
        "minute": 38,
        "minute_text": "38’",
        "period": "1T",
        "country": "Англия",
        "country_code": "GB",
        "league": "Премьер-лига",
        "link": "https://www.igscore.net/",
        "source": "demo",
    },
    {
        "id": "demo-real-barca",
        "home": "Real Madrid",
        "away": "Barcelona",
        "score_home": 3,
        "score_away": 2,
        "score": "3-2",
        "minute": 71,
        "minute_text": "71’",
        "period": "2T",
        "country": "Испания",
        "country_code": "ES",
        "league": "Ла Лига",
        "link": "https://www.igscore.net/",
        "source": "demo",
    },
]


def load_live_payload(force: bool = False) -> dict[str, Any]:
    now = time.time()
    with _cache_lock:
        if not force and _live_cache.get("payload") and now - float(_live_cache.get("saved_at") or 0) < LIVE_CACHE_SECONDS:
            return _live_cache["payload"]

    mode = DATA_MODE
    errors: list[str] = []
    raw: dict[str, dict[str, Any]] = {}

    if mode in {"auto", "igscore"}:
        try:
            matches, raw = fetch_live_igscore()
            payload = build_payload(matches, "igscore")
            with _cache_lock:
                _live_cache.update({"saved_at": now, "payload": payload, "raw": raw})
            return payload
        except Exception as exc:
            errors.append(f"igscore: {type(exc).__name__}: {exc}")
            if mode == "igscore":
                payload = build_payload(DEMO_MATCHES, "demo", "; ".join(errors))
                with _cache_lock:
                    _live_cache.update({"saved_at": now, "payload": payload, "raw": {}})
                return payload

    if mode in {"auto", "sqlite"}:
        try:
            matches = fetch_live_sqlite()
            payload = build_payload(matches, "sqlite")
            with _cache_lock:
                _live_cache.update({"saved_at": now, "payload": payload, "raw": {}})
            return payload
        except Exception as exc:
            errors.append(f"sqlite: {type(exc).__name__}: {exc}")
            if mode == "sqlite":
                payload = build_payload(DEMO_MATCHES, "demo", "; ".join(errors))
                with _cache_lock:
                    _live_cache.update({"saved_at": now, "payload": payload, "raw": {}})
                return payload

    payload = build_payload(DEMO_MATCHES, "demo", "; ".join(errors))
    with _cache_lock:
        _live_cache.update({"saved_at": now, "payload": payload, "raw": {}})
    return payload


def flatten_payload(payload: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for country in payload.get("countries") or []:
        for league in country.get("leagues") or []:
            for match in league.get("matches") or []:
                if isinstance(match, dict):
                    out.append(match)
    return out


def stat_pairs_from_response(resp: dict[str, Any]) -> dict[str, dict[str, int]]:
    result = resp.get("result") if isinstance(resp, dict) else None
    raw_stats = (result or {}).get("statistics") if isinstance(result, dict) else []
    by_type: dict[int, tuple[int, int]] = {}
    for row in raw_stats or []:
        if not isinstance(row, dict):
            continue
        t = _safe_int(row.get("type"), -1)
        by_type[t] = (_safe_int(row.get("home"), 0), _safe_int(row.get("away"), 0))

    shots_home = by_type.get(21, (0, 0))[0] + by_type.get(22, (0, 0))[0]
    shots_away = by_type.get(21, (0, 0))[1] + by_type.get(22, (0, 0))[1]

    return {
        "possession": {"home": by_type.get(25, (50, 50))[0], "away": by_type.get(25, (50, 50))[1]},
        "shots": {"home": shots_home, "away": shots_away},
        "on_target": {"home": by_type.get(21, (0, 0))[0], "away": by_type.get(21, (0, 0))[1]},
        "dangerous": {"home": by_type.get(24, (0, 0))[0], "away": by_type.get(24, (0, 0))[1]},
        "corners": {"home": by_type.get(2, (0, 0))[0], "away": by_type.get(2, (0, 0))[1]},
    }


def avg_total_for_team(team_id_value: str) -> dict[str, Any]:
    if not team_id_value:
        return {"avg": None, "count": 0, "zero_zero": 0}
    try:
        resp = team_recent(team_id_value, size=10)
        totals: list[int] = []
        zero_zero = 0
        for match in iter_result_matches(resp):
            h, a, score = score_from_match(match)
            totals.append(h + a)
            if h == 0 and a == 0:
                zero_zero += 1
        avg = round(sum(totals) / len(totals), 2) if totals else None
        return {"avg": avg, "count": len(totals), "zero_zero": zero_zero}
    except Exception:
        return {"avg": None, "count": 0, "zero_zero": 0}


def sqlite_detail_for_match(match_id_value: str) -> dict[str, Any] | None:
    path = sqlite_db_path()
    if not path.exists():
        return None
    try:
        conn = sqlite3.connect(str(path))
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT * FROM snapshots
            ORDER BY collected_at DESC
            LIMIT 2000
            """
        ).fetchall()
        for row in rows:
            public = row_to_match(row)
            if public.get("id") == match_id_value:
                stats = {
                    "possession": {"home": _safe_int(str(row["possession_home"]).replace("%", ""), 50), "away": _safe_int(str(row["possession_away"]).replace("%", ""), 50)},
                    "shots": {"home": _safe_int(row["shots_home"], 0), "away": _safe_int(row["shots_away"], 0)},
                    "on_target": {"home": _safe_int(row["on_target_home"], 0), "away": _safe_int(row["on_target_away"], 0)},
                    "dangerous": {"home": _safe_int(row["dangerous_home"], 0), "away": _safe_int(row["dangerous_away"], 0)},
                    "corners": {"home": _safe_int(row["corners_home"], 0), "away": _safe_int(row["corners_away"], 0)},
                }
                return {"match": public, "stats": stats}
    except Exception:
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass
    return None


def demo_detail(match: dict[str, Any]) -> dict[str, Any]:
    presets = {
        "demo-liv-ars": {
            "possession": {"home": 58, "away": 42},
            "shots": {"home": 14, "away": 8},
            "on_target": {"home": 6, "away": 4},
            "dangerous": {"home": 42, "away": 33},
            "corners": {"home": 5, "away": 3},
            "avg": {"home": {"avg": 3.1, "count": 10, "zero_zero": 1}, "away": {"avg": 2.8, "count": 10, "zero_zero": 2}},
        },
        "demo-mci-che": {
            "possession": {"home": 61, "away": 39},
            "shots": {"home": 9, "away": 5},
            "on_target": {"home": 3, "away": 2},
            "dangerous": {"home": 37, "away": 24},
            "corners": {"home": 4, "away": 2},
            "avg": {"home": {"avg": 3.2, "count": 10, "zero_zero": 0}, "away": {"avg": 2.4, "count": 10, "zero_zero": 2}},
        },
    }
    p = presets.get(match.get("id"), presets["demo-liv-ars"])
    return {"match": match, "stats": {k: v for k, v in p.items() if k != "avg"}, "avg": p["avg"]}


def detail_payload(match_id_value: str) -> dict[str, Any]:
    live_payload = load_live_payload(force=False)
    matches = flatten_payload(live_payload)
    match = next((m for m in matches if str(m.get("id")) == str(match_id_value)), None)

    if not match:
        db_detail = sqlite_detail_for_match(match_id_value)
        if db_detail:
            db_detail.setdefault("avg", {"home": {"avg": None, "count": 0, "zero_zero": 0}, "away": {"avg": None, "count": 0, "zero_zero": 0}})
            return {"ok": True, **db_detail}
        match = DEMO_MATCHES[0]

    if str(match.get("source")) == "demo":
        return {"ok": True, **demo_detail(match)}

    # SQLite match from /api/live
    if str(match.get("source")) == "sqlite":
        db_detail = sqlite_detail_for_match(str(match.get("id")))
        if db_detail:
            db_detail.setdefault("avg", {"home": {"avg": None, "count": 0, "zero_zero": 0}, "away": {"avg": None, "count": 0, "zero_zero": 0}})
            return {"ok": True, **db_detail}

    raw = {}
    with _cache_lock:
        raw = (_live_cache.get("raw") or {}).get(str(match.get("id"))) or {}

    stats: dict[str, dict[str, int]] = {}
    avg = {"home": {"avg": None, "count": 0, "zero_zero": 0}, "away": {"avg": None, "count": 0, "zero_zero": 0}}

    mid = str(match.get("id") or "")
    if mid:
        cached = _stats_cache.get(mid)
        if cached and time.time() - float(cached.get("saved_at") or 0) < STAT_CACHE_SECONDS:
            stats = cached.get("stats") or {}
        else:
            try:
                stats = stat_pairs_from_response(match_statistics(mid))
                _stats_cache[mid] = {"saved_at": time.time(), "stats": stats}
            except Exception:
                stats = {}

    if not stats:
        stats = demo_detail(match)["stats"]

    home_id = team_id(raw.get("homeTeam"))
    away_id = team_id(raw.get("awayTeam"))
    if home_id or away_id:
        avg = {"home": avg_total_for_team(home_id), "away": avg_total_for_team(away_id)}

    return {"ok": True, "match": match, "stats": stats, "avg": avg}


class MiniAppHandler(BaseHTTPRequestHandler):
    server_version = "TelegramMiniApp/1.0"

    def log_message(self, fmt: str, *args: Any) -> None:
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {self.address_string()} - {fmt % args}")

    def do_GET(self) -> None:
        try:
            url = urllib.parse.urlparse(self.path)
            path = url.path

            if path == "/healthz":
                return json_response(self, {"ok": True, "time": now_ts(), "mode": DATA_MODE})

            if path == "/api/live":
                params = urllib.parse.parse_qs(url.query)
                force = str((params.get("force") or ["0"])[0]).lower() in {"1", "true", "yes"}
                return json_response(self, load_live_payload(force=force))

            if path == "/api/match":
                params = urllib.parse.parse_qs(url.query)
                mid = str((params.get("id") or [""])[0]).strip()
                if not mid:
                    return json_response(self, {"ok": False, "error": "missing id"}, status=400)
                return json_response(self, detail_payload(mid))

            if path == "/" or path == "":
                path = "/index.html"

            safe = Path(path.lstrip("/"))
            if ".." in safe.parts:
                return text_response(self, "Forbidden", status=403)

            file_path = STATIC_DIR / safe
            if not file_path.exists() or not file_path.is_file():
                return text_response(self, "Not found", status=404)

            content = file_path.read_bytes()
            ctype = mimetypes.guess_type(str(file_path))[0] or "application/octet-stream"
            if file_path.suffix == ".js":
                ctype = "application/javascript; charset=utf-8"
            elif file_path.suffix == ".css":
                ctype = "text/css; charset=utf-8"
            elif file_path.suffix == ".html":
                ctype = "text/html; charset=utf-8"

            self.send_response(200)
            self.send_header("Content-Type", ctype)
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)
        except Exception as exc:
            traceback.print_exc()
            return json_response(self, {"ok": False, "error": f"{type(exc).__name__}: {exc}"}, status=500)


def main() -> None:
    DATA_DIR.mkdir(exist_ok=True)
    server = ThreadingHTTPServer((HOST, PORT), MiniAppHandler)
    print("=" * 72)
    print("Telegram Live Matches Mini App")
    print(f"Local:   http://127.0.0.1:{PORT}")
    print(f"Host:    {HOST}:{PORT}")
    print(f"Mode:    {DATA_MODE}")
    print("=" * 72)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
