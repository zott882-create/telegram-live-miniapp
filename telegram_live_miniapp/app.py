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
TEAM_LOGO_DIR = DATA_DIR / "team_logos"
TEAM_LOGO_DB = DATA_DIR / "team_logos.sqlite3"
TEAM_LOGO_MAX_BYTES = int(os.environ.get("TEAM_LOGO_MAX_BYTES", "1500000"))
TEAM_LOGO_DOWNLOAD = os.environ.get("TEAM_LOGO_DOWNLOAD", "1").strip().lower() not in {"0", "false", "no", "off"}

HOST = os.environ.get("HOST", "0.0.0.0")
PORT = int(os.environ.get("PORT", "8080"))
DATA_MODE = os.environ.get("DATA_MODE", "auto").strip().lower()

# v5: Telegram bot token for push notifications.
# Set BOT_TOKEN env var on the server to enable Telegram delivery.
BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
NOTIFY_DB = BASE_DIR / "data" / "notify_subs.json"
NOTIFY_POLL_INTERVAL = float(os.environ.get("NOTIFY_POLL_INTERVAL", "30"))
NOTIFY_COOLDOWN_PER_MATCH = int(os.environ.get("NOTIFY_COOLDOWN_PER_MATCH", "600"))  # 10 min

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
_logo_lock = threading.Lock()
_logo_db_ready = False


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


def _team_key(name: str) -> str:
    text = str(name or "").strip().lower()
    text = re.sub(r"[^\w\s-]+", " ", text, flags=re.UNICODE)
    text = re.sub(r"\s+", " ", text, flags=re.UNICODE).strip()
    return text[:160]


def _init_logo_db() -> None:
    """Create a small local cache DB for team logos."""
    global _logo_db_ready
    if _logo_db_ready:
        return
    with _logo_lock:
        if _logo_db_ready:
            return
        DATA_DIR.mkdir(exist_ok=True)
        TEAM_LOGO_DIR.mkdir(exist_ok=True)
        conn = sqlite3.connect(str(TEAM_LOGO_DB))
        try:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS team_logos (
                    team_key TEXT PRIMARY KEY,
                    team_id TEXT,
                    team_name TEXT NOT NULL,
                    logo_url TEXT,
                    local_file TEXT,
                    source TEXT,
                    updated_at INTEGER NOT NULL
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_team_logos_team_id ON team_logos(team_id)")
            conn.commit()
            _logo_db_ready = True
        finally:
            conn.close()


def _normalize_logo_url(value: Any) -> str:
    url = str(value or "").strip()
    if not url or url.startswith("data:"):
        return ""
    if url.startswith("//"):
        url = "https:" + url
    elif url.startswith("/"):
        url = WEB_ORIGIN + url
    elif not re.match(r"^https?://", url, flags=re.I):
        return ""
    return url


def _first_logo_url(obj: Any, depth: int = 0) -> str:
    """Try common API field names without depending on one exact provider schema."""
    if depth > 3:
        return ""
    if isinstance(obj, dict):
        preferred = (
            "logo", "logoUrl", "logo_url", "teamLogo", "team_logo", "teamLogoUrl",
            "image", "imageUrl", "image_url", "photo", "pic", "icon", "badge", "crest", "emblem",
        )
        for key in preferred:
            if key in obj:
                url = _normalize_logo_url(obj.get(key))
                if url:
                    return url
        for key, value in obj.items():
            lk = str(key).lower()
            if any(token in lk for token in ("logo", "badge", "crest", "emblem")):
                url = _normalize_logo_url(value)
                if url:
                    return url
        for value in obj.values():
            if isinstance(value, (dict, list)):
                url = _first_logo_url(value, depth + 1)
                if url:
                    return url
    elif isinstance(obj, list):
        for item in obj:
            url = _first_logo_url(item, depth + 1)
            if url:
                return url
    return ""


def _side_logo_from_match(match: dict[str, Any], side: str) -> str:
    prefix = "home" if side == "home" else "away"
    direct_keys = (
        f"{prefix}Logo", f"{prefix}LogoUrl", f"{prefix}TeamLogo", f"{prefix}TeamLogoUrl",
        f"{prefix}_logo", f"{prefix}_logo_url", f"{prefix}_team_logo", f"{prefix}_team_logo_url",
        f"{prefix}Icon", f"{prefix}Image", f"{prefix}ImageUrl",
    )
    for key in direct_keys:
        url = _normalize_logo_url(match.get(key))
        if url:
            return url
    team_obj = match.get("homeTeam" if side == "home" else "awayTeam")
    return _first_logo_url(team_obj)


def _logo_public_path(local_file: str) -> str:
    local_file = str(local_file or "").strip()
    if not local_file:
        return ""
    file_path = TEAM_LOGO_DIR / local_file
    if file_path.exists() and file_path.is_file():
        return "/team-logos/" + urllib.parse.quote(local_file)
    return ""


def _lookup_team_logo(team_id_value: str, team_name_value: str) -> str:
    _init_logo_db()
    team_id_value = str(team_id_value or "").strip()
    key = _team_key(team_name_value)
    if not key and not team_id_value:
        return ""
    conn = sqlite3.connect(str(TEAM_LOGO_DB))
    conn.row_factory = sqlite3.Row
    try:
        row = None
        if team_id_value:
            row = conn.execute(
                "SELECT * FROM team_logos WHERE team_id = ? ORDER BY updated_at DESC LIMIT 1",
                (team_id_value,),
            ).fetchone()
        if row is None and key:
            row = conn.execute("SELECT * FROM team_logos WHERE team_key = ? LIMIT 1", (key,)).fetchone()
        if not row:
            return ""
        local = _logo_public_path(row["local_file"] or "")
        return local or str(row["logo_url"] or "")
    except Exception:
        return ""
    finally:
        conn.close()


def _download_team_logo(team_id_value: str, team_name_value: str, logo_url: str) -> str:
    if not TEAM_LOGO_DOWNLOAD or not logo_url:
        return ""
    _init_logo_db()
    key_seed = f"{team_id_value}|{_team_key(team_name_value)}|{logo_url}"
    digest = hashlib.sha1(key_seed.encode("utf-8", "replace")).hexdigest()[:24]
    parsed = urllib.parse.urlparse(logo_url)
    ext = Path(parsed.path).suffix.lower()
    if ext not in {".png", ".jpg", ".jpeg", ".webp", ".gif", ".svg"}:
        ext = ".png"
    filename = digest + ext
    target = TEAM_LOGO_DIR / filename
    if target.exists() and target.stat().st_size > 0:
        return filename
    try:
        req = urllib.request.Request(logo_url, headers={"User-Agent": DEFAULT_HEADERS["User-Agent"], "Accept": "image/*,*/*;q=0.8"})
        with urllib.request.urlopen(req, timeout=5.0) as resp:
            ctype = (resp.headers.get("Content-Type") or "").lower()
            if ctype and "image" not in ctype and "svg" not in ctype:
                return ""
            data = resp.read(TEAM_LOGO_MAX_BYTES + 1)
        if not data or len(data) > TEAM_LOGO_MAX_BYTES:
            return ""
        TEAM_LOGO_DIR.mkdir(exist_ok=True)
        target.write_bytes(data)
        return filename
    except Exception:
        return ""


def _save_team_logo(team_id_value: str, team_name_value: str, logo_url: str, source: str = "igscore") -> str:
    logo_url = _normalize_logo_url(logo_url)
    team_name_value = str(team_name_value or "").strip()
    team_id_value = str(team_id_value or "").strip()
    key = _team_key(team_name_value) or ("id:" + team_id_value if team_id_value else "")
    if not key:
        return logo_url
    _init_logo_db()
    local_file = _download_team_logo(team_id_value, team_name_value, logo_url) if logo_url else ""
    with _logo_lock:
        conn = sqlite3.connect(str(TEAM_LOGO_DB))
        try:
            conn.execute(
                """
                INSERT INTO team_logos(team_key, team_id, team_name, logo_url, local_file, source, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(team_key) DO UPDATE SET
                    team_id=excluded.team_id,
                    team_name=excluded.team_name,
                    logo_url=COALESCE(NULLIF(excluded.logo_url, ''), team_logos.logo_url),
                    local_file=COALESCE(NULLIF(excluded.local_file, ''), team_logos.local_file),
                    source=excluded.source,
                    updated_at=excluded.updated_at
                """,
                (key, team_id_value, team_name_value, logo_url, local_file, source, now_ts()),
            )
            conn.commit()
        finally:
            conn.close()
    return _logo_public_path(local_file) or logo_url or _lookup_team_logo(team_id_value, team_name_value)


def resolve_team_logo(match: dict[str, Any], side: str, team_name_value: str) -> str:
    team_obj = match.get("homeTeam" if side == "home" else "awayTeam")
    tid = team_id(team_obj)
    logo_url = _side_logo_from_match(match, side)
    if logo_url:
        return _save_team_logo(tid, team_name_value, logo_url, source="igscore")
    return _lookup_team_logo(tid, team_name_value)


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
    home_logo = resolve_team_logo(match, "home", home)
    away_logo = resolve_team_logo(match, "away", away)
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
        "home_logo": home_logo,
        "away_logo": away_logo,
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
        # v5: empty stats — IGScore mode doesn't fetch per-match stats during list,
        # they get loaded on /api/match (detail). Filters by stats still work for
        # SQLite source which has full data inline.
        "stats": {},
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
    home_logo = _lookup_team_logo("", home)
    away_logo = _lookup_team_logo("", away)
    minute = _safe_int(row["minute_value"], 0)
    minute_raw = str(row["minute_raw"] or (f"{minute}’" if minute else "LIVE"))
    country = str(row["country"] or "Без страны")
    league = str(row["league"] or row["tournament"] or "Без лиги")
    mid = public_match_id("", link, home, away)
    # v5: inline stats so the frontend's filter pills/sheet can use them
    # without an extra round-trip per match.
    def _i(key: str) -> int:
        try:
            v = row[key]
        except Exception:
            return 0
        if v in (None, ""):
            return 0
        try:
            return int(float(str(v).replace("%", "").strip()))
        except Exception:
            return 0
    stats = {
        "possession_home": _i("possession_home") or 50,
        "possession_away": _i("possession_away") or 50,
        "shots_home": _i("shots_home"),
        "shots_away": _i("shots_away"),
        "on_target_home": _i("on_target_home"),
        "on_target_away": _i("on_target_away"),
        "dangerous_home": _i("dangerous_home"),
        "dangerous_away": _i("dangerous_away"),
        "attacks_home": _i("attacks_home"),
        "attacks_away": _i("attacks_away"),
        "corners_home": _i("corners_home"),
        "corners_away": _i("corners_away"),
    }
    stats["shots_total"] = stats["shots_home"] + stats["shots_away"]
    stats["on_target_total"] = stats["on_target_home"] + stats["on_target_away"]
    stats["dangerous_total"] = stats["dangerous_home"] + stats["dangerous_away"]
    stats["attacks_total"] = stats["attacks_home"] + stats["attacks_away"]
    stats["corners_total"] = stats["corners_home"] + stats["corners_away"]
    return {
        "id": mid,
        "home": home,
        "away": away,
        "home_logo": home_logo,
        "away_logo": away_logo,
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
        "stats": stats,
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
        "stats": {
            "possession_home": 58, "possession_away": 42,
            "shots_home": 14, "shots_away": 8, "shots_total": 22,
            "on_target_home": 6, "on_target_away": 4, "on_target_total": 10,
            "dangerous_home": 42, "dangerous_away": 33, "dangerous_total": 75,
            "attacks_home": 101, "attacks_away": 88, "attacks_total": 189,
            "corners_home": 5, "corners_away": 3, "corners_total": 8,
        },
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
        "stats": {
            "possession_home": 61, "possession_away": 39,
            "shots_home": 9, "shots_away": 5, "shots_total": 14,
            "on_target_home": 3, "on_target_away": 2, "on_target_total": 5,
            "dangerous_home": 37, "dangerous_away": 24, "dangerous_total": 61,
            "attacks_home": 72, "attacks_away": 54, "attacks_total": 126,
            "corners_home": 4, "corners_away": 2, "corners_total": 6,
        },
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
        "stats": {
            "possession_home": 53, "possession_away": 47,
            "shots_home": 18, "shots_away": 14, "shots_total": 32,
            "on_target_home": 8, "on_target_away": 6, "on_target_total": 14,
            "dangerous_home": 64, "dangerous_away": 51, "dangerous_total": 115,
            "attacks_home": 113, "attacks_away": 99, "attacks_total": 212,
            "corners_home": 7, "corners_away": 4, "corners_total": 11,
        },
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


# ============================================================================
#  v5: Telegram notification subsystem
# ============================================================================
#
# Flow:
#   1. User opens Mini App from a bot. JS reads tg.initData (signed by bot).
#   2. User toggles "Уведомления ON" → POST /api/subscribe with init_data + filter.
#   3. Server verifies init_data HMAC, stores {chat_id, filter} in NOTIFY_DB.
#   4. Background thread polls /api/live every 30s, checks each subscribed
#      filter against current matches, sends a Telegram message for new
#      matches not seen recently (cooldown per match).
#
# Each subscription stores:
#   chat_id, filter, seen_alerts: {match_id: timestamp}, updated_at
#
# In-app /api/notify endpoint is for the FRONTEND to push an immediate alert
# (in case the user keeps the Mini App open) — same code path delivers
# Telegram message.

import hmac as _hmac

_notify_lock = threading.Lock()
_notify_subs: dict[str, dict[str, Any]] = {}  # chat_id_str → sub dict


def _load_notify_subs() -> None:
    global _notify_subs
    try:
        if NOTIFY_DB.exists():
            _notify_subs = json.loads(NOTIFY_DB.read_text("utf-8") or "{}") or {}
    except Exception as exc:
        print(f"[notify] load failed: {exc}")
        _notify_subs = {}


def _save_notify_subs() -> None:
    try:
        NOTIFY_DB.parent.mkdir(parents=True, exist_ok=True)
        NOTIFY_DB.write_text(json.dumps(_notify_subs, ensure_ascii=False), encoding="utf-8")
    except Exception as exc:
        print(f"[notify] save failed: {exc}")


def verify_init_data(init_data: str) -> dict[str, Any] | None:
    """Verify Telegram WebApp initData HMAC. Returns user dict or None."""
    if not init_data or not BOT_TOKEN:
        return None
    try:
        parsed = dict(urllib.parse.parse_qsl(init_data, keep_blank_values=True))
        recv_hash = parsed.pop("hash", "")
        if not recv_hash:
            return None
        check = "\n".join(f"{k}={v}" for k, v in sorted(parsed.items()))
        secret = _hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
        digest = _hmac.new(secret, check.encode(), hashlib.sha256).hexdigest()
        if not _hmac.compare_digest(digest, recv_hash):
            return None
        # auth_date freshness check (24h)
        try:
            if time.time() - int(parsed.get("auth_date", "0")) > 86400:
                return None
        except Exception:
            return None
        user_raw = parsed.get("user")
        if not user_raw:
            return None
        return json.loads(user_raw)
    except Exception:
        return None


def send_telegram_message(chat_id: int | str, text: str, link: str = "") -> bool:
    if not BOT_TOKEN:
        return False
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": int(chat_id),
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    if link:
        payload["reply_markup"] = {
            "inline_keyboard": [[{"text": "↗ Открыть на IGScore", "url": link}]]
        }
    try:
        req = urllib.request.Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            method="POST",
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            resp.read()
        return True
    except Exception as exc:
        print(f"[notify] send failed for {chat_id}: {exc}")
        return False


def match_passes_filter(m: dict[str, Any], f: dict[str, Any]) -> bool:
    minute = _safe_int(m.get("minute"), 0)
    mn = _safe_int(f.get("minute_min"), 0)
    mx = _safe_int(f.get("minute_max"), 130)
    if minute < mn or minute > mx:
        return False
    s = m.get("stats") or {}
    def _tot(key: str) -> int:
        total = s.get(f"{key}_total")
        if total not in (None, ""):
            return _safe_int(total)
        return _safe_int(s.get(f"{key}_home")) + _safe_int(s.get(f"{key}_away"))
    if _safe_int(f.get("shots_min")) and _tot("shots") < _safe_int(f.get("shots_min")): return False
    if _safe_int(f.get("on_target_min")) and _tot("on_target") < _safe_int(f.get("on_target_min")): return False
    if _safe_int(f.get("dangerous_min")) and _tot("dangerous") < _safe_int(f.get("dangerous_min")): return False
    if _safe_int(f.get("attacks_min")) and _tot("attacks") < _safe_int(f.get("attacks_min")): return False
    if _safe_int(f.get("corners_min")) and _tot("corners") < _safe_int(f.get("corners_min")): return False
    scores = f.get("scores") or []
    if scores and m.get("score") not in scores:
        return False
    countries = f.get("countries") or []
    if countries and m.get("country") not in countries:
        return False
    return True


def notify_worker_loop() -> None:
    """Background loop: every NOTIFY_POLL_INTERVAL seconds check all subs."""
    while True:
        try:
            time.sleep(NOTIFY_POLL_INTERVAL)
            if not _notify_subs or not BOT_TOKEN:
                continue
            live = load_live_payload(force=False)
            matches = flatten_payload(live)
            now = time.time()
            with _notify_lock:
                for chat_id_str, sub in list(_notify_subs.items()):
                    cfg = sub.get("filter") or {}
                    if not cfg.get("enabled"):
                        continue
                    seen = sub.setdefault("seen", {})
                    for m in matches:
                        if not match_passes_filter(m, cfg):
                            continue
                        mid = str(m.get("id"))
                        prev = float(seen.get(mid) or 0)
                        if now - prev < NOTIFY_COOLDOWN_PER_MATCH:
                            continue
                        text = (
                            f"🔔 <b>{m.get('home')}</b> {_safe_int(m.get('score_home'))}-{_safe_int(m.get('score_away'))} <b>{m.get('away')}</b>\n"
                            f"⏱ {m.get('minute_text','')}  ·  🏆 {m.get('country','')} {m.get('league','')}"
                        )
                        s = m.get("stats") or {}
                        if s:
                            text += (
                                f"\n\nУдары: {_safe_int(s.get('shots_home'))}–{_safe_int(s.get('shots_away'))}"
                                f"  ·  В створ: {_safe_int(s.get('on_target_home'))}–{_safe_int(s.get('on_target_away'))}"
                                f"\nОпасные: {_safe_int(s.get('dangerous_home'))}–{_safe_int(s.get('dangerous_away'))}"
                                f"  ·  Угловые: {_safe_int(s.get('corners_home'))}–{_safe_int(s.get('corners_away'))}"
                            )
                        ok = send_telegram_message(chat_id_str, text, link=str(m.get("link") or ""))
                        if ok:
                            seen[mid] = now
                # Garbage-collect stale seen entries (older than 6h)
                cutoff = now - 6 * 3600
                for sub in _notify_subs.values():
                    seen = sub.get("seen") or {}
                    for k in list(seen.keys()):
                        if seen[k] < cutoff:
                            seen.pop(k, None)
                _save_notify_subs()
        except Exception as exc:
            print(f"[notify] worker error: {exc}")
            traceback.print_exc()


def start_notify_worker() -> None:
    t = threading.Thread(target=notify_worker_loop, daemon=True, name="notify-worker")
    t.start()


def handle_subscribe(body: dict[str, Any]) -> dict[str, Any]:
    init_data = str(body.get("init_data") or "")
    filt = body.get("filter") if isinstance(body.get("filter"), dict) else {}
    user = verify_init_data(init_data)
    if not user:
        # No bot token configured → degrade gracefully (just store nothing,
        # in-app alerts will still work)
        if not BOT_TOKEN:
            return {"ok": True, "warning": "BOT_TOKEN not set on server; in-app only"}
        return {"ok": False, "error": "init_data verification failed"}
    chat_id = str(user.get("id"))
    if not chat_id or chat_id == "None":
        return {"ok": False, "error": "no chat_id"}
    with _notify_lock:
        existing = _notify_subs.get(chat_id) or {}
        existing.update({
            "chat_id": chat_id,
            "filter": filt,
            "user": {"first_name": user.get("first_name"), "username": user.get("username")},
            "updated_at": int(time.time()),
        })
        existing.setdefault("seen", {})
        _notify_subs[chat_id] = existing
        _save_notify_subs()
    return {"ok": True, "chat_id": chat_id}


def handle_notify(body: dict[str, Any]) -> dict[str, Any]:
    """Direct push from the open Mini App. Verifies init_data, sends Telegram message."""
    init_data = str(body.get("init_data") or "")
    text = str(body.get("text") or "").strip()
    if not text:
        return {"ok": False, "error": "empty text"}
    user = verify_init_data(init_data)
    if not user:
        return {"ok": True, "warning": "no bot_token or unverified — in-app only"}
    chat_id = str(user.get("id"))
    ok = send_telegram_message(chat_id, text)
    return {"ok": ok}


def parse_json_body(handler: BaseHTTPRequestHandler) -> dict[str, Any]:
    try:
        length = int(handler.headers.get("Content-Length") or 0)
        if not length:
            return {}
        raw = handler.rfile.read(length)
        return json.loads(raw.decode("utf-8"))
    except Exception:
        return {}


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

            if path.startswith("/team-logos/"):
                safe_name = Path(urllib.parse.unquote(path.split("/team-logos/", 1)[1])).name
                if not safe_name:
                    return text_response(self, "Not found", status=404)
                file_path = TEAM_LOGO_DIR / safe_name
                if not file_path.exists() or not file_path.is_file():
                    return text_response(self, "Not found", status=404)
                content = file_path.read_bytes()
                ctype = mimetypes.guess_type(str(file_path))[0] or "image/png"
                self.send_response(200)
                self.send_header("Content-Type", ctype)
                self.send_header("Cache-Control", "public, max-age=604800, immutable")
                self.send_header("Content-Length", str(len(content)))
                self.end_headers()
                self.wfile.write(content)
                return

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

    def do_POST(self) -> None:
        try:
            url = urllib.parse.urlparse(self.path)
            path = url.path
            body = parse_json_body(self)
            if path == "/api/subscribe":
                return json_response(self, handle_subscribe(body))
            if path == "/api/notify":
                return json_response(self, handle_notify(body))
            return json_response(self, {"ok": False, "error": "unknown endpoint"}, status=404)
        except Exception as exc:
            traceback.print_exc()
            return json_response(self, {"ok": False, "error": f"{type(exc).__name__}: {exc}"}, status=500)


def main() -> None:
    DATA_DIR.mkdir(exist_ok=True)
    TEAM_LOGO_DIR.mkdir(exist_ok=True)
    _init_logo_db()
    _load_notify_subs()
    if BOT_TOKEN:
        start_notify_worker()
        print(f"[notify] worker started with BOT_TOKEN (***{BOT_TOKEN[-4:]})")
    else:
        print("[notify] BOT_TOKEN not set — push notifications disabled, in-app only")
    server = ThreadingHTTPServer((HOST, PORT), MiniAppHandler)
    print("=" * 72)
    print("Telegram Live Matches Mini App — v5 premium")
    print(f"Local:   http://127.0.0.1:{PORT}")
    print(f"Host:    {HOST}:{PORT}")
    print(f"Mode:    {DATA_MODE}")
    print(f"Notify:  {'enabled (' + str(len(_notify_subs)) + ' subs)' if BOT_TOKEN else 'disabled (no BOT_TOKEN)'}")
    print("=" * 72)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
