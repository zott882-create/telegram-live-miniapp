#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Telegram Live Matches Mini App
Standalone Python server + static frontend.
V5.7: local country flags pack, league-logo headers disabled.

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

import concurrent.futures
import datetime as _dt
import gzip
import hashlib
import html
import json
import mimetypes
import os
import random
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
NOTIFY_MATCHES_DB = BASE_DIR / "data" / "notify_matches.json"
NOTIFY_MATCHES_MAX_PER_USER = int(os.environ.get("NOTIFY_MATCHES_MAX_PER_USER", "200"))
NOTIFY_POLL_INTERVAL = float(os.environ.get("NOTIFY_POLL_INTERVAL", "30"))
NOTIFY_COOLDOWN_PER_MATCH = int(os.environ.get("NOTIFY_COOLDOWN_PER_MATCH", "600"))  # 10 min

API_BASE = os.environ.get("IGSCORE_API_BASE", "https://api.igscore.net:8080").rstrip("/")
WEB_ORIGIN = "https://www.igscore.net"
TIME_ZONE = os.environ.get("IGSCORE_TIME_ZONE", "+05:00")
LANG = os.environ.get("IGSCORE_LANG", "en")

LIVE_CACHE_SECONDS = float(os.environ.get("LIVE_CACHE_SECONDS", "7"))
STAT_CACHE_SECONDS = float(os.environ.get("STAT_CACHE_SECONDS", "15"))
# v5.1: put per-match live statistics directly into /api/live, so filters and
# notifications can work by shots/attacks/corners/cards without opening detail.
LIVE_STATS_IN_FEED = os.environ.get("LIVE_STATS_IN_FEED", "1").strip().lower() not in {"0", "false", "no", "off"}
LIVE_STATS_MAX_MATCHES = int(os.environ.get("LIVE_STATS_MAX_MATCHES", "120"))
LIVE_STATS_WORKERS = max(1, int(os.environ.get("LIVE_STATS_WORKERS", "8")))
IGSCORE_STAT_TIMEOUT = float(os.environ.get("IGSCORE_STAT_TIMEOUT", "6"))
SQLITE_RECENT_MINUTES = int(os.environ.get("SQLITE_RECENT_MINUTES", "240"))

# v5.2 collector mode: one shared background updater fills a local SQLite cache.
# Mini App users then read only from this cache, so 100/1000 users do not multiply
# requests to IGScore. Defaults are tuned for up to 300 live matches refreshed
# roughly once per minute.
COLLECTOR_ENABLED = os.environ.get("COLLECTOR_ENABLED", "1").strip().lower() not in {"0", "false", "no", "off"}
COLLECTOR_INTERVAL = float(os.environ.get("COLLECTOR_INTERVAL", "60"))
COLLECTOR_MAX_MATCHES = int(os.environ.get("COLLECTOR_MAX_MATCHES", "300"))
COLLECTOR_WORKERS = max(1, int(os.environ.get("COLLECTOR_WORKERS", "2")))
COLLECTOR_REQUEST_DELAY = float(os.environ.get("COLLECTOR_REQUEST_DELAY", "0.35"))
COLLECTOR_JITTER = float(os.environ.get("COLLECTOR_JITTER", "5"))
LIVE_FROM_DB_ONLY = os.environ.get("LIVE_FROM_DB_ONLY", "1").strip().lower() not in {"0", "false", "no", "off"}
DELETE_FINISHED_MATCHES = os.environ.get("DELETE_FINISHED_MATCHES", "1").strip().lower() not in {"0", "false", "no", "off"}
FINISHED_MATCH_GRACE_SECONDS = int(os.environ.get("FINISHED_MATCH_GRACE_SECONDS", "120"))
LIVE_CACHE_DB = DATA_DIR / "live_cache.sqlite3"

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
    "england": "GB", "scotland": "GB", "wales": "GB", "northern ireland": "GB", "great britain": "GB", "uk": "GB",
    "spain": "ES", "italy": "IT", "germany": "DE", "france": "FR", "netherlands": "NL", "portugal": "PT", "turkey": "TR",
    "brazil": "BR", "argentina": "AR", "usa": "US", "united states": "US", "russia": "RU",
    "bhutan": "BT", "egypt": "EG", "ethiopia": "ET", "kenya": "KE", "paraguay": "PY", "china": "CN", "india": "IN", "north korea": "KP", "south korea": "KR",
    "thailand": "TH", "japan": "JP", "australia": "AU", "new zealand": "NZ", "albania": "AL", "algeria": "DZ",
    "angola": "AO", "armenia": "AM", "austria": "AT", "azerbaijan": "AZ", "bahrain": "BH", "belarus": "BY",
    "belgium": "BE", "bolivia": "BO", "bosnia": "BA", "bosnia and herzegovina": "BA", "bulgaria": "BG",
    "cameroon": "CM", "canada": "CA", "chile": "CL", "colombia": "CO", "costa rica": "CR", "croatia": "HR",
    "cyprus": "CY", "czech republic": "CZ", "czechia": "CZ", "denmark": "DK", "ecuador": "EC", "egypt": "EG",
    "estonia": "EE", "finland": "FI", "georgia": "GE", "ghana": "GH", "greece": "GR", "guatemala": "GT",
    "honduras": "HN", "hong kong": "HK", "hungary": "HU", "iceland": "IS", "indonesia": "ID", "iran": "IR",
    "iraq": "IQ", "ireland": "IE", "israel": "IL", "jordan": "JO", "kazakhstan": "KZ", "kosovo": "XK",
    "kuwait": "KW", "latvia": "LV", "lebanon": "LB", "lithuania": "LT", "luxembourg": "LU", "malaysia": "MY",
    "malta": "MT", "mexico": "MX", "moldova": "MD", "montenegro": "ME", "morocco": "MA", "nigeria": "NG",
    "norway": "NO", "oman": "OM", "panama": "PA", "peru": "PE", "poland": "PL", "qatar": "QA",
    "romania": "RO", "saudi arabia": "SA", "serbia": "RS", "singapore": "SG", "slovakia": "SK", "slovenia": "SI",
    "south africa": "ZA", "sweden": "SE", "switzerland": "CH", "syria": "SY", "tunisia": "TN", "ukraine": "UA",
    "uruguay": "UY", "uzbekistan": "UZ", "venezuela": "VE", "vietnam": "VN", "zambia": "ZM", "zimbabwe": "ZW",
}

CONTINENT_NAMES = {
    "africa", "asia", "americas", "america", "europe", "oceania", "international", "world",
    "без страны", "без лиги",
}

COUNTRY_NAME_ALIASES = {
    "bhutan": "Bhutan", "bhutanese": "Bhutan", "egyptian": "Egypt", "ethiopian": "Ethiopia", "kenyan": "Kenya", "paraguayan": "Paraguay", "chinese": "China", "indian": "India",
    "hku": "Hong Kong", "korean": "South Korea", "north korean": "North Korea", "thai": "Thailand", "japanese": "Japan",
    "australian": "Australia", "albanian": "Albania", "brazilian": "Brazil", "argentine": "Argentina",
    "english": "England", "spanish": "Spain", "italian": "Italy", "german": "Germany", "french": "France",
    "dutch": "Netherlands", "portuguese": "Portugal", "turkish": "Turkey", "russian": "Russia",
    "polish": "Poland", "romanian": "Romania", "serbian": "Serbia", "swedish": "Sweden", "norwegian": "Norway",
}

_cache_lock = threading.Lock()
_live_cache: dict[str, Any] = {"saved_at": 0.0, "payload": None, "raw": {}}
_stats_cache: dict[str, dict[str, Any]] = {}
_collector_lock = threading.Lock()
_collector_thread_started = False
_collector_state: dict[str, Any] = {
    "enabled": COLLECTOR_ENABLED,
    "running": False,
    "last_start": 0,
    "last_finish": 0,
    "last_error": "",
    "last_matches": 0,
    "last_stats": 0,
}
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
    return post_json("/v1/football/match/statistics", {"matchId": str(match_id), **_base_payload()}, timeout=IGSCORE_STAT_TIMEOUT)


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


def resolve_competition_logo(match: dict[str, Any]) -> str:
    """Best-effort competition/league logo extraction for headers.

    IGScore can place competition/country images in different nested objects.
    Check competition, category and generic logo/flag/icon fields recursively.
    """
    comp = match.get("competition") if isinstance(match.get("competition"), dict) else {}
    cat = match.get("category") or (comp.get("category") if isinstance(comp, dict) else {}) or {}

    candidates = []
    for obj in (comp, cat if isinstance(cat, dict) else {}, match):
        if not isinstance(obj, dict):
            continue
        candidates.extend([
            obj.get("logo"), obj.get("logoUrl"), obj.get("logo_url"),
            obj.get("competitionLogo"), obj.get("competition_logo"),
            obj.get("leagueLogo"), obj.get("league_logo"),
            obj.get("tournamentLogo"), obj.get("tournament_logo"),
            obj.get("flag"), obj.get("flagUrl"), obj.get("flag_url"),
            obj.get("countryLogo"), obj.get("country_logo"),
            obj.get("icon"), obj.get("image"),
        ])

    for value in candidates:
        url = _normalize_logo_url(value)
        if url:
            return url

    for obj in (comp, cat if isinstance(cat, dict) else {}, match):
        url = _first_logo_url(obj)
        if url:
            return url
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


def _is_generic_region(value: Any) -> bool:
    text = str(value or "").strip().lower()
    return not text or text in CONTINENT_NAMES


def _country_name_from_text(*parts: Any) -> str:
    hay = " ".join(str(p or "") for p in parts).lower()
    hay = re.sub(r"[^a-z\s-]+", " ", hay)
    hay = re.sub(r"\s+", " ", hay).strip()
    for alias, country in sorted(COUNTRY_NAME_ALIASES.items(), key=lambda x: len(x[0]), reverse=True):
        pattern = r"(^|\s)" + re.escape(alias.lower()) + r"(\s|$)"
        if re.search(pattern, hay):
            return country
    for name in sorted(COUNTRY_CODE_MAP.keys(), key=len, reverse=True):
        pattern = r"(^|\s)" + re.escape(name.lower()) + r"(\s|$)"
        if re.search(pattern, hay):
            return " ".join(w.capitalize() for w in name.split())
    return ""


def _looks_like_league_name(value: str) -> bool:
    low = str(value or "").strip().lower()
    return any(token in low for token in (
        "league", "division", "cup", "premier", "championship", "reserve", "women", "u17", "u19", "u20", "u21", "u22", "u23"
    ))


def country_from_match(match: dict[str, Any]) -> str:
    comp = match.get("competition") if isinstance(match.get("competition"), dict) else {}
    cat = match.get("category") or (comp.get("category") if isinstance(comp, dict) else {}) or {}

    # Prefer explicit country fields. Do not use competition.name here: on IGScore it is usually the league name.
    for obj in (match, comp, cat if isinstance(cat, dict) else {}):
        if not isinstance(obj, dict):
            continue
        for key in ("countryName", "country_name", "country", "countryShortName"):
            value = str(obj.get(key) or "").strip()
            if value and not _is_generic_region(value) and not _looks_like_league_name(value):
                return value

    # category.name can be a real country, but can also be AFRICA/ASIA/AMERICAS.
    if isinstance(cat, dict):
        cat_name = str(cat.get("name") or "").strip()
        if cat_name and not _is_generic_region(cat_name) and not _looks_like_league_name(cat_name):
            return cat_name

    # If IGScore groups by continent, infer country from league/team text.
    inferred = _country_name_from_text(
        match.get("competitionName"),
        comp.get("name") if isinstance(comp, dict) else "",
        match.get("league"),
        match.get("tournament"),
        team_name(match.get("homeTeam")),
        team_name(match.get("awayTeam")),
    )
    if inferred:
        return inferred

    raw_country = str(match.get("country") or "").strip()
    if raw_country and not _is_generic_region(raw_country):
        return raw_country
    if isinstance(cat, dict):
        name = str(cat.get("name") or "").strip()
        if name:
            return name
    return "Без страны"


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
    home_team_obj = match.get("homeTeam") if isinstance(match.get("homeTeam"), dict) else {}
    away_team_obj = match.get("awayTeam") if isinstance(match.get("awayTeam"), dict) else {}
    home = team_name(home_team_obj) or str(match.get("team1") or match.get("team1_ru") or "Home").strip()
    away = team_name(away_team_obj) or str(match.get("team2") or match.get("team2_ru") or "Away").strip()
    home_id = team_id(home_team_obj)
    away_id = team_id(away_team_obj)
    home_logo = resolve_team_logo(match, "home", home)
    away_logo = resolve_team_logo(match, "away", away)
    link = str(match.get("link") or igscore_match_url(match.get("matchId") or match.get("id"))).strip()
    mid = public_match_id(match.get("matchId") or match.get("id"), link, home, away)
    sh, sa, score = score_from_match(match)
    minute_value, minute_text, period = minute_from_match(match, server_time)
    country = country_from_match(match)
    league = league_from_match(match)
    league_logo = resolve_competition_logo(match)
    return {
        "id": mid,
        "home": home,
        "away": away,
        "home_id": home_id,
        "away_id": away_id,
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
        "league_logo": league_logo,
        "link": link,
        # Filled after list load by enrich_public_matches_with_stats() in IGScore mode.
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


def _stats_json_default() -> str:
    return json.dumps({}, ensure_ascii=False, separators=(",", ":"))


def _init_live_cache_db() -> None:
    """Create the shared live cache used by the background collector."""
    DATA_DIR.mkdir(exist_ok=True)
    conn = sqlite3.connect(str(LIVE_CACHE_DB), timeout=30)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS live_matches (
                match_id TEXT PRIMARY KEY,
                home TEXT,
                away TEXT,
                home_id TEXT,
                away_id TEXT,
                home_logo TEXT,
                away_logo TEXT,
                score_home INTEGER DEFAULT 0,
                score_away INTEGER DEFAULT 0,
                score TEXT,
                minute INTEGER DEFAULT 0,
                minute_text TEXT,
                period TEXT,
                country TEXT,
                country_code TEXT,
                league TEXT,
                league_logo TEXT,
                link TEXT,
                source TEXT,
                last_seen_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS match_stats (
                match_id TEXT PRIMARY KEY,
                stats_json TEXT NOT NULL,
                updated_at INTEGER NOT NULL
            )
            """
        )
        # Migration for existing installs created before v5.3.
        existing_cols = {r[1] for r in conn.execute("PRAGMA table_info(live_matches)").fetchall()}
        if "home_id" not in existing_cols:
            conn.execute("ALTER TABLE live_matches ADD COLUMN home_id TEXT")
        if "away_id" not in existing_cols:
            conn.execute("ALTER TABLE live_matches ADD COLUMN away_id TEXT")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_live_matches_last_seen ON live_matches(last_seen_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_live_matches_country_league ON live_matches(country, league)")
        conn.commit()
    finally:
        conn.close()


def _collector_state_copy() -> dict[str, Any]:
    with _collector_lock:
        return dict(_collector_state)


def _db_match_from_row(row: sqlite3.Row) -> dict[str, Any]:
    stats: dict[str, Any] = {}
    try:
        stats = json.loads(row["stats_json"] or "{}") or {}
    except Exception:
        stats = {}
    return {
        "id": str(row["match_id"] or ""),
        "home": str(row["home"] or "Home"),
        "away": str(row["away"] or "Away"),
        "home_id": str(row["home_id"] or ""),
        "away_id": str(row["away_id"] or ""),
        "home_logo": str(row["home_logo"] or ""),
        "away_logo": str(row["away_logo"] or ""),
        "score_home": _safe_int(row["score_home"], 0),
        "score_away": _safe_int(row["score_away"], 0),
        "score": str(row["score"] or f"{_safe_int(row['score_home'], 0)}-{_safe_int(row['score_away'], 0)}"),
        "minute": _safe_int(row["minute"], 0),
        "minute_text": str(row["minute_text"] or "LIVE"),
        "period": str(row["period"] or "LIVE"),
        "country": str(row["country"] or "Без страны"),
        "country_code": str(row["country_code"] or ""),
        "league": str(row["league"] or "Без лиги"),
        "league_logo": str(row["league_logo"] or ""),
        "link": str(row["link"] or ""),
        "stats": stats,
        "source": "collector_db",
        "updated_at": _safe_int(row["updated_at"], 0),
        "stats_updated_at": _safe_int(row["stats_updated_at"], 0),
    }


def fetch_live_collector_db() -> list[dict[str, Any]]:
    _init_live_cache_db()
    conn = sqlite3.connect(str(LIVE_CACHE_DB), timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT lm.*, COALESCE(ms.stats_json, '{}') AS stats_json,
                   COALESCE(ms.updated_at, 0) AS stats_updated_at
            FROM live_matches lm
            LEFT JOIN match_stats ms ON ms.match_id = lm.match_id
            ORDER BY lm.country, lm.league, lm.minute DESC, lm.home
            """
        ).fetchall()
        return [_db_match_from_row(row) for row in rows]
    finally:
        conn.close()


def collector_detail_for_match(match_id_value: str) -> dict[str, Any] | None:
    mid = str(match_id_value or "").strip()
    if not mid:
        return None
    try:
        _init_live_cache_db()
        conn = sqlite3.connect(str(LIVE_CACHE_DB), timeout=30)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT lm.*, COALESCE(ms.stats_json, '{}') AS stats_json,
                   COALESCE(ms.updated_at, 0) AS stats_updated_at
            FROM live_matches lm
            LEFT JOIN match_stats ms ON ms.match_id = lm.match_id
            WHERE lm.match_id = ?
            LIMIT 1
            """,
            (mid,),
        ).fetchone()
        if not row:
            return None
        match = _db_match_from_row(row)
        stats_nested = {}
        try:
            stats_flat = json.loads(row["stats_json"] or "{}") or {}
        except Exception:
            stats_flat = {}
        # Detail page expects nested stats; rebuild from flat DB stats.
        for key in ("shots", "on_target", "off_target", "attacks", "dangerous", "corners", "yellow_cards", "red_cards"):
            stats_nested[key] = {
                "home": _safe_int(stats_flat.get(f"{key}_home"), 0),
                "away": _safe_int(stats_flat.get(f"{key}_away"), 0),
            }
        stats_nested["possession"] = {
            "home": _safe_int(stats_flat.get("possession_home"), 50),
            "away": _safe_int(stats_flat.get("possession_away"), 50),
        }
        return {"match": match, "stats": stats_nested}
    except Exception:
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _upsert_live_matches(matches: list[dict[str, Any]]) -> None:
    if not matches:
        return
    _init_live_cache_db()
    now = now_ts()
    conn = sqlite3.connect(str(LIVE_CACHE_DB), timeout=30)
    try:
        for m in matches:
            conn.execute(
                """
                INSERT INTO live_matches(
                    match_id, home, away, home_id, away_id, home_logo, away_logo,
                    score_home, score_away, score, minute, minute_text, period,
                    country, country_code, league, league_logo, link, source,
                    last_seen_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(match_id) DO UPDATE SET
                    home=excluded.home,
                    away=excluded.away,
                    home_id=COALESCE(NULLIF(excluded.home_id, ''), live_matches.home_id),
                    away_id=COALESCE(NULLIF(excluded.away_id, ''), live_matches.away_id),
                    home_logo=COALESCE(NULLIF(excluded.home_logo, ''), live_matches.home_logo),
                    away_logo=COALESCE(NULLIF(excluded.away_logo, ''), live_matches.away_logo),
                    score_home=excluded.score_home,
                    score_away=excluded.score_away,
                    score=excluded.score,
                    minute=excluded.minute,
                    minute_text=excluded.minute_text,
                    period=excluded.period,
                    country=excluded.country,
                    country_code=excluded.country_code,
                    league=excluded.league,
                    league_logo=COALESCE(NULLIF(excluded.league_logo, ''), live_matches.league_logo),
                    link=excluded.link,
                    source=excluded.source,
                    last_seen_at=excluded.last_seen_at,
                    updated_at=excluded.updated_at
                """,
                (
                    str(m.get("id") or ""), str(m.get("home") or ""), str(m.get("away") or ""),
                    str(m.get("home_id") or ""), str(m.get("away_id") or ""),
                    str(m.get("home_logo") or ""), str(m.get("away_logo") or ""),
                    _safe_int(m.get("score_home"), 0), _safe_int(m.get("score_away"), 0), str(m.get("score") or "0-0"),
                    _safe_int(m.get("minute"), 0), str(m.get("minute_text") or "LIVE"), str(m.get("period") or "LIVE"),
                    str(m.get("country") or "Без страны"), str(m.get("country_code") or ""),
                    str(m.get("league") or "Без лиги"), str(m.get("league_logo") or ""),
                    str(m.get("link") or ""), "collector_db", now, now,
                ),
            )
        conn.commit()
    finally:
        conn.close()


def _update_match_stats(match_id_value: str, stats_flat: dict[str, Any]) -> None:
    mid = str(match_id_value or "").strip()
    if not mid or not stats_flat:
        return
    _init_live_cache_db()
    conn = sqlite3.connect(str(LIVE_CACHE_DB), timeout=30)
    try:
        conn.execute(
            """
            INSERT INTO match_stats(match_id, stats_json, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(match_id) DO UPDATE SET
                stats_json=excluded.stats_json,
                updated_at=excluded.updated_at
            """,
            (mid, json.dumps(stats_flat, ensure_ascii=False, separators=(",", ":")), now_ts()),
        )
        conn.commit()
    finally:
        conn.close()


def _cleanup_finished_matches() -> int:
    if not DELETE_FINISHED_MATCHES:
        return 0
    _init_live_cache_db()
    cutoff = now_ts() - max(0, FINISHED_MATCH_GRACE_SECONDS)
    conn = sqlite3.connect(str(LIVE_CACHE_DB), timeout=30)
    try:
        ids = [r[0] for r in conn.execute("SELECT match_id FROM live_matches WHERE last_seen_at < ?", (cutoff,)).fetchall()]
        if not ids:
            return 0
        conn.executemany("DELETE FROM match_stats WHERE match_id = ?", [(mid,) for mid in ids])
        conn.executemany("DELETE FROM live_matches WHERE match_id = ?", [(mid,) for mid in ids])
        conn.commit()
        return len(ids)
    finally:
        conn.close()


def _collector_worker(items: list[dict[str, Any]]) -> int:
    updated = 0
    for item in items:
        mid = str(item.get("id") or "").strip()
        if not mid:
            continue
        if COLLECTOR_REQUEST_DELAY > 0:
            time.sleep(COLLECTOR_REQUEST_DELAY)
        try:
            stats_flat = flat_stats(stat_pairs_from_response(match_statistics(mid)))
            if stats_flat:
                _update_match_stats(mid, stats_flat)
                updated += 1
        except Exception as exc:
            print(f"[collector] stats failed {mid}: {type(exc).__name__}: {exc}")
    return updated


def collector_update_once() -> dict[str, Any]:
    """Refresh live list + stats once. This is the only code path that hits IGScore in collector mode."""
    started = now_ts()
    with _collector_lock:
        _collector_state.update({"running": True, "last_start": started, "last_error": ""})
    stats_updated = 0
    matches: list[dict[str, Any]] = []
    try:
        response = competition_list()
        server_time = int(response.get("server_time") or time.time())
        for match in iter_competition_matches(response):
            status = _safe_int(match.get("matchStatus") or match.get("statusId"), 0)
            if status not in LIVE_STATUSES:
                continue
            match["server_time"] = server_time
            matches.append(to_public_match(match, server_time=server_time, source="collector_db"))
        matches.sort(key=lambda x: (str(x.get("country") or ""), str(x.get("league") or ""), -int(x.get("minute") or 0)))
        _upsert_live_matches(matches)

        targets = [m for m in matches[:max(0, COLLECTOR_MAX_MATCHES)] if m.get("id")]
        if targets:
            workers = min(max(1, COLLECTOR_WORKERS), len(targets))
            if workers <= 1:
                stats_updated = _collector_worker(targets)
            else:
                buckets = [targets[i::workers] for i in range(workers)]
                with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
                    stats_updated = sum(pool.map(_collector_worker, buckets))
        removed = _cleanup_finished_matches()
        finished = now_ts()
        with _collector_lock:
            _collector_state.update({
                "running": False,
                "last_finish": finished,
                "last_error": "",
                "last_matches": len(matches),
                "last_stats": stats_updated,
                "last_removed": removed,
            })
        # Drop /api/live memory cache so users see the DB refresh quickly.
        with _cache_lock:
            _live_cache.update({"saved_at": 0.0, "payload": None, "raw": {}})
        print(f"[collector] refreshed matches={len(matches)} stats={stats_updated} removed={removed} seconds={finished-started}")
        return _collector_state_copy()
    except Exception as exc:
        finished = now_ts()
        err = f"{type(exc).__name__}: {exc}"
        with _collector_lock:
            _collector_state.update({
                "running": False,
                "last_finish": finished,
                "last_error": err,
                "last_matches": len(matches),
                "last_stats": stats_updated,
            })
        print(f"[collector] update failed: {err}")
        return _collector_state_copy()


def _collector_loop() -> None:
    # Start immediately, then keep refreshing every COLLECTOR_INTERVAL seconds.
    while True:
        t0 = time.time()
        collector_update_once()
        elapsed = time.time() - t0
        jitter = random.uniform(0, max(0.0, COLLECTOR_JITTER)) if COLLECTOR_JITTER > 0 else 0.0
        sleep_for = max(5.0, COLLECTOR_INTERVAL + jitter - elapsed)
        time.sleep(sleep_for)


def start_collector_worker() -> None:
    global _collector_thread_started
    if not COLLECTOR_ENABLED or DATA_MODE in {"demo", "sqlite"}:
        return
    _init_live_cache_db()
    with _collector_lock:
        if _collector_thread_started:
            return
        _collector_thread_started = True
    thread = threading.Thread(target=_collector_loop, name="live-collector", daemon=True)
    thread.start()
    print(
        f"[collector] enabled interval={COLLECTOR_INTERVAL}s max_matches={COLLECTOR_MAX_MATCHES} "
        f"workers={COLLECTOR_WORKERS} delay={COLLECTOR_REQUEST_DELAY}s db={LIVE_CACHE_DB}"
    )


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

    enrich_public_matches_with_stats(public)
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
        "home_id": "",
        "away_id": "",
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
        "league_logo": "",
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
            "off_target_home": 8, "off_target_away": 4, "off_target_total": 12,
            "dangerous_home": 42, "dangerous_away": 33, "dangerous_total": 75,
            "attacks_home": 101, "attacks_away": 88, "attacks_total": 189,
            "corners_home": 5, "corners_away": 3, "corners_total": 8,
            "yellow_cards_home": 1, "yellow_cards_away": 2, "yellow_cards_total": 3,
            "red_cards_home": 0, "red_cards_away": 0, "red_cards_total": 0,
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
            "off_target_home": 6, "off_target_away": 3, "off_target_total": 9,
            "dangerous_home": 37, "dangerous_away": 24, "dangerous_total": 61,
            "attacks_home": 72, "attacks_away": 54, "attacks_total": 126,
            "corners_home": 4, "corners_away": 2, "corners_total": 6,
            "yellow_cards_home": 0, "yellow_cards_away": 1, "yellow_cards_total": 1,
            "red_cards_home": 0, "red_cards_away": 0, "red_cards_total": 0,
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
            "off_target_home": 10, "off_target_away": 8, "off_target_total": 18,
            "dangerous_home": 64, "dangerous_away": 51, "dangerous_total": 115,
            "attacks_home": 113, "attacks_away": 99, "attacks_total": 212,
            "corners_home": 7, "corners_away": 4, "corners_total": 11,
            "yellow_cards_home": 2, "yellow_cards_away": 3, "yellow_cards_total": 5,
            "red_cards_home": 0, "red_cards_away": 1, "red_cards_total": 1,
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

    if mode in {"auto", "igscore"} and COLLECTOR_ENABLED and LIVE_FROM_DB_ONLY:
        try:
            matches = fetch_live_collector_db()
            payload = build_payload(matches, "collector_db")
            payload["collector"] = _collector_state_copy()
            with _cache_lock:
                _live_cache.update({"saved_at": now, "payload": payload, "raw": {}})
            return payload
        except Exception as exc:
            errors.append(f"collector_db: {type(exc).__name__}: {exc}")
            if mode == "igscore":
                payload = build_payload([], "collector_db", "; ".join(errors))
                payload["collector"] = _collector_state_copy()
                with _cache_lock:
                    _live_cache.update({"saved_at": now, "payload": payload, "raw": {}})
                return payload

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

    on_target = by_type.get(21, (0, 0))
    off_target = by_type.get(22, (0, 0))
    shots_home = on_target[0] + off_target[0]
    shots_away = on_target[1] + off_target[1]

    # IGScore type map used here:
    # 25 possession, 2 corners, 3 yellow cards, 4 red cards,
    # 21 shots on target, 22 shots off target, 23 attacks, 24 dangerous attacks.
    return {
        "possession": {"home": by_type.get(25, (50, 50))[0], "away": by_type.get(25, (50, 50))[1]},
        "shots": {"home": shots_home, "away": shots_away},
        "on_target": {"home": on_target[0], "away": on_target[1]},
        "off_target": {"home": off_target[0], "away": off_target[1]},
        "attacks": {"home": by_type.get(23, (0, 0))[0], "away": by_type.get(23, (0, 0))[1]},
        "dangerous": {"home": by_type.get(24, (0, 0))[0], "away": by_type.get(24, (0, 0))[1]},
        "corners": {"home": by_type.get(2, (0, 0))[0], "away": by_type.get(2, (0, 0))[1]},
        "yellow_cards": {"home": by_type.get(3, (0, 0))[0], "away": by_type.get(3, (0, 0))[1]},
        "red_cards": {"home": by_type.get(4, (0, 0))[0], "away": by_type.get(4, (0, 0))[1]},
    }


def flat_stats(stats: dict[str, Any]) -> dict[str, int]:
    """Convert nested detail stats to flat keys used by feed filters."""
    out: dict[str, int] = {}
    if not isinstance(stats, dict):
        return out

    for key in (
        "shots", "on_target", "off_target", "attacks", "dangerous",
        "corners", "yellow_cards", "red_cards",
    ):
        pair = stats.get(key) or {}
        if isinstance(pair, dict):
            home = _safe_int(pair.get("home"), 0)
            away = _safe_int(pair.get("away"), 0)
        else:
            home = away = 0
        out[f"{key}_home"] = home
        out[f"{key}_away"] = away
        out[f"{key}_total"] = home + away

    poss = stats.get("possession") or {}
    if isinstance(poss, dict):
        out["possession_home"] = _safe_int(poss.get("home"), 50)
        out["possession_away"] = _safe_int(poss.get("away"), 50)
    return out


def match_stats_cached(match_id_value: str) -> dict[str, dict[str, int]]:
    """Fetch/cached IGScore stats for a match. Used both by detail and by feed."""
    mid = str(match_id_value or "").strip()
    if not mid:
        return {}
    cached = _stats_cache.get(mid)
    if cached and time.time() - float(cached.get("saved_at") or 0) < STAT_CACHE_SECONDS:
        return cached.get("stats") or {}
    stats = stat_pairs_from_response(match_statistics(mid))
    _stats_cache[mid] = {"saved_at": time.time(), "stats": stats}
    return stats


def enrich_public_matches_with_stats(public: list[dict[str, Any]]) -> None:
    """Attach flat live stats to matches in /api/live so filters can use them."""
    if not LIVE_STATS_IN_FEED or not public:
        return
    targets = [m for m in public[:max(0, LIVE_STATS_MAX_MATCHES)] if m.get("id")]
    if not targets:
        return

    def _load(item: dict[str, Any]) -> tuple[str, dict[str, int]]:
        mid = str(item.get("id") or "")
        try:
            return mid, flat_stats(match_stats_cached(mid))
        except Exception:
            return mid, {}

    workers = min(LIVE_STATS_WORKERS, max(1, len(targets)))
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        future_map = {pool.submit(_load, item): item for item in targets}
        for fut in concurrent.futures.as_completed(future_map):
            item = future_map[fut]
            try:
                _mid, stats = fut.result()
            except Exception:
                stats = {}
            if stats:
                item["stats"] = stats


def _avg_empty() -> dict[str, Any]:
    return {
        "avg": None,
        "total_avg": None,
        "first_half_avg": None,
        "second_half_avg": None,
        "scored_avg": None,
        "conceded_avg": None,
        "count": 0,
        "zero_zero": 0,
        "total_sum": 0,
        "first_half_sum": 0,
        "second_half_sum": 0,
        "scored_sum": 0,
        "conceded_sum": 0,
    }


def _score_list(match: dict[str, Any], side: str) -> list[int]:
    key = "homeScores" if side == "home" else "awayScores"
    values = match.get(key) if isinstance(match.get(key), list) else []
    return [_safe_int(v, 0) for v in values]


def _first_half_pair(match: dict[str, Any]) -> tuple[int | None, int | None]:
    explicit_pairs = [
        ("homeHalfScore", "awayHalfScore"),
        ("homeHtScore", "awayHtScore"),
        ("htHomeScore", "htAwayScore"),
        ("halfTimeHomeScore", "halfTimeAwayScore"),
        ("homeFirstHalfScore", "awayFirstHalfScore"),
        ("firstHalfHomeScore", "firstHalfAwayScore"),
    ]
    for hk, ak in explicit_pairs:
        if hk in match and ak in match:
            return _safe_int(match.get(hk), 0), _safe_int(match.get(ak), 0)

    hs = _score_list(match, "home")
    a_s = _score_list(match, "away")
    # IGScore commonly returns [full, first half, second half, ...].
    if len(hs) > 1 and len(a_s) > 1:
        return _safe_int(hs[1], 0), _safe_int(a_s[1], 0)
    return None, None


def _second_half_pair(match: dict[str, Any], total_home: int, total_away: int, first_home: int | None, first_away: int | None) -> tuple[int | None, int | None]:
    hs = _score_list(match, "home")
    a_s = _score_list(match, "away")
    if len(hs) > 2 and len(a_s) > 2:
        return _safe_int(hs[2], 0), _safe_int(a_s[2], 0)
    if first_home is not None and first_away is not None:
        return max(0, total_home - first_home), max(0, total_away - first_away)
    return None, None


def avg_total_for_team(team_id_value: str) -> dict[str, Any]:
    """Average recent goals/total profile for one team over up to 10 last matches."""
    if not team_id_value:
        return _avg_empty()
    try:
        resp = team_recent(team_id_value, size=10)
        totals: list[int] = []
        first_half_totals: list[int] = []
        second_half_totals: list[int] = []
        scored: list[int] = []
        conceded: list[int] = []
        zero_zero = 0
        team_id_norm = str(team_id_value)
        for match in iter_result_matches(resp):
            h, a, _score = score_from_match(match)
            total = h + a
            totals.append(total)
            if h == 0 and a == 0:
                zero_zero += 1

            home_id = team_id(match.get("homeTeam"))
            away_id = team_id(match.get("awayTeam"))
            if team_id_norm and team_id_norm == home_id:
                scored.append(h)
                conceded.append(a)
            elif team_id_norm and team_id_norm == away_id:
                scored.append(a)
                conceded.append(h)
            else:
                # Fallback when IDs are absent: team_recent should still only return this team's matches.
                scored.append(h)
                conceded.append(a)

            fh, fa = _first_half_pair(match)
            sh, sa = _second_half_pair(match, h, a, fh, fa)
            if fh is not None and fa is not None:
                first_half_totals.append(fh + fa)
            if sh is not None and sa is not None:
                second_half_totals.append(sh + sa)

        total_avg = round(sum(totals) / len(totals), 2) if totals else None
        first_avg = round(sum(first_half_totals) / len(first_half_totals), 2) if first_half_totals else None
        second_avg = round(sum(second_half_totals) / len(second_half_totals), 2) if second_half_totals else None
        scored_avg = round(sum(scored) / len(scored), 2) if scored else None
        conceded_avg = round(sum(conceded) / len(conceded), 2) if conceded else None
        return {
            "avg": total_avg,
            "total_avg": total_avg,
            "first_half_avg": first_avg,
            "second_half_avg": second_avg,
            "scored_avg": scored_avg,
            "conceded_avg": conceded_avg,
            "count": len(totals),
            "zero_zero": zero_zero,
            "total_sum": sum(totals),
            "first_half_sum": sum(first_half_totals),
            "second_half_sum": sum(second_half_totals),
            "scored_sum": sum(scored),
            "conceded_sum": sum(conceded),
        }
    except Exception:
        return _avg_empty()


def avg_payload_for_match(match_id_value: str) -> dict[str, Any]:
    mid = str(match_id_value or "").strip()
    if not mid:
        return {"ok": False, "error": "missing id"}

    # Demo keeps the UI testable without network.
    for demo_match in DEMO_MATCHES:
        if str(demo_match.get("id")) == mid:
            d = demo_detail(demo_match)
            return {"ok": True, "match_id": mid, "avg": d.get("avg") or {"home": _avg_empty(), "away": _avg_empty()}}

    home_id = ""
    away_id = ""

    detail = collector_detail_for_match(mid)
    if detail:
        m = detail.get("match") or {}
        home_id = str(m.get("home_id") or "")
        away_id = str(m.get("away_id") or "")

    if not home_id and not away_id:
        live_payload = load_live_payload(force=False)
        m = next((x for x in flatten_payload(live_payload) if str(x.get("id")) == mid), None)
        if m:
            home_id = str(m.get("home_id") or "")
            away_id = str(m.get("away_id") or "")

    if not home_id and not away_id:
        with _cache_lock:
            raw = (_live_cache.get("raw") or {}).get(mid) or {}
        home_id = team_id(raw.get("homeTeam"))
        away_id = team_id(raw.get("awayTeam"))

    return {
        "ok": True,
        "match_id": mid,
        "avg": {
            "home": avg_total_for_team(home_id),
            "away": avg_total_for_team(away_id),
        },
    }


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
                    "off_target": {"home": max(0, _safe_int(row["shots_home"], 0) - _safe_int(row["on_target_home"], 0)), "away": max(0, _safe_int(row["shots_away"], 0) - _safe_int(row["on_target_away"], 0))},
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
            "off_target": {"home": 8, "away": 4},
            "attacks": {"home": 101, "away": 88},
            "dangerous": {"home": 42, "away": 33},
            "corners": {"home": 5, "away": 3},
            "yellow_cards": {"home": 1, "away": 2},
            "red_cards": {"home": 0, "away": 0},
            "avg": {"home": {"avg": 3.1, "total_avg": 3.1, "first_half_avg": 1.2, "second_half_avg": 1.9, "scored_avg": 1.7, "conceded_avg": 1.4, "count": 10, "zero_zero": 1, "total_sum": 31, "first_half_sum": 12, "second_half_sum": 19, "scored_sum": 17, "conceded_sum": 14}, "away": {"avg": 2.8, "total_avg": 2.8, "first_half_avg": 1.0, "second_half_avg": 1.8, "scored_avg": 1.3, "conceded_avg": 1.5, "count": 10, "zero_zero": 2, "total_sum": 28, "first_half_sum": 10, "second_half_sum": 18, "scored_sum": 13, "conceded_sum": 15}},
        },
        "demo-mci-che": {
            "possession": {"home": 61, "away": 39},
            "shots": {"home": 9, "away": 5},
            "on_target": {"home": 3, "away": 2},
            "off_target": {"home": 6, "away": 3},
            "attacks": {"home": 72, "away": 54},
            "dangerous": {"home": 37, "away": 24},
            "corners": {"home": 4, "away": 2},
            "yellow_cards": {"home": 0, "away": 1},
            "red_cards": {"home": 0, "away": 0},
            "avg": {"home": {"avg": 3.2, "total_avg": 3.2, "first_half_avg": 1.4, "second_half_avg": 1.8, "scored_avg": 2.0, "conceded_avg": 1.2, "count": 10, "zero_zero": 0, "total_sum": 32, "first_half_sum": 14, "second_half_sum": 18, "scored_sum": 20, "conceded_sum": 12}, "away": {"avg": 2.4, "total_avg": 2.4, "first_half_avg": 0.9, "second_half_avg": 1.5, "scored_avg": 1.0, "conceded_avg": 1.4, "count": 10, "zero_zero": 2, "total_sum": 24, "first_half_sum": 9, "second_half_sum": 15, "scored_sum": 10, "conceded_sum": 14}},
        },
    }
    p = presets.get(match.get("id"), presets["demo-liv-ars"])
    return {"match": match, "stats": {k: v for k, v in p.items() if k != "avg"}, "avg": p["avg"]}


def detail_payload(match_id_value: str) -> dict[str, Any]:
    live_payload = load_live_payload(force=False)
    matches = flatten_payload(live_payload)
    match = next((m for m in matches if str(m.get("id")) == str(match_id_value)), None)

    if not match:
        collector_detail = collector_detail_for_match(match_id_value)
        if collector_detail:
            collector_detail.setdefault("avg", {"home": _avg_empty(), "away": _avg_empty()})
            return {"ok": True, **collector_detail}
        db_detail = sqlite_detail_for_match(match_id_value)
        if db_detail:
            db_detail.setdefault("avg", {"home": _avg_empty(), "away": _avg_empty()})
            return {"ok": True, **db_detail}
        match = DEMO_MATCHES[0]

    if str(match.get("source")) == "demo":
        return {"ok": True, **demo_detail(match)}

    # Collector DB match from /api/live
    if str(match.get("source")) == "collector_db":
        collector_detail = collector_detail_for_match(str(match.get("id")))
        if collector_detail:
            collector_detail.setdefault("avg", {"home": _avg_empty(), "away": _avg_empty()})
            return {"ok": True, **collector_detail}

    # SQLite match from /api/live
    if str(match.get("source")) == "sqlite":
        db_detail = sqlite_detail_for_match(str(match.get("id")))
        if db_detail:
            db_detail.setdefault("avg", {"home": _avg_empty(), "away": _avg_empty()})
            return {"ok": True, **db_detail}

    raw = {}
    with _cache_lock:
        raw = (_live_cache.get("raw") or {}).get(str(match.get("id"))) or {}

    stats: dict[str, dict[str, int]] = {}
    avg = {"home": _avg_empty(), "away": _avg_empty()}

    mid = str(match.get("id") or "")
    if mid:
        try:
            stats = match_stats_cached(mid)
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
_notify_matches: dict[str, list[dict[str, Any]]] = {}  # chat_id_str → found matches


def _load_notify_matches() -> None:
    global _notify_matches
    try:
        if NOTIFY_MATCHES_DB.exists():
            _notify_matches = json.loads(NOTIFY_MATCHES_DB.read_text("utf-8") or "{}") or {}
        else:
            _notify_matches = {}
    except Exception as exc:
        print(f"[notify] matches load failed: {exc}")
        _notify_matches = {}


def _save_notify_matches() -> None:
    try:
        NOTIFY_MATCHES_DB.parent.mkdir(parents=True, exist_ok=True)
        NOTIFY_MATCHES_DB.write_text(json.dumps(_notify_matches, ensure_ascii=False), encoding="utf-8")
    except Exception as exc:
        print(f"[notify] matches save failed: {exc}")


def _notify_match_record(m: dict[str, Any], ts: int | None = None) -> dict[str, Any]:
    stats = m.get("stats") if isinstance(m.get("stats"), dict) else {}
    return {
        "id": str(m.get("id") or ""),
        "home": str(m.get("home") or ""),
        "away": str(m.get("away") or ""),
        "home_logo": str(m.get("home_logo") or ""),
        "away_logo": str(m.get("away_logo") or ""),
        "score_home": _safe_int(m.get("score_home"), 0),
        "score_away": _safe_int(m.get("score_away"), 0),
        "score": str(m.get("score") or f"{_safe_int(m.get('score_home'), 0)}-{_safe_int(m.get('score_away'), 0)}"),
        "minute": _safe_int(m.get("minute"), 0),
        "minute_text": str(m.get("minute_text") or ""),
        "country": str(m.get("country") or ""),
        "country_code": str(m.get("country_code") or ""),
        "league": str(m.get("league") or ""),
        "league_logo": str(m.get("league_logo") or ""),
        "stats": stats,
        "alert_kind": str(m.get("alert_kind") or ""),
        "alert_title": str(m.get("alert_title") or ""),
        "alert_subtitle": str(m.get("alert_subtitle") or ""),
        "found_at": int(ts or time.time()),
    }


def _store_notify_match(chat_id: str | int, m: dict[str, Any], ts: int | None = None) -> None:
    chat_key = str(chat_id)
    if not chat_key:
        return
    rec = _notify_match_record(m, ts=ts)
    if not rec.get("id"):
        return
    items = _notify_matches.setdefault(chat_key, [])
    # Replace existing record for the same match, then put it at the top.
    items[:] = [x for x in items if str(x.get("id")) != str(rec.get("id"))]
    items.insert(0, rec)
    del items[NOTIFY_MATCHES_MAX_PER_USER:]


def _notification_text_for_match(m: dict[str, Any]) -> str:
    home = html.escape(str(m.get("home") or "Home"))
    away = html.escape(str(m.get("away") or "Away"))
    return f"🔔 Матч нашёлся: <b>{home}</b> — <b>{away}</b>"


def _goal_notification_text_for_match(m: dict[str, Any]) -> str:
    home = html.escape(str(m.get("home") or "Home"))
    away = html.escape(str(m.get("away") or "Away"))
    sh = _safe_int(m.get("score_home"), 0)
    sa = _safe_int(m.get("score_away"), 0)
    minute = html.escape(str(m.get("minute_text") or ""))
    minute_part = f" · {minute}" if minute else ""
    return f"⚽ Гол! <b>{home}</b> {sh}-{sa} <b>{away}</b>{minute_part}"


def _goal_total_for_match(m: dict[str, Any]) -> int:
    return _safe_int(m.get("score_home"), 0) + _safe_int(m.get("score_away"), 0)


def _goal_score_for_match(m: dict[str, Any]) -> str:
    return f"{_safe_int(m.get('score_home'), 0)}-{_safe_int(m.get('score_away'), 0)}"


def _chat_id_from_init_data(init_data: str) -> str:
    user = verify_init_data(init_data)
    if not user:
        return ""
    chat_id = str(user.get("id") or "")
    return "" if chat_id == "None" else chat_id


def _find_live_match(match_id: str) -> dict[str, Any] | None:
    mid = str(match_id or "").strip()
    if not mid:
        return None
    live = load_live_payload(force=False)
    return next((m for m in flatten_payload(live) if str(m.get("id")) == mid), None)


def handle_notify_matches(init_data: str) -> dict[str, Any]:
    chat_id = _chat_id_from_init_data(init_data)
    if not chat_id:
        if BOT_TOKEN:
            return {"ok": False, "error": "init_data verification failed", "items": []}
        chat_id = "local"
    with _notify_lock:
        items = list(_notify_matches.get(chat_id) or [])
    return {"ok": True, "chat_id": chat_id, "items": items, "total": len(items)}


def handle_notify_matches_clear(body: dict[str, Any]) -> dict[str, Any]:
    chat_id = _chat_id_from_init_data(str(body.get("init_data") or ""))
    if not chat_id:
        if BOT_TOKEN:
            return {"ok": False, "error": "init_data verification failed"}
        chat_id = "local"
    with _notify_lock:
        _notify_matches[chat_id] = []
        _save_notify_matches()
    return {"ok": True, "chat_id": chat_id}


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
    if _safe_int(f.get("off_target_min")) and _tot("off_target") < _safe_int(f.get("off_target_min")): return False
    if _safe_int(f.get("dangerous_min")) and _tot("dangerous") < _safe_int(f.get("dangerous_min")): return False
    if _safe_int(f.get("attacks_min")) and _tot("attacks") < _safe_int(f.get("attacks_min")): return False
    if _safe_int(f.get("corners_min")) and _tot("corners") < _safe_int(f.get("corners_min")): return False
    if _safe_int(f.get("yellow_cards_min")) and _tot("yellow_cards") < _safe_int(f.get("yellow_cards_min")): return False
    if _safe_int(f.get("red_cards_min")) and _tot("red_cards") < _safe_int(f.get("red_cards_min")): return False
    if _safe_int(f.get("possession_min")):
        poss = max(_safe_int(s.get("possession_home"), 50), _safe_int(s.get("possession_away"), 50))
        if poss < _safe_int(f.get("possession_min")):
            return False
    scores = f.get("scores") or []
    if scores and m.get("score") not in scores:
        return False
    countries = f.get("countries") or []
    if countries and m.get("country") not in countries:
        return False
    return True


def notify_worker_loop() -> None:
    """Background loop: check subscriptions and save found matches for the app."""
    while True:
        try:
            time.sleep(NOTIFY_POLL_INTERVAL)
            if not _notify_subs:
                continue
            live = load_live_payload(force=False)
            matches = flatten_payload(live)
            now = time.time()
            changed = False
            with _notify_lock:
                match_by_id = {str(m.get("id") or ""): m for m in matches if str(m.get("id") or "")}
                for chat_id_str, sub in list(_notify_subs.items()):
                    cfg = sub.get("filter") or {}
                    seen = sub.setdefault("seen", {})

                    if cfg.get("enabled"):
                        for m in matches:
                            if not match_passes_filter(m, cfg):
                                continue
                            mid = str(m.get("id") or "")
                            if not mid:
                                continue
                            prev = float(seen.get(mid) or 0)
                            if now - prev < NOTIFY_COOLDOWN_PER_MATCH:
                                continue
                            _store_notify_match(chat_id_str, m, ts=int(now))
                            # Telegram push is intentionally short: details are visible only in the app.
                            if BOT_TOKEN:
                                ok = send_telegram_message(chat_id_str, _notification_text_for_match(m), link="")
                            else:
                                ok = True
                            if ok:
                                seen[mid] = now
                                changed = True

                    goal_alerts = sub.setdefault("goal_alerts", {})
                    for mid, alert in list(goal_alerts.items()):
                        m = match_by_id.get(str(mid))
                        if not m:
                            continue
                        current_total = _goal_total_for_match(m)
                        current_score = _goal_score_for_match(m)
                        last_total = _safe_int(alert.get("last_total"), current_total)
                        if current_total > last_total:
                            goal_match = dict(m)
                            goal_match["alert_kind"] = "goal"
                            goal_match["alert_title"] = f"⚽ Гол! {m.get('home') or ''} {current_score} {m.get('away') or ''}"
                            goal_match["alert_subtitle"] = f"{m.get('minute_text') or ''} · {m.get('country') or ''} · {m.get('league') or ''}"
                            _store_notify_match(chat_id_str, goal_match, ts=int(now))
                            if BOT_TOKEN:
                                ok = send_telegram_message(chat_id_str, _goal_notification_text_for_match(m), link="")
                            else:
                                ok = True
                            if ok:
                                changed = True
                        if current_total != last_total or str(alert.get("last_score") or "") != current_score:
                            alert["last_total"] = current_total
                            alert["last_score"] = current_score
                            alert["updated_at"] = int(now)
                            changed = True
                cutoff = now - 6 * 3600
                for sub in _notify_subs.values():
                    seen = sub.get("seen") or {}
                    for k in list(seen.keys()):
                        if seen[k] < cutoff:
                            seen.pop(k, None)
                            changed = True
                if changed:
                    _save_notify_subs()
                    _save_notify_matches()
        except Exception as exc:
            print(f"[notify] worker error: {exc}")
            traceback.print_exc()

def start_notify_worker() -> None:
    t = threading.Thread(target=notify_worker_loop, daemon=True, name="notify-worker")
    t.start()


def handle_goal_subscribe(body: dict[str, Any]) -> dict[str, Any]:
    """Subscribe/unsubscribe one match for goal-only alerts after the user presses ★."""
    init_data = str(body.get("init_data") or "")
    match_id = str(body.get("match_id") or "").strip()
    enabled = bool(body.get("enabled"))
    chat_id = _chat_id_from_init_data(init_data)
    if not chat_id:
        if BOT_TOKEN:
            return {"ok": False, "error": "init_data verification failed"}
        chat_id = "local"
    if not match_id:
        return {"ok": False, "error": "missing match_id"}

    with _notify_lock:
        sub = _notify_subs.setdefault(chat_id, {"chat_id": chat_id, "filter": {}, "seen": {}, "goal_alerts": {}})
        goal_alerts = sub.setdefault("goal_alerts", {})
        if enabled:
            m = _find_live_match(match_id)
            if not m:
                return {"ok": False, "error": "match not found"}
            goal_alerts[match_id] = {
                "match_id": match_id,
                "last_score": _goal_score_for_match(m),
                "last_total": _goal_total_for_match(m),
                "home": str(m.get("home") or ""),
                "away": str(m.get("away") or ""),
                "enabled_at": int(time.time()),
            }
        else:
            goal_alerts.pop(match_id, None)
        sub["updated_at"] = int(time.time())
        _notify_subs[chat_id] = sub
        _save_notify_subs()
    return {"ok": True, "chat_id": chat_id, "match_id": match_id, "enabled": enabled}


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
    """Immediate push from the open Mini App. Stores the found match and sends a short Telegram message."""
    init_data = str(body.get("init_data") or "")
    match_id = str(body.get("match_id") or "").strip()
    chat_id = _chat_id_from_init_data(init_data)
    if not chat_id:
        if BOT_TOKEN:
            return {"ok": False, "error": "init_data verification failed"}
        chat_id = "local"

    m = _find_live_match(match_id) if match_id else None
    if not m:
        text = str(body.get("text") or "").strip()
        if not text:
            return {"ok": False, "error": "missing match_id"}
        ok = send_telegram_message(chat_id, text, link="") if BOT_TOKEN and chat_id != "local" else True
        return {"ok": ok, "stored": False}

    now = time.time()
    with _notify_lock:
        sub = _notify_subs.setdefault(chat_id, {"chat_id": chat_id, "filter": {}, "seen": {}})
        seen = sub.setdefault("seen", {})
        prev = float(seen.get(match_id) or 0)
        duplicate = (now - prev) < NOTIFY_COOLDOWN_PER_MATCH
        _store_notify_match(chat_id, m, ts=int(now))
        if not duplicate:
            seen[match_id] = now
        _save_notify_subs()
        _save_notify_matches()

    ok = True
    if not duplicate and BOT_TOKEN and chat_id != "local":
        ok = send_telegram_message(chat_id, _notification_text_for_match(m), link="")
    return {"ok": ok, "stored": True, "duplicate": duplicate, "match": _notify_match_record(m, ts=int(now))}

def parse_json_body(handler: BaseHTTPRequestHandler) -> dict[str, Any]:
    try:
        length = int(handler.headers.get("Content-Length") or 0)
        if not length:
            return {}
        raw = handler.rfile.read(length)
        return json.loads(raw.decode("utf-8"))
    except Exception:
        return {}


def _collect_asset_fields(obj: Any, prefix: str = "", depth: int = 0, out: list[dict[str, str]] | None = None) -> list[dict[str, str]]:
    out = out if out is not None else []
    if depth > 5:
        return out
    if isinstance(obj, dict):
        for key, value in obj.items():
            path = f"{prefix}.{key}" if prefix else str(key)
            lk = str(key).lower()
            if isinstance(value, str) and any(t in lk for t in ("logo", "flag", "icon", "image", "img", "crest", "badge")):
                out.append({"path": path, "value": value, "normalized": _normalize_logo_url(value)})
            elif isinstance(value, (dict, list)):
                _collect_asset_fields(value, path, depth + 1, out)
    elif isinstance(obj, list):
        for i, item in enumerate(obj[:20]):
            _collect_asset_fields(item, f"{prefix}[{i}]", depth + 1, out)
    return out


def debug_league_assets(limit: int = 80) -> dict[str, Any]:
    """Inspect IGScore raw live competition objects to discover logo/flag fields."""
    try:
        response = competition_list()
        result = response.get("result") if isinstance(response, dict) else {}
        comps = (result or {}).get("competitions") if isinstance(result, dict) else []
        rows = []
        for comp in comps[:max(1, min(int(limit), 300))]:
            if not isinstance(comp, dict):
                continue
            fake_match = {
                "competition": {
                    "id": comp.get("competitionId"),
                    "name": comp.get("competitionName"),
                    "logo": comp.get("logo"),
                    "category": comp.get("category"),
                    "additionalCompetitionName": comp.get("additionalCompetitionName"),
                },
                "competitionName": comp.get("competitionName"),
                "category": comp.get("category"),
            }
            country_guess = country_from_match(fake_match)
            rows.append({
                "competition_id": comp.get("competitionId"),
                "competition_name": comp.get("competitionName"),
                "country_guess": country_guess,
                "country_code": country_code(country_guess),
                "league_logo": resolve_competition_logo(fake_match),
                "asset_fields": _collect_asset_fields(comp),
                "category": comp.get("category"),
            })
        return {"ok": True, "total_competitions": len(comps or []), "items": rows}
    except Exception as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


class MiniAppHandler(BaseHTTPRequestHandler):
    server_version = "TelegramMiniApp/1.0"

    def log_message(self, fmt: str, *args: Any) -> None:
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {self.address_string()} - {fmt % args}")

    def do_GET(self) -> None:
        try:
            url = urllib.parse.urlparse(self.path)
            path = url.path

            if path == "/healthz":
                return json_response(self, {"ok": True, "time": now_ts(), "mode": DATA_MODE, "collector": _collector_state_copy()})

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

            if path == "/api/match/avg":
                params = urllib.parse.parse_qs(url.query)
                mid = str((params.get("id") or [""])[0]).strip()
                if not mid:
                    return json_response(self, {"ok": False, "error": "missing id"}, status=400)
                return json_response(self, avg_payload_for_match(mid))

            if path == "/api/debug/league-assets":
                params = urllib.parse.parse_qs(url.query)
                limit = _safe_int((params.get("limit") or ["80"])[0], 80)
                return json_response(self, debug_league_assets(limit=limit))

            if path == "/api/notify/matches":
                params = urllib.parse.parse_qs(url.query)
                init_data = str((params.get("init_data") or [""])[0])
                return json_response(self, handle_notify_matches(init_data))

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
            if path == "/api/goal-subscribe":
                return json_response(self, handle_goal_subscribe(body))
            if path == "/api/notify/clear":
                return json_response(self, handle_notify_matches_clear(body))
            return json_response(self, {"ok": False, "error": "unknown endpoint"}, status=404)
        except Exception as exc:
            traceback.print_exc()
            return json_response(self, {"ok": False, "error": f"{type(exc).__name__}: {exc}"}, status=500)


def main() -> None:
    DATA_DIR.mkdir(exist_ok=True)
    TEAM_LOGO_DIR.mkdir(exist_ok=True)
    _init_logo_db()
    start_collector_worker()
    _load_notify_subs()
    _load_notify_matches()
    # The worker stores found matches for the app and sends Telegram push when BOT_TOKEN is set.
    start_notify_worker()
    if BOT_TOKEN:
        print(f"[notify] worker started with BOT_TOKEN (***{BOT_TOKEN[-4:]})")
    else:
        print("[notify] BOT_TOKEN not set — Telegram push disabled, in-app/local storage only")
    server = ThreadingHTTPServer((HOST, PORT), MiniAppHandler)
    print("=" * 72)
    print("Telegram Live Matches Mini App — v5 premium collector")
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
