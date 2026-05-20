#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Telegram Live Matches Mini App
Standalone Python server + static frontend.
V9.13: better country flags on league headers.

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
import queue
import random
import re
import sqlite3
import threading
import time
import traceback
import unicodedata
import urllib.error
import urllib.parse
import urllib.request

try:
    import psycopg
except Exception:  # optional: only required when NOTIFY_STORAGE=postgres
    psycopg = None

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
DATA_DIR = BASE_DIR / "data"
TEAM_LOGO_DIR = DATA_DIR / "team_logos"
TEAM_LOGO_DB = DATA_DIR / "team_logos.sqlite3"
TEAM_LOGO_MAX_BYTES = int(os.environ.get("TEAM_LOGO_MAX_BYTES", "1500000"))
# v9.77: Render has an ephemeral disk, so downloading logos there creates stale
# file references after restarts/deploys. Default to OFF on Render unless the
# env var is explicitly set. On VPS/local the previous default stays ON.
TEAM_LOGO_DOWNLOAD_DEFAULT = "0" if (os.environ.get("RENDER") or os.environ.get("RENDER_SERVICE_ID") or os.environ.get("RENDER_EXTERNAL_URL")) else "1"
TEAM_LOGO_DOWNLOAD = os.environ.get("TEAM_LOGO_DOWNLOAD", TEAM_LOGO_DOWNLOAD_DEFAULT).strip().lower() not in {"0", "false", "no", "off"}

HOST = os.environ.get("HOST", "0.0.0.0")
PORT = int(os.environ.get("PORT", "8080"))
DATA_MODE = os.environ.get("DATA_MODE", "auto").strip().lower()

# v5: Telegram bot token for push notifications.
# Set BOT_TOKEN env var on the server to enable Telegram delivery.
# v9.60: accept common Render/Telegram env names too, so push does not silently
# stay disabled when the token was added as TELEGRAM_BOT_TOKEN/TG_BOT_TOKEN.
BOT_TOKEN = (
    os.environ.get("BOT_TOKEN")
    or os.environ.get("TELEGRAM_BOT_TOKEN")
    or os.environ.get("TG_BOT_TOKEN")
    or os.environ.get("TELEGRAM_TOKEN")
    or ""
).strip()

# v9.71: links from Telegram notification buttons into the exact match.
# Set BOT_USERNAME to your bot username without @. If your Mini App has a
# short name in BotFather, set MINIAPP_SHORT_NAME too. PUBLIC_BASE_URL is a
# fallback for normal browser links.
BOT_USERNAME = (
    os.environ.get("BOT_USERNAME")
    or os.environ.get("TELEGRAM_BOT_USERNAME")
    or os.environ.get("TG_BOT_USERNAME")
    or ""
).strip().lstrip("@")
MINIAPP_SHORT_NAME = (
    os.environ.get("MINIAPP_SHORT_NAME")
    or os.environ.get("TELEGRAM_MINIAPP_SHORT_NAME")
    or os.environ.get("WEBAPP_SHORT_NAME")
    or ""
).strip().strip("/")
PUBLIC_BASE_URL = (
    os.environ.get("PUBLIC_BASE_URL")
    or os.environ.get("RENDER_EXTERNAL_URL")
    or os.environ.get("APP_URL")
    or ""
).strip().rstrip("/")
# Telegram startapp parameters are limited; keep it short and deterministic.
NOTIFY_OPEN_BUTTON_TEXT = os.environ.get("NOTIFY_OPEN_BUTTON_TEXT", "⚽ Открыть матч").strip() or "⚽ Открыть матч"
# Telegram WebApp initData can be cached by mobile Telegram. 24h was too strict
# and made /api/subscribe fail even though the switch looked enabled in the app.
# 0 disables the age check; default is 30 days.
TELEGRAM_INIT_DATA_MAX_AGE_SECONDS = int(os.environ.get("TELEGRAM_INIT_DATA_MAX_AGE_SECONDS", str(30 * 24 * 3600)))

# v9.64: private Telegram admin panel.
# ADMIN_IDS is a comma-separated list of numeric Telegram user ids.
# For this build the owner id is prefilled; it can still be overridden on Render.
def _parse_int_set(raw: str) -> set[int]:
    out: set[int] = set()
    for part in str(raw or "").replace(";", ",").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            out.add(int(part))
        except Exception:
            continue
    return out

def _parse_admin_ids_env(raw: str, defaults: set[int]) -> set[int]:
    """Парсит ADMIN_IDS с подмесом дефолтов.

    v9.80: владелец бота (851766591) всегда админ, плюс всё, что задано в
    переменной окружения ADMIN_IDS (через запятую/пробелы)."""
    result = set(defaults)
    result.update(_parse_int_set(raw))
    return result


# v9.80: ID владельца — Евгений Зотов. Можно расширять через ADMIN_IDS env.
_DEFAULT_OWNER_IDS = {851766591}
ADMIN_IDS = _parse_admin_ids_env(os.environ.get("ADMIN_IDS", ""), _DEFAULT_OWNER_IDS)
ADMIN_PANEL_ENABLED = os.environ.get("ADMIN_PANEL_ENABLED", "1").strip().lower() not in {"0", "false", "no", "off"}
ADMIN_POLLING_ENABLED = os.environ.get("ADMIN_POLLING_ENABLED", "1").strip().lower() not in {"0", "false", "no", "off"}
# Optional strict mode: when enabled, only admin ids and manually added users may subscribe/use notifications.
ADMIN_REQUIRE_ALLOWLIST = os.environ.get("ADMIN_REQUIRE_ALLOWLIST", "0").strip().lower() in {"1", "true", "yes", "on"}

# v9.76: production hardening. Keep public API cheap and prevent Telegram sends
# from blocking notification state locks.
API_RATE_LIMIT_WINDOW_SECONDS = float(os.environ.get("API_RATE_LIMIT_WINDOW_SECONDS", "10"))
API_RATE_LIMIT_MAX_REQUESTS = int(os.environ.get("API_RATE_LIMIT_MAX_REQUESTS", "90"))
TELEGRAM_SEND_WORKERS = max(1, int(os.environ.get("TELEGRAM_SEND_WORKERS", "2")))
TELEGRAM_QUEUE_MAXSIZE = max(100, int(os.environ.get("TELEGRAM_QUEUE_MAXSIZE", "5000")))
# v9.78: persisted Telegram jobs survive restarts and are retried with backoff.
TELEGRAM_JOB_LOAD_INTERVAL_SECONDS = float(os.environ.get("TELEGRAM_JOB_LOAD_INTERVAL_SECONDS", "5"))
TELEGRAM_JOB_LOAD_LIMIT = max(10, int(os.environ.get("TELEGRAM_JOB_LOAD_LIMIT", "100")))
TELEGRAM_JOB_MAX_ATTEMPTS = max(1, int(os.environ.get("TELEGRAM_JOB_MAX_ATTEMPTS", "12")))
TELEGRAM_JOB_RETRY_BASE_SECONDS = max(5, int(os.environ.get("TELEGRAM_JOB_RETRY_BASE_SECONDS", "20")))
TELEGRAM_JOB_RETRY_MAX_SECONDS = max(30, int(os.environ.get("TELEGRAM_JOB_RETRY_MAX_SECONDS", "600")))
TELEGRAM_LOG_INTERVAL_SECONDS = max(30, int(os.environ.get("TELEGRAM_LOG_INTERVAL_SECONDS", "300")))
ODDS_DEBUG_LOG = os.environ.get("ODDS_DEBUG_LOG", "0").strip().lower() in {"1", "true", "yes", "on"}
# v9.79: debug endpoints are disabled in production unless explicitly enabled.
# Even when enabled, they require Telegram admin init_data.
DEBUG_ENDPOINTS_ENABLED = os.environ.get("DEBUG_ENDPOINTS", "0").strip().lower() in {"1", "true", "yes", "on"}
# v9.77: gzip large JSON responses to reduce bandwidth for /api/live and /api/match.
API_GZIP_ENABLED = os.environ.get("API_GZIP_ENABLED", "1").strip().lower() not in {"0", "false", "no", "off"}
API_GZIP_MIN_BYTES = int(os.environ.get("API_GZIP_MIN_BYTES", "1024"))
# Runtime cache cleanup prevents long-running VPS instances from keeping stale
# per-match data forever. Set to 0 to disable a specific cache limit.
RUNTIME_CACHE_CLEANUP_INTERVAL_SECONDS = int(os.environ.get("RUNTIME_CACHE_CLEANUP_INTERVAL_SECONDS", "300"))
RUNTIME_CACHE_HARD_MAX = int(os.environ.get("RUNTIME_CACHE_HARD_MAX", "4096"))

# v9.73: online gate for Mini App users. New users are not allowed in when
# the number of active online users reaches this limit. Admin can change it
# from the private Telegram admin panel.
ONLINE_USER_LIMIT_DEFAULT = int(os.environ.get("ONLINE_USER_LIMIT", "100"))
ONLINE_USER_TTL_SECONDS = int(os.environ.get("ONLINE_USER_TTL_SECONDS", "90"))
ONLINE_SETTINGS_DB = DATA_DIR / "online_settings.json"
NOTIFY_DB = BASE_DIR / "data" / "notify_subs.json"
NOTIFY_MATCHES_DB = BASE_DIR / "data" / "notify_matches.json"
# Persistent user state. SQLite is the default because JSON files do not scale
# well when many Telegram users update filters/notifications at the same time.
# Old JSON files are migrated automatically on startup. Keep NOTIFY_STORAGE=json
# only for emergency rollback.
NOTIFY_STATE_DB = DATA_DIR / "notify_state.sqlite3"
# For many users, use Render Postgres. Set DATABASE_URL and either leave
# NOTIFY_STORAGE=auto or set NOTIFY_STORAGE=postgres explicitly.
DATABASE_URL = (
    os.environ.get("DATABASE_URL")
    or os.environ.get("POSTGRES_URL")
    or os.environ.get("POSTGRESQL_URL")
    or ""
).strip()
_requested_notify_storage = os.environ.get("NOTIFY_STORAGE", "auto").strip().lower()
if _requested_notify_storage in {"postgres", "postgresql", "pg"}:
    NOTIFY_STORAGE = "postgres"
elif _requested_notify_storage == "auto":
    NOTIFY_STORAGE = "postgres" if DATABASE_URL and psycopg is not None else "sqlite"
else:
    NOTIFY_STORAGE = _requested_notify_storage
NOTIFY_MIGRATE_JSON = os.environ.get("NOTIFY_MIGRATE_JSON", "1").strip().lower() not in {"0", "false", "no", "off"}
NOTIFY_MIGRATE_SQLITE = os.environ.get("NOTIFY_MIGRATE_SQLITE", "1").strip().lower() not in {"0", "false", "no", "off"}
NOTIFY_MATCHES_MAX_PER_USER = int(os.environ.get("NOTIFY_MATCHES_MAX_PER_USER", "1000"))
NOTIFY_POLL_INTERVAL = float(os.environ.get("NOTIFY_POLL_INTERVAL", "30"))
NOTIFY_COOLDOWN_PER_MATCH = int(os.environ.get("NOTIFY_COOLDOWN_PER_MATCH", "600"))  # 10 min, kept for old state compatibility
# Persistent Telegram de-duplication. A delivered key is stored in the user
# subscription payload, so the same match/alert is not sent again after a
# server restart. Live match ids can be re-used by providers eventually, so old
# delivery keys are cleaned after a few days instead of being kept forever.
NOTIFY_DEDUPE_TTL_SECONDS = int(os.environ.get("NOTIFY_DEDUPE_TTL_SECONDS", str(72 * 3600)))
# Telegram delivery reliability. If Telegram/API/network is temporarily down,
# messages are stored in each user subscription and retried by the background worker.
NOTIFY_SEND_RETRIES = max(1, int(os.environ.get("NOTIFY_SEND_RETRIES", "3")))
NOTIFY_PENDING_MAX_PER_USER = max(20, int(os.environ.get("NOTIFY_PENDING_MAX_PER_USER", "200")))
NOTIFY_PENDING_TTL_SECONDS = int(os.environ.get("NOTIFY_PENDING_TTL_SECONDS", str(48 * 3600)))
NOTIFY_DISMISSED_TTL_SECONDS = int(os.environ.get("NOTIFY_DISMISSED_TTL_SECONDS", str(72 * 3600)))

API_BASE = os.environ.get("IGSCORE_API_BASE", "https://api.igscore.net:8080").rstrip("/")
WEB_ORIGIN = "https://www.igscore.net"
TIME_ZONE = os.environ.get("IGSCORE_TIME_ZONE", "+05:00")
LANG = os.environ.get("IGSCORE_LANG", "en")

LIVE_CACHE_SECONDS = float(os.environ.get("LIVE_CACHE_SECONDS", "7"))
STAT_CACHE_SECONDS = float(os.environ.get("STAT_CACHE_SECONDS", "15"))
EVENT_CACHE_SECONDS = float(os.environ.get("EVENT_CACHE_SECONDS", "25"))
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
# v6: keep finished matches for a while so favorites/detail links still work.
# v9.77: expose a friendlier MATCH_RETENTION_HOURS setting; the old
# FINISHED_MATCH_GRACE_SECONDS env still wins when explicitly provided.
# v9.79: separate retention windows: live feed should hide stale rows quickly,
# finished/detail rows can stay longer, and notification cards should survive
# long enough for old Telegram buttons to open.
MATCH_RETENTION_HOURS = float(os.environ.get("MATCH_RETENTION_HOURS", "24"))
FINISHED_MATCH_RETENTION_HOURS = float(os.environ.get("FINISHED_MATCH_RETENTION_HOURS", str(MATCH_RETENTION_HOURS)))
NOTIFICATION_MATCH_RETENTION_HOURS = float(os.environ.get("NOTIFICATION_MATCH_RETENTION_HOURS", "48"))
FINISHED_MATCH_GRACE_SECONDS = int(os.environ.get("FINISHED_MATCH_GRACE_SECONDS", str(int(max(0.0, FINISHED_MATCH_RETENTION_HOURS) * 3600))))
# v8: how long a match stays in the LIVE FEED after last being seen.
# v9.79: STALE_LIVE_HIDE_AFTER_MINUTES is the friendlier setting; old
# LIVE_FEED_GRACE_SECONDS still wins if explicitly supplied.
STALE_LIVE_HIDE_AFTER_MINUTES = float(os.environ.get("STALE_LIVE_HIDE_AFTER_MINUTES", "10"))
LIVE_FEED_GRACE_SECONDS = int(os.environ.get("LIVE_FEED_GRACE_SECONDS", str(int(max(1.0, STALE_LIVE_HIDE_AFTER_MINUTES) * 60))))
LIVE_CACHE_DB = DATA_DIR / "live_cache.sqlite3"

LIVE_STATUSES = {2, 3, 4}
FINISHED_STATUSES = {8, 9, 10, 11, 12, 13}
# v9.75: if a live source keeps old kickoff timestamps, the calculated
# minute can become 130+, 300+, etc. Treat these matches as stale and hide
# them from the live feed instead of showing fake minutes.
STALE_LIVE_MINUTE_MAX = int(os.environ.get("STALE_LIVE_MINUTE_MAX", "129"))
FIRST_HALF_STALE_MINUTE_MAX = int(os.environ.get("FIRST_HALF_STALE_MINUTE_MAX", "60"))

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
    "brazil": "BR", "argentina": "AR", "arg": "AR", "usa": "US", "united states": "US", "russia": "RU",
    "barbados": "BB", "dominican republic": "DO", "dominicana": "DO", "colombia": "CO",
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
    "yemen": "YE",
    # v9.13: extra Africa/low-tier league countries for league-header flags
    "tanzania": "TZ", "uganda": "UG", "congo": "CG", "republic of the congo": "CG",
    "dr congo": "CD", "drc": "CD", "democratic republic of the congo": "CD",
    "democratic republic congo": "CD", "congo dr": "CD", "congo kinshasa": "CD",
    "liberia": "LR", "sierra leone": "SL", "gambia": "GM", "guinea": "GN", "guinea-bissau": "GW",
    "burkina faso": "BF", "malawi": "MW", "eswatini": "SZ", "swaziland": "SZ", "lesotho": "LS",
    "libya": "LY", "mauritania": "MR", "niger": "NE", "togo": "TG", "benin": "BJ",
    "burundi": "BI", "central african republic": "CF", "chad": "TD", "equatorial guinea": "GQ",
    "gabon": "GA", "madagascar": "MG", "mauritius": "MU", "seychelles": "SC",
    # v8: extra
    "afghanistan": "AF", "bangladesh": "BD", "nepal": "NP", "pakistan": "PK", "maldives": "MV",
    "sri lanka": "LK", "myanmar": "MM", "laos": "LA", "cambodia": "KH", "brunei": "BN", "mongolia": "MN",
    "turkmenistan": "TM", "kyrgyzstan": "KG", "tajikistan": "TJ",
    "ivory coast": "CI", "ghana": "GH", "senegal": "SN", "mali": "ML", "rwanda": "RW",
    "mozambique": "MZ", "namibia": "NA", "botswana": "BW", "angola": "AO",
    "ethiopia": "ET", "somalia": "SO", "eritrea": "ER", "sudan": "SD", "djibouti": "DJ",
    "united arab emirates": "AE", "uae": "AE",
    # v9.66: extra leagues/countries from latest user screenshots
    "andorra": "AD", "fiji": "FJ", "curacao": "CW",
}


CONTINENT_NAMES = {
    "africa", "asia", "americas", "america", "europe", "oceania", "international", "world",
    "без страны", "без лиги",
}

# v9.30: backend sort helper mirrors the frontend: strongest leagues first,
# then weaker/lower leagues. This also makes the collector collect stats from
# top leagues first when COLLECTOR_MAX_MATCHES is limited.
def _sort_text(value: Any) -> str:
    text = unicodedata.normalize("NFD", str(value or "").lower())
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    text = text.replace("ё", "е")
    text = re.sub(r"[^a-zа-я0-9]+", " ", text, flags=re.I).strip()
    return re.sub(r"\s+", " ", text)


def _has_phrase(text: str, phrase: str) -> bool:
    phrase_n = _sort_text(phrase)
    return bool(phrase_n) and f" {phrase_n} " in f" {_sort_text(text)} "


LEAGUE_POWER_RULES: list[tuple[int, tuple[str, ...], tuple[str, ...]]] = [
    (1, (), ("uefa champions league", "champions league", "лига чемпионов")),
    (2, (), ("uefa europa league", "europa league", "лига европы")),
    (3, (), ("conference league", "лига конференций")),
    (4, (), ("copa libertadores", "libertadores")),
    (5, (), ("copa sudamericana", "sudamericana")),
    (10, ("england", "англия"), ("premier league", "epl", "apl", "апл", "премьер лига")),
    (11, ("spain", "испания"), ("la liga", "laliga", "primera division", "ла лига")),
    (12, ("italy", "италия"), ("serie a", "серия a", "серия а")),
    (13, ("germany", "германия"), ("bundesliga", "бундеслига")),
    (14, ("france", "франция"), ("ligue 1", "лига 1")),
    (20, ("netherlands", "нидерланды"), ("eredivisie", "эредивизи")),
    (21, ("portugal", "португалия"), ("primeira liga", "liga portugal")),
    (22, ("turkey", "турция"), ("super lig", "super league")),
    (23, ("belgium", "бельгия"), ("pro league", "first division a", "jupiler")),
    (24, ("scotland", "шотландия"), ("premiership", "premier league")),
    (40, ("brazil", "бразилия"), ("serie a", "brasileirao", "brasileiro serie a")),
    (41, ("argentina", "аргентина"), ("liga profesional", "primera division")),
    (42, ("usa", "united states", "сша"), ("major league soccer", "mls")),
    (43, ("mexico", "мексика"), ("liga mx",)),
    (44, ("colombia", "колумбия"), ("categoria primera a", "primera a")),
    (50, ("japan", "япония"), ("j1", "j1 league", "j league")),
    (51, ("south korea", "korea", "южная корея"), ("k league 1",)),
    (100, ("england", "англия"), ("championship",)),
    (101, ("spain", "испания"), ("segunda", "la liga 2", "laliga 2")),
    (102, ("italy", "италия"), ("serie b", "серия b", "серия б")),
    (103, ("germany", "германия"), ("2 bundesliga", "bundesliga 2")),
    (104, ("france", "франция"), ("ligue 2", "лига 2")),
    (107, ("brazil", "бразилия"), ("serie b", "brasileiro serie b")),
    (108, ("argentina", "аргентина"), ("primera nacional", "arg primera nacional")),
    (109, ("usa", "united states", "сша"), ("usl championship",)),
]

COUNTRY_POWER_ORDER = (
    "england", "англия", "spain", "испания", "italy", "италия", "germany", "германия", "france", "франция",
    "netherlands", "нидерланды", "portugal", "португалия", "turkey", "турция", "brazil", "бразилия",
    "argentina", "аргентина", "usa", "united states", "сша", "belgium", "бельгия", "scotland", "шотландия",
    "mexico", "мексика", "colombia", "колумбия", "japan", "япония", "south korea", "korea", "южная корея",
)


def country_power_rank(country: Any) -> int:
    c = _sort_text(country)
    for i, name in enumerate(COUNTRY_POWER_ORDER):
        if _has_phrase(c, name):
            return 300 + i
    return 900


def league_power_rank(country: Any, league: Any) -> int:
    c = _sort_text(country)
    l = _sort_text(league)
    for rank, countries, leagues in LEAGUE_POWER_RULES:
        if countries and not any(_has_phrase(c, x) for x in countries):
            continue
        if any(_has_phrase(l, x) for x in leagues):
            return rank
    low_text = f"{c} {l}"
    if any(_has_phrase(low_text, x) for x in ("women", "u19", "u20", "u21", "u23", "reserve", "youth", "amateur", "regional", "division 2", "division 3", "жен", "молод", "резерв")):
        return 1200 + country_power_rank(country)
    return country_power_rank(country)


def league_sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    return (
        league_power_rank(item.get("country"), item.get("league")),
        -_safe_int(item.get("minute"), 0),
        str(item.get("country") or ""),
        str(item.get("league") or ""),
        str(item.get("home") or ""),
    )

COUNTRY_NAME_ALIASES = {
    "bhutan": "Bhutan", "bhutanese": "Bhutan", "egyptian": "Egypt", "ethiopian": "Ethiopia", "kenyan": "Kenya", "paraguayan": "Paraguay", "chinese": "China", "indian": "India",
    "hku": "Hong Kong", "korean": "South Korea", "north korean": "North Korea", "thai": "Thailand", "japanese": "Japan",
    "australian": "Australia", "albanian": "Albania", "brazilian": "Brazil", "argentine": "Argentina", "argentinian": "Argentina",
    "english": "England", "spanish": "Spain", "italian": "Italy", "german": "Germany", "french": "France",
    "dutch": "Netherlands", "portuguese": "Portugal", "turkish": "Turkey", "russian": "Russia",
    "polish": "Poland", "romanian": "Romania", "serbian": "Serbia", "swedish": "Sweden", "norwegian": "Norway",
    # v6: extra aliases
    "indonesian": "Indonesia", "indonesia": "Indonesia",
    "malaysian": "Malaysia", "vietnamese": "Vietnam", "filipino": "Philippines", "philippines": "Philippines",
    "mexican": "Mexico", "colombian": "Colombia", "chilean": "Chile", "peruvian": "Peru",
    "ecuadorian": "Ecuador", "venezuelan": "Venezuela", "uruguayan": "Uruguay", "bolivian": "Bolivia",
    "costarica": "Costa Rica", "costaricean": "Costa Rica", "costarican": "Costa Rica",
    "honduran": "Honduras", "guatemalan": "Guatemala", "panamanian": "Panama",
    "saudi": "Saudi Arabia", "emirati": "United Arab Emirates", "qatari": "Qatar", "iranian": "Iran",
    "iraqi": "Iraq", "lebanese": "Lebanon", "syrian": "Syria", "moroccan": "Morocco", "tunisian": "Tunisia",
    "algerian": "Algeria", "nigerian": "Nigeria", "ghanaian": "Ghana", "ugandan": "Uganda",
    "tanzanian": "Tanzania", "tanzania": "Tanzania", "tanzanian premier": "Tanzania",
    "ugandan": "Uganda", "uganda": "Uganda", "uganda premier": "Uganda",
    "dr congo": "DR Congo", "drc": "DR Congo", "congo dr": "DR Congo",
    "democratic republic of the congo": "DR Congo", "democratic republic congo": "DR Congo",
    "congo kinshasa": "DR Congo", "vodacom ligue": "DR Congo",
    "congolese": "Congo", "congo": "Congo",
    "sudanese": "Sudan", "zambian": "Zambia", "zimbabwean": "Zimbabwe",
    "kazakhstani": "Kazakhstan", "uzbek": "Uzbekistan", "azerbaijani": "Azerbaijan", "georgian": "Georgia",
    "armenian": "Armenia", "belarusian": "Belarus", "ukrainian": "Ukraine", "moldovan": "Moldova",
    "estonian": "Estonia", "latvian": "Latvia", "lithuanian": "Lithuania", "finnish": "Finland",
    "danish": "Denmark", "icelandic": "Iceland", "irish": "Ireland", "scottish": "Scotland", "welsh": "Wales",
    "czech": "Czech Republic", "slovak": "Slovakia", "slovenian": "Slovenia", "croatian": "Croatia",
    "bosnian": "Bosnia and Herzegovina", "macedonian": "North Macedonia", "north macedonian": "North Macedonia",
    "north macedonia": "North Macedonia", "kosovan": "Kosovo", "kosovar": "Kosovo",
    "montenegrin": "Montenegro", "bulgarian": "Bulgaria", "greek": "Greece", "cypriot": "Cyprus",
    "maltese": "Malta", "austrian": "Austria", "swiss": "Switzerland", "belgian": "Belgium",
    "luxembourgish": "Luxembourg", "hungarian": "Hungary", "israeli": "Israel", "jordanian": "Jordan",
    "south african": "South Africa", "newzealand": "New Zealand", "new zealander": "New Zealand",
    "kiwi": "New Zealand", "canadian": "Canada", "american": "USA", "usa": "USA", "us": "USA", "usl": "USA", "usl league two": "USA",
    "barbados": "Barbados", "barbadian": "Barbados", "barbados premier": "Barbados", "barbados premier league": "Barbados",
    "dominican": "Dominican Republic", "dominicana": "Dominican Republic", "dominican republic": "Dominican Republic", "liga dominicana": "Dominican Republic", "liga dominicana de futbol": "Dominican Republic",
    "colombia": "Colombia", "colombian": "Colombia", "categoria primera a": "Colombia", "categor a primera a": "Colombia", "primera a": "Colombia",
    "arg": "Argentina", "arg primera nacional": "Argentina", "primera nacional": "Argentina",
    "scottish premiership": "Scotland", "english premier": "England",
    # v9.25: leagues observed in user screenshots that previously fell back to
    # a continent emoji because IGScore reported only the region.
    "sweden division": "Sweden", "swedish division": "Sweden",
    "yemen league": "Yemen", "yemeni": "Yemen", "yemen league division": "Yemen",
    "j1": "Japan", "j2": "Japan", "j3": "Japan", "j2 j3": "Japan",
    "j league": "Japan", "100 year vision": "Japan", "100 year vision league": "Japan",
    "bra lp": "Brazil", "bra serie": "Brazil", "brasileiro": "Brazil",
    "zanzibar": "Tanzania", "zanzibar premier": "Tanzania", "zanzibar premier league": "Tanzania",
    "sand2": "South Africa", "sand 2": "South Africa", "safa sab": "South Africa",
    # v8: common IGScore 2-3 letter country prefix in league name codes
    # e.g. "ETH WL" → Ethiopia, "IND DSD" → India, "TKM" → Turkmenistan etc.
    "eth": "Ethiopia", "ethio": "Ethiopia",
    "ind": "India", "isl": "Iceland",
    "tkm": "Turkmenistan", "turkmenistani": "Turkmenistan", "afghanistan": "Afghanistan", "afghan": "Afghanistan",
    "bangladeshi": "Bangladesh", "bangladesh": "Bangladesh",
    "nepali": "Nepal", "nepalese": "Nepal",
    "myanmar": "Myanmar", "burmese": "Myanmar",
    "laotian": "Laos", "laos": "Laos", "cambodian": "Cambodia",
    "bruneian": "Brunei",
    "mongolian": "Mongolia",
    "tibetan": "China",  # TIB → China
    "maldivian": "Maldives",
    "srilankan": "Sri Lanka", "sri lankan": "Sri Lanka",
    "pakistani": "Pakistan",
    "rwandan": "Rwanda", "senegalese": "Senegal", "malian": "Mali",
    "ivorian": "Ivory Coast", "ivory coast": "Ivory Coast",
    "cameroonian": "Cameroon", "congolese": "Congo",
    "angolan": "Angola", "mozambican": "Mozambique",
    "namibian": "Namibia", "botswanan": "Botswana",
    "liberian": "Liberia", "sierra leonean": "Sierra Leone",
    "gambian": "Gambia", "guinean": "Guinea", "burkinabe": "Burkina Faso",
    "malawian": "Malawi", "swazi": "Eswatini", "lesotho": "Lesotho",
    "somali": "Somalia", "eritrean": "Eritrea", "djiboutian": "Djibouti",
    "libyan": "Libya", "tunisian": "Tunisia",
    "cafa": "Afghanistan",  # CAFA U-20 is a Central Asian Football Association (mostly AFG/TKM)
    "ofc": "Oceania",  # OFC Pro League = Oceania Football Confederation
    "cfa": "China",   # CFA = Chinese Football Association
    "national youth school football league": "China", "youth school football league": "China",
    "ningbo university": "China", "guizhou police academy": "China", "chongqing normal university": "China", "kashi university": "China",
    "afc": "Asia",    # AFC = Asian Football Confederation
    "caf": "Africa",  # CAF = Confederation of African Football
    "concacaf": "Americas",
    "conmebol": "South America",
    "fifa": "International",
    # v9.66: extra country/league aliases from latest user screenshots
    "eredivisie": "Netherlands", "netherlands eredivisie": "Netherlands",
    "andorran": "Andorra", "andorra": "Andorra", "andorran primera divisio": "Andorra",
    "south australia reserve league": "Australia", "south australia": "Australia",
    "fijian": "Fiji", "fiji": "Fiji", "fijian national league": "Fiji",
    "chi liga de ascenso": "Chile", "chi liga de primera": "Chile",
    "lux l1 w": "Luxembourg",
    "ireland women s league": "Ireland", "ireland women's league": "Ireland",
    "curacao": "Curacao", "curacao liga mcb 1st division": "Curacao", "liga mcb 1st division": "Curacao",
    "ligapro serie a": "Ecuador",
}

# v6: Continent / region → emoji flag fallback (so we don't show a globe in
# the UI when IGScore groups by continent rather than country).
CONTINENT_FLAG_EMOJI = {
    "europe": "🇪🇺",
    "africa": "🌍",
    "americas": "🌎", "america": "🌎",
    "north america": "🌎", "south america": "🌎", "central america": "🌎",
    "asia": "🌏",
    "oceania": "🌏",
    "international": "🏳️", "world": "🌐",
}

_cache_lock = threading.Lock()
_live_cache: dict[str, Any] = {"saved_at": 0.0, "payload": None, "raw": {}}
# v9.80: single-flight для load_live_payload. На cache miss раньше N
# параллельных пользователей дёргали IGScore одновременно (thundering herd).
# Теперь один поток фетчит, остальные ждут результат через Event.
_live_fetch_lock = threading.Lock()
_live_fetch_done = threading.Event()
_live_fetch_in_progress = False
_stats_cache: dict[str, dict[str, Any]] = {}
_events_cache: dict[str, dict[str, Any]] = {}
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
    "last_scan_seconds": 0.0,
    "avg_scan_seconds": 0.0,
    "max_scan_seconds": 0.0,
}
_collector_scan_history: list[float] = []
APP_STARTED_AT = int(time.time())

_notify_worker_state: dict[str, Any] = {
    "last_start": 0,
    "last_finish": 0,
    "last_error": "",
    "last_matches": 0,
    "last_subs": 0,
    "last_scan_seconds": 0.0,
    "avg_scan_seconds": 0.0,
    "max_scan_seconds": 0.0,
}
_notify_scan_history: list[float] = []
_runtime_cache_last_cleanup = 0.0


def _cleanup_cache_dict(cache: dict[str, dict[str, Any]], ttl_seconds: float, now_f: float, hard_max: int | None = None) -> int:
    """Remove stale entries from a {key: {saved_at: ...}} runtime cache."""
    removed = 0
    if ttl_seconds > 0:
        cutoff = now_f - ttl_seconds
        for key in list(cache.keys()):
            try:
                saved = float((cache.get(key) or {}).get("saved_at") or 0)
            except Exception:
                saved = 0.0
            if saved <= 0 or saved < cutoff:
                cache.pop(key, None)
                removed += 1
    hard = RUNTIME_CACHE_HARD_MAX if hard_max is None else int(hard_max or 0)
    if hard > 0 and len(cache) > hard:
        items = sorted(cache.items(), key=lambda kv: float((kv[1] or {}).get("saved_at") or 0))
        for key, _value in items[:max(0, len(cache) - hard)]:
            cache.pop(key, None)
            removed += 1
    return removed


def cleanup_runtime_caches(force: bool = False) -> int:
    """Best-effort cleanup for in-memory caches used by long-running VPS deploys."""
    global _runtime_cache_last_cleanup
    now_f = time.time()
    if not force and RUNTIME_CACHE_CLEANUP_INTERVAL_SECONDS > 0 and now_f - _runtime_cache_last_cleanup < RUNTIME_CACHE_CLEANUP_INTERVAL_SECONDS:
        return 0
    _runtime_cache_last_cleanup = now_f
    removed = 0
    # Keep a few TTLs beyond each normal cache TTL so hot matches keep reusing data,
    # but stale matches from previous days do not stay in RAM forever.
    with _cache_lock:
        removed += _cleanup_cache_dict(_stats_cache, max(60.0, STAT_CACHE_SECONDS * 20), now_f)
        removed += _cleanup_cache_dict(_events_cache, max(300.0, EVENT_CACHE_SECONDS * 20), now_f)
        removed += _cleanup_cache_dict(_match_info_cache, max(300.0, MATCH_INFO_CACHE_SECONDS * 20), now_f)
        removed += _cleanup_cache_dict(_pressure_chart_cache, max(300.0, PRESSURE_CHART_CACHE_SECONDS * 20), now_f)
        removed += _cleanup_cache_dict(_team_avg_cache, max(1800.0, TEAM_AVG_CACHE_SECONDS * 6), now_f)
        removed += _cleanup_cache_dict(_odds_cache, max(300.0, ODDS_CACHE_SECONDS * 20), now_f)
        removed += _cleanup_cache_dict(_odds_presence_cache, max(300.0, ODDS_FEED_CACHE_SECONDS * 20), now_f)
    if removed:
        print(f"[cleanup] runtime caches removed={removed}")
    return removed

_admin_state_lock = threading.Lock()
_admin_state: dict[str, Any] = {"allowed_users": {}, "blocked_users": {}, "known_users": {}, "events": []}
_admin_runtime_steps: dict[str, str] = {}
_admin_polling_started = False
_admin_update_offset = 0

_online_lock = threading.Lock()
_online_users: dict[str, dict[str, Any]] = {}
_online_limit_cache: int | None = None
_telegram_send_stats: dict[str, Any] = {"ok": 0, "fail": 0, "last_ok": 0, "last_fail": 0, "last_error": "", "persisted": 0, "loaded": 0, "dropped": 0, "last_log": 0}
_telegram_send_stats_lock = threading.Lock()


def _telegram_stats_incr(key: str, delta: int = 1) -> None:
    """v9.80: атомарный инкремент счётчиков. Раньше использовалось
    `_telegram_send_stats[k] = _safe_int(_telegram_send_stats.get(k), 0) + 1`
    без лока — под нагрузкой инкременты терялись."""
    with _telegram_send_stats_lock:
        _telegram_send_stats[key] = _safe_int(_telegram_send_stats.get(key), 0) + delta


def _telegram_stats_set(updates: dict[str, Any]) -> None:
    with _telegram_send_stats_lock:
        for k, v in updates.items():
            _telegram_send_stats[k] = v
_telegram_send_queue: queue.Queue[dict[str, Any]] = queue.Queue(maxsize=TELEGRAM_QUEUE_MAXSIZE)
_telegram_queue_started = False
_telegram_jobs_loader_started = False
_telegram_queue_lock = threading.Lock()
_telegram_enqueued_keys: set[str] = set()
_telegram_enqueued_lock = threading.Lock()
_logo_lock = threading.Lock()
_logo_db_ready = False


def now_ts() -> int:
    return int(time.time())


def _format_age_ru(ts: int | float | None) -> str:
    try:
        diff = max(0, int(time.time() - float(ts or 0)))
    except Exception:
        return "нет данных"
    if not ts:
        return "нет данных"
    if diff < 60:
        return f"{diff} сек назад"
    if diff < 3600:
        return f"{diff // 60} мин назад"
    if diff < 86400:
        return f"{diff // 3600} ч назад"
    return f"{diff // 86400} д назад"


def _fmt_seconds(value: Any) -> str:
    try:
        v = float(value or 0)
    except Exception:
        v = 0.0
    if v <= 0:
        return "нет данных"
    if v < 10:
        return f"{v:.1f} сек"
    return f"{int(round(v))} сек"


def _record_scan_duration(state: dict[str, Any], history: list[float], seconds: float) -> None:
    try:
        sec = max(0.0, float(seconds or 0.0))
    except Exception:
        sec = 0.0
    if sec <= 0:
        return
    history.append(sec)
    del history[:-50]
    state["last_scan_seconds"] = sec
    state["avg_scan_seconds"] = sum(history) / max(1, len(history))
    state["max_scan_seconds"] = max(history) if history else sec


def _should_gzip_json(handler: BaseHTTPRequestHandler, raw_len: int, status: int) -> bool:
    if not API_GZIP_ENABLED or status != 200 or raw_len < max(0, API_GZIP_MIN_BYTES):
        return False
    try:
        path = urllib.parse.urlparse(getattr(handler, "path", "")).path
    except Exception:
        path = ""
    if path not in {"/api/live", "/api/match"}:
        return False
    enc = str(handler.headers.get("Accept-Encoding") or "").lower()
    return "gzip" in enc


def json_response(handler: BaseHTTPRequestHandler, payload: Any, status: int = 200) -> None:
    raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    use_gzip = _should_gzip_json(handler, len(raw), status)
    body = gzip.compress(raw, compresslevel=5) if use_gzip else raw
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Cache-Control", "no-store")
    # v9.80: базовые security-заголовки
    handler.send_header("X-Content-Type-Options", "nosniff")
    handler.send_header("Referrer-Policy", "no-referrer")
    if use_gzip:
        handler.send_header("Content-Encoding", "gzip")
        handler.send_header("Vary", "Accept-Encoding")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def text_response(handler: BaseHTTPRequestHandler, text: str, status: int = 200) -> None:
    raw = text.encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "text/plain; charset=utf-8")
    handler.send_header("X-Content-Type-Options", "nosniff")
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


# Short cache for exact match details. Used to refresh finished favorites, because
# /api/live removes finished matches from the main feed before the user cache can
# always see the final score.
MATCH_INFO_CACHE_SECONDS = float(os.environ.get("MATCH_INFO_CACHE_SECONDS", "20"))
_match_info_cache: dict[str, dict[str, Any]] = {}

# v9.21: cache for pressure_chart_from_history results. The chart only changes
# when the background collector writes a new snapshot, which happens every
# COLLECTOR_INTERVAL seconds (~60s). A 20-second TTL gives a comfortable buffer
# under the collector cadence while letting many concurrent users viewing the
# same match share one DB read + one delta-computation pass. Without this
# cache, 100 users polling /api/match every 15s for the same hot match would
# run 100 SELECTs and 100 bar-computations per cycle instead of ~5.
PRESSURE_CHART_CACHE_SECONDS = float(os.environ.get("PRESSURE_CHART_CACHE_SECONDS", "20"))
_pressure_chart_cache: dict[str, dict[str, Any]] = {}

# v9.36: cache for avg_total_for_team. The "last 10 matches" form data updates
# only when a team plays a new match, so a long TTL (default 10 minutes) is
# safe and saves a /v1/football/match/analysis/recent call per team per
# request. Without this, the /api/avg/bulk endpoint would hit IGScore once
# per team for every UI tab switch — that is ~200 requests for 100 live
# matches just to filter the visible list.
TEAM_AVG_CACHE_SECONDS = float(os.environ.get("TEAM_AVG_CACHE_SECONDS", "600"))
_team_avg_cache: dict[str, dict[str, Any]] = {}


def match_odds_last(match_id: str) -> dict[str, Any]:
    """Fetch live odds from /v1/football/match/odds/last.

    Payload:  {"matchIds": [matchId], ...}
    Response: result.matchRecentOdds[matchId] = list of items.
      oddsType "eu":   oddsData = [П1,   X,        П2,  0]
      oddsType "asia": oddsData = [home, handicap, away, 0]
      oddsType "bs":   oddsData = [over, line,    under, 0]
    """
    return post_json("/v1/football/match/odds/last", {"matchIds": [str(match_id)], **_base_payload()}, timeout=10.0)


def match_odds_last_many(match_ids: list[str]) -> dict[str, Any]:
    """Fetch live odds for many matches in one IGScore request."""
    ids: list[str] = []
    seen: set[str] = set()
    for value in match_ids or []:
        mid = str(value or "").strip()
        if mid and mid not in seen:
            ids.append(mid)
            seen.add(mid)
    if not ids:
        return {"result": {"matchRecentOdds": {}}}
    return post_json("/v1/football/match/odds/last", {"matchIds": ids, **_base_payload()}, timeout=15.0)


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


def _is_safe_external_url(url: str) -> bool:
    """v9.80: защита от SSRF при скачивании логотипов команд.

    Раньше URL логотипа брался из ответа IGScore и грузился через urllib
    без валидации схемы и хоста. Если провайдер (или MITM) вернёт
    `http://169.254.169.254/...` (метаданные AWS), `file:///etc/passwd`
    или `http://127.0.0.1:6379` — мы это безоговорочно скачаем.
    Здесь — белый список схем и блок приватных диапазонов.
    """
    try:
        parsed = urllib.parse.urlparse(url)
    except Exception:
        return False
    if parsed.scheme not in {"http", "https"}:
        return False
    host = (parsed.hostname or "").strip().lower()
    if not host:
        return False
    # Простейшая защита: блокируем localhost / приватные IP по имени.
    if host in {"localhost", "ip6-localhost", "ip6-loopback"}:
        return False
    if host.endswith(".local") or host.endswith(".internal"):
        return False
    try:
        import ipaddress
        ip = ipaddress.ip_address(host)
        if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_multicast or ip.is_reserved:
            return False
    except ValueError:
        # Не IP — оставляем как доменное имя. Полная защита потребовала бы
        # DNS-резолва, что замедлит горячий путь; для нашего случая хватит
        # проверки схемы + явных приватных хостов.
        pass
    return True


def _download_team_logo(team_id_value: str, team_name_value: str, logo_url: str) -> str:
    if not TEAM_LOGO_DOWNLOAD or not logo_url:
        return ""
    if not _is_safe_external_url(logo_url):
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


def _raw_live_minute(match: dict[str, Any], server_time: int | None = None) -> int:
    """Return the uncapped live minute from source timestamps/fields."""
    now = int(server_time or match.get("server_time") or time.time())
    second = _safe_int(match.get("secondHalfKickOffTime"), 0)
    first = _safe_int(match.get("firstHalfKickOffTime"), 0)
    if second > 0:
        return 45 + max(0, (now - second) // 60)
    if first > 0:
        return max(1, (now - first) // 60)
    return _safe_int(match.get("minute_value") or match.get("minute"), 0)


def is_stale_live_match(match: dict[str, Any], server_time: int | None = None) -> bool:
    """Hide broken live rows whose timer has run far beyond a real match."""
    status = _safe_int(match.get("matchStatus") or match.get("statusId"), 0)
    if status not in LIVE_STATUSES or status == 3:
        return False
    raw_minute = _raw_live_minute(match, server_time)
    if raw_minute >= STALE_LIVE_MINUTE_MAX:
        return True
    first = _safe_int(match.get("firstHalfKickOffTime"), 0)
    second = _safe_int(match.get("secondHalfKickOffTime"), 0)
    if first > 0 and second <= 0 and raw_minute > FIRST_HALF_STALE_MINUTE_MAX:
        return True
    return False


def minute_from_match(match: dict[str, Any], server_time: int | None = None) -> tuple[int, str, str]:
    now = int(server_time or match.get("server_time") or time.time())
    status = _safe_int(match.get("matchStatus") or match.get("statusId"), 0)

    if status == 3:
        return 45, "HT", "HT"

    second = _safe_int(match.get("secondHalfKickOffTime"), 0)
    first = _safe_int(match.get("firstHalfKickOffTime"), 0)

    if second > 0:
        minute = 45 + max(0, (now - second) // 60)
        shown = int(min(max(minute, 46), STALE_LIVE_MINUTE_MAX - 1))
        return shown, f"{shown}’", "2T"

    if first > 0:
        minute = max(1, (now - first) // 60)
        shown = int(min(max(minute, 1), 45))
        return shown, f"{shown}’", "1T"

    raw = str(match.get("minute_raw") or match.get("minute_source") or "").strip()
    if raw:
        mv = int(min(max(_safe_int(match.get("minute_value") or match.get("minute"), 0), 0), STALE_LIVE_MINUTE_MAX - 1))
        txt = f"{mv}’" if mv > 0 and re.search(r"\d", raw) else raw
        return mv, txt, "2T" if mv > 45 else "1T"

    mv = int(min(max(_safe_int(match.get("minute") or match.get("minute_value"), 0), 0), STALE_LIVE_MINUTE_MAX - 1))
    return mv, (f"{mv}’" if mv else "LIVE"), "2T" if mv > 45 else ("1T" if mv else "LIVE")


def _is_generic_region(value: Any) -> bool:
    text = str(value or "").strip().lower()
    return not text or text in CONTINENT_NAMES


def _country_name_from_text(*parts: Any) -> str:
    hay = " ".join(str(p or "") for p in parts).lower()
    hay = re.sub(r"[^a-z\s-]+", " ", hay)
    hay = re.sub(r"\s+", " ", hay).strip()
    # v6: also build a "smushed" version with no separators so e.g. "costarica"
    # in the source text matches the alias "costarica" → "Costa Rica".
    hay_smushed = re.sub(r"[\s-]+", "", hay)
    for alias, country in sorted(COUNTRY_NAME_ALIASES.items(), key=lambda x: len(x[0]), reverse=True):
        alias_low = alias.lower()
        pattern = r"(^|\s)" + re.escape(alias_low) + r"(\s|$)"
        if re.search(pattern, hay):
            return country
        # also try smushed match for multi-word aliases when the source ran the
        # words together (e.g. "CostaRica" in IGScore league names).
        if " " in alias_low or "-" in alias_low:
            if re.search(r"(^|[^a-z])" + re.escape(re.sub(r"[\s-]+", "", alias_low)) + r"([^a-z]|$)", hay_smushed):
                return country
    for name in sorted(COUNTRY_CODE_MAP.keys(), key=len, reverse=True):
        name_low = name.lower()
        pattern = r"(^|\s)" + re.escape(name_low) + r"(\s|$)"
        if re.search(pattern, hay):
            return " ".join(w.capitalize() for w in name.split())
        # smushed match for multi-word country names
        if " " in name_low:
            smushed = re.sub(r"\s+", "", name_low)
            if re.search(r"(^|[^a-z])" + re.escape(smushed) + r"([^a-z]|$)", hay_smushed):
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
    # v6: as a last resort keep the continent / region label rather than
    # collapsing everything to "Без страны". The frontend renders a continent
    # emoji for these.
    if isinstance(cat, dict):
        cat_name = str(cat.get("name") or "").strip()
        if cat_name:
            return cat_name
    if raw_country:
        return raw_country
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


def _is_finished_status(status: int) -> bool:
    """Return True for terminal/finished IGScore statuses.

    Known live statuses in this app are 2, 3 and 4. Status 1 is upcoming.
    IGScore can vary terminal codes by sport/competition, so any non-zero
    status outside live/upcoming is treated as finished for favorites.
    """
    status = _safe_int(status, 0)
    return status in FINISHED_STATUSES or (status not in LIVE_STATUSES and status not in {0, 1})


def _merge_fresh_match(base: dict[str, Any], fresh: dict[str, Any]) -> dict[str, Any]:
    """Overlay fresh score/status fields onto a cached/live match shell."""
    if not isinstance(base, dict):
        base = {}
    if not isinstance(fresh, dict) or not fresh:
        return dict(base)
    out = dict(base)
    # Always trust the direct match-info score and timer/status.
    for key in ("score_home", "score_away", "score", "minute", "minute_text", "period", "finished", "status", "status_id"):
        if key in fresh:
            out[key] = fresh[key]
    # Fill identity fields from match-info when available; otherwise preserve
    # the cached/live row values that may have better local assets.
    for key in ("home", "away", "home_id", "away_id", "country", "country_code", "league", "link"):
        value = fresh.get(key)
        if value not in (None, ""):
            out[key] = value
    for key in ("home_logo", "away_logo", "league_logo"):
        value = fresh.get(key)
        if value:
            out[key] = value
    out["id"] = str(out.get("id") or fresh.get("id") or "")
    return out


def fresh_match_info_public(match_id_value: str, force: bool = False) -> dict[str, Any] | None:
    """Fetch the exact match card from IGScore and convert it to frontend shape."""
    mid = str(match_id_value or "").strip()
    if not mid:
        return None
    now = time.time()
    if not force:
        with _cache_lock:
            cached = _match_info_cache.get(mid)
            if cached and now - float(cached.get("saved_at") or 0) < MATCH_INFO_CACHE_SECONDS:
                return dict(cached.get("match") or {})
    try:
        resp = match_info(mid)
        result = resp.get("result") if isinstance(resp, dict) else None
        if not isinstance(result, dict):
            return None
        server_time = _safe_int(resp.get("server_time"), int(time.time()))
        result = dict(result)
        result.setdefault("server_time", server_time)
        public = to_public_match(result, server_time=server_time, source="igscore_info")
        status = _safe_int(result.get("matchStatus") or result.get("statusId"), 0)
        public["status"] = status
        public["status_id"] = status
        public["finished"] = _is_finished_status(status)
        with _cache_lock:
            _match_info_cache[mid] = {"saved_at": now, "match": dict(public)}
        return public
    except Exception as exc:
        print(f"[match-info] fetch failed {mid}: {type(exc).__name__}: {exc}")
        return None


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
        leagues.sort(key=lambda x: (league_power_rank(c["country"], x.get("league")), -int(x.get("match_count") or 0), str(x.get("league") or "")))
        out.append({
            "country": c["country"],
            "country_code": c["country_code"],
            "match_count": c["match_count"],
            "leagues": leagues,
        })
    out.sort(key=lambda x: (min((league_power_rank(x.get("country"), l.get("league")) for l in x.get("leagues", [])), default=999), -int(x.get("match_count") or 0), str(x.get("country") or "")))
    return out


def build_payload(matches: list[dict[str, Any]], source: str, error: str | None = None) -> dict[str, Any]:
    payload = {
        "ok": True,
        "source": source,
        "updated_at": now_ts(),
        "total": len(matches),
        "countries": group_matches(matches),
        "error": error or "",
    }
    # v9.6: annotate live feed with a light has_odds flag so the frontend can
    # move leagues/matches with coefficients to the top without opening detail.
    try:
        annotate_live_payload_with_odds(payload)
    except Exception as exc:
        payload["odds_sort_error"] = str(exc)[:180]
    return payload


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
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS match_stats_history (
                match_id TEXT NOT NULL,
                captured_at INTEGER NOT NULL,
                stats_json TEXT NOT NULL,
                PRIMARY KEY (match_id, captured_at)
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_match_stats_history_lookup ON match_stats_history(match_id, captured_at)")
        # v7: events table — derived from score changes detected by the collector
        # between snapshots. Guarantees "Ход матча" shows goals even when
        # IGScore /match/info doesn't return them.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS match_events (
                match_id TEXT NOT NULL,
                event_id TEXT NOT NULL,
                kind TEXT NOT NULL,
                minute INTEGER DEFAULT 0,
                minute_text TEXT,
                team TEXT,
                side TEXT,
                player TEXT,
                detail TEXT,
                score_home INTEGER DEFAULT 0,
                score_away INTEGER DEFAULT 0,
                detected_at INTEGER NOT NULL,
                PRIMARY KEY (match_id, event_id)
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_match_events_match ON match_events(match_id)")
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
    # v6: re-infer country if the stored row has a continent / generic value.
    # Old data was saved before country inference was robust; rather than
    # forcing a re-collect, we fix it on read.
    country_raw = str(row["country"] or "").strip()
    country_code_raw = str(row["country_code"] or "").strip()
    if not country_raw or _is_generic_region(country_raw):
        inferred = _country_name_from_text(
            row["league"], row["home"], row["away"],
        )
        if inferred:
            country_raw = inferred
            country_code_raw = country_code(inferred)
        elif country_raw:
            # keep the continent label; frontend will pick a continent emoji
            pass
        else:
            country_raw = "Без страны"
    elif not country_code_raw:
        country_code_raw = country_code(country_raw)
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
        "country": country_raw,
        "country_code": country_code_raw,
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
    # v8: only return matches seen within LIVE_FEED_GRACE_SECONDS.
    # Finished matches stay in the DB (for favorites/detail) but vanish from
    # the main live feed after two collector cycles (~90s).
    feed_cutoff = now_ts() - max(0, LIVE_FEED_GRACE_SECONDS)
    conn = sqlite3.connect(str(LIVE_CACHE_DB), timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT lm.*, COALESCE(ms.stats_json, '{}') AS stats_json,
                   COALESCE(ms.updated_at, 0) AS stats_updated_at
            FROM live_matches lm
            LEFT JOIN match_stats ms ON ms.match_id = lm.match_id
            WHERE lm.last_seen_at >= ?
              AND COALESCE(lm.minute, 0) < ?
            ORDER BY lm.country, lm.league, lm.minute DESC, lm.home
            """,
            (feed_cutoff, STALE_LIVE_MINUTE_MAX),
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
        events = match_events_cached(mid, match, stats_nested)
        odds = fetch_match_odds_cached(mid) if mid else {}
        pressure = pressure_from_stats_history(mid) if mid else {}
        pressure_chart = pressure_chart_from_history(mid, match) if mid else {}
        return {"match": match, "stats": stats_nested, "events": events, "odds": odds, "pressure": pressure, "pressure_chart": pressure_chart}
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
        # v7: fetch previous scores so we can detect goal events vs. snapshot.
        ids = [str(m.get("id") or "") for m in matches if m.get("id")]
        prev_scores: dict[str, tuple[int, int]] = {}
        if ids:
            qmarks = ",".join("?" * len(ids))
            for row in conn.execute(
                f"SELECT match_id, score_home, score_away FROM live_matches WHERE match_id IN ({qmarks})",
                ids,
            ).fetchall():
                prev_scores[str(row[0])] = (_safe_int(row[1], 0), _safe_int(row[2], 0))

        new_events: list[tuple[Any, ...]] = []
        for m in matches:
            mid = str(m.get("id") or "")
            new_h = _safe_int(m.get("score_home"), 0)
            new_a = _safe_int(m.get("score_away"), 0)
            minute = _safe_int(m.get("minute"), 0)
            minute_text = str(m.get("minute_text") or (f"{minute}'" if minute else "LIVE"))
            home_name = str(m.get("home") or "Хозяева")
            away_name = str(m.get("away") or "Гости")

            if mid in prev_scores:
                prev_h, prev_a = prev_scores[mid]
                # Goal for home — emit one event per increment (handles double-update).
                for i in range(max(0, new_h - prev_h)):
                    new_events.append((
                        mid, f"goal-h-{prev_h + i + 1}",
                        "goal", minute, minute_text,
                        home_name, "home", "", "Гол",
                        prev_h + i + 1, new_a if i == max(0, new_h - prev_h) - 1 else prev_a,
                        now,
                    ))
                for i in range(max(0, new_a - prev_a)):
                    new_events.append((
                        mid, f"goal-a-{prev_a + i + 1}",
                        "goal", minute, minute_text,
                        away_name, "away", "", "Гол",
                        new_h if i == max(0, new_a - prev_a) - 1 else prev_h,
                        prev_a + i + 1,
                        now,
                    ))
            else:
                # v9.77: do NOT backfill score goals when a match first appears.
                # If the first seen snapshot is already 2-1, old builds created
                # three fake goal events at the current minute. Real provider
                # events still appear through match_events_cached().
                pass

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
                    mid, str(m.get("home") or ""), str(m.get("away") or ""),
                    str(m.get("home_id") or ""), str(m.get("away_id") or ""),
                    str(m.get("home_logo") or ""), str(m.get("away_logo") or ""),
                    new_h, new_a, str(m.get("score") or f"{new_h}-{new_a}"),
                    minute, minute_text, str(m.get("period") or "LIVE"),
                    str(m.get("country") or "Без страны"), str(m.get("country_code") or ""),
                    str(m.get("league") or "Без лиги"), str(m.get("league_logo") or ""),
                    str(m.get("link") or ""), "collector_db", now, now,
                ),
            )

        if new_events:
            conn.executemany(
                """
                INSERT OR IGNORE INTO match_events
                (match_id, event_id, kind, minute, minute_text, team, side, player, detail,
                 score_home, score_away, detected_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                new_events,
            )

        conn.commit()
    finally:
        conn.close()


def _events_from_db(match_id_value: str) -> list[dict[str, Any]]:
    """v7: return events stored by the collector (derived from score changes)."""
    mid = str(match_id_value or "").strip()
    if not mid:
        return []
    try:
        _init_live_cache_db()
        conn = sqlite3.connect(str(LIVE_CACHE_DB), timeout=30)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT kind, minute, minute_text, team, side, player, detail,
                   score_home, score_away
            FROM match_events
            WHERE match_id = ?
            ORDER BY detected_at ASC, minute ASC
            """,
            (mid,),
        ).fetchall()
        out: list[dict[str, Any]] = []
        for r in rows:
            out.append({
                "type": str(r["kind"] or "goal"),
                "minute": _safe_int(r["minute"], 0),
                "minute_text": str(r["minute_text"] or "—"),
                "team": str(r["team"] or ""),
                "side": str(r["side"] or ""),
                "player": str(r["player"] or ""),
                "detail": str(r["detail"] or ""),
                "score": f"{_safe_int(r['score_home'], 0)}-{_safe_int(r['score_away'], 0)}",
            })
        return out
    except Exception:
        return []
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _update_match_stats(match_id_value: str, stats_flat: dict[str, Any], match_meta: dict[str, Any] | None = None) -> None:
    mid = str(match_id_value or "").strip()
    if not mid or not stats_flat:
        return
    # Store tiny chart-only timing metadata alongside the flat stats.
    # The pressure formula ignores these __ keys, but the graph can use them
    # to keep the 1st half and 2nd half in separate 50-slot lanes.
    if isinstance(match_meta, dict) and match_meta:
        stats_flat = dict(stats_flat)
        stats_flat["__chart_minute"] = _safe_int(match_meta.get("minute"), 0)
        stats_flat["__chart_period"] = str(match_meta.get("period") or "").strip()
        stats_flat["__chart_minute_text"] = str(match_meta.get("minute_text") or "").strip()
    _init_live_cache_db()
    ts = now_ts()
    stats_json = json.dumps(stats_flat, ensure_ascii=False, separators=(",", ":"))
    conn = sqlite3.connect(str(LIVE_CACHE_DB), timeout=30)
    try:
        # Derive yellow/red-card timeline items from stat increments. This gives
        # the frontend card events even when the provider does not expose a rich
        # timeline endpoint for the match.
        prev_flat: dict[str, Any] = {}
        try:
            prev_row = conn.execute("SELECT stats_json FROM match_stats WHERE match_id = ? LIMIT 1", (mid,)).fetchone()
            if prev_row and prev_row[0]:
                prev_flat = json.loads(prev_row[0] or "{}") or {}
        except Exception:
            prev_flat = {}

        meta = match_meta or {}
        minute = _safe_int(meta.get("minute") or stats_flat.get("__chart_minute"), 0)
        minute_text = str(meta.get("minute_text") or stats_flat.get("__chart_minute_text") or (f"{minute}'" if minute else "LIVE"))
        home_name = str(meta.get("home") or "Хозяева")
        away_name = str(meta.get("away") or "Гости")
        score_home = _safe_int(meta.get("score_home"), 0)
        score_away = _safe_int(meta.get("score_away"), 0)

        card_rows: list[tuple[Any, ...]] = []
        for kind, stat_key, detail in (("yellow", "yellow_cards", "Жёлтая карточка"), ("red", "red_cards", "Красная карточка")):
            for side, team_name, suffix in (("home", home_name, "home"), ("away", away_name, "away")):
                key = f"{stat_key}_{suffix}"
                prev_n = _safe_int(prev_flat.get(key), 0)
                new_n = _safe_int(stats_flat.get(key), 0)
                for i in range(max(0, new_n - prev_n)):
                    idx = prev_n + i + 1
                    card_rows.append((
                        mid, f"{kind}-{suffix[0]}-{idx}",
                        kind, minute, minute_text,
                        team_name, side, "", detail,
                        score_home, score_away, ts,
                    ))
        if card_rows:
            conn.executemany(
                """
                INSERT OR IGNORE INTO match_events
                (match_id, event_id, kind, minute, minute_text, team, side, player, detail,
                 score_home, score_away, detected_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                card_rows,
            )

        conn.execute(
            """
            INSERT INTO match_stats(match_id, stats_json, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(match_id) DO UPDATE SET
                stats_json=excluded.stats_json,
                updated_at=excluded.updated_at
            """,
            (mid, stats_json, ts),
        )
        conn.execute(
            """
            INSERT OR REPLACE INTO match_stats_history(match_id, captured_at, stats_json)
            VALUES (?, ?, ?)
            """,
            (mid, ts, stats_json),
        )
        conn.execute(
            "DELETE FROM match_stats_history WHERE captured_at < ?",
            (ts - max(PRESSURE_HISTORY_KEEP_SECONDS, PRESSURE_WINDOW_SECONDS * 2),),
        )
        conn.commit()
    finally:
        conn.close()


def _pressure_num(stats: dict[str, Any], key: str) -> int:
    return max(0, _safe_int(stats.get(key), 0))


def _pressure_delta(latest: dict[str, Any], oldest: dict[str, Any], key: str) -> int:
    return max(0, _pressure_num(latest, key) - _pressure_num(oldest, key))


def _pressure_side_payload(latest: dict[str, Any], oldest: dict[str, Any], side: str) -> dict[str, Any]:
    suffix = "_home" if side == "home" else "_away"
    attacks = _pressure_delta(latest, oldest, "attacks" + suffix)
    dangerous = _pressure_delta(latest, oldest, "dangerous" + suffix)
    shots = _pressure_delta(latest, oldest, "shots" + suffix)
    on_target = _pressure_delta(latest, oldest, "on_target" + suffix)
    corners = _pressure_delta(latest, oldest, "corners" + suffix)
    # Weighted pressure score. Dangerous attacks, shots on target and corners
    # matter more than simple attacks because they are stronger danger signals.
    score = round((attacks * 0.6) + (dangerous * 3.0) + (shots * 4.0) + (on_target * 6.0) + (corners * 5.0), 1)
    return {
        "score": score,
        "attacks_delta": attacks,
        "dangerous_delta": dangerous,
        "shots_delta": shots,
        "on_target_delta": on_target,
        "corners_delta": corners,
    }


def pressure_from_stats_history(match_id_value: str) -> dict[str, Any]:
    """Return pressure over the last PRESSURE_WINDOW_SECONDS using stored stat deltas."""
    mid = str(match_id_value or "").strip()
    if not mid:
        return {}
    _init_live_cache_db()
    now = now_ts()
    cutoff = now - max(60, PRESSURE_WINDOW_SECONDS)
    conn = sqlite3.connect(str(LIVE_CACHE_DB), timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        latest_row = conn.execute(
            "SELECT captured_at, stats_json FROM match_stats_history WHERE match_id = ? ORDER BY captured_at DESC LIMIT 1",
            (mid,),
        ).fetchone()
        if not latest_row:
            row = conn.execute("SELECT updated_at AS captured_at, stats_json FROM match_stats WHERE match_id = ? LIMIT 1", (mid,)).fetchone()
            if not row:
                return {"available": False, "reason": "no_stats_history", "window_minutes": int(PRESSURE_WINDOW_SECONDS / 60)}
            latest_row = row

        oldest_row = conn.execute(
            """
            SELECT captured_at, stats_json
            FROM match_stats_history
            WHERE match_id = ? AND captured_at >= ? AND captured_at <= ?
            ORDER BY captured_at ASC
            LIMIT 1
            """,
            (mid, cutoff, int(latest_row["captured_at"])),
        ).fetchone()
        if not oldest_row:
            oldest_row = conn.execute(
                "SELECT captured_at, stats_json FROM match_stats_history WHERE match_id = ? ORDER BY captured_at ASC LIMIT 1",
                (mid,),
            ).fetchone()

        if not oldest_row or int(oldest_row["captured_at"]) >= int(latest_row["captured_at"]):
            return {
                "available": False,
                "reason": "collecting",
                "window_minutes": int(PRESSURE_WINDOW_SECONDS / 60),
                "sample_count": 1 if latest_row else 0,
            }

        latest = json.loads(latest_row["stats_json"] or "{}") or {}
        oldest = json.loads(oldest_row["stats_json"] or "{}") or {}
        home = _pressure_side_payload(latest, oldest, "home")
        away = _pressure_side_payload(latest, oldest, "away")
        total = float(home["score"] or 0) + float(away["score"] or 0)
        if total > 0:
            home["percent"] = int(round((float(home["score"]) / total) * 100))
            away["percent"] = max(0, 100 - int(home["percent"]))
        else:
            home["percent"] = 50
            away["percent"] = 50

        diff = abs(float(home["score"]) - float(away["score"]))
        if total <= 0:
            leader = "none"
        elif diff <= max(3.0, total * 0.12):
            leader = "balanced"
        else:
            leader = "home" if float(home["score"]) > float(away["score"]) else "away"

        return {
            "available": True,
            "window_minutes": int(PRESSURE_WINDOW_SECONDS / 60),
            "span_seconds": max(0, int(latest_row["captured_at"]) - int(oldest_row["captured_at"])),
            "sample_count": conn.execute(
                "SELECT COUNT(*) FROM match_stats_history WHERE match_id = ? AND captured_at >= ? AND captured_at <= ?",
                (mid, int(oldest_row["captured_at"]), int(latest_row["captured_at"])),
            ).fetchone()[0],
            "leader": leader,
            "home": home,
            "away": away,
        }
    except Exception as exc:
        return {"available": False, "reason": f"{type(exc).__name__}: {exc}", "window_minutes": int(PRESSURE_WINDOW_SECONDS / 60)}
    finally:
        conn.close()


def pressure_chart_from_history(match_id_value: str, match_meta: dict[str, Any] | None = None) -> dict[str, Any]:
    """Return per-snapshot pressure delta-bars covering the *whole* stored
    history of a match, plus a filtered copy for the last 20 minutes.

    Used by the frontend "график давления" widgets. Snapshots are captured by
    the background collector every COLLECTOR_INTERVAL seconds (≈60s) for every
    live match independent of any user activity, so as soon as the collector
    has seen this match at least twice the chart will have data.

    Shape:
        {
          available, now_ts, earliest_ts, latest_ts,
          window_seconds_20m, sample_count,
          bars_full: [{t, h, a}, ...],   # whole match
          bars_20m:  [{t, h, a}, ...],   # last 20 min only
        }
    where t = unix-seconds timestamp of the later snapshot in the pair, and
    h / a are the weighted pressure scores for that interval (same formula
    as _pressure_side_payload — attacks·0.6 + dangerous·3 + shots·4 +
    on_target·6 + corners·5).

    Results are TTL-cached for PRESSURE_CHART_CACHE_SECONDS (default 20s) so
    that many users viewing the same hot match share one DB read.
    """
    mid = str(match_id_value or "").strip()
    if not mid:
        return {}

    now = now_ts()
    # v9.23: include current phase in the cache key. At HT the renderer trims
    # trailing empty break rows, while during 2T it should resume immediately.
    cache_key = mid + ":" + (_pressure_chart_phase(match_meta or {}) or "LIVE")
    if PRESSURE_CHART_CACHE_SECONDS > 0:
        with _cache_lock:
            cached = _pressure_chart_cache.get(cache_key)
            if cached and now - float(cached.get("saved_at") or 0) < PRESSURE_CHART_CACHE_SECONDS:
                # Return a shallow copy so callers can't mutate the cached
                # dict; bar lists are read-only on the frontend so we can
                # share them by reference.
                return dict(cached.get("payload") or {})

    payload = _pressure_chart_from_history_impl(mid, now, match_meta)

    if PRESSURE_CHART_CACHE_SECONDS > 0:
        with _cache_lock:
            _pressure_chart_cache[cache_key] = {"saved_at": now, "payload": payload}
            # Keep the cache from growing unbounded over a long-running
            # process. Drop entries that haven't been touched recently.
            if len(_pressure_chart_cache) > 4096:
                stale_cutoff = now - max(60.0, PRESSURE_CHART_CACHE_SECONDS * 10)
                for k in [k for k, v in _pressure_chart_cache.items() if float(v.get("saved_at") or 0) < stale_cutoff]:
                    _pressure_chart_cache.pop(k, None)

    return payload


def _pressure_chart_phase(stats: dict[str, Any]) -> str:
    """Return 1T / HT / 2T for a stored pressure snapshot, when known."""
    period = str((stats or {}).get("__chart_period") or "").strip().upper()
    minute_text = str((stats or {}).get("__chart_minute_text") or "").strip().upper()
    minute = _safe_int((stats or {}).get("__chart_minute"), 0)
    if period == "HT" or "HT" in minute_text or "ПЕРЕРЫВ" in minute_text:
        return "HT"
    if period in {"2T", "2H", "H2", "SECOND"} or minute > 45:
        return "2T"
    if period in {"1T", "1H", "H1", "FIRST"} or (1 <= minute <= 45):
        return "1T"
    return ""


def _pressure_chart_is_half_time(match_meta: dict[str, Any] | None) -> bool:
    """True when the live match is at the break; pressure history must pause."""
    return _pressure_chart_phase(match_meta or {}) == "HT"


def _pressure_chart_slot_from_minute(stats: dict[str, Any], phase: str) -> int | None:
    """Map game minute to the fixed 100-slot chart.

    The full-match graph has 50 fixed slots for 1T and 50 fixed slots for 2T.
    Bars stay thin and fixed-width; the slot is chosen from the football minute
    instead of stretching old bars to fill the half. 1T added time clamps to
    the last 1T slot, and 2T added time clamps to the last match slot.
    """
    minute = _safe_int((stats or {}).get("__chart_minute"), 0)
    if phase == "1T" and minute > 0:
        if minute >= 45:
            return 49
        return max(0, min(49, int(round(((minute - 1) / 44.0) * 49))))
    if phase == "2T" and minute > 45:
        if minute >= 90:
            return 99
        return 50 + max(0, min(49, int(round(((minute - 46) / 44.0) * 49))))
    return None


def _pressure_chart_from_history_impl(mid: str, now: int, match_meta: dict[str, Any] | None = None) -> dict[str, Any]:
    _init_live_cache_db()
    window_sec = max(60, PRESSURE_WINDOW_SECONDS)
    cutoff_20m = now - window_sec
    conn = sqlite3.connect(str(LIVE_CACHE_DB), timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            "SELECT captured_at, stats_json FROM match_stats_history "
            "WHERE match_id = ? ORDER BY captured_at ASC",
            (mid,),
        ).fetchall()
    except Exception as exc:
        try:
            conn.close()
        except Exception:
            pass
        return {
            "available": False,
            "reason": f"{type(exc).__name__}: {exc}",
            "now_ts": now,
            "window_seconds_20m": window_sec,
        }
    finally:
        try:
            conn.close()
        except Exception:
            pass

    if not rows or len(rows) < 2:
        return {
            "available": False,
            "reason": "no_history" if not rows else "collecting",
            "sample_count": len(rows) if rows else 0,
            "now_ts": now,
            "window_seconds_20m": window_sec,
        }

    parsed: list[tuple[int, dict[str, Any]]] = []
    for r in rows:
        try:
            data = json.loads(r["stats_json"] or "{}") or {}
        except Exception:
            data = {}
        parsed.append((int(r["captured_at"]), data))

    bars_full: list[dict[str, Any]] = []
    bars_20m: list[dict[str, Any]] = []
    half1_slots = 0
    half2_slots = 0
    for i in range(1, len(parsed)):
        _ts_prev, prev_stats = parsed[i - 1]
        ts_cur, cur_stats = parsed[i]
        phase = _pressure_chart_phase(cur_stats)
        # Do not let half-time create new bars / move the red cursor.
        # The graph resumes only when a real 2T snapshot arrives.
        if phase == "HT":
            continue

        minute_slot = _pressure_chart_slot_from_minute(cur_stats, phase)
        if minute_slot is not None:
            slot = minute_slot
            if phase == "1T":
                half1_slots = max(half1_slots, min(50, slot + 1))
            elif phase == "2T":
                half2_slots = max(half2_slots, min(50, slot - 49))
        elif phase == "2T":
            # Fallback for rows that somehow have phase but no minute metadata.
            slot = 50 + half2_slots
            half2_slots += 1
        elif phase == "1T":
            slot = half1_slots
            half1_slots += 1
        else:
            # Backward-compatible fallback for older stored rows without
            # metadata: fill the first half first, then the second half.
            if half1_slots < 50 and half2_slots == 0:
                slot = half1_slots
                half1_slots += 1
            else:
                slot = 50 + half2_slots
                half2_slots += 1
        if slot < 0 or slot >= 100:
            continue
        home = _pressure_side_payload(cur_stats, prev_stats, "home")
        away = _pressure_side_payload(cur_stats, prev_stats, "away")
        # Keep only what the chart needs — t (epoch sec), 100-slot position
        # and the two weighted pressure scores.
        bar = {"t": ts_cur, "slot": int(slot), "phase": phase or ("2T" if slot >= 50 else "1T"), "h": float(home["score"] or 0.0), "a": float(away["score"] or 0.0)}
        bars_full.append(bar)
        if ts_cur >= cutoff_20m:
            bars_20m.append(bar)

    # If the match is currently at half-time, trim trailing empty rows from an
    # older deployment that may have collected repeated 0/0 deltas during HT.
    current_phase = _pressure_chart_phase(match_meta or {})
    if current_phase == "HT":
        while bars_full and float(bars_full[-1].get("h") or 0) <= 0 and float(bars_full[-1].get("a") or 0) <= 0:
            bars_full.pop()
        cut_ts = int(bars_full[-1]["t"]) if bars_full else 0
        bars_20m = [b for b in bars_20m if int(b.get("t") or 0) <= cut_ts]

    if not bars_full:
        return {
            "available": False,
            "reason": "collecting",
            "sample_count": len(parsed),
            "now_ts": now,
            "window_seconds_20m": window_sec,
        }

    return {
        "available": True,
        "now_ts": now,
        "earliest_ts": int(parsed[0][0]),
        "latest_ts": int(parsed[-1][0]),
        "window_seconds_20m": window_sec,
        "sample_count": len(parsed),
        "bars_full": bars_full,
        "bars_20m": bars_20m,
    }


def _cleanup_finished_matches() -> int:
    """Cleanup old live rows/history plus in-memory runtime caches."""
    cleanup_runtime_caches()
    if not DELETE_FINISHED_MATCHES:
        return 0
    _init_live_cache_db()
    now_i = now_ts()
    cutoff = now_i - max(0, FINISHED_MATCH_GRACE_SECONDS)
    history_cutoff = now_i - max(3600, FINISHED_MATCH_GRACE_SECONDS)
    conn = sqlite3.connect(str(LIVE_CACHE_DB), timeout=30)
    try:
        ids = [r[0] for r in conn.execute("SELECT match_id FROM live_matches WHERE last_seen_at < ?", (cutoff,)).fetchall()]
        stale_ids = [r[0] for r in conn.execute("SELECT match_id FROM live_matches WHERE COALESCE(minute, 0) >= ?", (STALE_LIVE_MINUTE_MAX,)).fetchall()]
        all_ids = list(dict.fromkeys([str(x) for x in ids + stale_ids if str(x or '').strip()]))
        if all_ids:
            params = [(mid,) for mid in all_ids]
            conn.executemany("DELETE FROM match_stats WHERE match_id = ?", params)
            conn.executemany("DELETE FROM match_stats_history WHERE match_id = ?", params)
            conn.executemany("DELETE FROM match_events WHERE match_id = ?", params)
            conn.executemany("DELETE FROM live_matches WHERE match_id = ?", params)
        # Also trim old history rows even if the live row is still retained for a
        # favorite/detail card. This keeps pressure/event tables compact.
        old_stats = conn.execute("DELETE FROM match_stats_history WHERE captured_at < ?", (history_cutoff,)).rowcount or 0
        old_events = conn.execute("DELETE FROM match_events WHERE created_at < ?", (history_cutoff,)).rowcount or 0
        conn.commit()
        return len(all_ids) + int(old_stats) + int(old_events)
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
            period = str(item.get("period") or "").strip().upper()
            minute_text = str(item.get("minute_text") or "").strip().upper()
            if period == "HT" or "HT" in minute_text or "ПЕРЕРЫВ" in minute_text:
                # Pause pressure collection during the break. The graph should
                # resume only when the second half actually starts.
                continue
            stats_flat = flat_stats(stat_pairs_from_response(match_statistics(mid)))
            if stats_flat:
                _update_match_stats(mid, stats_flat, item)
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
            if is_stale_live_match(match, server_time):
                continue
            match["server_time"] = server_time
            matches.append(to_public_match(match, server_time=server_time, source="collector_db"))
        matches.sort(key=league_sort_key)
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
            _record_scan_duration(_collector_state, _collector_scan_history, finished - started)
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
            _record_scan_duration(_collector_state, _collector_scan_history, finished - started)
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
        if is_stale_live_match(match, server_time):
            continue
        match["server_time"] = server_time
        item = to_public_match(match, server_time=server_time, source="igscore")
        public.append(item)
        raw_matches[item["id"]] = match

    enrich_public_matches_with_stats(public)
    public.sort(key=league_sort_key)
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

    # v9.80: single-flight. Раньше при cache miss N параллельных пользователей
    # одновременно дёргали IGScore (thundering herd). Теперь один поток фетчит,
    # остальные ждут на _live_fetch_lock и переиспользуют свежий кеш.
    with _live_fetch_lock:
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


def match_stats_cached(match_id_value: str, match_meta: dict[str, Any] | None = None) -> dict[str, dict[str, int]]:
    """Fetch/cached IGScore stats for a match. Used both by detail and by feed.

    Pressure-chart history is paused during HT. We still return the current
    stats to the UI, but we do not write a new history row until 2T starts.
    """
    mid = str(match_id_value or "").strip()
    if not mid:
        return {}
    cached = _stats_cache.get(mid)
    if cached and time.time() - float(cached.get("saved_at") or 0) < STAT_CACHE_SECONDS:
        return cached.get("stats") or {}
    stats = stat_pairs_from_response(match_statistics(mid))
    if stats:
        try:
            if not _pressure_chart_is_half_time(match_meta):
                _update_match_stats(mid, flat_stats(stats), match_meta or {})
        except Exception:
            pass
    _stats_cache[mid] = {"saved_at": time.time(), "stats": stats}
    return stats




def _deep_find_event_lists(obj: Any, path: str = "") -> list[tuple[str, list[Any]]]:
    """Find possible event/timeline arrays in an unknown IGScore response shape."""
    hits: list[tuple[str, list[Any]]] = []
    if isinstance(obj, dict):
        for key, value in obj.items():
            k = str(key or "").lower()
            child_path = f"{path}.{k}" if path else k
            if isinstance(value, list) and any(token in k for token in (
                "event", "incident", "timeline", "tlive", "score", "goal", "card", "yellow", "red"
            )):
                hits.append((child_path, value))
            hits.extend(_deep_find_event_lists(value, child_path))
    elif isinstance(obj, list):
        for value in obj:
            hits.extend(_deep_find_event_lists(value, path))
    return hits


def _first_text(row: dict[str, Any], keys: tuple[str, ...]) -> str:
    for key in keys:
        value = row.get(key)
        if value is None:
            continue
        if isinstance(value, dict):
            nested = _first_text(value, ("name", "shortName", "displayName", "title", "teamName", "value"))
            if nested:
                return nested
        elif isinstance(value, (str, int, float)):
            text = str(value).strip()
            if text:
                return text
    return ""


def _collect_text_values(obj: Any, limit: int = 60) -> str:
    parts: list[str] = []
    def rec(value: Any) -> None:
        if len(parts) >= limit:
            return
        if isinstance(value, dict):
            for v in value.values():
                rec(v)
        elif isinstance(value, list):
            for v in value[:20]:
                rec(v)
        elif isinstance(value, (str, int, float)):
            text = str(value).strip()
            if text:
                parts.append(text)
    rec(obj)
    return " | ".join(parts).lower()


def _event_type_from_row(row: dict[str, Any], source_path: str = "") -> str:
    text = _collect_text_values(row)
    path = str(source_path or "").lower()
    if any(x in text for x in ("red card", "straight red", "sent off", "sending off", "dismissal", "красн", "удален", "удалён")) or "red" in path:
        return "red"
    if any(x in text for x in ("second yellow", "yellow red", "red yellow")):
        return "red"
    if any(x in text for x in ("yellow", "жёлт", "желт", "yellow card", "booking")) or "yellow" in path:
        return "yellow"
    if any(x in text for x in ("goal", "гол", "penalty scored", "own goal")) or "goal" in path:
        if not any(x in text for x in ("disallowed", "cancelled", "canceled", "var no goal", "missed")):
            return "goal"
    # Common compact event codes in football providers. We only use them when the
    # surrounding field names strongly look like an events/incidents list.
    code_values = []
    for key in ("type", "eventType", "event_type", "incidentType", "incident_type", "code", "kind"):
        if key in row:
            code_values.append(str(row.get(key)).strip().lower())
    looks_like_events = any(x in path for x in ("event", "incident", "timeline", "goal", "card"))
    if looks_like_events:
        if any(v in {"1", "goal", "g"} for v in code_values):
            return "goal"
        if any(v in {"4", "red", "red_card", "rc", "card_red"} for v in code_values):
            return "red"
        if any(v in {"3", "yellow", "yellow_card", "yc", "card_yellow"} for v in code_values):
            return "yellow"
    return ""


def _minute_from_event(row: dict[str, Any]) -> tuple[int, str]:
    for key in ("minute", "minutes", "matchMinute", "match_time", "matchTime", "time", "occurTime", "eventTime", "eventMinute", "gameTime", "periodTime", "clock"):
        value = row.get(key)
        if value is None:
            continue
        if isinstance(value, dict):
            value = value.get("minute") or value.get("display") or value.get("time") or value.get("value")
        text = str(value).strip()
        if not text:
            continue
        m = re.search(r"(\d{1,3})(?:\s*\+\s*(\d{1,2}))?", text)
        if m:
            base = _safe_int(m.group(1), 0)
            extra = _safe_int(m.group(2), 0) if m.group(2) else 0
            minute = base + extra
            label = f"{base}+{extra}’" if extra else f"{base}’"
            return minute, label
    return 0, "—"


def _event_minute_is_valid(minute: int, minute_text: str = "") -> bool:
    """Hide provider event rows with impossible/dirty football minutes."""
    try:
        m = int(minute or 0)
    except Exception:
        m = 0
    if m <= 0:
        # Unknown minute is allowed; it will be shown as an undated event.
        return True
    if m >= STALE_LIVE_MINUTE_MAX:
        return False
    text = str(minute_text or "").strip()
    if re.search(r"\d{3,}", text) and m >= 100:
        return False
    return True


def _team_from_event(row: dict[str, Any], match: dict[str, Any]) -> tuple[str, str]:
    home = str(match.get("home") or "Хозяева").strip() or "Хозяева"
    away = str(match.get("away") or "Гости").strip() or "Гости"
    home_id = str(match.get("home_id") or "")
    away_id = str(match.get("away_id") or "")

    team_id_value = _first_text(row, ("teamId", "team_id", "participantId", "competitorId"))
    if team_id_value and home_id and str(team_id_value) == home_id:
        return home, "home"
    if team_id_value and away_id and str(team_id_value) == away_id:
        return away, "away"

    side_text = _first_text(row, ("side", "homeAway", "home_away", "teamSide", "position", "belong", "location", "teamType", "isHome"))
    low = side_text.lower()
    if low in {"home", "h", "1", "true", "home_team", "team1"} or "home" in low or "host" in low:
        return home, "home"
    if low in {"away", "a", "2", "false", "away_team", "team2"} or "away" in low or "guest" in low:
        return away, "away"

    team_text = _first_text(row, ("teamName", "team_name", "team", "participantName", "competitorName", "clubName"))
    if team_text:
        if isinstance(row.get("team"), dict):
            team_text = _first_text(row.get("team") or {}, ("name", "shortName", "displayName")) or team_text
        low_team = team_text.lower()
        if low_team == home.lower() or low_team in home.lower() or home.lower() in low_team:
            return home, "home"
        if low_team == away.lower() or low_team in away.lower() or away.lower() in low_team:
            return away, "away"
        return team_text, ""

    # A few feeds encode the acting side with small booleans.
    for key in ("home", "isHomeTeam", "homeTeam"):
        if isinstance(row.get(key), bool):
            return (home, "home") if row.get(key) else (away, "away")
    return "", ""


def _normalise_event(row: Any, match: dict[str, Any], source_path: str = "") -> dict[str, Any] | None:
    if not isinstance(row, dict):
        return None
    kind = _event_type_from_row(row, source_path)
    if kind not in {"goal", "yellow", "red"}:
        return None
    minute, minute_text = _minute_from_event(row)
    if not _event_minute_is_valid(minute, minute_text):
        return None
    team, side = _team_from_event(row, match)
    label = "Гол" if kind == "goal" else ("Жёлтая карточка" if kind == "yellow" else "Красная карточка")
    icon = "⚽" if kind == "goal" else ("🟨" if kind == "yellow" else "🟥")
    # Prefer provider player text as a small note if it is present.
    player = _first_text(row, ("playerName", "player_name", "player", "athlete", "name"))
    if player and player == team:
        player = ""
    return {
        "minute": minute,
        "minute_text": minute_text,
        "type": kind,
        "label": label,
        "team": team,
        "side": side,
        "player": player,
        "icon": icon,
    }


def _event_key(ev: dict[str, Any]) -> tuple[Any, ...]:
    return (ev.get("type"), ev.get("minute"), ev.get("team"), ev.get("player"))


def events_from_match_info(info: dict[str, Any], match: dict[str, Any]) -> list[dict[str, Any]]:
    result = info.get("result") if isinstance(info, dict) else info
    events: list[dict[str, Any]] = []
    seen: set[tuple[Any, ...]] = set()

    for path, rows in _deep_find_event_lists(result):
        for row in rows or []:
            ev = _normalise_event(row, match, path)
            if not ev:
                continue
            key = _event_key(ev)
            if key in seen:
                continue
            seen.add(key)
            events.append(ev)

    # Some APIs put goals/cards in dictionaries instead of event arrays.
    if isinstance(result, dict):
        for key, value in result.items():
            k = str(key or "").lower()
            if any(token in k for token in ("goal", "yellow", "red", "card")):
                rows = value if isinstance(value, list) else []
                for row in rows:
                    ev = _normalise_event(row, match, k)
                    if ev and _event_key(ev) not in seen:
                        seen.add(_event_key(ev))
                        events.append(ev)

    events.sort(key=lambda e: (_safe_int(e.get("minute"), 999), 0 if e.get("type") == "goal" else 1, str(e.get("team") or "")))
    return events[:40]


def match_events_cached(match_id_value: str, match: dict[str, Any] | None = None, stats: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    mid = str(match_id_value or "").strip()
    if not mid:
        return []
    # v7: prefer events the collector derived from score changes — they are
    # always populated for any match we've ever seen go from 0-0 → 1-0 etc.
    db_events = _events_from_db(mid)
    now = time.time()
    with _cache_lock:
        cached = _events_cache.get(mid)
        if cached and now - float(cached.get("saved_at") or 0) < EVENT_CACHE_SECONDS:
            api_events = list(cached.get("events") or [])
        else:
            cached = None
            api_events = None
    if api_events is None:
        try:
            info = match_info(mid)
            api_events = events_from_match_info(info, match or {})
        except Exception:
            api_events = []
        with _cache_lock:
            _events_cache[mid] = {"saved_at": now, "events": api_events}
    # Merge — API events (with player names) and DB-derived goal/card changes.
    # DB fills gaps and also keeps yellow/red cards detected from live stats.
    merged: list[dict[str, Any]] = []
    seen: set[tuple[Any, ...]] = set()
    for ev in list(api_events or []) + list(db_events or []):
        if not _event_minute_is_valid(_safe_int(ev.get("minute"), 0), str(ev.get("minute_text") or "")):
            continue
        key = _event_key(ev)
        if key in seen:
            continue
        seen.add(key)
        merged.append(ev)
    merged.sort(key=lambda e: (_safe_int(e.get("minute"), 999), 0 if e.get("type") == "goal" else 1, str(e.get("team") or "")))
    return merged[:60]

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
            return mid, flat_stats(match_stats_cached(mid, item))
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
        "matches": [],
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


def _recent_match_date_text(match: dict[str, Any]) -> str:
    """Compact dd.mm date for recent-match cards, tolerant to IGScore field names."""
    for key in ("matchTime", "startTime", "kickoffTime", "beginTime", "openTime", "timestamp"):
        value = match.get(key)
        if value in (None, ""):
            continue
        try:
            n = float(value)
            if n > 1000000000000:
                n = n / 1000.0
            if n > 100000000:
                return _dt.datetime.fromtimestamp(n).strftime("%d.%m")
        except Exception:
            pass
    for key in ("matchDate", "startDate", "date", "match_time", "time"):
        value = str(match.get(key) or "").strip()
        if not value:
            continue
        m = re.search(r"(\d{4})[-./](\d{1,2})[-./](\d{1,2})", value)
        if m:
            return f"{int(m.group(3)):02d}.{int(m.group(2)):02d}"
        m = re.search(r"(\d{1,2})[-./](\d{1,2})(?:[-./]\d{2,4})?", value)
        if m:
            return f"{int(m.group(1)):02d}.{int(m.group(2)):02d}"
    return ""


def _recent_match_summary(match: dict[str, Any], team_id_value: str, result: str, gs: int, gc: int) -> dict[str, Any]:
    h, a, score = score_from_match(match)
    home_id = team_id(match.get("homeTeam"))
    away_id = team_id(match.get("awayTeam"))
    home_name = team_name(match.get("homeTeam")) or str(match.get("home") or match.get("homeName") or "Home").strip()
    away_name = team_name(match.get("awayTeam")) or str(match.get("away") or match.get("awayName") or "Away").strip()
    team_norm = str(team_id_value or "")
    side = "home" if team_norm and team_norm == home_id else "away" if team_norm and team_norm == away_id else ""
    opponent = away_name if side == "home" else home_name if side == "away" else (away_name or home_name)
    return {
        "date": _recent_match_date_text(match),
        "league": league_from_match(match),
        "country": country_from_match(match),
        "home": home_name,
        "away": away_name,
        "opponent": opponent,
        "side": side,
        "score": score,
        "score_home": h,
        "score_away": a,
        "scored": gs,
        "conceded": gc,
        "total": h + a,
        "result": result,
    }


def avg_total_for_team(team_id_value: str) -> dict[str, Any]:
    """Average recent goals/total profile for one team over up to 10 last matches.

    v9.36: TTL-cached for TEAM_AVG_CACHE_SECONDS (default 10 min). The
    underlying /v1/football/match/analysis/recent response only changes when
    the team plays a new match, so a long TTL is safe.
    """
    tid = str(team_id_value or "").strip()
    if not tid:
        return _avg_empty()

    now = now_ts()
    if TEAM_AVG_CACHE_SECONDS > 0:
        with _cache_lock:
            cached = _team_avg_cache.get(tid)
            if cached and now - float(cached.get("saved_at") or 0) < TEAM_AVG_CACHE_SECONDS:
                # Return a shallow copy so callers can't mutate the cached
                # dict (e.g. by appending to `matches`). Nested lists are
                # treated as read-only by all callers today.
                return dict(cached.get("payload") or {})

    payload = _avg_total_for_team_impl(tid)

    if TEAM_AVG_CACHE_SECONDS > 0:
        with _cache_lock:
            _team_avg_cache[tid] = {"saved_at": now, "payload": payload}
            # Keep memory bounded. With a 10-min TTL the cache should stay
            # under a few hundred entries for a typical workload, but cap it
            # anyway in case of long uptime.
            if len(_team_avg_cache) > 4096:
                stale_cutoff = now - max(60.0, TEAM_AVG_CACHE_SECONDS * 4)
                for k in [k for k, v in _team_avg_cache.items() if float(v.get("saved_at") or 0) < stale_cutoff]:
                    _team_avg_cache.pop(k, None)

    return payload


def _avg_total_for_team_impl(team_id_value: str) -> dict[str, Any]:
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
        wins = 0
        draws = 0
        losses = 0
        form: list[str] = []          # "W" / "D" / "L" newest first
        recent_matches: list[dict[str, Any]] = []
        team_id_norm = str(team_id_value)
        for match in iter_result_matches(resp):
            h, a, _score = score_from_match(match)
            total = h + a
            totals.append(total)
            if h == 0 and a == 0:
                zero_zero += 1

            home_id = team_id(match.get("homeTeam"))
            away_id = team_id(match.get("awayTeam"))
            is_home = team_id_norm and team_id_norm == home_id
            is_away = team_id_norm and team_id_norm == away_id

            if is_home:
                gs, gc = h, a
            elif is_away:
                gs, gc = a, h
            else:
                gs, gc = h, a

            scored.append(gs)
            conceded.append(gc)

            # W / D / L from this team's perspective
            if gs > gc:
                result_code = "W"
                wins += 1; form.append(result_code)
            elif gs == gc:
                result_code = "D"
                draws += 1; form.append(result_code)
            else:
                result_code = "L"
                losses += 1; form.append(result_code)

            recent_matches.append(_recent_match_summary(match, team_id_norm, result_code, gs, gc))

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
            # v8c: W/D/L for the last 10 matches
            "wins": wins,
            "draws": draws,
            "losses": losses,
            "form": form,          # list of "W"/"D"/"L", newest first
            "matches": recent_matches[:10],
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


def avg_bulk_payload_for_matches(match_ids: list[str]) -> dict[str, Any]:
    """Return the average-total profile for many matches at once.

    Built for the "ТБ 2.5 / ТМ 1.5" filter tabs which need the two team
    averages for every visible live match. We resolve match_id → (home_id,
    away_id) from the live payload first (no external calls), then fetch each
    distinct team's averages via avg_total_for_team — which is TTL-cached, so
    the second user opening the same view pays almost nothing.

    Shape:
        {
          "ok": True,
          "items": {
            "<match_id>": {
              "h": <home_total_avg or null>,
              "a": <away_total_avg or null>,
              "t": <(h + a)/2 or null>
            },
            ...
          },
          "count": N,                 # number of matches resolved
          "fetched_teams": M,         # distinct teams actually queried
        }

    Matches we couldn't resolve (no team ids in any cache) are still included
    in `items` but with h=a=t=None, so the frontend can show "—" instead of
    filtering them out silently.
    """
    seen_ids: list[str] = []
    seen_set: set[str] = set()
    for raw in match_ids or []:
        mid = str(raw or "").strip()
        if mid and mid not in seen_set:
            seen_set.add(mid)
            seen_ids.append(mid)

    if not seen_ids:
        return {"ok": True, "items": {}, "count": 0, "fetched_teams": 0}

    # Resolve match_id → (home_id, away_id). Try (in order):
    #   1. The collector DB row (always present for currently-live matches);
    #   2. The cached live payload from IGScore;
    #   3. The raw IGScore live cache.
    live_payload = load_live_payload(force=False)
    live_lookup: dict[str, dict[str, Any]] = {}
    for m in flatten_payload(live_payload):
        mid = str(m.get("id") or "").strip()
        if mid:
            live_lookup[mid] = m

    with _cache_lock:
        raw_live = dict(_live_cache.get("raw") or {})

    match_pairs: dict[str, tuple[str, str]] = {}
    for mid in seen_ids:
        home_id = ""
        away_id = ""
        # 1. collector DB
        detail = collector_detail_for_match(mid)
        if detail:
            m = detail.get("match") or {}
            home_id = str(m.get("home_id") or "")
            away_id = str(m.get("away_id") or "")
        # 2. live payload
        if not home_id or not away_id:
            m = live_lookup.get(mid) or {}
            home_id = home_id or str(m.get("home_id") or "")
            away_id = away_id or str(m.get("away_id") or "")
        # 3. raw IGScore
        if not home_id or not away_id:
            raw = raw_live.get(mid) or {}
            home_id = home_id or team_id(raw.get("homeTeam"))
            away_id = away_id or team_id(raw.get("awayTeam"))
        match_pairs[mid] = (home_id, away_id)

    # Build the set of distinct team ids we actually need to fetch averages
    # for. Empty strings are ignored.
    needed_teams: set[str] = set()
    for h, a in match_pairs.values():
        if h:
            needed_teams.add(h)
        if a:
            needed_teams.add(a)

    # Fetch (or read from cache) the average profile for each distinct team.
    # avg_total_for_team already memoises results, so this loop is cheap on
    # warm caches and expensive only on a cold start.
    team_avg: dict[str, dict[str, Any]] = {}
    fetched = 0
    for tid in needed_teams:
        before_cached = False
        if TEAM_AVG_CACHE_SECONDS > 0:
            with _cache_lock:
                c = _team_avg_cache.get(tid)
                if c and now_ts() - float(c.get("saved_at") or 0) < TEAM_AVG_CACHE_SECONDS:
                    before_cached = True
        team_avg[tid] = avg_total_for_team(tid)
        if not before_cached:
            fetched += 1

    items: dict[str, dict[str, Any]] = {}
    for mid, (h_id, a_id) in match_pairs.items():
        h_avg = team_avg.get(h_id, {}).get("total_avg") if h_id else None
        a_avg = team_avg.get(a_id, {}).get("total_avg") if a_id else None
        t_avg = None
        if h_avg is not None and a_avg is not None:
            t_avg = round((float(h_avg) + float(a_avg)) / 2.0, 2)
        items[mid] = {
            "h": h_avg,
            "a": a_avg,
            "t": t_avg,
        }

    return {
        "ok": True,
        "items": items,
        "count": len(items),
        "fetched_teams": fetched,
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
    demo_events = [
        {"minute": 12, "minute_text": "12’", "type": "goal", "label": "Гол", "team": match.get("home") or "Хозяева", "side": "home", "icon": "⚽"},
        {"minute": 27, "minute_text": "27’", "type": "yellow", "label": "Жёлтая карточка", "team": match.get("away") or "Гости", "side": "away", "icon": "🟨"},
        {"minute": 41, "minute_text": "41’", "type": "goal", "label": "Гол", "team": match.get("away") or "Гости", "side": "away", "icon": "⚽"},
        {"minute": 63, "minute_text": "63’", "type": "yellow", "label": "Жёлтая карточка", "team": match.get("home") or "Хозяева", "side": "home", "icon": "🟨"},
    ]
    return {"match": match, "stats": {k: v for k, v in p.items() if k != "avg"}, "avg": p["avg"], "events": demo_events}


def _safe_odds(v: Any) -> float | None:
    """Convert various odds formats to a float or None."""
    try:
        f = float(str(v).strip().replace(",", "."))
        return round(f, 2) if 1.0 <= f <= 100.0 else None
    except Exception:
        return None


# Cache: match_id → {"saved_at": float, "odds": dict}
_odds_cache: dict[str, dict[str, Any]] = {}
ODDS_CACHE_SECONDS = 30

# v9.6: lightweight odds presence cache for the main feed sorting.
# It stores only whether a match currently has live coefficients. Details still
# load through /api/match when the user opens a card.
ODDS_FEED_ENABLED = os.environ.get("ODDS_FEED_ENABLED", "1").strip().lower() not in {"0", "false", "no", "off"}
ODDS_FEED_CACHE_SECONDS = float(os.environ.get("ODDS_FEED_CACHE_SECONDS", "45"))
ODDS_FEED_MAX_MATCHES = int(os.environ.get("ODDS_FEED_MAX_MATCHES", "180"))
ODDS_FEED_BATCH_SIZE = max(1, int(os.environ.get("ODDS_FEED_BATCH_SIZE", "60")))
_odds_presence_cache: dict[str, dict[str, Any]] = {}

# Pressure indicator: compare current live statistics with the oldest stored
# snapshot inside the last 20 minutes. The collector writes one stats snapshot
# per match on every refresh cycle.
PRESSURE_WINDOW_SECONDS = int(os.environ.get("PRESSURE_WINDOW_SECONDS", "1200"))
PRESSURE_HISTORY_KEEP_SECONDS = int(os.environ.get("PRESSURE_HISTORY_KEEP_SECONDS", "10800"))


def parse_odds_response(resp: dict[str, Any], match_id: str = "") -> dict[str, Any]:
    """Parse result.matchRecentOdds[matchId] list from /match/odds/last."""
    result = resp.get("result") if isinstance(resp, dict) else {}
    if not isinstance(result, dict):
        return {}

    match_recent = result.get("matchRecentOdds") or {}
    items: list[Any] = []
    if match_id and match_id in match_recent:
        items = match_recent[match_id] or []
    elif match_recent:
        # fallback: first key (should not happen normally)
        items = next(iter(match_recent.values())) or []

    out: dict[str, Any] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        odds_type = str(item.get("oddsType") or "").lower()
        raw_data = item.get("oddsData") or []
        if not isinstance(raw_data, list) or len(raw_data) < 2:
            continue

        if odds_type == "eu" and "eu" not in out:
            o1 = _safe_odds(raw_data[0])
            ox = _safe_odds(raw_data[1]) if len(raw_data) > 1 else None
            o2 = _safe_odds(raw_data[2]) if len(raw_data) > 2 else None
            if o1 or o2:
                out["eu"] = {"1": o1, "X": ox, "2": o2}

        elif odds_type == "bs" and "bs" not in out:
            ov   = _safe_odds(raw_data[0])
            line = raw_data[1] if len(raw_data) > 1 else None
            un   = _safe_odds(raw_data[2]) if len(raw_data) > 2 else None
            try:
                line_f = round(float(str(line).replace(",", ".")), 2) if line is not None else None
            except Exception:
                line_f = None
            if ov or un:
                out["bs"] = {"over": ov, "line": line_f, "under": un}

        elif odds_type == "asia" and "asia" not in out:
            oh  = _safe_odds(raw_data[0])
            hcp = raw_data[1] if len(raw_data) > 1 else None
            oa  = _safe_odds(raw_data[2]) if len(raw_data) > 2 else None
            try:
                hcp_f = round(float(str(hcp).replace(",", ".")), 2) if hcp is not None else None
            except Exception:
                hcp_f = None
            if oh or oa:
                out["asia"] = {"home": oh, "handicap": hcp_f, "away": oa}

    return out




def _odds_items_have_values(items: Any) -> bool:
    """Return True when IGScore returned at least one usable market item."""
    if not isinstance(items, list):
        return False
    for item in items:
        if not isinstance(item, dict):
            continue
        odds_type = str(item.get("oddsType") or "").lower()
        if odds_type not in {"eu", "bs", "asia"}:
            continue
        raw = item.get("oddsData") or []
        if not isinstance(raw, list) or len(raw) < 3:
            continue
        if _safe_odds(raw[0]) is not None or _safe_odds(raw[2]) is not None:
            return True
    return False


def odds_presence_for_match_ids(match_ids: list[str]) -> dict[str, bool]:
    """Return match_id -> has_odds using the bulk odds endpoint + short cache."""
    if not ODDS_FEED_ENABLED:
        return {}
    now = time.time()
    ids: list[str] = []
    seen: set[str] = set()
    for value in match_ids or []:
        mid = str(value or "").strip()
        if mid and mid not in seen:
            ids.append(mid)
            seen.add(mid)
    ids = ids[:max(0, ODDS_FEED_MAX_MATCHES)]
    if not ids:
        return {}

    out: dict[str, bool] = {}
    to_fetch: list[str] = []
    with _cache_lock:
        for mid in ids:
            cached = _odds_presence_cache.get(mid)
            if cached and now - float(cached.get("saved_at") or 0) < ODDS_FEED_CACHE_SECONDS:
                out[mid] = bool(cached.get("has_odds"))
            else:
                to_fetch.append(mid)

    for start in range(0, len(to_fetch), ODDS_FEED_BATCH_SIZE):
        batch = to_fetch[start:start + ODDS_FEED_BATCH_SIZE]
        try:
            resp = match_odds_last_many(batch)
            result = resp.get("result") if isinstance(resp, dict) else {}
            recent = (result or {}).get("matchRecentOdds") if isinstance(result, dict) else {}
            recent = recent if isinstance(recent, dict) else {}
        except Exception as exc:
            print(f"[odds-feed] bulk fetch failed: {exc}")
            recent = {}

        with _cache_lock:
            for mid in batch:
                items = recent.get(mid) or []
                parsed = parse_odds_response({"result": {"matchRecentOdds": {mid: items}}}, match_id=mid) if items else {}
                has = bool(parsed) or _odds_items_have_values(items)
                _odds_presence_cache[mid] = {"saved_at": now, "has_odds": has}
                if parsed:
                    _odds_cache[mid] = {"saved_at": now, "odds": parsed}
                out[mid] = has

    return out


def annotate_live_payload_with_odds(payload: dict[str, Any]) -> dict[str, Any]:
    """Mutate /api/live payload: add match.has_odds and league.has_odds."""
    if not isinstance(payload, dict) or not ODDS_FEED_ENABLED:
        return payload
    ids: list[str] = []
    leagues: list[dict[str, Any]] = []
    for country in payload.get("countries") or []:
        for league in country.get("leagues") or []:
            if isinstance(league, dict):
                leagues.append(league)
                for m in league.get("matches") or []:
                    if isinstance(m, dict) and m.get("id"):
                        ids.append(str(m.get("id")))
    presence = odds_presence_for_match_ids(ids)
    for league in leagues:
        league_has = False
        league_count = 0
        for m in league.get("matches") or []:
            if not isinstance(m, dict):
                continue
            has = bool(presence.get(str(m.get("id") or "")))
            m["has_odds"] = has
            if has:
                league_has = True
                league_count += 1
        league["has_odds"] = league_has
        league["odds_count"] = league_count
    payload["odds_feed_enabled"] = True
    return payload


def fetch_match_odds_cached(match_id: str) -> dict[str, Any]:
    """Fetch and cache live odds for a match. Returns parsed odds dict."""
    mid = str(match_id or "").strip()
    if not mid:
        return {}
    now = time.time()
    with _cache_lock:
        cached = _odds_cache.get(mid)
        if cached and now - float(cached.get("saved_at") or 0) < ODDS_CACHE_SECONDS:
            return dict(cached.get("odds") or {})
    try:
        resp = match_odds_last(mid)
        parsed = parse_odds_response(resp, match_id=mid)
        if ODDS_DEBUG_LOG:
            raw_preview = str(resp)[:400]
            print(f"[odds] raw for {mid}: {raw_preview}")
            print(f"[odds] parsed for {mid}: {parsed}")
    except Exception as exc:
        print(f"[odds] fetch failed {mid}: {exc}")
        parsed = {}
    with _cache_lock:
        _odds_cache[mid] = {"saved_at": now, "odds": parsed}
    return parsed


def detail_payload(match_id_value: str) -> dict[str, Any]:
    live_payload = load_live_payload(force=False)
    matches = flatten_payload(live_payload)
    match = next((m for m in matches if str(m.get("id")) == str(match_id_value)), None)

    if not match:
        # Finished favorites often disappear from /api/live before the browser
        # cache receives the final score. Refresh the exact match card from
        # /v1/football/match/info and merge its score into the DB/local detail.
        fresh_info = fresh_match_info_public(match_id_value, force=True)

        collector_detail = collector_detail_for_match(match_id_value)
        if collector_detail:
            collector_detail.setdefault("avg", {"home": _avg_empty(), "away": _avg_empty()})
            if fresh_info and isinstance(collector_detail.get("match"), dict):
                collector_detail["match"] = _merge_fresh_match(collector_detail["match"], fresh_info)
            collector_detail["finished"] = True
            return {"ok": True, **collector_detail}

        db_detail = sqlite_detail_for_match(match_id_value)
        if db_detail:
            db_detail.setdefault("avg", {"home": _avg_empty(), "away": _avg_empty()})
            if fresh_info and isinstance(db_detail.get("match"), dict):
                db_detail["match"] = _merge_fresh_match(db_detail["match"], fresh_info)
            db_detail["finished"] = True
            return {"ok": True, **db_detail}

        if fresh_info:
            stats = {}
            try:
                stats = match_stats_cached(str(match_id_value), fresh_info)
            except Exception:
                stats = {}
            return {
                "ok": True,
                "match": {**fresh_info, "finished": True},
                "stats": stats or {},
                "events": _events_from_db(str(match_id_value)),
                "avg": {"home": _avg_empty(), "away": _avg_empty()},
                "odds": {},
                "pressure": pressure_from_stats_history(str(match_id_value)),
                "pressure_chart": pressure_chart_from_history(str(match_id_value), fresh_info),
                "finished": True,
            }

        # v6: match not found anywhere — was likely deleted after finishing.
        # Return a clear "not found" so the frontend can fall back to its
        # localStorage cache (for favorites that were saved while the match
        # was still live). Do NOT return a random demo match.
        return {
            "ok": False,
            "error": "match_not_found",
            "match_id": str(match_id_value),
            "message": "Матч завершён и удалён из лайв-кэша. Покажу из локального кэша избранного.",
        }

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
            stats = match_stats_cached(mid, match)
        except Exception:
            stats = {}

    if not stats:
        stats = demo_detail(match)["stats"]

    events = match_events_cached(mid, match, stats) if mid else []

    home_id = team_id(raw.get("homeTeam"))
    away_id = team_id(raw.get("awayTeam"))
    if home_id or away_id:
        avg = {"home": avg_total_for_team(home_id), "away": avg_total_for_team(away_id)}

    # v9: fetch live odds from /v1/football/match/odds/last (30s cache)
    odds: dict[str, Any] = {}
    if mid:
        try:
            odds = fetch_match_odds_cached(mid)
        except Exception:
            odds = {}

    pressure = pressure_from_stats_history(mid) if mid else {}
    pressure_chart = pressure_chart_from_history(mid, match) if mid else {}

    return {"ok": True, "match": match, "stats": stats, "avg": avg, "events": events, "odds": odds, "pressure": pressure, "pressure_chart": pressure_chart}


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
_notify_storage_ready = False


def _json_dumps_compact(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _notify_sqlite_conn() -> sqlite3.Connection:
    NOTIFY_STATE_DB.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(NOTIFY_STATE_DB), timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def _notify_pg_conn():
    if psycopg is None:
        raise RuntimeError("psycopg is not installed; add psycopg[binary] to requirements.txt")
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is empty; connect Render Postgres or set DATABASE_URL")
    return psycopg.connect(DATABASE_URL, connect_timeout=10)


def _notify_table_name(table: str) -> str:
    if table not in {"notify_subs", "notify_matches"}:
        raise ValueError(f"invalid notify table: {table}")
    return table


def _read_json_file(path: Path, default: Any) -> Any:
    try:
        if path.exists():
            return json.loads(path.read_text("utf-8") or "null") or default
    except Exception as exc:
        print(f"[notify] JSON read failed for {path.name}: {exc}")
    return default


def _iter_sqlite_notify_table(table: str) -> list[tuple[str, Any, int]]:
    """Read old SQLite notification rows for one-time Postgres migration."""
    table = _notify_table_name(table)
    if not NOTIFY_STATE_DB.exists():
        return []
    rows: list[tuple[str, Any, int]] = []
    try:
        with _notify_sqlite_conn() as conn:
            found = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (table,),
            ).fetchone()
            if not found:
                return []
            for row in conn.execute(f"SELECT chat_id,payload,updated_at FROM {table}"):
                try:
                    rows.append((str(row["chat_id"]), json.loads(row["payload"] or "null"), _safe_int(row["updated_at"], int(time.time()))))
                except Exception:
                    continue
    except Exception as exc:
        print(f"[notify] old SQLite migration read failed for {table}: {exc}")
    return rows


def _init_notify_storage() -> None:
    """Create/migrate notification storage. Safe to call repeatedly.

    Storage modes:
      - postgres: recommended for many users; uses DATABASE_URL.
      - sqlite: local file fallback.
      - json: emergency rollback only.

    Old notify_subs.json, notify_matches.json and notify_state.sqlite3 are
    migrated automatically when available.
    """
    global _notify_storage_ready, NOTIFY_STORAGE
    if NOTIFY_STORAGE == "json":
        _notify_storage_ready = True
        return
    if NOTIFY_STORAGE == "postgres":
        try:
            with _notify_pg_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS schema_migrations (
                            name TEXT PRIMARY KEY,
                            applied_at BIGINT NOT NULL
                        )
                        """
                    )
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS notify_subs (
                            chat_id TEXT PRIMARY KEY,
                            payload TEXT NOT NULL,
                            updated_at BIGINT NOT NULL
                        )
                        """
                    )
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS notify_matches (
                            chat_id TEXT PRIMARY KEY,
                            payload TEXT NOT NULL,
                            updated_at BIGINT NOT NULL
                        )
                        """
                    )
                    cur.execute("CREATE INDEX IF NOT EXISTS idx_notify_subs_updated_at ON notify_subs(updated_at)")
                    cur.execute("CREATE INDEX IF NOT EXISTS idx_notify_matches_updated_at ON notify_matches(updated_at)")
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS telegram_jobs (
                            chat_id TEXT NOT NULL,
                            job_key TEXT NOT NULL,
                            text TEXT NOT NULL,
                            link TEXT NOT NULL DEFAULT '',
                            legacy_keys TEXT NOT NULL DEFAULT '[]',
                            attempts BIGINT NOT NULL DEFAULT 0,
                            next_try_at BIGINT NOT NULL DEFAULT 0,
                            created_at BIGINT NOT NULL,
                            updated_at BIGINT NOT NULL,
                            last_error TEXT NOT NULL DEFAULT '',
                            PRIMARY KEY(chat_id, job_key)
                        )
                        """
                    )
                    cur.execute("CREATE INDEX IF NOT EXISTS idx_telegram_jobs_next_try ON telegram_jobs(next_try_at, updated_at)")
                    cur.execute("SELECT 1 FROM schema_migrations WHERE name=%s", ("001_json_notify_state",))
                    migrated_json = cur.fetchone()
                    if NOTIFY_MIGRATE_JSON and not migrated_json:
                        now = int(time.time())
                        old_subs = _read_json_file(NOTIFY_DB, {})
                        old_matches = _read_json_file(NOTIFY_MATCHES_DB, {})
                        if isinstance(old_subs, dict):
                            for chat_id, sub in old_subs.items():
                                if not chat_id or not isinstance(sub, dict):
                                    continue
                                cur.execute(
                                    """
                                    INSERT INTO notify_subs(chat_id,payload,updated_at)
                                    VALUES(%s,%s,%s)
                                    ON CONFLICT(chat_id) DO UPDATE SET
                                        payload=EXCLUDED.payload,
                                        updated_at=EXCLUDED.updated_at
                                    """,
                                    (str(chat_id), _json_dumps_compact(sub), _safe_int(sub.get("updated_at"), now)),
                                )
                        if isinstance(old_matches, dict):
                            for chat_id, items in old_matches.items():
                                if not chat_id or not isinstance(items, list):
                                    continue
                                cur.execute(
                                    """
                                    INSERT INTO notify_matches(chat_id,payload,updated_at)
                                    VALUES(%s,%s,%s)
                                    ON CONFLICT(chat_id) DO UPDATE SET
                                        payload=EXCLUDED.payload,
                                        updated_at=EXCLUDED.updated_at
                                    """,
                                    (str(chat_id), _json_dumps_compact(items[:NOTIFY_MATCHES_MAX_PER_USER]), now),
                                )
                        cur.execute(
                            "INSERT INTO schema_migrations(name,applied_at) VALUES(%s,%s) ON CONFLICT(name) DO NOTHING",
                            ("001_json_notify_state", now),
                        )
                    cur.execute("SELECT 1 FROM schema_migrations WHERE name=%s", ("002_sqlite_notify_state",))
                    migrated_sqlite = cur.fetchone()
                    if NOTIFY_MIGRATE_SQLITE and not migrated_sqlite:
                        now = int(time.time())
                        for chat_id, payload, updated_at in _iter_sqlite_notify_table("notify_subs"):
                            if not chat_id or not isinstance(payload, dict):
                                continue
                            cur.execute(
                                """
                                INSERT INTO notify_subs(chat_id,payload,updated_at)
                                VALUES(%s,%s,%s)
                                ON CONFLICT(chat_id) DO UPDATE SET
                                    payload=EXCLUDED.payload,
                                    updated_at=EXCLUDED.updated_at
                                """,
                                (chat_id, _json_dumps_compact(payload), _safe_int(updated_at, now)),
                            )
                        for chat_id, payload, updated_at in _iter_sqlite_notify_table("notify_matches"):
                            if not chat_id or not isinstance(payload, list):
                                continue
                            cur.execute(
                                """
                                INSERT INTO notify_matches(chat_id,payload,updated_at)
                                VALUES(%s,%s,%s)
                                ON CONFLICT(chat_id) DO UPDATE SET
                                    payload=EXCLUDED.payload,
                                    updated_at=EXCLUDED.updated_at
                                """,
                                (chat_id, _json_dumps_compact(payload[:NOTIFY_MATCHES_MAX_PER_USER]), _safe_int(updated_at, now)),
                            )
                        cur.execute(
                            "INSERT INTO schema_migrations(name,applied_at) VALUES(%s,%s) ON CONFLICT(name) DO NOTHING",
                            ("002_sqlite_notify_state", now),
                        )
                conn.commit()
            _notify_storage_ready = True
            return
        except Exception as exc:
            print(f"[notify] postgres init failed, falling back to SQLite: {exc}")
            traceback.print_exc()
            NOTIFY_STORAGE = "sqlite"
    if NOTIFY_STORAGE != "sqlite":
        _notify_storage_ready = True
        return
    try:
        with _notify_sqlite_conn() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    name TEXT PRIMARY KEY,
                    applied_at INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS notify_subs (
                    chat_id TEXT PRIMARY KEY,
                    payload TEXT NOT NULL,
                    updated_at INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS notify_matches (
                    chat_id TEXT PRIMARY KEY,
                    payload TEXT NOT NULL,
                    updated_at INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS telegram_jobs (
                    chat_id TEXT NOT NULL,
                    job_key TEXT NOT NULL,
                    text TEXT NOT NULL,
                    link TEXT NOT NULL DEFAULT '',
                    legacy_keys TEXT NOT NULL DEFAULT '[]',
                    attempts INTEGER NOT NULL DEFAULT 0,
                    next_try_at INTEGER NOT NULL DEFAULT 0,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    last_error TEXT NOT NULL DEFAULT '',
                    PRIMARY KEY(chat_id, job_key)
                );
                CREATE INDEX IF NOT EXISTS idx_notify_subs_updated_at ON notify_subs(updated_at);
                CREATE INDEX IF NOT EXISTS idx_notify_matches_updated_at ON notify_matches(updated_at);
                CREATE INDEX IF NOT EXISTS idx_telegram_jobs_next_try ON telegram_jobs(next_try_at, updated_at);
                """
            )
            migrated = conn.execute(
                "SELECT 1 FROM schema_migrations WHERE name=?",
                ("001_json_notify_state",),
            ).fetchone()
            if NOTIFY_MIGRATE_JSON and not migrated:
                now = int(time.time())
                old_subs = _read_json_file(NOTIFY_DB, {})
                old_matches = _read_json_file(NOTIFY_MATCHES_DB, {})
                if isinstance(old_subs, dict):
                    for chat_id, sub in old_subs.items():
                        if not chat_id or not isinstance(sub, dict):
                            continue
                        conn.execute(
                            "INSERT OR REPLACE INTO notify_subs(chat_id,payload,updated_at) VALUES(?,?,?)",
                            (str(chat_id), _json_dumps_compact(sub), _safe_int(sub.get("updated_at"), now)),
                        )
                if isinstance(old_matches, dict):
                    for chat_id, items in old_matches.items():
                        if not chat_id or not isinstance(items, list):
                            continue
                        conn.execute(
                            "INSERT OR REPLACE INTO notify_matches(chat_id,payload,updated_at) VALUES(?,?,?)",
                            (str(chat_id), _json_dumps_compact(items[:NOTIFY_MATCHES_MAX_PER_USER]), now),
                        )
                conn.execute(
                    "INSERT OR REPLACE INTO schema_migrations(name,applied_at) VALUES(?,?)",
                    ("001_json_notify_state", now),
                )
            conn.commit()
        _notify_storage_ready = True
    except Exception as exc:
        print(f"[notify] sqlite init failed, falling back to JSON: {exc}")
        traceback.print_exc()
        NOTIFY_STORAGE = "json"
        _notify_storage_ready = True


def _load_notify_table(table: str) -> dict[str, Any]:
    table = _notify_table_name(table)
    if NOTIFY_STORAGE not in {"sqlite", "postgres"}:
        return {}
    _init_notify_storage()
    out: dict[str, Any] = {}
    try:
        if NOTIFY_STORAGE == "postgres":
            with _notify_pg_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"SELECT chat_id,payload FROM {table}")
                    for chat_id, payload in cur.fetchall():
                        try:
                            out[str(chat_id)] = json.loads(payload or "null")
                        except Exception:
                            continue
            return out
        with _notify_sqlite_conn() as conn:
            for row in conn.execute(f"SELECT chat_id,payload FROM {table}"):
                try:
                    out[str(row["chat_id"])] = json.loads(row["payload"] or "null")
                except Exception:
                    continue
    except Exception as exc:
        print(f"[notify] {NOTIFY_STORAGE} load failed for {table}: {exc}")
    return out


def _save_notify_table(table: str, values: dict[str, Any]) -> None:
    """Persist current notify state without DELETE ALL -> INSERT ALL.

    v9.76: this performs per-chat UPSERTs.  Empty lists/dicts are still saved as
    payloads, so clear/remove operations remain durable without rewriting the
    whole table on every tiny change.
    """
    table = _notify_table_name(table)
    if NOTIFY_STORAGE not in {"sqlite", "postgres"}:
        return
    _init_notify_storage()
    now = int(time.time())
    try:
        if NOTIFY_STORAGE == "postgres":
            with _notify_pg_conn() as conn:
                with conn.cursor() as cur:
                    for chat_id, payload in values.items():
                        if not str(chat_id):
                            continue
                        updated_at = now
                        if isinstance(payload, dict):
                            updated_at = _safe_int(payload.get("updated_at"), now)
                        cur.execute(
                            f"""
                            INSERT INTO {table}(chat_id,payload,updated_at)
                            VALUES(%s,%s,%s)
                            ON CONFLICT(chat_id) DO UPDATE SET
                                payload=EXCLUDED.payload,
                                updated_at=EXCLUDED.updated_at
                            """,
                            (str(chat_id), _json_dumps_compact(payload), updated_at),
                        )
                conn.commit()
            return
        with _notify_sqlite_conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            for chat_id, payload in values.items():
                if not str(chat_id):
                    continue
                updated_at = now
                if isinstance(payload, dict):
                    updated_at = _safe_int(payload.get("updated_at"), now)
                conn.execute(
                    f"INSERT OR REPLACE INTO {table}(chat_id,payload,updated_at) VALUES(?,?,?)",
                    (str(chat_id), _json_dumps_compact(payload), updated_at),
                )
            conn.commit()
    except Exception as exc:
        print(f"[notify] {NOTIFY_STORAGE} save failed for {table}: {exc}")
        traceback.print_exc()



def _online_limit_clamp(value: Any) -> int:
    try:
        n = int(value)
    except Exception:
        n = ONLINE_USER_LIMIT_DEFAULT
    return max(1, min(n, 1000000))


def _online_load_limit() -> int:
    global _online_limit_cache
    if _online_limit_cache is not None:
        return _online_limit_cache
    limit = ONLINE_USER_LIMIT_DEFAULT
    try:
        if ONLINE_SETTINGS_DB.exists():
            data = json.loads(ONLINE_SETTINGS_DB.read_text("utf-8") or "{}")
            limit = data.get("limit", limit)
    except Exception as exc:
        print(f"[online] limit load failed: {exc}")
    _online_limit_cache = _online_limit_clamp(limit)
    return _online_limit_cache


def _online_save_limit(limit: Any, admin_id: str | int = "") -> int:
    global _online_limit_cache
    n = _online_limit_clamp(limit)
    _online_limit_cache = n
    try:
        ONLINE_SETTINGS_DB.parent.mkdir(parents=True, exist_ok=True)
        ONLINE_SETTINGS_DB.write_text(json.dumps({
            "limit": n,
            "updated_at": int(time.time()),
            "updated_by": str(admin_id or ""),
        }, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as exc:
        print(f"[online] limit save failed: {exc}")
    return n


def _online_cleanup(now: float | None = None) -> None:
    now = float(now or time.time())
    ttl = max(15, int(ONLINE_USER_TTL_SECONDS or 90))
    cutoff = now - ttl
    for key, info in list(_online_users.items()):
        try:
            last_seen = float((info or {}).get("last_seen") or 0)
        except Exception:
            last_seen = 0
        if last_seen < cutoff:
            _online_users.pop(key, None)


def _online_snapshot(include_users: bool = False) -> dict[str, Any]:
    now = time.time()
    with _online_lock:
        _online_cleanup(now)
        users = list(_online_users.values())
        payload: dict[str, Any] = {
            "online": len(users),
            "limit": _online_load_limit(),
            "ttl": max(15, int(ONLINE_USER_TTL_SECONDS or 90)),
        }
        if include_users:
            payload["users"] = sorted(users, key=lambda x: float(x.get("last_seen") or 0), reverse=True)[:200]
        return payload


def _online_user_from_body(body: dict[str, Any]) -> dict[str, Any] | None:
    init_data = str((body or {}).get("init_data") or "")
    user = verify_init_data(init_data) if init_data else None
    return user if isinstance(user, dict) else None


def _online_user_key(body: dict[str, Any], handler: BaseHTTPRequestHandler | None = None) -> tuple[str, dict[str, Any]]:
    user = _online_user_from_body(body)
    if user and user.get("id"):
        uid = str(user.get("id"))
        return "tg:" + uid, {
            "id": uid,
            "kind": "telegram",
            "first_name": str(user.get("first_name") or ""),
            "username": str(user.get("username") or ""),
        }
    client_id = str((body or {}).get("client_id") or (body or {}).get("user_id") or "").strip()
    if client_id:
        safe = re.sub(r"[^A-Za-z0-9_.:-]", "", client_id)[:96]
        if safe:
            return "client:" + safe, {"id": safe, "kind": "client", "first_name": "", "username": ""}
    ip = ""
    ua = ""
    try:
        if handler is not None:
            ip = str(handler.client_address[0] or "")
            ua = str(handler.headers.get("User-Agent") or "")[:160]
    except Exception:
        pass
    digest = hashlib.sha256((ip + "|" + ua).encode("utf-8", "ignore")).hexdigest()[:24]
    return "anon:" + digest, {"id": digest, "kind": "anonymous", "first_name": "", "username": ""}


def handle_online_checkin(body: dict[str, Any], handler: BaseHTTPRequestHandler | None = None) -> dict[str, Any]:
    now = time.time()
    key, info = _online_user_key(body or {}, handler)
    with _online_lock:
        _online_cleanup(now)
        limit = _online_load_limit()
        already_inside = key in _online_users
        if not already_inside and len(_online_users) >= limit:
            return {
                "ok": False,
                "allowed": False,
                "reason": "online_limit",
                "message": "Лимит онлайн пользователей заполнен. Попробуйте позже.",
                "online": len(_online_users),
                "limit": limit,
                "ttl": max(15, int(ONLINE_USER_TTL_SECONDS or 90)),
            }
        item = dict(info)
        item.update({"key": key, "last_seen": now, "last_seen_ts": int(now)})
        _online_users[key] = item
        return {
            "ok": True,
            "allowed": True,
            "online": len(_online_users),
            "limit": limit,
            "ttl": max(15, int(ONLINE_USER_TTL_SECONDS or 90)),
        }


ADMIN_STATE_DB = DATA_DIR / "admin_state.json"


def _init_admin_storage() -> None:
    """Create persistent admin tables for allowed/blocked users and stats."""
    try:
        if NOTIFY_STORAGE == "postgres":
            with _notify_pg_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS admin_state (
                            key TEXT PRIMARY KEY,
                            payload TEXT NOT NULL,
                            updated_at BIGINT NOT NULL
                        )
                        """
                    )
            return
        if NOTIFY_STORAGE == "sqlite":
            with _notify_sqlite_conn() as conn:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS admin_state (
                        key TEXT PRIMARY KEY,
                        payload TEXT NOT NULL,
                        updated_at INTEGER NOT NULL
                    )
                    """
                )
    except Exception as exc:
        print(f"[admin] storage init failed: {exc}")


def _load_admin_state() -> None:
    global _admin_state
    default = {"allowed_users": {}, "blocked_users": {}, "known_users": {}, "events": []}
    try:
        _init_admin_storage()
        loaded = None
        if NOTIFY_STORAGE == "postgres":
            with _notify_pg_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT payload FROM admin_state WHERE key=%s", ("state",))
                    row = cur.fetchone()
                    if row:
                        loaded = json.loads(row[0] or "{}")
        elif NOTIFY_STORAGE == "sqlite":
            with _notify_sqlite_conn() as conn:
                row = conn.execute("SELECT payload FROM admin_state WHERE key=?", ("state",)).fetchone()
                if row:
                    loaded = json.loads(row["payload"] or "{}")
        elif ADMIN_STATE_DB.exists():
            loaded = json.loads(ADMIN_STATE_DB.read_text("utf-8") or "{}")
        if isinstance(loaded, dict):
            default.update(loaded)
    except Exception as exc:
        print(f"[admin] state load failed: {exc}")
    with _admin_state_lock:
        _admin_state = default


def _save_admin_state() -> None:
    try:
        with _admin_state_lock:
            payload = _json_dumps_compact(_admin_state)
        now = int(time.time())
        if NOTIFY_STORAGE == "postgres":
            with _notify_pg_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO admin_state(key,payload,updated_at)
                        VALUES(%s,%s,%s)
                        ON CONFLICT(key) DO UPDATE SET payload=EXCLUDED.payload, updated_at=EXCLUDED.updated_at
                        """,
                        ("state", payload, now),
                    )
            return
        if NOTIFY_STORAGE == "sqlite":
            with _notify_sqlite_conn() as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO admin_state(key,payload,updated_at) VALUES(?,?,?)",
                    ("state", payload, now),
                )
            return
        ADMIN_STATE_DB.parent.mkdir(parents=True, exist_ok=True)
        ADMIN_STATE_DB.write_text(payload, encoding="utf-8")
    except Exception as exc:
        print(f"[admin] state save failed: {exc}")


def _admin_log(action: str, user_id: str | int = "", admin_id: str | int = "") -> None:
    with _admin_state_lock:
        events = _admin_state.setdefault("events", [])
        events.append({"ts": int(time.time()), "action": str(action), "user_id": str(user_id or ""), "admin_id": str(admin_id or "")})
        del events[:-50]
    _save_admin_state()


def _remember_known_user(user_id: str | int, user: dict[str, Any] | None = None) -> None:
    uid = str(user_id or "").strip()
    if not uid:
        return
    user = user if isinstance(user, dict) else {}
    with _admin_state_lock:
        known = _admin_state.setdefault("known_users", {})
        prev = known.get(uid) if isinstance(known.get(uid), dict) else {}
        known[uid] = {
            **prev,
            "id": uid,
            "first_name": user.get("first_name") or prev.get("first_name") or "",
            "last_name": user.get("last_name") or prev.get("last_name") or "",
            "username": user.get("username") or prev.get("username") or "",
            "last_seen": int(time.time()),
        }
    _save_admin_state()


def _admin_user_is_blocked(user_id: str | int) -> bool:
    uid = str(user_id or "").strip()
    if not uid:
        return False
    with _admin_state_lock:
        return uid in (_admin_state.get("blocked_users") or {})


def _admin_user_is_allowed(user_id: str | int) -> bool:
    uid = str(user_id or "").strip()
    if not uid:
        return False
    try:
        if int(uid) in ADMIN_IDS:
            return True
    except Exception:
        pass
    with _admin_state_lock:
        return uid in (_admin_state.get("allowed_users") or {})


def _admin_user_can_use(user_id: str | int) -> bool:
    uid = str(user_id or "").strip()
    if not uid:
        return False
    if _admin_user_is_blocked(uid):
        return False
    if ADMIN_REQUIRE_ALLOWLIST and not _admin_user_is_allowed(uid):
        return False
    return True


def _admin_add_user(user_id: str | int, admin_id: str | int = "") -> bool:
    uid = str(user_id or "").strip()
    if not uid or not uid.isdigit():
        return False
    now = int(time.time())
    with _admin_state_lock:
        _admin_state.setdefault("allowed_users", {})[uid] = {"id": uid, "added_at": now, "added_by": str(admin_id or "")}
        (_admin_state.setdefault("blocked_users", {})).pop(uid, None)
        _admin_state.setdefault("known_users", {}).setdefault(uid, {"id": uid, "first_name": "", "username": "", "last_seen": now})
    _save_admin_state()
    _admin_log("add_user", uid, admin_id)
    return True


def _admin_delete_user(user_id: str | int, admin_id: str | int = "") -> bool:
    uid = str(user_id or "").strip()
    if not uid or not uid.isdigit():
        return False
    with _notify_lock:
        _notify_subs.pop(uid, None)
        _notify_matches.pop(uid, None)
        # v9.81: точечный DELETE строки в БД, а не перезапись всего словаря.
        _save_one_notify_sub(uid)
        _save_one_notify_matches(uid)
    with _admin_state_lock:
        (_admin_state.setdefault("allowed_users", {})).pop(uid, None)
        (_admin_state.setdefault("known_users", {})).pop(uid, None)
    _save_admin_state()
    _admin_log("delete_user", uid, admin_id)
    return True


def _admin_block_user(user_id: str | int, admin_id: str | int = "") -> bool:
    uid = str(user_id or "").strip()
    if not uid or not uid.isdigit():
        return False
    now = int(time.time())
    with _admin_state_lock:
        _admin_state.setdefault("blocked_users", {})[uid] = {"id": uid, "blocked_at": now, "blocked_by": str(admin_id or "")}
    with _notify_lock:
        sub = _notify_subs.get(uid)
        if isinstance(sub, dict):
            filt = sub.get("filter") if isinstance(sub.get("filter"), dict) else {}
            filt["enabled"] = False
            sub["filter"] = filt
            goal_wait = sub.get("goal_wait") if isinstance(sub.get("goal_wait"), dict) else {}
            goal_wait["enabled"] = False
            sub["goal_wait"] = goal_wait
            sub["pending_telegram"] = []
            sub["updated_at"] = now
            _notify_subs[uid] = sub
            _save_one_notify_sub(uid)
    _save_admin_state()
    _admin_log("block_user", uid, admin_id)
    return True


def _admin_unblock_user(user_id: str | int, admin_id: str | int = "") -> bool:
    uid = str(user_id or "").strip()
    if not uid or not uid.isdigit():
        return False
    with _admin_state_lock:
        existed = (_admin_state.setdefault("blocked_users", {})).pop(uid, None) is not None
    _save_admin_state()
    _admin_log("unblock_user", uid, admin_id)
    return existed


def _load_notify_matches() -> None:
    global _notify_matches
    try:
        if NOTIFY_STORAGE in {"sqlite", "postgres"}:
            loaded = _load_notify_table("notify_matches")
            _notify_matches = {str(k): (v if isinstance(v, list) else []) for k, v in loaded.items()}
            return
        if NOTIFY_MATCHES_DB.exists():
            _notify_matches = json.loads(NOTIFY_MATCHES_DB.read_text("utf-8") or "{}") or {}
        else:
            _notify_matches = {}
    except Exception as exc:
        print(f"[notify] matches load failed: {exc}")
        _notify_matches = {}


def _cleanup_notify_matches_unlocked(now: float | None = None) -> int:
    """Remove old saved notification cards while keeping recent Telegram links open."""
    now_i = int(now or time.time())
    ttl = int(max(1.0, NOTIFICATION_MATCH_RETENTION_HOURS) * 3600)
    cutoff = now_i - ttl
    removed = 0
    for chat_id, items in list(_notify_matches.items()):
        if not isinstance(items, list):
            _notify_matches.pop(chat_id, None)
            removed += 1
            continue
        kept = []
        for item in items:
            if not isinstance(item, dict):
                removed += 1
                continue
            ts = _safe_int(item.get("found_at") or item.get("ts") or item.get("created_at"), now_i)
            if ts and ts < cutoff:
                removed += 1
                continue
            kept.append(item)
        if kept:
            _notify_matches[chat_id] = kept[:NOTIFY_MATCHES_MAX_PER_USER]
        else:
            _notify_matches.pop(chat_id, None)
    return removed


def _save_notify_matches(snapshot: dict[str, Any] | None = None) -> None:
    # v9.80: тот же шаблон, что и в _save_notify_subs — позволяет писать
    # снимок вне _notify_lock.
    data = snapshot if snapshot is not None else _notify_matches
    try:
        if NOTIFY_STORAGE in {"sqlite", "postgres"}:
            _save_notify_table("notify_matches", data)
            return
        NOTIFY_MATCHES_DB.parent.mkdir(parents=True, exist_ok=True)
        NOTIFY_MATCHES_DB.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    except Exception as exc:
        print(f"[notify] matches save failed: {exc}")


def _save_one_notify_matches(chat_id: str | int) -> None:
    """v9.81: точечная запись найденных матчей одного пользователя.
    Аналог _save_one_notify_sub, см. там.
    """
    chat_key = str(chat_id or "").strip()
    if not chat_key:
        return
    try:
        if NOTIFY_STORAGE in {"sqlite", "postgres"}:
            items = _notify_matches.get(chat_key)
            if items is None:
                _delete_notify_row("notify_matches", chat_key)
            else:
                _save_notify_table("notify_matches", {chat_key: items})
            return
        NOTIFY_MATCHES_DB.parent.mkdir(parents=True, exist_ok=True)
        NOTIFY_MATCHES_DB.write_text(json.dumps(_notify_matches, ensure_ascii=False), encoding="utf-8")
    except Exception as exc:
        print(f"[notify] save_one_matches failed for {chat_key}: {exc}")


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
        "alert_score": _safe_int(m.get("alert_score"), 0),
        "alert_reasons": _alert_reasons_list(m.get("alert_reasons") or m.get("alert_reason") or []),
        "alert_reason": str(m.get("alert_reason") or ""),
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
    _cleanup_notify_matches_unlocked(now=ts or time.time())


def _notification_text_for_match(m: dict[str, Any]) -> str:
    kind = str(m.get("alert_kind") or "").strip().lower()
    if kind == "goal":
        return _goal_notification_text_for_match(m)
    home = html.escape(str(m.get("home") or "Home"))
    away = html.escape(str(m.get("away") or "Away"))
    minute = html.escape(str(m.get("minute_text") or (str(m.get("minute")) + "'" if m.get("minute") else "LIVE")))
    score = html.escape(str(m.get("score") or f"{_safe_int(m.get('score_home'), 0)}-{_safe_int(m.get('score_away'), 0)}"))
    prefix = "Ждём гол" if kind == "goal_wait" else "Матч нашёлся"
    rating = _safe_int(m.get("alert_score"), 0)
    extra = ""
    if rating:
        extra += f"\nСила: <b>{rating}/100</b>"
    return f"{prefix}: <b>{home}</b> — <b>{away}</b> · {score} · {minute}{extra}"


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


def _merge_notify_snapshot(live_match: dict[str, Any] | None, snap: dict[str, Any] | None, match_id: str = "") -> dict[str, Any] | None:
    """Merge a live row with the frontend snapshot while preserving alert metadata."""
    if not live_match and not snap:
        return None
    out = dict(live_match or {})
    snap = snap if isinstance(snap, dict) else {}
    if not out:
        out.update(snap)
    else:
        for key in (
            "alert_kind", "alert_title", "alert_subtitle", "alert_score", "alert_reasons", "alert_reason",
            "score", "home_logo", "away_logo", "league_logo", "country_code", "period", "finished",
        ):
            if key in snap and snap.get(key) not in (None, ""):
                out[key] = snap.get(key)
        # If the snapshot is newer than the live lookup, keep its score/minute too.
        for key in ("score_home", "score_away", "minute", "minute_text", "stats"):
            if key in snap and snap.get(key) not in (None, ""):
                out[key] = snap.get(key)
    if match_id:
        out.setdefault("id", match_id)
    return out


def _ensure_goal_alert_for_match(sub: dict[str, Any], m: dict[str, Any], now: int | None = None) -> bool:
    """After a filter/Ждём гол notification, automatically watch that match for goals."""
    mid = str(m.get("id") or "").strip()
    if not mid:
        return False
    if _notify_match_dismissed(sub, mid):
        return False
    now_i = int(now or time.time())
    goal_alerts = sub.setdefault("goal_alerts", {})
    current_score = _goal_score_for_match(m)
    current_total = _goal_total_for_match(m)
    existing = goal_alerts.get(mid)
    if isinstance(existing, dict):
        changed = False
        if "last_total" not in existing:
            existing["last_total"] = current_total
            changed = True
        if not existing.get("last_score"):
            existing["last_score"] = current_score
            changed = True
        for key in ("home", "away"):
            value = str(m.get(key) or "")
            if value and existing.get(key) != value:
                existing[key] = value
                changed = True
        existing.setdefault("auto", True)
        if changed:
            existing["updated_at"] = now_i
        return changed
    goal_alerts[mid] = {
        "match_id": mid,
        "last_score": current_score,
        "last_total": current_total,
        "home": str(m.get("home") or ""),
        "away": str(m.get("away") or ""),
        "auto": True,
        "enabled_at": now_i,
        "updated_at": now_i,
    }
    return True


def _mark_goal_alert_current(sub: dict[str, Any], m: dict[str, Any], now: int | None = None) -> bool:
    """Mark a goal alert as already delivered so the background worker won't duplicate it."""
    mid = str(m.get("id") or "").strip()
    if not mid:
        return False
    changed = _ensure_goal_alert_for_match(sub, m, now=now)
    alert = sub.setdefault("goal_alerts", {}).setdefault(mid, {})
    current_score = _goal_score_for_match(m)
    current_total = _goal_total_for_match(m)
    if _safe_int(alert.get("last_total"), -1) != current_total:
        alert["last_total"] = current_total
        changed = True
    if str(alert.get("last_score") or "") != current_score:
        alert["last_score"] = current_score
        changed = True
    alert["updated_at"] = int(now or time.time())
    return changed


def _notify_kind(kind: str | None) -> str:
    kind_s = str(kind or "").strip().lower()
    if kind_s in {"goal", "goal_wait", "filter"}:
        return kind_s
    return "filter"


def _notify_seen_key(match_id: str, kind: str) -> str:
    """Legacy key used by older builds; keep it for migration/compatibility."""
    kind = _notify_kind(kind)
    if kind in {"goal", "goal_wait"}:
        return f"{kind}:{match_id}"
    return str(match_id)


def _notify_delivery_key(match_id: str, kind: str, m: dict[str, Any] | None = None) -> str:
    """Stable per-Telegram-message key.

    Filter and Ждём гол alerts are one-time per match. Goal alerts are one-time
    per match+score, so a later 2-0 goal can still be delivered after a 1-0 goal
    while a repeated 1-0 message is blocked even after a restart.
    """
    mid = str(match_id or "").strip()
    kind_s = _notify_kind(kind)
    if kind_s == "goal":
        score = _goal_score_for_match(m or {}) if m else ""
        return f"goal:{mid}:{score}" if score else f"goal:{mid}"
    if kind_s == "goal_wait":
        return f"goal_wait:{mid}"
    return f"filter:{mid}"


def _notify_legacy_keys(match_id: str, kind: str) -> set[str]:
    """Old keys that may already exist in saved user state."""
    mid = str(match_id or "").strip()
    kind_s = _notify_kind(kind)
    if not mid:
        return set()
    if kind_s == "goal_wait":
        return {mid, f"goal_wait:{mid}"}
    if kind_s == "filter":
        return {mid, f"filter:{mid}"}
    # Do not use the old goal:<id> key as a hard block for all future scores;
    # otherwise a second/third goal in the same match could be suppressed.
    return set()


def _notify_key_match_id(key: str | None) -> str:
    """Return match id from a delivery/pending key."""
    key_s = str(key or "").strip()
    if not key_s:
        return ""
    parts = key_s.split(":")
    if parts[0] in {"filter", "goal_wait", "goal"} and len(parts) >= 2:
        return parts[1]
    return key_s


def _notify_match_dismissed(sub: dict[str, Any], match_id_or_key: str | None) -> bool:
    mid = _notify_key_match_id(match_id_or_key)
    dismissed = sub.get("dismissed")
    return bool(mid and isinstance(dismissed, dict) and mid in dismissed)


def _dismiss_notify_match(sub: dict[str, Any], match_id: str | int, snapshot: dict[str, Any] | None = None, now: int | float | None = None) -> bool:
    """Stop future Telegram sends for a match after user removed/cleared it."""
    mid = str(match_id or "").strip()
    if not mid:
        return False
    now_f = float(now or time.time())
    changed = False

    dismissed = sub.setdefault("dismissed", {})
    if isinstance(dismissed, dict) and dismissed.get(mid) != now_f:
        dismissed[mid] = now_f
        changed = True

    # Mark current filter/goal-wait notification keys as delivered too.
    for kind in ("filter", "goal_wait"):
        key = _notify_delivery_key(mid, kind, snapshot or {})
        before = json.dumps({
            "delivered": sub.get("delivered"),
            "seen": sub.get("seen"),
            "goal_wait_seen": sub.get("goal_wait_seen"),
        }, sort_keys=True, default=str)
        _mark_notify_delivered(sub, key, now_f, _notify_legacy_keys(mid, kind))
        after = json.dumps({
            "delivered": sub.get("delivered"),
            "seen": sub.get("seen"),
            "goal_wait_seen": sub.get("goal_wait_seen"),
        }, sort_keys=True, default=str)
        if after != before:
            changed = True

    # Remove queued Telegram retries for this match.
    queue = sub.get("pending_telegram")
    if isinstance(queue, list) and queue:
        filtered = []
        for item in queue:
            if not isinstance(item, dict):
                changed = True
                continue
            key_mid = _notify_key_match_id(item.get("key"))
            legacy_mids = {_notify_key_match_id(x) for x in (item.get("legacy_keys") or []) if str(x)}
            if key_mid == mid or mid in legacy_mids:
                changed = True
                continue
            filtered.append(item)
        if len(filtered) != len(queue):
            sub["pending_telegram"] = filtered[:NOTIFY_PENDING_MAX_PER_USER]
            changed = True

    # Stop automatic goal follow-up for the same match.
    goal_alerts = sub.get("goal_alerts")
    if isinstance(goal_alerts, dict) and mid in goal_alerts:
        goal_alerts.pop(mid, None)
        changed = True

    return changed


def _dismiss_notify_matches_for_chat(chat_id: str | int, items: list[dict[str, Any]] | None, now: int | float | None = None) -> bool:
    sub = _notify_subs.get(str(chat_id or ""))
    if not isinstance(sub, dict):
        return False
    changed = False
    for item in (items or []):
        if not isinstance(item, dict):
            continue
        mid = str(item.get("id") or item.get("match_id") or "").strip()
        if _dismiss_notify_match(sub, mid, snapshot=item, now=now):
            changed = True
    return changed


def _collect_current_notify_ids_for_clear(sub: dict[str, Any], matches: list[dict[str, Any]]) -> set[str]:
    ids: set[str] = set()
    if not isinstance(sub, dict):
        return ids
    cfg = sanitize_notify_filter(sub.get("filter") or {})
    goal_wait = sub.get("goal_wait") if isinstance(sub.get("goal_wait"), dict) else {}
    goal_cfg = fixed_goal_wait_filter(enabled=True) if bool((goal_wait or {}).get("enabled")) else None
    for m in matches or []:
        mid = str(m.get("id") or "").strip()
        if not mid:
            continue
        try:
            if cfg.get("enabled") and match_passes_filter(m, cfg):
                ids.add(mid)
                continue
            if goal_cfg and match_passes_filter(m, goal_cfg):
                ids.add(mid)
        except Exception:
            continue
    return ids


def _notify_pending_queue(sub: dict[str, Any]) -> list[dict[str, Any]]:
    queue = sub.get("pending_telegram")
    if not isinstance(queue, list):
        queue = []
        sub["pending_telegram"] = queue
    return queue


def _notify_pending_contains(sub: dict[str, Any], key: str) -> bool:
    if not key:
        return False
    queue = sub.get("pending_telegram")
    if not isinstance(queue, list):
        return False
    return any(str(item.get("key") or "") == str(key) for item in queue if isinstance(item, dict))


def _queue_telegram_message(
    sub: dict[str, Any],
    key: str,
    text: str,
    link: str = "",
    now: int | float | None = None,
    legacy_keys: set[str] | None = None,
) -> bool:
    if not key or not text:
        return False
    mid_for_key = _notify_key_match_id(key)
    if mid_for_key and _notify_bucket_contains(sub, "dismissed", mid_for_key):
        return False
    if _notify_pending_contains(sub, key):
        return False
    now_i = int(now or time.time())
    queue = _notify_pending_queue(sub)
    queue.insert(0, {
        "key": str(key),
        "match_id": mid_for_key,
        "text": str(text),
        "link": str(link or ""),
        "legacy_keys": sorted(str(x) for x in (legacy_keys or set()) if str(x)),
        "tries": 0,
        "created_at": now_i,
        "last_try": 0,
    })
    del queue[NOTIFY_PENDING_MAX_PER_USER:]
    return True



def _telegram_job_id(chat_id: str | int, key: str) -> str:
    return f"{str(chat_id or '').strip()}::{str(key or '').strip()}"


def _telegram_job_backoff_seconds(attempts: int) -> int:
    attempts_i = max(1, int(attempts or 1))
    return int(min(TELEGRAM_JOB_RETRY_MAX_SECONDS, TELEGRAM_JOB_RETRY_BASE_SECONDS * (2 ** min(attempts_i - 1, 6))))


def _persist_telegram_job(item: dict[str, Any]) -> bool:
    """Persist a pending Telegram send so restarts do not lose alerts."""
    if NOTIFY_STORAGE not in {"sqlite", "postgres"}:
        return False
    chat_id = str(item.get("chat_id") or "").strip()
    key = str(item.get("key") or "").strip()
    text_msg = str(item.get("text") or "")
    if not chat_id or not key or not text_msg:
        return False
    _init_notify_storage()
    now_i = int(time.time())
    legacy_keys = item.get("legacy_keys") or []
    payload = (
        chat_id,
        key,
        text_msg,
        str(item.get("link") or ""),
        _json_dumps_compact(legacy_keys if isinstance(legacy_keys, list) else list(legacy_keys or [])),
        _safe_int(item.get("attempts"), 0),
        _safe_int(item.get("next_try_at"), 0),
        _safe_int(item.get("created_at"), now_i),
        now_i,
        str(item.get("last_error") or "")[:300],
    )
    try:
        if NOTIFY_STORAGE == "postgres":
            with _notify_pg_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO telegram_jobs(chat_id,job_key,text,link,legacy_keys,attempts,next_try_at,created_at,updated_at,last_error)
                        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT(chat_id, job_key) DO UPDATE SET
                            text=EXCLUDED.text,
                            link=EXCLUDED.link,
                            legacy_keys=EXCLUDED.legacy_keys,
                            updated_at=EXCLUDED.updated_at
                        """,
                        payload,
                    )
                conn.commit()
        else:
            with _notify_sqlite_conn() as conn:
                conn.execute(
                    """
                    INSERT INTO telegram_jobs(chat_id,job_key,text,link,legacy_keys,attempts,next_try_at,created_at,updated_at,last_error)
                    VALUES(?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(chat_id, job_key) DO UPDATE SET
                        text=excluded.text,
                        link=excluded.link,
                        legacy_keys=excluded.legacy_keys,
                        updated_at=excluded.updated_at
                    """,
                    payload,
                )
                conn.commit()
        _telegram_stats_incr("persisted")
        return True
    except Exception as exc:
        _telegram_send_stats["last_error"] = f"persist job: {str(exc)[:180]}"
        print(f"[notify] persist telegram job failed: {exc}")
        return False


def _delete_telegram_job(chat_id: str | int, key: str) -> None:
    if NOTIFY_STORAGE not in {"sqlite", "postgres"}:
        return
    chat_id_s = str(chat_id or "").strip()
    key_s = str(key or "").strip()
    if not chat_id_s or not key_s:
        return
    try:
        if NOTIFY_STORAGE == "postgres":
            with _notify_pg_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM telegram_jobs WHERE chat_id=%s AND job_key=%s", (chat_id_s, key_s))
                conn.commit()
        else:
            with _notify_sqlite_conn() as conn:
                conn.execute("DELETE FROM telegram_jobs WHERE chat_id=? AND job_key=?", (chat_id_s, key_s))
                conn.commit()
    except Exception as exc:
        print(f"[notify] delete telegram job failed: {exc}")


def _update_telegram_job_failure(item: dict[str, Any], error: str = "") -> None:
    if NOTIFY_STORAGE not in {"sqlite", "postgres"}:
        return
    chat_id = str(item.get("chat_id") or "").strip()
    key = str(item.get("key") or "").strip()
    if not chat_id or not key:
        return
    attempts = _safe_int(item.get("attempts"), 0) + 1
    now_i = int(time.time())
    if attempts >= TELEGRAM_JOB_MAX_ATTEMPTS:
        _delete_telegram_job(chat_id, key)
        _telegram_stats_incr("dropped")
        print(f"[notify] dropped telegram job after {attempts} attempts chat={chat_id} key={key}")
        return
    retry_after = _safe_int(_telegram_send_stats.get("retry_after"), 0)
    if retry_after > 0:
        next_try = now_i + min(max(retry_after, TELEGRAM_JOB_RETRY_BASE_SECONDS), TELEGRAM_JOB_RETRY_MAX_SECONDS)
        _telegram_send_stats["retry_after"] = 0
    else:
        next_try = now_i + _telegram_job_backoff_seconds(attempts)
    try:
        if NOTIFY_STORAGE == "postgres":
            with _notify_pg_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE telegram_jobs SET attempts=%s,next_try_at=%s,updated_at=%s,last_error=%s WHERE chat_id=%s AND job_key=%s",
                        (attempts, next_try, now_i, str(error or "")[:300], chat_id, key),
                    )
                conn.commit()
        else:
            with _notify_sqlite_conn() as conn:
                conn.execute(
                    "UPDATE telegram_jobs SET attempts=?,next_try_at=?,updated_at=?,last_error=? WHERE chat_id=? AND job_key=?",
                    (attempts, next_try, now_i, str(error or "")[:300], chat_id, key),
                )
                conn.commit()
    except Exception as exc:
        print(f"[notify] update telegram job failed: {exc}")


def _load_due_telegram_jobs(limit: int | None = None) -> list[dict[str, Any]]:
    if NOTIFY_STORAGE not in {"sqlite", "postgres"}:
        return []
    _init_notify_storage()
    now_i = int(time.time())
    max_rows = int(limit or TELEGRAM_JOB_LOAD_LIMIT)
    rows: list[dict[str, Any]] = []
    try:
        if NOTIFY_STORAGE == "postgres":
            with _notify_pg_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT chat_id,job_key,text,link,legacy_keys,attempts,next_try_at,created_at,last_error
                        FROM telegram_jobs
                        WHERE next_try_at <= %s
                        ORDER BY updated_at ASC
                        LIMIT %s
                        """,
                        (now_i, max_rows),
                    )
                    raw_rows = cur.fetchall()
        else:
            with _notify_sqlite_conn() as conn:
                raw_rows = conn.execute(
                    """
                    SELECT chat_id,job_key,text,link,legacy_keys,attempts,next_try_at,created_at,last_error
                    FROM telegram_jobs
                    WHERE next_try_at <= ?
                    ORDER BY updated_at ASC
                    LIMIT ?
                    """,
                    (now_i, max_rows),
                ).fetchall()
        for r in raw_rows:
            # psycopg returns tuples; sqlite rows support mapping access.
            try:
                chat_id, job_key, text_msg, link, legacy_raw, attempts, next_try_at, created_at, last_error = tuple(r)
            except Exception:
                continue
            try:
                legacy_keys = json.loads(legacy_raw or "[]")
            except Exception:
                legacy_keys = []
            rows.append({
                "chat_id": str(chat_id),
                "key": str(job_key),
                "text": str(text_msg or ""),
                "link": str(link or ""),
                "legacy_keys": legacy_keys if isinstance(legacy_keys, list) else [],
                "attempts": _safe_int(attempts, 0),
                "next_try_at": _safe_int(next_try_at, 0),
                "created_at": _safe_int(created_at, now_i),
                "last_error": str(last_error or ""),
                "from_db": True,
            })
    except Exception as exc:
        _telegram_send_stats["last_error"] = f"load jobs: {str(exc)[:180]}"
        print(f"[notify] load due telegram jobs failed: {exc}")
    return rows


def _queue_put_telegram_item(item: dict[str, Any]) -> bool:
    chat_id = str(item.get("chat_id") or "").strip()
    key = str(item.get("key") or "").strip()
    ident = _telegram_job_id(chat_id, key)
    if not chat_id or not key:
        return False
    with _telegram_enqueued_lock:
        if ident in _telegram_enqueued_keys:
            return True
        try:
            _telegram_send_queue.put_nowait(item)
            _telegram_enqueued_keys.add(ident)
            return True
        except queue.Full:
            _telegram_send_stats["last_error"] = "telegram send memory queue full"
            return False


def _enqueue_telegram_delivery(chat_id: str | int, key: str, text_msg: str, link: str = "", legacy_keys: set[str] | None = None) -> bool:
    """Persist and enqueue a Telegram send without doing network I/O under _notify_lock."""
    if not BOT_TOKEN or str(chat_id or "") == "local":
        return False
    item = {
        "chat_id": str(chat_id or "").strip(),
        "key": str(key or ""),
        "text": str(text_msg or ""),
        "link": str(link or ""),
        "legacy_keys": [str(x) for x in (legacy_keys or set()) if str(x)],
        "attempts": 0,
        "next_try_at": 0,
        "created_at": int(time.time()),
    }
    persisted = _persist_telegram_job(item)
    queued = _queue_put_telegram_item(item)
    if not queued and not persisted:
        _telegram_stats_incr("fail")
        _telegram_send_stats["last_fail"] = int(time.time())
        _telegram_send_stats["last_error"] = "telegram send queue full"
    return bool(queued or persisted)


def _log_telegram_queue_status(force: bool = False) -> None:
    now_i = int(time.time())
    last = _safe_int(_telegram_send_stats.get("last_log"), 0)
    if not force and now_i - last < TELEGRAM_LOG_INTERVAL_SECONDS:
        return
    _telegram_send_stats["last_log"] = now_i
    print(
        "[notify] telegram queue "
        f"mem={_telegram_send_queue.qsize()} ok={_safe_int(_telegram_send_stats.get('ok'),0)} "
        f"fail={_safe_int(_telegram_send_stats.get('fail'),0)} persisted={_safe_int(_telegram_send_stats.get('persisted'),0)} "
        f"loaded={_safe_int(_telegram_send_stats.get('loaded'),0)} dropped={_safe_int(_telegram_send_stats.get('dropped'),0)}"
    )


def telegram_jobs_loader_loop() -> None:
    """Load due persisted Telegram jobs back into memory after restarts/failures."""
    while True:
        try:
            time.sleep(max(1.0, TELEGRAM_JOB_LOAD_INTERVAL_SECONDS))
            if not BOT_TOKEN:
                continue
            due = _load_due_telegram_jobs(TELEGRAM_JOB_LOAD_LIMIT)
            loaded = 0
            for item in due:
                if _queue_put_telegram_item(item):
                    loaded += 1
            if loaded:
                _telegram_stats_incr("loaded", loaded)
            _log_telegram_queue_status()
        except Exception as exc:
            _telegram_send_stats["last_error"] = str(exc)[:200]
            print(f"[notify] telegram job loader error: {exc}")


def telegram_send_worker_loop() -> None:
    """Send queued Telegram messages outside notify locks and at a safe pace."""
    while True:
        item = _telegram_send_queue.get()
        ident = ""
        try:
            if not isinstance(item, dict):
                continue
            chat_id = str(item.get("chat_id") or "").strip()
            key = str(item.get("key") or "")
            ident = _telegram_job_id(chat_id, key)
            text_msg = str(item.get("text") or "")
            link = str(item.get("link") or "")
            legacy_keys = {str(x) for x in (item.get("legacy_keys") or []) if str(x)}
            if not chat_id or not key or not text_msg:
                continue
            ok = send_telegram_message(chat_id, text_msg, link=link)
            now_i = int(time.time())
            if ok:
                _delete_telegram_job(chat_id, key)
            else:
                _update_telegram_job_failure(item, error=str(_telegram_send_stats.get("last_error") or "send failed"))
            with _notify_lock:
                sub = _notify_subs.get(chat_id)
                if not isinstance(sub, dict):
                    continue
                queue_items = sub.get("pending_telegram")
                if isinstance(queue_items, list) and queue_items:
                    sub["pending_telegram"] = [x for x in queue_items if not (isinstance(x, dict) and str(x.get("key") or "") == key)]
                if ok:
                    _mark_notify_delivered(sub, key, now_i, legacy_keys)
                else:
                    _queue_telegram_message(sub, key, text_msg, link=link, now=now_i, legacy_keys=legacy_keys)
                # v9.81: per-row save вместо записи всех подписок целиком.
                _save_one_notify_sub(chat_id)
        except Exception as exc:
            _telegram_stats_incr("fail")
            _telegram_send_stats["last_fail"] = int(time.time())
            _telegram_send_stats["last_error"] = str(exc)[:200]
            print(f"[notify] telegram queue worker error: {exc}")
            traceback.print_exc()
        finally:
            if ident:
                with _telegram_enqueued_lock:
                    _telegram_enqueued_keys.discard(ident)
            try:
                _telegram_send_queue.task_done()
            except Exception:
                pass
            time.sleep(0.05)


def start_telegram_send_workers() -> None:
    global _telegram_queue_started, _telegram_jobs_loader_started
    with _telegram_queue_lock:
        if _telegram_queue_started:
            return
        _telegram_queue_started = True
        for idx in range(max(1, TELEGRAM_SEND_WORKERS)):
            t = threading.Thread(target=telegram_send_worker_loop, name=f"telegram-send-{idx+1}", daemon=True)
            t.start()
        if not _telegram_jobs_loader_started:
            _telegram_jobs_loader_started = True
            t = threading.Thread(target=telegram_jobs_loader_loop, name="telegram-jobs-loader", daemon=True)
            t.start()
        _log_telegram_queue_status(force=True)


def _flush_pending_telegram_for_sub(chat_id: str | int, sub: dict[str, Any], now: int | float | None = None) -> bool:
    """Retry Telegram messages that failed earlier.

    Returns True when subscription state changed. We keep failed messages for a
    limited time, so leaving the Mini App cannot silently lose Telegram pushes.
    """
    if not BOT_TOKEN or str(chat_id or "") == "local":
        return False
    now_i = int(now or time.time())
    queue = sub.get("pending_telegram")
    if not isinstance(queue, list) or not queue:
        return False
    changed = False
    keep: list[dict[str, Any]] = []
    for raw in list(queue):
        if not isinstance(raw, dict):
            changed = True
            continue
        key = str(raw.get("key") or "")
        text_msg = str(raw.get("text") or "")
        link = str(raw.get("link") or "")
        legacy_keys = {str(x) for x in (raw.get("legacy_keys") or []) if str(x)}
        created_at = _safe_int(raw.get("created_at"), now_i)
        tries = _safe_int(raw.get("tries"), 0)
        last_try = _safe_int(raw.get("last_try"), 0)
        if not key or not text_msg:
            changed = True
            continue
        if _notify_match_dismissed(sub, key):
            changed = True
            continue
        if _notify_bucket_contains(sub, "delivered", key):
            changed = True
            continue
        if now_i - created_at > NOTIFY_PENDING_TTL_SECONDS:
            changed = True
            continue
        # Small backoff while still retrying often enough for live matches.
        wait_s = min(120, 10 * max(1, tries))
        if last_try and now_i - last_try < wait_s:
            keep.append(raw)
            continue
        raw["tries"] = tries + 1
        raw["last_try"] = now_i
        # v9.76: enqueue network send outside _notify_lock. Keep the pending item
        # until the queue worker confirms delivery.
        if _enqueue_telegram_delivery(chat_id, key, text_msg, link=link, legacy_keys=legacy_keys):
            keep.append(raw)
            changed = True
            continue
        keep.append(raw)
        changed = True
    if changed:
        sub["pending_telegram"] = keep[:NOTIFY_PENDING_MAX_PER_USER]
    return changed


def _send_or_queue_telegram(
    chat_id: str | int,
    sub: dict[str, Any],
    key: str,
    text_msg: str,
    link: str = "",
    now: int | float | None = None,
    legacy_keys: set[str] | None = None,
) -> tuple[bool, bool]:
    """Send Telegram message now, or queue it for background retry.

    Returns (sent_or_considered_done, queued). Dedupe is marked only after a
    real Telegram success, so a temporary failure cannot suppress the alert.
    """
    now_f = float(now or time.time())
    mid_for_key = _notify_key_match_id(key)
    if mid_for_key and _notify_bucket_contains(sub, "dismissed", mid_for_key):
        return True, False
    if not BOT_TOKEN or str(chat_id or "") == "local":
        _mark_notify_delivered(sub, key, now_f, legacy_keys or set())
        return True, False
    queued = _queue_telegram_message(sub, key, text_msg, link=link, now=now_f, legacy_keys=legacy_keys or set())
    if queued:
        _enqueue_telegram_delivery(chat_id, key, text_msg, link=link, legacy_keys=legacy_keys or set())
    return False, queued


def _notify_bucket_contains(sub: dict[str, Any], bucket: str, key: str) -> bool:
    data = sub.get(bucket)
    return isinstance(data, dict) and key in data


def _notify_already_delivered(sub: dict[str, Any], key: str, legacy_keys: set[str] | None = None) -> bool:
    if not key:
        return False
    if _notify_match_dismissed(sub, key):
        return True
    if _notify_bucket_contains(sub, "delivered", key):
        return True
    if _notify_pending_contains(sub, key):
        return True
    for old_key in (legacy_keys or set()):
        if _notify_bucket_contains(sub, "seen", old_key) or _notify_bucket_contains(sub, "goal_wait_seen", old_key):
            return True
    return False


def _mark_notify_delivered(sub: dict[str, Any], key: str, now: int | float | None = None, legacy_keys: set[str] | None = None) -> None:
    now_f = float(now or time.time())
    delivered = sub.setdefault("delivered", {})
    if isinstance(delivered, dict):
        delivered[str(key)] = now_f
    seen = sub.setdefault("seen", {})
    if isinstance(seen, dict):
        for old_key in (legacy_keys or set()):
            seen[str(old_key)] = now_f
    if str(key).startswith("goal_wait:"):
        mid = str(key).split(":", 1)[1]
        goal_wait_seen = sub.setdefault("goal_wait_seen", {})
        if isinstance(goal_wait_seen, dict) and mid:
            goal_wait_seen[mid] = now_f


def _cleanup_notify_dedupe(sub: dict[str, Any], now: int | float | None = None) -> bool:
    """Keep de-dupe and retry state bounded without re-sending active matches."""
    now_f = float(now or time.time())
    cutoff = now_f - max(NOTIFY_DEDUPE_TTL_SECONDS, 3600)
    dismissed_cutoff = now_f - max(NOTIFY_DISMISSED_TTL_SECONDS, 3600)
    pending_cutoff = now_f - max(NOTIFY_PENDING_TTL_SECONDS, 3600)
    changed = False
    for bucket in ("seen", "goal_wait_seen", "delivered"):
        data = sub.get(bucket)
        if not isinstance(data, dict):
            continue
        for k, v in list(data.items()):
            try:
                ts = float(v)
            except Exception:
                ts = 0.0
            if ts and ts < cutoff:
                data.pop(k, None)
                changed = True
    dismissed = sub.get("dismissed")
    if isinstance(dismissed, dict):
        for k, v in list(dismissed.items()):
            try:
                ts = float(v)
            except Exception:
                ts = 0.0
            if ts and ts < dismissed_cutoff:
                dismissed.pop(k, None)
                changed = True
    queue = sub.get("pending_telegram")
    if isinstance(queue, list) and queue:
        filtered = []
        for item in queue:
            if not isinstance(item, dict):
                changed = True
                continue
            mid_for_key = str(item.get("match_id") or "").strip() or _notify_key_match_id(item.get("key"))
            if mid_for_key and _notify_match_dismissed(sub, mid_for_key):
                changed = True
                continue
            created_at = _safe_int(item.get("created_at"), int(now_f))
            if created_at < pending_cutoff:
                changed = True
                continue
            filtered.append(item)
        if len(filtered) != len(queue):
            sub["pending_telegram"] = filtered[:NOTIFY_PENDING_MAX_PER_USER]
            changed = True
    return changed


def _goal_alert_already_at_score(sub: dict[str, Any], m: dict[str, Any]) -> bool:
    mid = str(m.get("id") or "").strip()
    if not mid:
        return False
    alert = (sub.get("goal_alerts") or {}).get(mid)
    if not isinstance(alert, dict):
        return False
    current_total = _goal_total_for_match(m)
    current_score = _goal_score_for_match(m)
    return _safe_int(alert.get("last_total"), -1) >= current_total and str(alert.get("last_score") or "") == current_score


def _chat_id_from_init_data(init_data: str) -> str:
    user = verify_init_data(init_data)
    if not user:
        return ""
    chat_id = str(user.get("id") or "")
    return "" if chat_id == "None" else chat_id


def _chat_id_unverified_from_init_data(init_data: str) -> str:
    """Best-effort user id extraction for non-sensitive cleanup actions.

    Telegram can keep a WebView cached and initData verification may fail while
    the user still needs to clear/mute already found matches. We only use this
    fallback for cleanup endpoints and only when this chat already has saved
    notify state on the server.
    """
    try:
        parsed = dict(urllib.parse.parse_qsl(str(init_data or ""), keep_blank_values=True))
        raw = parsed.get("user") or ""
        if not raw:
            return ""
        user = json.loads(raw)
        chat_id = str(user.get("id") or "").strip()
        return "" if chat_id == "None" else chat_id
    except Exception:
        return ""


def _chat_id_for_notify_cleanup(body: dict[str, Any]) -> str:
    init_data = str(body.get("init_data") or "")
    chat_id = _chat_id_from_init_data(init_data)
    if chat_id:
        return chat_id
    candidate = str(body.get("chat_id") or body.get("user_id") or "").strip()
    if not candidate:
        candidate = _chat_id_unverified_from_init_data(init_data)
    if candidate and (candidate in _notify_subs or candidate in _notify_matches):
        return candidate
    if not BOT_TOKEN:
        return "local"
    return ""


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
        removed_old = _cleanup_notify_matches_unlocked()
        if removed_old:
            _save_notify_matches()
        items = list(_notify_matches.get(chat_id) or [])
    return {"ok": True, "chat_id": chat_id, "items": items, "total": len(items)}


def handle_notify_matches_clear(body: dict[str, Any]) -> dict[str, Any]:
    chat_id = _chat_id_for_notify_cleanup(body)
    body_ids_raw = body.get("match_ids") or body.get("ids") or []
    if not chat_id:
        return {"ok": False, "error": "init_data verification failed"}

    body_ids: set[str] = set()
    if isinstance(body_ids_raw, list):
        body_ids = {str(x).strip() for x in body_ids_raw if str(x).strip()}

    live_matches: list[dict[str, Any]] = []
    try:
        live_matches = flatten_payload(load_live_payload(force=False))
    except Exception:
        live_matches = []

    now = time.time()
    with _notify_lock:
        old_items = list(_notify_matches.get(chat_id) or [])
        ids = {str(x.get("id") or x.get("match_id") or "").strip() for x in old_items if isinstance(x, dict)}
        ids = {x for x in ids if x}
        ids.update(body_ids)
        sub = _notify_subs.get(chat_id)
        sub_changed = False
        if isinstance(sub, dict):
            ids.update(_collect_current_notify_ids_for_clear(sub, live_matches))
            suppress_items = [{"id": mid} for mid in sorted(ids)]
            sub_changed = _dismiss_notify_matches_for_chat(chat_id, suppress_items, now=now)
        _notify_matches[chat_id] = []
        _save_one_notify_matches(chat_id)
        if sub_changed:
            _save_one_notify_sub(chat_id)
    return {"ok": True, "chat_id": chat_id, "suppressed": len(ids)}


def handle_notify_match_remove(body: dict[str, Any]) -> dict[str, Any]:
    """Remove one saved notification match. Saved notification matches have no time TTL."""
    chat_id = _chat_id_for_notify_cleanup(body)
    if not chat_id:
        return {"ok": False, "error": "init_data verification failed"}
    match_id = str(body.get("match_id") or body.get("id") or "").strip()
    if not match_id:
        return {"ok": False, "error": "missing match_id"}
    with _notify_lock:
        items = list(_notify_matches.get(chat_id) or [])
        before = len(items)
        removed_items = [x for x in items if str(x.get("id") or x.get("match_id") or "") == match_id]
        items = [x for x in items if str(x.get("id") or x.get("match_id") or "") != match_id]
        sub_changed = _dismiss_notify_matches_for_chat(chat_id, removed_items or [{"id": match_id}], now=time.time())
        _notify_matches[chat_id] = items
        _save_one_notify_matches(chat_id)
        if sub_changed:
            _save_one_notify_sub(chat_id)
    return {"ok": True, "chat_id": chat_id, "match_id": match_id, "removed": before - len(items), "suppressed": 1, "total": len(items)}


def _load_notify_subs() -> None:
    global _notify_subs
    try:
        if NOTIFY_STORAGE in {"sqlite", "postgres"}:
            loaded = _load_notify_table("notify_subs")
            _notify_subs = {str(k): (v if isinstance(v, dict) else {}) for k, v in loaded.items()}
            return
        if NOTIFY_DB.exists():
            _notify_subs = json.loads(NOTIFY_DB.read_text("utf-8") or "{}") or {}
    except Exception as exc:
        print(f"[notify] load failed: {exc}")
        _notify_subs = {}


def _save_notify_subs(snapshot: dict[str, Any] | None = None) -> None:
    # v9.80: можно передать shallow snapshot, чтобы сохранить вне лока
    # (см. notify_worker_loop — раньше запись держала _notify_lock десятки секунд).
    data = snapshot if snapshot is not None else _notify_subs
    try:
        if NOTIFY_STORAGE in {"sqlite", "postgres"}:
            _save_notify_table("notify_subs", data)
            return
        NOTIFY_DB.parent.mkdir(parents=True, exist_ok=True)
        NOTIFY_DB.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    except Exception as exc:
        print(f"[notify] save failed: {exc}")


def _save_one_notify_sub(chat_id: str | int) -> None:
    """v9.81: сохранить ОДНУ подписку без записи всего словаря.

    Раньше каждое успешное Telegram-сообщение или блок пользователя
    вызывали _save_notify_subs() без аргумента — это UPSERT'ит N подписок
    в БД. При 1000 пользователей одно сообщение = 1000 UPSERT'ов в Postgres.
    Теперь точечная запись только для нужного chat_id.
    """
    chat_key = str(chat_id or "").strip()
    if not chat_key:
        return
    try:
        if NOTIFY_STORAGE in {"sqlite", "postgres"}:
            sub = _notify_subs.get(chat_key)
            if sub is None:
                # запись удалена — удалим из БД
                _delete_notify_row("notify_subs", chat_key)
            else:
                _save_notify_table("notify_subs", {chat_key: sub})
            return
        # JSON-fallback: пишем весь файл, для совместимости
        NOTIFY_DB.parent.mkdir(parents=True, exist_ok=True)
        NOTIFY_DB.write_text(json.dumps(_notify_subs, ensure_ascii=False), encoding="utf-8")
    except Exception as exc:
        print(f"[notify] save_one_sub failed for {chat_key}: {exc}")


def _delete_notify_row(table: str, chat_id: str | int) -> None:
    """v9.81: точечный DELETE одной подписки из БД."""
    table = _notify_table_name(table)
    chat_key = str(chat_id or "").strip()
    if not chat_key or NOTIFY_STORAGE not in {"sqlite", "postgres"}:
        return
    _init_notify_storage()
    try:
        if NOTIFY_STORAGE == "postgres":
            with _notify_pg_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"DELETE FROM {table} WHERE chat_id=%s", (chat_key,))
                conn.commit()
            return
        with _notify_sqlite_conn() as conn:
            conn.execute(f"DELETE FROM {table} WHERE chat_id=?", (chat_key,))
            conn.commit()
    except Exception as exc:
        print(f"[notify] delete failed for {table} {chat_key}: {exc}")



def _match_start_param(match_id: Any) -> str:
    mid = str(match_id or "").strip()
    if not mid:
        return ""
    # Keep the value URL-safe for Telegram startapp and readable for JS.
    return "match_" + urllib.parse.quote(mid, safe="")


def notification_match_link(match_id: Any) -> str:
    """Build a link that opens the Mini App directly on a match card."""
    start_param = _match_start_param(match_id)
    if not start_param:
        return ""
    if BOT_USERNAME:
        if MINIAPP_SHORT_NAME:
            return f"https://t.me/{BOT_USERNAME}/{MINIAPP_SHORT_NAME}?startapp={start_param}"
        return f"https://t.me/{BOT_USERNAME}?startapp={start_param}"
    if PUBLIC_BASE_URL:
        mid = urllib.parse.quote(str(match_id or "").strip(), safe="")
        return f"{PUBLIC_BASE_URL}/?match_id={mid}"
    return ""

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
        # auth_date freshness check.  v9.60: mobile Telegram may keep the
        # Mini App page cached for longer than 24h, so make the max age
        # configurable and use a safer default of 30 days.
        try:
            auth_age = time.time() - int(parsed.get("auth_date", "0"))
            if TELEGRAM_INIT_DATA_MAX_AGE_SECONDS > 0 and auth_age > TELEGRAM_INIT_DATA_MAX_AGE_SECONDS:
                return None
        except Exception:
            return None
        user_raw = parsed.get("user")
        if not user_raw:
            return None
        return json.loads(user_raw)
    except Exception:
        return None


def _handle_permanent_telegram_failure(chat_id: str, http_code: int, reason: str = "") -> None:
    """Disable a chat after Telegram reports the bot is blocked or chat is invalid."""
    chat_id_s = str(chat_id or "").strip()
    if not chat_id_s or chat_id_s == "local":
        return
    now_i = int(time.time())
    changed = False
    with _notify_lock:
        sub = _notify_subs.get(chat_id_s)
        if isinstance(sub, dict):
            filt = sub.get("filter") if isinstance(sub.get("filter"), dict) else {}
            if filt.get("enabled"):
                filt["enabled"] = False
                changed = True
            sub["filter"] = filt
            goal_wait = sub.get("goal_wait") if isinstance(sub.get("goal_wait"), dict) else {}
            if goal_wait.get("enabled"):
                goal_wait["enabled"] = False
                changed = True
            # v9.81: убрана дублирующая строка `goal_wait["enabled"] = False`,
            # которая выполнялась после if'а и обнуляла changed-флаг для
            # тех случаев, когда сигнал и так уже был выключен.
            sub["goal_wait"] = goal_wait
            if sub.get("pending_telegram"):
                sub["pending_telegram"] = []
                changed = True
            sub["telegram_disabled_at"] = now_i
            sub["telegram_disabled_reason"] = f"HTTP {http_code} {reason}"[:180]
            sub["updated_at"] = now_i
            _notify_subs[chat_id_s] = sub
        if _notify_matches.pop(chat_id_s, None) is not None:
            changed = True
        if changed:
            # v9.81: точечная запись только этого chat_id, а не всей таблицы.
            _save_one_notify_sub(chat_id_s)
            _save_one_notify_matches(chat_id_s)
    try:
        with _admin_state_lock:
            _admin_state.setdefault("blocked_users", {})[chat_id_s] = {
                "id": chat_id_s,
                "blocked_at": now_i,
                "blocked_by": "telegram_api",
                "reason": f"HTTP {http_code} {reason}"[:180],
            }
        _save_admin_state()
    except Exception:
        pass


def send_telegram_message(chat_id: int | str, text: str, link: str = "", button_text: str = "") -> bool:
    if not BOT_TOKEN:
        return False
    chat_id_s = str(chat_id or "").strip()
    if not chat_id_s or chat_id_s == "local":
        return False
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    try:
        chat_id_value: int | str = int(chat_id_s)
    except Exception:
        chat_id_value = chat_id_s
    payload = {
        "chat_id": chat_id_value,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    if link:
        payload["reply_markup"] = {
            "inline_keyboard": [[{"text": button_text or NOTIFY_OPEN_BUTTON_TEXT, "url": link}]]
        }
    last_exc: Exception | None = None
    for attempt in range(max(1, NOTIFY_SEND_RETRIES)):
        try:
            req = urllib.request.Request(
                url,
                data=json.dumps(payload).encode("utf-8"),
                method="POST",
                headers={"Content-Type": "application/json"},
            )
            with urllib.request.urlopen(req, timeout=12) as resp:
                resp.read()
            _telegram_stats_incr("ok")
            _telegram_send_stats["last_ok"] = int(time.time())
            return True
        except urllib.error.HTTPError as exc:
            last_exc = exc
            retry_after = 0
            try:
                raw = exc.read().decode("utf-8", "ignore")
                data = json.loads(raw or "{}")
                retry_after = _safe_int((data.get("parameters") or {}).get("retry_after"), 0)
            except Exception:
                retry_after = 0
            # 400/403 usually means the user blocked the bot or the chat id is invalid;
            # retrying will not help. 429/5xx can be temporary.
            if exc.code in {400, 403}:
                print(f"[notify] send failed permanently for {chat_id_s}: HTTP {exc.code}")
                _handle_permanent_telegram_failure(chat_id_s, exc.code, str(last_exc or ""))
                _telegram_stats_incr("fail")
                _telegram_send_stats["last_fail"] = int(time.time())
                _telegram_send_stats["last_error"] = f"HTTP {exc.code}"
                return False
            if retry_after > 0:
                _telegram_send_stats["retry_after"] = retry_after
                _telegram_send_stats["last_error"] = f"HTTP 429 retry_after={retry_after}"
                time.sleep(min(retry_after, 15))
            else:
                time.sleep(min(1.5 * (attempt + 1), 5))
        except Exception as exc:
            last_exc = exc
            time.sleep(min(1.5 * (attempt + 1), 5))
    print(f"[notify] send failed for {chat_id_s}: {last_exc}")

    _telegram_stats_incr("fail")
    _telegram_send_stats["last_fail"] = int(time.time())
    _telegram_send_stats["last_error"] = str(last_exc or "unknown")[:200]
    return False


# ============================================================================
#  v9.64: Private Telegram admin panel
# ============================================================================

def _is_admin_id(user_id: str | int | None) -> bool:
    try:
        return int(str(user_id or "").strip()) in ADMIN_IDS
    except Exception:
        return False


def _tg_api(method: str, payload: dict[str, Any], timeout: int = 20) -> dict[str, Any]:
    if not BOT_TOKEN:
        return {"ok": False, "description": "BOT_TOKEN is empty"}
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"
    try:
        req = urllib.request.Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            method="POST",
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", "ignore")
        return json.loads(raw or "{}")
    except urllib.error.HTTPError as exc:
        try:
            return json.loads(exc.read().decode("utf-8", "ignore") or "{}")
        except Exception:
            return {"ok": False, "description": f"HTTP {exc.code}"}
    except Exception as exc:
        return {"ok": False, "description": str(exc)}


def _admin_keyboard(section: str = "main") -> dict[str, Any]:
    if section == "online":
        return {
            "inline_keyboard": [
                [
                    {"text": "➖ -10", "callback_data": "admin:online_dec10"},
                    {"text": "➕ +10", "callback_data": "admin:online_inc10"},
                ],
                [
                    {"text": "➖ -1", "callback_data": "admin:online_dec1"},
                    {"text": "➕ +1", "callback_data": "admin:online_inc1"},
                ],
                [{"text": "✏️ Установить число", "callback_data": "admin:online_set"}],
                [
                    {"text": "🔄 Обновить", "callback_data": "admin:online"},
                    {"text": "⬅️ Назад", "callback_data": "admin:main"},
                ],
            ]
        }
    return {
        "inline_keyboard": [
            [
                {"text": "📊 Статистика", "callback_data": "admin:stats"},
                {"text": "👥 Пользователи", "callback_data": "admin:users"},
            ],
            [
                {"text": "🔔 Уведомления", "callback_data": "admin:notify"},
                {"text": "⚙️ Система", "callback_data": "admin:system"},
            ],
            [{"text": "👤 Онлайн лимит", "callback_data": "admin:online"}],
            [
                {"text": "➕ Добавить ID", "callback_data": "admin:add"},
                {"text": "🗑 Удалить ID", "callback_data": "admin:delete"},
            ],
            [
                {"text": "⛔ Блок", "callback_data": "admin:block"},
                {"text": "✅ Разблок", "callback_data": "admin:unblock"},
            ],
            [{"text": "🔄 Обновить", "callback_data": f"admin:{section}"}],
        ]
    }


def _admin_send(chat_id: str | int, text: str, reply_markup: dict[str, Any] | None = None) -> None:
    payload: dict[str, Any] = {
        "chat_id": int(chat_id) if str(chat_id).isdigit() else str(chat_id),
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup
    _tg_api("sendMessage", payload, timeout=15)


def _admin_edit(chat_id: str | int, message_id: str | int, text: str, reply_markup: dict[str, Any] | None = None) -> None:
    payload: dict[str, Any] = {
        "chat_id": int(chat_id) if str(chat_id).isdigit() else str(chat_id),
        "message_id": int(message_id),
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup
    result = _tg_api("editMessageText", payload, timeout=15)
    if not result.get("ok"):
        _admin_send(chat_id, text, reply_markup=reply_markup)


def _admin_answer_callback(callback_id: str, text: str = "") -> None:
    if callback_id:
        _tg_api("answerCallbackQuery", {"callback_query_id": callback_id, "text": text[:180], "show_alert": False}, timeout=10)


def _all_known_user_ids() -> set[str]:
    ids: set[str] = set()
    with _notify_lock:
        ids.update(str(x) for x in _notify_subs.keys())
        ids.update(str(x) for x in _notify_matches.keys())
    with _admin_state_lock:
        ids.update(str(x) for x in (_admin_state.get("known_users") or {}).keys())
        ids.update(str(x) for x in (_admin_state.get("allowed_users") or {}).keys())
        ids.update(str(x) for x in (_admin_state.get("blocked_users") or {}).keys())
    return {x for x in ids if x and x != "local"}


def _admin_stats_snapshot() -> dict[str, Any]:
    with _notify_lock:
        subs = {str(k): (v if isinstance(v, dict) else {}) for k, v in _notify_subs.items()}
        matches_by_user = {str(k): (v if isinstance(v, list) else []) for k, v in _notify_matches.items()}
    now_i = int(time.time())
    user_ids = _all_known_user_ids()
    active_24h = 0
    filter_on = 0
    goal_wait_on = 0
    goal_alerts = 0
    pending = 0
    delivered_keys = 0
    for uid, sub in subs.items():
        if now_i - _safe_int(sub.get("updated_at"), 0) <= 86400:
            active_24h += 1
        if bool((sub.get("filter") or {}).get("enabled")):
            filter_on += 1
        if bool((sub.get("goal_wait") or {}).get("enabled")):
            goal_wait_on += 1
        if isinstance(sub.get("goal_alerts"), dict):
            goal_alerts += len(sub.get("goal_alerts") or {})
        if isinstance(sub.get("pending_telegram"), list):
            pending += len(sub.get("pending_telegram") or [])
        if isinstance(sub.get("delivered"), dict):
            delivered_keys += len(sub.get("delivered") or {})
    found_total = sum(len(v) for v in matches_by_user.values())
    live_count = 0
    try:
        live_count = len(flatten_payload(load_live_payload(force=False)))
    except Exception:
        live_count = _safe_int(_collector_state.get("last_matches"), 0)
    with _admin_state_lock:
        allowed = len(_admin_state.get("allowed_users") or {})
        blocked = len(_admin_state.get("blocked_users") or {})
    online = _online_snapshot()
    return {
        "users_total": len(user_ids),
        "active_24h": active_24h,
        "allowed": allowed,
        "blocked": blocked,
        "filter_on": filter_on,
        "goal_wait_on": goal_wait_on,
        "goal_alerts": goal_alerts,
        "pending": pending,
        "found_total": found_total,
        "delivered_keys": delivered_keys,
        "live_count": live_count,
        "online": _safe_int(online.get("online"), 0),
        "online_limit": _safe_int(online.get("limit"), ONLINE_USER_LIMIT_DEFAULT),
    }


def _admin_main_text() -> str:
    return (
        "<b>Live ZOT · Админ-панель</b>\n"
        "Доступ: только ADMIN_IDS.\n\n"
        "Выбери раздел кнопками ниже.\n"
        "Команды: /admin, /stats, /users, /system"
    )


def _admin_stats_text() -> str:
    snap = _admin_stats_snapshot()
    c = _collector_state_copy()
    return (
        "<b>📊 Статистика Live ZOT</b>\n\n"
        f"<b>Пользователи</b>\n"
        f"Всего в базе: <b>{snap['users_total']}</b>\n"
        f"Активные за 24ч: <b>{snap['active_24h']}</b>\n"
        f"Онлайн сейчас: <b>{snap['online']}/{snap['online_limit']}</b>\n"
        f"Добавлены вручную: <b>{snap['allowed']}</b>\n"
        f"Заблокированы: <b>{snap['blocked']}</b>\n\n"
        f"<b>Матчи</b>\n"
        f"LIVE сейчас: <b>{snap['live_count']}</b>\n"
        f"Найдено в уведомлениях: <b>{snap['found_total']}</b>\n\n"
        f"<b>Скан лайва</b>\n"
        f"Последний скан: <b>{_format_age_ru(c.get('last_finish'))}</b>\n"
        f"Потрачено на скан: <b>{_fmt_seconds(c.get('last_scan_seconds'))}</b>\n"
        f"Среднее: <b>{_fmt_seconds(c.get('avg_scan_seconds'))}</b>\n"
        f"Максимум: <b>{_fmt_seconds(c.get('max_scan_seconds'))}</b>\n"
        f"Матчей в последнем скане: <b>{_safe_int(c.get('last_matches'), 0)}</b>\n"
        f"Статистик собрано: <b>{_safe_int(c.get('last_stats'), 0)}</b>"
    )


def _telegram_job_counts() -> dict[str, int]:
    out = {"pending": 0, "retrying": 0, "due": 0}
    if NOTIFY_STORAGE not in {"sqlite", "postgres"}:
        return out
    now_i = int(time.time())
    try:
        _init_notify_storage()
        if NOTIFY_STORAGE == "postgres":
            with _notify_pg_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT COUNT(*), COALESCE(SUM(CASE WHEN attempts>0 THEN 1 ELSE 0 END),0), COALESCE(SUM(CASE WHEN next_try_at<=%s THEN 1 ELSE 0 END),0) FROM telegram_jobs", (now_i,))
                    row = cur.fetchone() or (0, 0, 0)
        else:
            with _notify_sqlite_conn() as conn:
                row = conn.execute("SELECT COUNT(*) AS c, COALESCE(SUM(CASE WHEN attempts>0 THEN 1 ELSE 0 END),0) AS r, COALESCE(SUM(CASE WHEN next_try_at<=? THEN 1 ELSE 0 END),0) AS d FROM telegram_jobs", (now_i,)).fetchone()
                row = (row["c"], row["r"], row["d"]) if row else (0, 0, 0)
        out = {"pending": _safe_int(row[0], 0), "retrying": _safe_int(row[1], 0), "due": _safe_int(row[2], 0)}
    except Exception as exc:
        _telegram_send_stats["last_error"] = f"job counts: {str(exc)[:160]}"
    return out


def _admin_notify_text() -> str:
    snap = _admin_stats_snapshot()
    jobs = _telegram_job_counts()
    return (
        "<b>🔔 Уведомления</b>\n\n"
        f"Фильтр уведомлений ON: <b>{snap['filter_on']}</b>\n"
        f"Ждём гол ON: <b>{snap['goal_wait_on']}</b>\n"
        f"Матчей на ожидании гола: <b>{snap['goal_alerts']}</b>\n"
        f"Старые pending в профилях: <b>{snap['pending']}</b>\n"
        f"Очередь в памяти: <b>{_telegram_send_queue.qsize()}</b>\n"
        f"Очередь в БД: <b>{jobs['pending']}</b> · к отправке: <b>{jobs['due']}</b> · retry: <b>{jobs['retrying']}</b>\n"
        f"Загружено из persistent queue: <b>{_safe_int(_telegram_send_stats.get('loaded'), 0)}</b>\n"
        f"Отложено в БД: <b>{_safe_int(_telegram_send_stats.get('persisted'), 0)}</b>\n"
        f"Удалено после лимита попыток: <b>{_safe_int(_telegram_send_stats.get('dropped'), 0)}</b>\n"
        f"Доставленных ключей: <b>{snap['delivered_keys']}</b>\n\n"
        f"Отправлено успешно с запуска: <b>{_safe_int(_telegram_send_stats.get('ok'), 0)}</b>\n"
        f"Ошибок отправки с запуска: <b>{_safe_int(_telegram_send_stats.get('fail'), 0)}</b>\n"
        f"Последняя успешная: <b>{_format_age_ru(_telegram_send_stats.get('last_ok'))}</b>\n"
        f"Последняя ошибка: <code>{html.escape(str(_telegram_send_stats.get('last_error') or 'нет'))}</code>"
    )


def _admin_system_text() -> str:
    c = _collector_state_copy()
    db_ok = bool(DATABASE_URL) if NOTIFY_STORAGE == "postgres" else True
    return (
        "<b>⚙️ Система</b>\n\n"
        f"Uptime: <b>{_format_age_ru(APP_STARTED_AT)}</b>\n"
        f"BOT_TOKEN: <b>{'есть' if BOT_TOKEN else 'нет'}</b>\n"
        f"ADMIN_IDS: <code>{html.escape(','.join(str(x) for x in sorted(ADMIN_IDS)) or 'нет — админка не запустится')}</code>\n"
        f"Admin polling: <b>{'ON' if ADMIN_PANEL_ENABLED and ADMIN_POLLING_ENABLED else 'OFF'}</b>\n"
        f"Notify storage: <b>{html.escape(str(NOTIFY_STORAGE))}</b>\n"
        f"DATABASE_URL: <b>{'есть' if DATABASE_URL else 'нет'}</b>\n"
        f"База доступна: <b>{'да' if db_ok else 'нет'}</b>\n"
        f"Collector: <b>{'ON' if COLLECTOR_ENABLED else 'OFF'}</b>\n"
        f"Collector running: <b>{'да' if c.get('running') else 'нет'}</b>\n"
        f"Последняя ошибка collector: <code>{html.escape(str(c.get('last_error') or 'нет'))}</code>\n"
        f"Allowlist strict: <b>{'ON' if ADMIN_REQUIRE_ALLOWLIST else 'OFF'}</b>\n"
        f"Online limit: <b>{_safe_int(_online_snapshot().get('online'), 0)}/{_safe_int(_online_snapshot().get('limit'), ONLINE_USER_LIMIT_DEFAULT)}</b>"
    )


def _admin_online_text() -> str:
    snap = _online_snapshot(include_users=True)
    online = _safe_int(snap.get("online"), 0)
    limit = _safe_int(snap.get("limit"), ONLINE_USER_LIMIT_DEFAULT)
    ttl = _safe_int(snap.get("ttl"), ONLINE_USER_TTL_SECONDS)
    rows = []
    for u in (snap.get("users") or [])[:10]:
        username = str(u.get("username") or "")
        first_name = str(u.get("first_name") or "")
        label = first_name or ("@" + username if username else str(u.get("id") or ""))
        rows.append(f"• <code>{html.escape(str(u.get('id') or ''))}</code> {html.escape(label)} · {_format_age_ru(u.get('last_seen_ts'))}")
    users_text = "\n".join(rows) if rows else "пока никого нет"
    return (
        "<b>👤 Онлайн лимит Mini App</b>\n\n"
        f"Сейчас онлайн: <b>{online}</b> из <b>{limit}</b>\n"
        f"TTL активности: <b>{ttl} сек</b>\n\n"
        "Если онлайн достигнет лимита, новые пользователи увидят экран ожидания и не попадут в приложение. "
        "Пользователи, которые уже внутри, продолжают обновлять heartbeat.\n\n"
        f"<b>Последние онлайн:</b>\n{users_text}"
    )


def _admin_users_text(limit: int = 12) -> str:
    rows: list[tuple[int, str, dict[str, Any]]] = []
    with _notify_lock:
        for uid, sub in _notify_subs.items():
            if not isinstance(sub, dict):
                continue
            rows.append((_safe_int(sub.get("updated_at"), 0), str(uid), sub))
    rows.sort(reverse=True, key=lambda x: x[0])
    with _admin_state_lock:
        blocked = set(str(x) for x in (_admin_state.get("blocked_users") or {}).keys())
        allowed = set(str(x) for x in (_admin_state.get("allowed_users") or {}).keys())
    text = "<b>👥 Пользователи</b>\n\n"
    text += f"Всего известных ID: <b>{len(_all_known_user_ids())}</b>\n"
    text += f"Разрешены вручную: <b>{len(allowed)}</b> · Заблокированы: <b>{len(blocked)}</b>\n\n"
    if not rows:
        return text + "Пока нет подписанных пользователей."
    for _, uid, sub in rows[:limit]:
        user = sub.get("user") if isinstance(sub.get("user"), dict) else {}
        name = (str(user.get("first_name") or "") + " " + str(user.get("last_name") or "")).strip() or "без имени"
        username = str(user.get("username") or "").strip()
        flags = []
        if bool((sub.get("filter") or {}).get("enabled")):
            flags.append("фильтр")
        if bool((sub.get("goal_wait") or {}).get("enabled")):
            flags.append("ждём гол")
        if uid in blocked:
            flags.append("БЛОК")
        if uid in allowed:
            flags.append("добавлен")
        text += f"• <code>{html.escape(uid)}</code> — {html.escape(name)}"
        if username:
            text += f" @{html.escape(username)}"
        text += f"\n  {', '.join(flags) if flags else 'без активных уведомлений'} · {_format_age_ru(sub.get('updated_at'))}\n"
    return text


def _admin_handle_step(admin_id: str, text: str) -> bool:
    step = _admin_runtime_steps.pop(admin_id, "")
    if not step:
        return False
    if step == "online_set":
        m = re.search(r"\d{1,7}", text or "")
        if not m:
            _admin_send(admin_id, "Не нашёл число. Отправь лимит, например <code>100</code>.", reply_markup=_admin_keyboard("online"))
            return True
        limit = _online_save_limit(m.group(0), admin_id)
        _admin_log("online_limit_set", limit, admin_id)
        _admin_send(admin_id, f"✅ Онлайн лимит установлен: <b>{limit}</b>", reply_markup=_admin_keyboard("online"))
        return True
    uid_match = re.search(r"\d{5,}", text or "")
    uid = uid_match.group(0) if uid_match else ""
    if not uid:
        _admin_send(admin_id, "Не нашёл Telegram ID. Отправь только цифры, например <code>123456789</code>.", reply_markup=_admin_keyboard("main"))
        return True
    if step == "add":
        ok = _admin_add_user(uid, admin_id)
        _admin_send(admin_id, f"✅ Пользователь <code>{uid}</code> добавлен." if ok else "Не удалось добавить ID.", reply_markup=_admin_keyboard("users"))
    elif step == "delete":
        ok = _admin_delete_user(uid, admin_id)
        _admin_send(admin_id, f"🗑 Пользователь <code>{uid}</code> удалён из базы уведомлений." if ok else "Не удалось удалить ID.", reply_markup=_admin_keyboard("users"))
    elif step == "block":
        ok = _admin_block_user(uid, admin_id)
        _admin_send(admin_id, f"⛔ Пользователь <code>{uid}</code> заблокирован. Уведомления выключены." if ok else "Не удалось заблокировать ID.", reply_markup=_admin_keyboard("users"))
    elif step == "unblock":
        existed = _admin_unblock_user(uid, admin_id)
        _admin_send(admin_id, f"✅ Пользователь <code>{uid}</code> разблокирован." if existed else f"ID <code>{uid}</code> не был в блоке.", reply_markup=_admin_keyboard("users"))
    return True


def _admin_callback_text(action: str) -> tuple[str, str]:
    if action == "stats":
        return _admin_stats_text(), "stats"
    if action == "users":
        return _admin_users_text(), "users"
    if action == "notify":
        return _admin_notify_text(), "notify"
    if action == "system":
        return _admin_system_text(), "system"
    if action == "online":
        return _admin_online_text(), "online"
    return _admin_main_text(), "main"


def _handle_admin_callback(query: dict[str, Any]) -> None:
    from_user = query.get("from") if isinstance(query.get("from"), dict) else {}
    admin_id = str(from_user.get("id") or "").strip()
    callback_id = str(query.get("id") or "")
    if not _is_admin_id(admin_id):
        _admin_answer_callback(callback_id, "Нет доступа")
        return
    data = str(query.get("data") or "")
    msg = query.get("message") if isinstance(query.get("message"), dict) else {}
    chat = msg.get("chat") if isinstance(msg.get("chat"), dict) else {}
    chat_id = str(chat.get("id") or admin_id)
    message_id = msg.get("message_id") or 0
    action = data.split(":", 1)[1] if data.startswith("admin:") else "main"
    if action in {"online_inc10", "online_inc1", "online_dec10", "online_dec1"}:
        delta = 10 if action.endswith("10") else 1
        if "dec" in action:
            delta = -delta
        current = _safe_int(_online_snapshot().get("limit"), ONLINE_USER_LIMIT_DEFAULT)
        limit = _online_save_limit(current + delta, admin_id)
        _admin_log("online_limit_change", limit, admin_id)
        _admin_answer_callback(callback_id, f"Лимит: {limit}")
        _admin_edit(chat_id, message_id, _admin_online_text(), reply_markup=_admin_keyboard("online"))
        return
    if action == "online_set":
        _admin_runtime_steps[admin_id] = action
        _admin_answer_callback(callback_id, "Жду число")
        _admin_send(chat_id, "Отправь новый лимит онлайн пользователей, например <code>100</code>.", reply_markup=_admin_keyboard("online"))
        return
    if action in {"add", "delete", "block", "unblock"}:
        _admin_runtime_steps[admin_id] = action
        labels = {"add": "добавить", "delete": "удалить", "block": "заблокировать", "unblock": "разблокировать"}
        _admin_answer_callback(callback_id, "Жду Telegram ID")
        _admin_send(chat_id, f"Отправь Telegram ID пользователя, которого нужно <b>{labels[action]}</b>.\n\nМожно просто цифры одним сообщением.", reply_markup=_admin_keyboard("users"))
        return
    text, section = _admin_callback_text(action)
    _admin_answer_callback(callback_id, "Обновлено")
    _admin_edit(chat_id, message_id, text, reply_markup=_admin_keyboard(section))


def _handle_admin_message(message: dict[str, Any]) -> None:
    from_user = message.get("from") if isinstance(message.get("from"), dict) else {}
    user_id = str(from_user.get("id") or "").strip()
    text = str(message.get("text") or "").strip()
    chat = message.get("chat") if isinstance(message.get("chat"), dict) else {}
    chat_id = str(chat.get("id") or user_id)
    if not user_id:
        return
    if _is_admin_id(user_id):
        _remember_known_user(user_id, from_user)
        if _admin_handle_step(user_id, text):
            return
        cmd = text.split()[0].split("@", 1)[0].lower() if text else ""
        if cmd in {"/admin", "/start"}:
            _admin_send(chat_id, _admin_main_text(), reply_markup=_admin_keyboard("main"))
        elif cmd == "/stats":
            _admin_send(chat_id, _admin_stats_text(), reply_markup=_admin_keyboard("stats"))
        elif cmd == "/users":
            _admin_send(chat_id, _admin_users_text(), reply_markup=_admin_keyboard("users"))
        elif cmd == "/system":
            _admin_send(chat_id, _admin_system_text(), reply_markup=_admin_keyboard("system"))
        return
    if text and text.split()[0].split("@", 1)[0].lower() == "/admin":
        _admin_send(chat_id, "Нет доступа.")


def _admin_delete_webhook_for_polling() -> None:
    """Polling admin panel cannot run while Telegram webhook is configured."""
    if not BOT_TOKEN:
        return
    try:
        result = _tg_api("deleteWebhook", {"drop_pending_updates": False}, timeout=10)
        if result.get("ok"):
            print("[admin] deleteWebhook ok; polling can use getUpdates")
        else:
            print(f"[admin] deleteWebhook warning: {result.get('description') or result}")
    except Exception as exc:
        print(f"[admin] deleteWebhook failed: {exc}")


def _admin_poll_loop() -> None:
    global _admin_update_offset
    print(f"[admin] panel polling started for ADMIN_IDS={sorted(ADMIN_IDS)}")
    while True:
        try:
            if not BOT_TOKEN or not ADMIN_PANEL_ENABLED or not ADMIN_POLLING_ENABLED or not ADMIN_IDS:
                time.sleep(10)
                continue
            payload = {
                "offset": _admin_update_offset,
                "timeout": 25,
                "limit": 20,
                "allowed_updates": ["message", "callback_query"],
            }
            result = _tg_api("getUpdates", payload, timeout=35)
            if not result.get("ok"):
                desc = str(result.get("description") or "")
                if "webhook" in desc.lower() or "conflict" in desc.lower():
                    print(f"[admin] getUpdates conflict: {desc}. Remove Telegram webhook or set ADMIN_POLLING_ENABLED=0.")
                    time.sleep(30)
                else:
                    print(f"[admin] getUpdates error: {desc}")
                    time.sleep(5)
                continue
            for upd in result.get("result") or []:
                try:
                    _admin_update_offset = max(_admin_update_offset, _safe_int(upd.get("update_id"), 0) + 1)
                    if isinstance(upd.get("callback_query"), dict):
                        _handle_admin_callback(upd["callback_query"])
                    elif isinstance(upd.get("message"), dict):
                        _handle_admin_message(upd["message"])
                except Exception as exc:
                    print(f"[admin] update handle failed: {exc}")
                    traceback.print_exc()
        except Exception as exc:
            print(f"[admin] polling error: {exc}")
            time.sleep(5)


def start_admin_panel_worker() -> None:
    global _admin_polling_started
    if _admin_polling_started:
        return
    if not (BOT_TOKEN and ADMIN_PANEL_ENABLED and ADMIN_POLLING_ENABLED and ADMIN_IDS):
        print("[admin] panel disabled or missing BOT_TOKEN/ADMIN_IDS")
        return
    _admin_polling_started = True
    _admin_delete_webhook_for_polling()
    t = threading.Thread(target=_admin_poll_loop, daemon=True, name="admin-panel")
    t.start()


def fixed_goal_wait_filter(enabled: bool | None = None) -> dict[str, Any]:
    """Locked server-side copy of the Mini App "Ждём гол" signal."""
    out = {
        "signal_type": "goal_wait",
        "goal_signal_enabled": True,
        "minute_min": 55,
        "minute_max": 65,
        "goals_min": 0,
        "goals_max": 0,
        "score_diff_max": 0,
        "shots_min": 14,
        "on_target_min": 5,
        "off_target_min": 0,
        "dangerous_min": 45,
        "attacks_min": 0,
        "corners_min": 5,
        "pressure_min": 60,
        "yellow_cards_min": 0,
        "red_cards_min": 0,
        "possession_min": 0,
        "scores": ["0-0"],
        "countries": [],
    }
    if enabled is not None:
        out["enabled"] = bool(enabled)
    return out


def sanitize_notify_filter(f: dict[str, Any] | None) -> dict[str, Any]:
    """Sanitize the editable notification filter.

    The fixed "Ждём гол" signal has its own subscription flag and does not
    overwrite the user's 1/2/3 notification profiles.

    v9.80: раньше sanitize молча выбрасывал goals_max / score_diff_max /
    pressure_min / signal_type / goal_signal_enabled — поля, которые
    match_passes_filter продолжает читать. Из-за этого половина ползунков в
    Mini App не работала — фильтр всегда применял дефолты. Теперь
    эти поля сохраняются, и добавлен новый goals_min.
    """
    src = f or {}
    # v9.80: лимиты против раздувания подписки. Атакующий с валидным
    # init_data мог прислать список scores/countries на миллион элементов.
    MAX_LIST_LEN = 64
    MAX_ITEM_LEN = 64
    def _scores(value: Any) -> list[str]:
        if isinstance(value, str):
            items = [x.strip() for x in value.split(",") if x.strip()]
        elif isinstance(value, list):
            items = [str(x).strip() for x in value if str(x).strip()]
        else:
            items = []
        # каноничный формат счёта — короткий, обрезаем мусор
        out = []
        for x in items[:MAX_LIST_LEN]:
            x = x[:MAX_ITEM_LEN]
            # принимаем только "N-N" чтобы исключить произвольные строки
            if re.match(r"^\d{1,2}-\d{1,2}$", x):
                out.append(x)
        return out
    def _countries(value: Any) -> list[str]:
        if not isinstance(value, list):
            return []
        out = []
        for x in value[:MAX_LIST_LEN]:
            s = str(x).strip()[:MAX_ITEM_LEN]
            if s:
                out.append(s)
        return out
    minute_min = max(0, min(130, _safe_int(src.get("minute_min"), 45)))
    minute_max = max(0, min(130, _safe_int(src.get("minute_max"), 65)))
    # v9.80: если пользователь случайно поставил min > max,
    # меняем местами вместо тихого "бот выключился".
    if minute_min > minute_max:
        minute_min, minute_max = minute_max, minute_min
    return {
        "enabled": bool(src.get("enabled")),
        "minute_min": minute_min,
        "minute_max": minute_max,
        "shots_min": max(0, _safe_int(src.get("shots_min"), 14)),
        "on_target_min": max(0, _safe_int(src.get("on_target_min"), 6)),
        "off_target_min": max(0, _safe_int(src.get("off_target_min"), 0)),
        "dangerous_min": max(0, _safe_int(src.get("dangerous_min"), 51)),
        "attacks_min": max(0, _safe_int(src.get("attacks_min"), 101)),
        "corners_min": max(0, _safe_int(src.get("corners_min"), 0)),
        "yellow_cards_min": max(0, _safe_int(src.get("yellow_cards_min"), 0)),
        "red_cards_min": max(0, _safe_int(src.get("red_cards_min"), 0)),
        "possession_min": max(0, min(100, _safe_int(src.get("possession_min"), 0))),
        # v9.80: ранее потерянные поля:
        "goals_min": max(0, _safe_int(src.get("goals_min"), 0)),
        "goals_max": max(0, _safe_int(src.get("goals_max"), 0)),
        "score_diff_max": _safe_int(src.get("score_diff_max"), -1),
        "pressure_min": max(0, min(100, _safe_int(src.get("pressure_min"), 0))),
        "signal_type": str(src.get("signal_type") or ""),
        "goal_signal_enabled": bool(src.get("goal_signal_enabled", True)),
        "scores": _scores(src.get("scores") or ["0-0", "1-0", "0-1"]),
        "countries": _countries(src.get("countries")),
    }


def _stat_total_from_match_stats(stats: dict[str, Any], key: str) -> int:
    total = stats.get(f"{key}_total")
    if total not in (None, ""):
        return _safe_int(total)
    nested = stats.get(key)
    if isinstance(nested, dict):
        return _safe_int(nested.get("home")) + _safe_int(nested.get("away"))
    return _safe_int(stats.get(f"{key}_home")) + _safe_int(stats.get(f"{key}_away"))


def _stat_side_from_match_stats(stats: dict[str, Any], key: str, side: str) -> int:
    nested = stats.get(key)
    if isinstance(nested, dict):
        return _safe_int(nested.get(side))
    return _safe_int(stats.get(f"{key}_{side}"))


def _pressure_power_from_stats(stats: dict[str, Any], side: str) -> float:
    attacks = _stat_side_from_match_stats(stats, "attacks", side)
    dangerous = _stat_side_from_match_stats(stats, "dangerous", side)
    shots = _stat_side_from_match_stats(stats, "shots", side)
    on_target = _stat_side_from_match_stats(stats, "on_target", side)
    corners = _stat_side_from_match_stats(stats, "corners", side)
    return (attacks * 0.6) + (dangerous * 3.0) + (shots * 4.0) + (on_target * 6.0) + (corners * 5.0)


def match_passes_filter(m: dict[str, Any], f: dict[str, Any]) -> bool:
    """Server-side copy of the Mini App notification filter.

    v9.16 uses a locked Ждём гол signal for Telegram push notifications.
    """
    if str(f.get("signal_type") or "") == "goal_wait" and f.get("goal_signal_enabled") is False:
        return False

    minute = _safe_int(m.get("minute"), 0)
    mn = _safe_int(f.get("minute_min"), 0)
    mx = _safe_int(f.get("minute_max"), 130)
    if minute < mn or minute > mx:
        return False

    sh = _safe_int(m.get("score_home"), 0)
    sa = _safe_int(m.get("score_away"), 0)
    # Use normalized score_home-score_away for filtering. Some providers return
    # m["score"] as "0 : 0" / "0 - 0", while Mini App filters store "0-0".
    # Exact matching on the raw string made server-side Telegram filters miss
    # matches that the app itself showed as found.
    score_text = f"{sh}-{sa}"
    # v9.80: было только goals_max; добавлен goals_min, чтобы можно
    # было ждать результативные матчи (например тотал ≥ 2).
    goals_min = _safe_int(f.get("goals_min"), 0)
    if goals_min > 0 and (sh + sa) < goals_min:
        return False
    goals_max = _safe_int(f.get("goals_max"), 0)
    if goals_max > 0 and (sh + sa) > goals_max:
        return False
    score_diff_max = _safe_int(f.get("score_diff_max"), -1)
    if score_diff_max >= 0 and abs(sh - sa) > score_diff_max:
        return False

    s = m.get("stats") or {}
    if _safe_int(f.get("shots_min")) and _stat_total_from_match_stats(s, "shots") < _safe_int(f.get("shots_min")): return False
    if _safe_int(f.get("on_target_min")) and _stat_total_from_match_stats(s, "on_target") < _safe_int(f.get("on_target_min")): return False
    if _safe_int(f.get("off_target_min")) and _stat_total_from_match_stats(s, "off_target") < _safe_int(f.get("off_target_min")): return False
    if _safe_int(f.get("dangerous_min")) and _stat_total_from_match_stats(s, "dangerous") < _safe_int(f.get("dangerous_min")): return False
    if _safe_int(f.get("attacks_min")) and _stat_total_from_match_stats(s, "attacks") < _safe_int(f.get("attacks_min")): return False
    if _safe_int(f.get("corners_min")) and _stat_total_from_match_stats(s, "corners") < _safe_int(f.get("corners_min")): return False
    if _safe_int(f.get("yellow_cards_min")) and _stat_total_from_match_stats(s, "yellow_cards") < _safe_int(f.get("yellow_cards_min")): return False
    if _safe_int(f.get("red_cards_min")) and _stat_total_from_match_stats(s, "red_cards") < _safe_int(f.get("red_cards_min")): return False
    if _safe_int(f.get("possession_min")):
        # v9.80: было `or 50` — при отсутствии данных о владении
        # (0% / 0%) фильтр всегда проходил порог ≤ 50%. Теперь нет данных — не проходит.
        poss_home = _stat_side_from_match_stats(s, "possession", "home")
        poss_away = _stat_side_from_match_stats(s, "possession", "away")
        if (poss_home + poss_away) <= 0:
            return False
        if max(poss_home, poss_away) < _safe_int(f.get("possession_min")):
            return False
    if _safe_int(f.get("pressure_min")):
        hp = _pressure_power_from_stats(s, "home")
        ap = _pressure_power_from_stats(s, "away")
        total_power = hp + ap
        if total_power <= 0:
            return False
        max_pct = round((max(hp, ap) / total_power) * 100)
        if max_pct < _safe_int(f.get("pressure_min"), 60):
            return False
    scores = f.get("scores") or []
    if isinstance(scores, str):
        scores = [x.strip() for x in scores.split(",") if x.strip()]
    if scores and score_text not in scores:
        return False
    countries = f.get("countries") or []
    if countries and m.get("country") not in countries:
        return False
    return True


def _rating_bonus(value: Any, threshold: Any, max_bonus: float, full_extra_ratio: float = 0.75) -> float:
    threshold_i = _safe_int(threshold, 0)
    if threshold_i <= 0:
        return 0.0
    value_i = _safe_int(value, 0)
    if value_i < threshold_i:
        return 0.0
    needed = max(1, int((threshold_i * full_extra_ratio) + 0.999))
    return min(float(max_bonus), ((value_i - threshold_i) / needed) * float(max_bonus))


def _filter_rating_label(score: int) -> str:
    if score >= 90:
        return "🔥 Топ"
    if score >= 82:
        return "🟢 Сильный"
    if score >= 72:
        return "🟡 Хороший"
    if score >= 62:
        return "⚪ Подходит"
    return "Слабый"


def _stat_reason(label: str, value: Any, threshold: Any) -> str:
    v = _safe_int(value, 0)
    t = _safe_int(threshold, 0)
    return f"{label} {v}/{t}" if t > 0 else f"{label} {v}"


def _pressure_summary(stats: dict[str, Any]) -> tuple[int, str]:
    hp = _pressure_power_from_stats(stats, "home")
    ap = _pressure_power_from_stats(stats, "away")
    total = hp + ap
    pct = round((max(hp, ap) / total) * 100) if total > 0 else 0
    side = "хозяев" if hp > ap else ("гостей" if ap > hp else "обоюдно")
    return int(pct), side


def _filter_signal_for_match(m: dict[str, Any], f: dict[str, Any], kind: str = "filter", profile: str = "") -> dict[str, Any] | None:
    if not match_passes_filter(m, f or {}):
        return None

    stats = m.get("stats") if isinstance(m.get("stats"), dict) else {}
    minute = _safe_int(m.get("minute"), 0)
    mn = max(0, min(130, _safe_int((f or {}).get("minute_min"), 0)))
    mx = max(0, min(130, _safe_int((f or {}).get("minute_max"), 130)))
    mid = (mn + mx) / 2
    span = max(1, mx - mn)
    minute_fit = 1 - min(1, abs(minute - mid) / max(1, span / 2))

    sh = _safe_int(m.get("score_home"), 0)
    sa = _safe_int(m.get("score_away"), 0)
    score_text = str(m.get("score") or f"{sh}-{sa}")
    total_goals = sh + sa
    diff = abs(sh - sa)

    shots = _stat_total_from_match_stats(stats, "shots")
    on_target = _stat_total_from_match_stats(stats, "on_target")
    dangerous = _stat_total_from_match_stats(stats, "dangerous")
    attacks = _stat_total_from_match_stats(stats, "attacks")
    corners = _stat_total_from_match_stats(stats, "corners")
    off_target = _stat_total_from_match_stats(stats, "off_target")
    pressure_pct, pressure_side = _pressure_summary(stats)

    index = 55.0
    index += 7 + minute_fit * 5
    if score_text == "0-0":
        index += 10
    elif diff == 0:
        index += 7
    elif diff == 1:
        index += 3
    if total_goals <= 1:
        index += 3

    index += _rating_bonus(shots, (f or {}).get("shots_min"), 10)
    index += _rating_bonus(on_target, (f or {}).get("on_target_min"), 11)
    index += _rating_bonus(dangerous, (f or {}).get("dangerous_min"), 11)
    index += _rating_bonus(attacks, (f or {}).get("attacks_min"), 5)
    index += _rating_bonus(corners, (f or {}).get("corners_min"), 6)
    index += _rating_bonus(off_target, (f or {}).get("off_target_min"), 3)
    pressure_min = _safe_int((f or {}).get("pressure_min"), 0)
    if pressure_min > 0 and pressure_pct:
        index += min(7, max(0, pressure_pct - pressure_min) * 0.45)

    # Server stays deliberately lightweight: no extra external lookups for league strength.
    final_score = max(1, min(100, int(round(index))))
    reasons = [f"{minute or 'LIVE'} минута ({mn}–{mx})", f"счёт {score_text}"]
    if _safe_int((f or {}).get("shots_min"), 0) > 0:
        reasons.append(_stat_reason("удары", shots, (f or {}).get("shots_min")))
    if _safe_int((f or {}).get("on_target_min"), 0) > 0:
        reasons.append(_stat_reason("в створ", on_target, (f or {}).get("on_target_min")))
    if _safe_int((f or {}).get("dangerous_min"), 0) > 0:
        reasons.append(_stat_reason("опасные", dangerous, (f or {}).get("dangerous_min")))
    if _safe_int((f or {}).get("attacks_min"), 0) > 0:
        reasons.append(_stat_reason("атаки", attacks, (f or {}).get("attacks_min")))
    if _safe_int((f or {}).get("corners_min"), 0) > 0:
        reasons.append(_stat_reason("угловые", corners, (f or {}).get("corners_min")))
    if pressure_min > 0 and pressure_pct:
        reasons.append(f"давление {pressure_pct}% {pressure_side}")

    return {
        "type": kind or "filter",
        "score": final_score,
        "label": _filter_rating_label(final_score),
        "reason": " · ".join(reasons[:5]),
        "reasons": reasons,
        "profile": profile or "",
    }


def _apply_filter_signal(m: dict[str, Any], f: dict[str, Any], kind: str = "filter", profile: str = "") -> dict[str, Any]:
    sig = _filter_signal_for_match(m, f, kind=kind, profile=profile)
    if not sig:
        return m
    m["filter_score"] = sig["score"]
    m["filter_label"] = sig["label"]
    m["filter_reason"] = sig["reason"]
    m["filter_reasons"] = sig["reasons"]
    m["filter_profile"] = sig.get("profile") or ("Ждём гол" if kind == "goal_wait" else "")
    return m


def _alert_reasons_list(value: Any) -> list[str]:
    """Normalize reason text for storage, UI and Telegram output."""
    if isinstance(value, str):
        parts = [x.strip() for x in re.split(r"[·\n|;]+", value) if x.strip()]
    elif isinstance(value, list):
        parts = [str(x).strip() for x in value if str(x).strip()]
    else:
        parts = []
    out: list[str] = []
    seen: set[str] = set()
    for part in parts:
        # Keep messages short enough for Telegram cards and JSON storage.
        clean = re.sub(r"\s+", " ", part)[:80]
        if clean and clean not in seen:
            out.append(clean)
            seen.add(clean)
        if len(out) >= 8:
            break
    return out


def _notify_filter_analysis(m: dict[str, Any], f: dict[str, Any], label: str = "Фильтр") -> dict[str, Any]:
    """Build a zero-extra-request reason list and 1-100 strength score.

    The match is already known to pass the filter; this function only explains
    why and ranks the strength from the same live row/stats.
    """
    f = f or {}
    s = m.get("stats") if isinstance(m.get("stats"), dict) else {}
    minute = _safe_int(m.get("minute"), 0)
    mn = _safe_int(f.get("minute_min"), 0)
    mx = _safe_int(f.get("minute_max"), 130)
    sh = _safe_int(m.get("score_home"), 0)
    sa = _safe_int(m.get("score_away"), 0)
    total_goals = sh + sa
    score_text = str(m.get("score") or f"{sh}-{sa}")
    reasons: list[str] = []

    def add(reason: str) -> None:
        reason = re.sub(r"\s+", " ", str(reason or "")).strip()
        if reason and reason not in reasons and len(reasons) < 8:
            reasons.append(reason[:80])

    if minute:
        add(f"{minute} минута в диапазоне {mn}-{mx}")
    else:
        add("LIVE в нужном диапазоне")

    scores = f.get("scores") or []
    if isinstance(scores, str):
        scores = [x.strip() for x in scores.split(",") if x.strip()]
    if scores:
        add(f"счёт {score_text}")

    goals_max = _safe_int(f.get("goals_max"), 0)
    if goals_max > 0:
        add(f"голов {total_goals}/{goals_max}")
    score_diff_max = _safe_int(f.get("score_diff_max"), -1)
    if score_diff_max >= 0:
        add(f"разница {abs(sh - sa)}/{score_diff_max}")

    rating = 48.0

    # Time quality: the middle of the selected interval gets a little more.
    if minute and mx >= mn:
        span = max(1.0, float(mx - mn))
        center = (mn + mx) / 2.0
        dist = min(1.0, abs(minute - center) / max(1.0, span / 2.0))
        rating += 5.0 + (4.0 * (1.0 - dist))
    else:
        rating += 4.0

    # Low/equal score is usually better for goal-wait style filters.
    if total_goals == 0:
        rating += 10.0
    elif abs(sh - sa) == 0:
        rating += 8.0
    elif abs(sh - sa) == 1:
        rating += 5.0
    else:
        rating += 2.0

    stat_specs = [
        ("shots", "удары", 8.0, 0.65),
        ("on_target", "в створ", 9.0, 1.35),
        ("off_target", "мимо", 4.0, 0.45),
        ("dangerous", "опасные атаки", 9.0, 0.16),
        ("attacks", "атаки", 5.0, 0.045),
        ("corners", "угловые", 6.0, 0.85),
        ("yellow_cards", "жёлтые", 2.0, 0.35),
        ("red_cards", "красные", 2.0, 0.8),
    ]
    for key, ru, cap, slope in stat_specs:
        min_v = _safe_int(f.get(f"{key}_min"), 0)
        if min_v <= 0:
            continue
        val = _stat_total_from_match_stats(s, key)
        add(f"{ru} {val}/{min_v}")
        rating += min(cap, (cap * 0.50) + max(0, val - min_v) * slope)

    poss_min = _safe_int(f.get("possession_min"), 0)
    if poss_min > 0:
        poss = max(_stat_side_from_match_stats(s, "possession", "home") or 50, _stat_side_from_match_stats(s, "possession", "away") or 50)
        add(f"владение {poss}%/{poss_min}%")
        rating += min(4.0, 2.0 + max(0, poss - poss_min) * 0.10)

    pressure_min = _safe_int(f.get("pressure_min"), 0)
    if pressure_min > 0:
        hp = _pressure_power_from_stats(s, "home")
        ap = _pressure_power_from_stats(s, "away")
        total_power = hp + ap
        pressure_pct = round((max(hp, ap) / total_power) * 100) if total_power > 0 else 0
        side = "хозяев" if hp > ap else "гостей" if ap > hp else "обоюдно"
        add(f"давление {pressure_pct}%/{pressure_min}% {side}")
        rating += min(8.0, 4.0 + max(0, pressure_pct - pressure_min) * 0.18)

    countries = f.get("countries") or []
    if countries and m.get("country") in countries:
        add(f"страна {m.get('country')}")
        rating += 2.0

    score = max(1, min(100, int(round(rating))))
    return {"alert_score": score, "alert_reasons": _alert_reasons_list(reasons), "alert_reason": " · ".join(reasons)}


def _apply_notify_analysis(m: dict[str, Any], f: dict[str, Any], label: str = "Фильтр") -> dict[str, Any]:
    analysis = _notify_filter_analysis(m, f, label=label)
    m["alert_score"] = analysis["alert_score"]
    m["alert_reasons"] = analysis["alert_reasons"]
    m["alert_reason"] = analysis["alert_reason"]
    return m


def notify_worker_loop() -> None:
    """Background loop: check subscriptions and save found matches for the app."""
    while True:
        try:
            time.sleep(NOTIFY_POLL_INTERVAL)
            if not _notify_subs:
                continue
            notify_started = time.time()
            _notify_worker_state["last_start"] = int(notify_started)
            live = load_live_payload(force=False)
            matches = flatten_payload(live)
            _notify_worker_state["last_matches"] = len(matches)
            _notify_worker_state["last_subs"] = len(_notify_subs)
            now = time.time()
            changed = False
            with _notify_lock:
                match_by_id = {str(m.get("id") or ""): m for m in matches if str(m.get("id") or "")}
                for chat_id_str, sub in list(_notify_subs.items()):
                    if _flush_pending_telegram_for_sub(chat_id_str, sub, now=now):
                        changed = True
                    cfg = sanitize_notify_filter(sub.get("filter") or {})
                    seen = sub.setdefault("seen", {})

                    if cfg.get("enabled"):
                        for m in matches:
                            if not match_passes_filter(m, cfg):
                                continue
                            mid = str(m.get("id") or "")
                            if not mid:
                                continue
                            if _notify_match_dismissed(sub, mid):
                                continue
                            notify_match = dict(m)
                            _apply_notify_analysis(notify_match, cfg, label="Фильтр")
                            notify_match.setdefault("alert_kind", "filter")
                            notify_match.setdefault("alert_title", f"Матч нашёлся: {m.get('home') or ''} {m.get('score_home') or 0}-{m.get('score_away') or 0} {m.get('away') or ''}")
                            notify_match.setdefault("alert_subtitle", f"{m.get('minute_text') or ''} · {m.get('country') or ''} · {m.get('league') or ''}")
                            delivery_key = _notify_delivery_key(mid, "filter", notify_match)
                            legacy_keys = _notify_legacy_keys(mid, "filter")
                            if _notify_already_delivered(sub, delivery_key, legacy_keys):
                                continue
                            _store_notify_match(chat_id_str, notify_match, ts=int(now))
                            # Telegram push is intentionally short: details are visible only in the app.
                            ok, queued = _send_or_queue_telegram(
                                chat_id_str,
                                sub,
                                delivery_key,
                                _notification_text_for_match(notify_match),
                                link=notification_match_link(mid),
                                now=now,
                                legacy_keys=legacy_keys,
                            )
                            if ok or queued:
                                # v9.67: regular filter notifications do not auto-enable goal alerts.
                                # Goal watching is manual or handled by the separate Ждём гол mode.
                                changed = True

                    goal_wait = sub.setdefault("goal_wait", {})
                    if bool(goal_wait.get("enabled")):
                        sub.setdefault("goal_wait_seen", {})
                        goal_cfg = fixed_goal_wait_filter(enabled=True)
                        for m in matches:
                            if not match_passes_filter(m, goal_cfg):
                                continue
                            mid = str(m.get("id") or "")
                            if not mid:
                                continue
                            if _notify_match_dismissed(sub, mid):
                                continue
                            goal_wait_match = dict(m)
                            _apply_notify_analysis(goal_wait_match, goal_cfg, label="Ждём гол")
                            goal_wait_match["alert_kind"] = "goal_wait"
                            goal_wait_match["alert_title"] = f"Ждём гол: {m.get('home') or ''} {m.get('score_home') or 0}-{m.get('score_away') or 0} {m.get('away') or ''}"
                            goal_wait_match["alert_subtitle"] = f"{m.get('minute_text') or ''} · {m.get('country') or ''} · {m.get('league') or ''}"
                            delivery_key = _notify_delivery_key(mid, "goal_wait", goal_wait_match)
                            legacy_keys = _notify_legacy_keys(mid, "goal_wait")
                            if _notify_already_delivered(sub, delivery_key, legacy_keys):
                                continue
                            _store_notify_match(chat_id_str, goal_wait_match, ts=int(now))
                            ok, queued = _send_or_queue_telegram(
                                chat_id_str,
                                sub,
                                delivery_key,
                                _notification_text_for_match(goal_wait_match),
                                link=notification_match_link(mid),
                                now=now,
                                legacy_keys=legacy_keys,
                            )
                            if ok:
                                if _ensure_goal_alert_for_match(sub, goal_wait_match, now=int(now)):
                                    changed = True
                                changed = True
                            elif queued:
                                if _ensure_goal_alert_for_match(sub, goal_wait_match, now=int(now)):
                                    changed = True
                                changed = True

                    goal_alerts = sub.setdefault("goal_alerts", {})
                    for mid, alert in list(goal_alerts.items()):
                        if _notify_match_dismissed(sub, str(mid)):
                            goal_alerts.pop(str(mid), None)
                            changed = True
                            continue
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
                            delivery_key = _notify_delivery_key(str(mid), "goal", goal_match)
                            if not _notify_already_delivered(sub, delivery_key, set()):
                                _store_notify_match(chat_id_str, goal_match, ts=int(now))
                                ok, queued = _send_or_queue_telegram(
                                    chat_id_str,
                                    sub,
                                    delivery_key,
                                    _goal_notification_text_for_match(m),
                                    link=notification_match_link(mid),
                                    now=now,
                                    legacy_keys=set(),
                                )
                                if ok or queued:
                                    changed = True
                            else:
                                changed = True
                        if current_total != last_total or str(alert.get("last_score") or "") != current_score:
                            alert["last_total"] = current_total
                            alert["last_score"] = current_score
                            alert["updated_at"] = int(now)
                            changed = True
                for sub in _notify_subs.values():
                    if _cleanup_notify_dedupe(sub, now):
                        changed = True
                # v9.80: снимок под локом — запись на диск идёт уже без лока.
                # Раньше _save_notify_* выполнялись внутри _notify_lock и могли
                # блокировать /api/subscribe / /api/notify на десятки секунд.
                subs_snapshot = dict(_notify_subs) if changed else None
                matches_snapshot = dict(_notify_matches) if changed else None
            if changed:
                _save_notify_subs(subs_snapshot)
                _save_notify_matches(matches_snapshot)
            notify_finished = time.time()
            _notify_worker_state["last_finish"] = int(notify_finished)
            _notify_worker_state["last_error"] = ""
            _record_scan_duration(_notify_worker_state, _notify_scan_history, notify_finished - notify_started)
            if int(notify_finished) - _safe_int(_notify_worker_state.get("last_log"), 0) >= TELEGRAM_LOG_INTERVAL_SECONDS:
                _notify_worker_state["last_log"] = int(notify_finished)
                print(f"[notify] scan subs={_safe_int(_notify_worker_state.get('last_subs'),0)} matches={_safe_int(_notify_worker_state.get('last_matches'),0)} seconds={_fmt_seconds(_notify_worker_state.get('last_scan_seconds'))} queue={_telegram_send_queue.qsize()}")
        except Exception as exc:
            _notify_worker_state["last_finish"] = int(time.time())
            _notify_worker_state["last_error"] = str(exc)[:200]
            print(f"[notify] worker error: {exc}")
            traceback.print_exc()


def _scan_notify_subscription_once(chat_id_str: str | int, include_goal_wait: bool = True) -> None:
    """Run one immediate server-side notification scan for a just-saved subscription.

    This fixes the common case where the user enables the filter and closes the
    Mini App before the background worker's next 30s poll or before the frontend
    /api/notify request finishes.  The worker still keeps running for future
    matches; this one-shot scan just makes the switch reliable immediately.
    """
    chat_key = str(chat_id_str or "").strip()
    if not chat_key:
        return
    try:
        live = load_live_payload(force=False)
        matches = flatten_payload(live)
        now = time.time()
        changed = False
        with _notify_lock:
            sub = _notify_subs.get(chat_key)
            if not isinstance(sub, dict):
                return

            if _flush_pending_telegram_for_sub(chat_key, sub, now=now):
                changed = True

            cfg = sanitize_notify_filter(sub.get("filter") or {})
            if cfg.get("enabled"):
                for m in matches:
                    if not match_passes_filter(m, cfg):
                        continue
                    mid = str(m.get("id") or "")
                    if not mid:
                        continue
                    if _notify_match_dismissed(sub, mid):
                        continue
                    notify_match = dict(m)
                    _apply_notify_analysis(notify_match, cfg, label="Фильтр")
                    notify_match.setdefault("alert_kind", "filter")
                    notify_match.setdefault("alert_title", f"Матч нашёлся: {m.get('home') or ''} {m.get('score_home') or 0}-{m.get('score_away') or 0} {m.get('away') or ''}")
                    notify_match.setdefault("alert_subtitle", f"{m.get('minute_text') or ''} · {m.get('country') or ''} · {m.get('league') or ''}")
                    delivery_key = _notify_delivery_key(mid, "filter", notify_match)
                    legacy_keys = _notify_legacy_keys(mid, "filter")
                    if _notify_already_delivered(sub, delivery_key, legacy_keys):
                        continue
                    _store_notify_match(chat_key, notify_match, ts=int(now))
                    ok, queued = _send_or_queue_telegram(
                        chat_key,
                        sub,
                        delivery_key,
                        _notification_text_for_match(notify_match),
                        link=notification_match_link(mid),
                        now=now,
                        legacy_keys=legacy_keys,
                    )
                    if ok or queued:
                        # v9.67: do not auto-enable goal alerts for regular filter hits.
                        changed = True

            if include_goal_wait:
                goal_wait = sub.setdefault("goal_wait", {})
                if bool(goal_wait.get("enabled")):
                    goal_cfg = fixed_goal_wait_filter(enabled=True)
                    for m in matches:
                        if not match_passes_filter(m, goal_cfg):
                            continue
                        mid = str(m.get("id") or "")
                        if not mid:
                            continue
                        if _notify_match_dismissed(sub, mid):
                            continue
                        goal_wait_match = dict(m)
                        _apply_notify_analysis(goal_wait_match, goal_cfg, label="Ждём гол")
                        goal_wait_match["alert_kind"] = "goal_wait"
                        goal_wait_match["alert_title"] = f"Ждём гол: {m.get('home') or ''} {m.get('score_home') or 0}-{m.get('score_away') or 0} {m.get('away') or ''}"
                        goal_wait_match["alert_subtitle"] = f"{m.get('minute_text') or ''} · {m.get('country') or ''} · {m.get('league') or ''}"
                        delivery_key = _notify_delivery_key(mid, "goal_wait", goal_wait_match)
                        legacy_keys = _notify_legacy_keys(mid, "goal_wait")
                        if _notify_already_delivered(sub, delivery_key, legacy_keys):
                            continue
                        _store_notify_match(chat_key, goal_wait_match, ts=int(now))
                        ok, queued = _send_or_queue_telegram(
                            chat_key,
                            sub,
                            delivery_key,
                            _notification_text_for_match(goal_wait_match),
                            link=notification_match_link(mid),
                            now=now,
                            legacy_keys=legacy_keys,
                        )
                        if ok or queued:
                            _ensure_goal_alert_for_match(sub, goal_wait_match, now=int(now))
                            changed = True

            if _cleanup_notify_dedupe(sub, now):
                changed = True

            if changed:
                sub["updated_at"] = int(now)
                _notify_subs[chat_key] = sub
                _save_one_notify_sub(chat_key)
                _save_one_notify_matches(chat_key)
    except Exception as exc:
        print(f"[notify] immediate scan failed for {chat_key}: {exc}")
        traceback.print_exc()


def _scan_notify_subscription_async(chat_id_str: str | int, include_goal_wait: bool = True) -> None:
    try:
        threading.Thread(
            target=_scan_notify_subscription_once,
            args=(str(chat_id_str), include_goal_wait),
            daemon=True,
            name=f"notify-scan-{str(chat_id_str)[-6:]}",
        ).start()
    except Exception:
        _scan_notify_subscription_once(chat_id_str, include_goal_wait=include_goal_wait)


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
    if not _admin_user_can_use(chat_id):
        return {"ok": False, "error": "access denied"}
    _remember_known_user(chat_id, {})

    with _notify_lock:
        sub = _notify_subs.setdefault(chat_id, {"chat_id": chat_id, "filter": {}, "seen": {}, "goal_alerts": {}})
        goal_alerts = sub.setdefault("goal_alerts", {})
        if enabled:
            m = _find_live_match(match_id)
            if not m:
                return {"ok": False, "error": "match not found"}
            existing_alert = goal_alerts.get(match_id) if isinstance(goal_alerts.get(match_id), dict) else {}
            goal_alerts[match_id] = {
                **existing_alert,
                "match_id": match_id,
                "last_score": _goal_score_for_match(m),
                "last_total": _goal_total_for_match(m),
                "home": str(m.get("home") or ""),
                "away": str(m.get("away") or ""),
                "manual": True,
                "enabled_at": int(time.time()),
                "updated_at": int(time.time()),
            }
        else:
            existing_alert = goal_alerts.get(match_id)
            if isinstance(existing_alert, dict) and existing_alert.get("auto"):
                existing_alert.pop("manual", None)
                existing_alert["updated_at"] = int(time.time())
            else:
                goal_alerts.pop(match_id, None)
        sub["updated_at"] = int(time.time())
        _notify_subs[chat_id] = sub
        _save_one_notify_sub(chat_id)
    return {"ok": True, "chat_id": chat_id, "match_id": match_id, "enabled": enabled}


def handle_goal_wait_subscribe(body: dict[str, Any]) -> dict[str, Any]:
    """Enable/disable the fixed "Ждём гол" signal without touching user filters."""
    init_data = str(body.get("init_data") or "")
    enabled = bool(body.get("enabled"))
    user = verify_init_data(init_data)
    if not user:
        if not BOT_TOKEN:
            return {"ok": True, "warning": "BOT_TOKEN not set on server; in-app only", "enabled": enabled}
        return {"ok": False, "error": "init_data verification failed"}
    chat_id = str(user.get("id"))
    if not chat_id or chat_id == "None":
        return {"ok": False, "error": "no chat_id"}
    if not _admin_user_can_use(chat_id):
        return {"ok": False, "error": "access denied"}
    _remember_known_user(chat_id, user)
    with _notify_lock:
        sub = _notify_subs.setdefault(chat_id, {"chat_id": chat_id, "filter": {}, "seen": {}, "goal_alerts": {}})
        sub["goal_wait"] = {"enabled": enabled, "updated_at": int(time.time())}
        sub.setdefault("goal_wait_seen", {})
        sub["user"] = {"first_name": user.get("first_name"), "username": user.get("username")}
        sub["updated_at"] = int(time.time())
        _notify_subs[chat_id] = sub
        _save_one_notify_sub(chat_id)
    if enabled:
        _scan_notify_subscription_async(chat_id, include_goal_wait=True)
    return {"ok": True, "chat_id": chat_id, "enabled": enabled}


def handle_subscribe(body: dict[str, Any]) -> dict[str, Any]:
    init_data = str(body.get("init_data") or "")
    raw_filt = body.get("filter") if isinstance(body.get("filter"), dict) else {}
    filt = sanitize_notify_filter(raw_filt)
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
    if not _admin_user_can_use(chat_id):
        return {"ok": False, "error": "access denied"}
    _remember_known_user(chat_id, user)
    with _notify_lock:
        existing = _notify_subs.get(chat_id) or {}
        existing.update({
            "chat_id": chat_id,
            "filter": filt,
            "profile_id": str(body.get("profile_id") or existing.get("profile_id") or ""),
            "user": {"first_name": user.get("first_name"), "username": user.get("username")},
            "updated_at": int(time.time()),
        })
        if "goal_wait_enabled" in body:
            existing["goal_wait"] = {"enabled": bool(body.get("goal_wait_enabled")), "updated_at": int(time.time())}
            existing.setdefault("goal_wait_seen", {})
        existing.setdefault("seen", {})
        existing.setdefault("goal_alerts", {})
        _notify_subs[chat_id] = existing
        _save_one_notify_sub(chat_id)
        should_scan_now = bool((existing.get("filter") or {}).get("enabled")) or bool((existing.get("goal_wait") or {}).get("enabled"))
    if should_scan_now:
        _scan_notify_subscription_async(chat_id, include_goal_wait=True)
    return {"ok": True, "chat_id": chat_id}


def handle_notify(body: dict[str, Any]) -> dict[str, Any]:
    """Immediate push from the open Mini App. Stores the match and sends Telegram.

    Ждём гол notifications can subscribe the match for the next goal.
    Regular filter notifications do not auto-enable goal alerts.
    Goal notifications use a separate cooldown key so they are not blocked by the
    initial "Матч нашёлся" message for the same match.
    """
    init_data = str(body.get("init_data") or "")
    match_id = str(body.get("match_id") or "").strip()
    snap = body.get("match") if isinstance(body.get("match"), dict) else {}
    chat_id = _chat_id_from_init_data(init_data)
    if not chat_id:
        if BOT_TOKEN:
            return {"ok": False, "error": "init_data verification failed"}
        chat_id = "local"
    if chat_id != "local" and not _admin_user_can_use(chat_id):
        return {"ok": False, "error": "access denied"}

    live_match = _find_live_match(match_id) if match_id else None
    m = _merge_notify_snapshot(live_match, snap, match_id=match_id)
    if not m:
        text = str(body.get("text") or "").strip()
        if not text:
            return {"ok": False, "error": "missing_match_id"}
        ok = send_telegram_message(chat_id, text, link="") if BOT_TOKEN and chat_id != "local" else True
        return {"ok": ok, "stored": False}

    match_id = str(m.get("id") or match_id or "").strip()
    m.setdefault("id", match_id)
    kind = _notify_kind(m.get("alert_kind"))
    if bool(body.get("auto_goal")) and kind not in {"goal", "goal_wait"}:
        m.setdefault("alert_kind", "filter")
        kind = "filter"
    else:
        m["alert_kind"] = kind

    now = time.time()
    with _notify_lock:
        sub = _notify_subs.setdefault(chat_id, {"chat_id": chat_id, "filter": {}, "seen": {}, "delivered": {}, "goal_alerts": {}})
        sub.setdefault("seen", {})
        sub.setdefault("delivered", {})
        if _notify_match_dismissed(sub, match_id):
            sub["updated_at"] = int(now)
            _notify_subs[chat_id] = sub
            _save_one_notify_sub(chat_id)
            return {"ok": True, "stored": False, "duplicate": True, "suppressed": True}
        if kind in {"filter", "goal_wait"} and not _safe_int(m.get("alert_score"), 0):
            analysis_cfg = fixed_goal_wait_filter(enabled=True) if kind == "goal_wait" else sanitize_notify_filter(sub.get("filter") or {})
            _apply_notify_analysis(m, analysis_cfg, label="Ждём гол" if kind == "goal_wait" else "Фильтр")
        delivery_key = _notify_delivery_key(match_id, kind, m)
        legacy_keys = _notify_legacy_keys(match_id, kind)
        duplicate = _notify_already_delivered(sub, delivery_key, legacy_keys)
        if kind == "goal" and _goal_alert_already_at_score(sub, m):
            duplicate = True
        _store_notify_match(chat_id, m, ts=int(now))
        if kind == "goal":
            _mark_goal_alert_current(sub, m, now=int(now))
        elif kind == "goal_wait":
            _ensure_goal_alert_for_match(sub, m, now=int(now))
        ok = True
        queued = False
        if not duplicate:
            ok, queued = _send_or_queue_telegram(
                chat_id,
                sub,
                delivery_key,
                _notification_text_for_match(m),
                link=notification_match_link(match_id),
                now=now,
                legacy_keys=legacy_keys,
            )
        sub["updated_at"] = int(now)
        _notify_subs[chat_id] = sub
        _save_one_notify_sub(chat_id)
        _save_one_notify_matches(chat_id)

    return {"ok": ok, "stored": True, "duplicate": duplicate, "queued": queued, "match": _notify_match_record(m, ts=int(now))}

def parse_json_body(handler: BaseHTTPRequestHandler) -> dict[str, Any]:
    # v9.80: лимит на размер тела запроса. Раньше Content-Length читался
    # без верхней границы — атакующий мог отправить `Content-Length:
    # 10000000000` и положить процесс на попытке чтения 10 ГБ в RAM.
    MAX_BODY_BYTES = 256 * 1024  # 256 KB достаточно для любого штатного запроса
    try:
        length = int(handler.headers.get("Content-Length") or 0)
        if not length:
            return {}
        if length < 0 or length > MAX_BODY_BYTES:
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


_api_rate_lock = threading.Lock()
_api_rate_buckets: dict[str, list[float]] = {}


def _client_ip_from_handler(handler: BaseHTTPRequestHandler) -> str:
    xff = str(handler.headers.get("X-Forwarded-For") or "").split(",")[0].strip()
    if xff:
        return xff
    try:
        return str(handler.client_address[0])
    except Exception:
        return "unknown"


def _rate_limit_ok(handler: BaseHTTPRequestHandler, bucket: str) -> bool:
    if API_RATE_LIMIT_MAX_REQUESTS <= 0:
        return True
    key = f"{bucket}:{_client_ip_from_handler(handler)}"
    now_f = time.time()
    cutoff = now_f - max(1.0, API_RATE_LIMIT_WINDOW_SECONDS)
    with _api_rate_lock:
        arr = [x for x in _api_rate_buckets.get(key, []) if x >= cutoff]
        if len(arr) >= API_RATE_LIMIT_MAX_REQUESTS:
            _api_rate_buckets[key] = arr
            return False
        arr.append(now_f)
        _api_rate_buckets[key] = arr
        # v9.80: чистим устаревшие бакеты, а не "первые 1000 по словарю".
        # Прежняя реализация удаляла произвольных пользователей и реальная
        # защита размывалась — активные клиенты могли всё время выпадать из
        # окна. Теперь удаляем только бакеты, целиком вышедшие из окна.
        if len(_api_rate_buckets) > 5000:
            stale_keys = [k for k, v in _api_rate_buckets.items()
                          if not v or v[-1] < cutoff]
            for k in stale_keys[:2000]:
                _api_rate_buckets.pop(k, None)
        return True



def _storage_health() -> dict[str, Any]:
    info = {"storage": NOTIFY_STORAGE, "database_url": bool(DATABASE_URL), "ok": True, "error": ""}
    if NOTIFY_STORAGE not in {"sqlite", "postgres"}:
        return info
    try:
        _init_notify_storage()
        if NOTIFY_STORAGE == "postgres":
            with _notify_pg_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    cur.fetchone()
        else:
            with _notify_sqlite_conn() as conn:
                conn.execute("SELECT 1").fetchone()
    except Exception as exc:
        info["ok"] = False
        info["error"] = str(exc)[:200]
    return info


def _health_payload(ready: bool = False) -> dict[str, Any]:
    collector = _collector_state_copy()
    notify_state = dict(_notify_worker_state)
    storage = _storage_health() if ready else {"storage": NOTIFY_STORAGE, "database_url": bool(DATABASE_URL), "ok": True, "error": ""}
    now_i = now_ts()
    collector_age = now_i - _safe_int(collector.get("last_finish"), 0) if collector.get("last_finish") else None
    ok = bool(storage.get("ok"))
    if ready and COLLECTOR_ENABLED:
        warmup = now_i - APP_STARTED_AT < max(90, int(COLLECTOR_INTERVAL * 2))
        if not warmup:
            if collector.get("last_error"):
                ok = False
            if collector_age is None or collector_age > max(180, int(COLLECTOR_INTERVAL * 4)):
                ok = False
    return {
        "ok": ok,
        "time": now_i,
        "uptime": now_i - APP_STARTED_AT,
        "mode": DATA_MODE,
        "bot_token": bool(BOT_TOKEN),
        "storage": storage,
        "collector": collector,
        "notify": notify_state,
        "telegram_queue": {
            "memory": _telegram_send_queue.qsize(),
            "ok": _safe_int(_telegram_send_stats.get("ok"), 0),
            "fail": _safe_int(_telegram_send_stats.get("fail"), 0),
            "persisted": _safe_int(_telegram_send_stats.get("persisted"), 0),
            "loaded": _safe_int(_telegram_send_stats.get("loaded"), 0),
            "dropped": _safe_int(_telegram_send_stats.get("dropped"), 0),
            "last_error": str(_telegram_send_stats.get("last_error") or ""),
        },
        "online": _online_snapshot(include_users=False),
    }


def _admin_user_from_body(body: dict[str, Any] | None) -> dict[str, Any] | None:
    user = verify_init_data(str((body or {}).get("init_data") or ""))
    if not user or not _is_admin_id(user.get("id")):
        return None
    return user


def _admin_user_from_query(url: urllib.parse.ParseResult) -> dict[str, Any] | None:
    params = urllib.parse.parse_qs(url.query)
    user = verify_init_data(str((params.get("init_data") or [""])[0]))
    if not user or not _is_admin_id(user.get("id")):
        return None
    return user


class MiniAppHandler(BaseHTTPRequestHandler):
    server_version = "TelegramMiniApp/1.0"

    def log_message(self, fmt: str, *args: Any) -> None:
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {self.address_string()} - {fmt % args}")

    def do_GET(self) -> None:
        try:
            url = urllib.parse.urlparse(self.path)
            path = url.path

            if path in {"/health", "/healthz"}:
                return json_response(self, _health_payload(ready=False))

            if path == "/ready":
                payload = _health_payload(ready=True)
                return json_response(self, payload, status=200 if payload.get("ok") else 503)

            if path in {"/api/admin/status", "/api/admin/online-limit"}:
                return json_response(self, {"ok": False, "error": "use POST body"}, status=405)

            if path == "/api/live":
                if not _rate_limit_ok(self, "live"):
                    return json_response(self, {"ok": False, "error": "rate limit"}, status=429)
                params = urllib.parse.parse_qs(url.query)
                force = str((params.get("force") or ["0"])[0]).lower() in {"1", "true", "yes"}
                return json_response(self, load_live_payload(force=force))

            if path == "/api/match":
                if not _rate_limit_ok(self, "match"):
                    return json_response(self, {"ok": False, "error": "rate limit"}, status=429)
                params = urllib.parse.parse_qs(url.query)
                mid = str((params.get("id") or [""])[0]).strip()
                if not mid:
                    return json_response(self, {"ok": False, "error": "missing id"}, status=400)
                return json_response(self, detail_payload(mid))

            if path == "/api/match/avg":
                # v9.80: раньше эндпоинт не имел rate-limit, хотя под капотом
                # дёргает team_recent на IGScore. Теперь — общий бакет с /api/match.
                if not _rate_limit_ok(self, "match"):
                    return json_response(self, {"ok": False, "error": "rate limit"}, status=429)
                params = urllib.parse.parse_qs(url.query)
                mid = str((params.get("id") or [""])[0]).strip()
                if not mid:
                    return json_response(self, {"ok": False, "error": "missing id"}, status=400)
                return json_response(self, avg_payload_for_match(mid))

            if path == "/api/debug/league-assets":
                if not DEBUG_ENDPOINTS_ENABLED:
                    return json_response(self, {"ok": False, "error": "debug endpoints disabled"}, status=404)
                user = _admin_user_from_query(url)
                if not user:
                    return json_response(self, {"ok": False, "error": "access denied"}, status=403)
                params = urllib.parse.parse_qs(url.query)
                limit = _safe_int((params.get("limit") or ["80"])[0], 80)
                return json_response(self, debug_league_assets(limit=limit))

            if path == "/api/debug/odds-raw":
                if not DEBUG_ENDPOINTS_ENABLED:
                    return json_response(self, {"ok": False, "error": "debug endpoints disabled"}, status=404)
                user = _admin_user_from_query(url)
                if not user:
                    return json_response(self, {"ok": False, "error": "access denied"}, status=403)
                # Debug: returns the raw /v1/football/match/odds/last response
                # so you can see exactly what IGScore sends.
                params = urllib.parse.parse_qs(url.query)
                mid = str((params.get("id") or [""])[0]).strip()
                if not mid:
                    return json_response(self, {"ok": False, "error": "missing id"}, status=400)
                try:
                    raw = match_odds_last(mid)
                    parsed = parse_odds_response(raw, match_id=mid)
                    return json_response(self, {"ok": True, "raw": raw, "parsed": parsed})
                except Exception as exc:
                    return json_response(self, {"ok": False, "error": str(exc)})

            if path == "/api/notify/matches":
                return json_response(self, {"ok": False, "error": "use POST body"}, status=405)

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
            # v9.80: защита от выхода за пределы static/ через симлинки.
            # `..` уже отсеян выше, но symlink внутри static/ мог увести наружу.
            try:
                resolved = file_path.resolve()
                static_root = STATIC_DIR.resolve()
                if not str(resolved).startswith(str(static_root) + os.sep) and resolved != static_root:
                    return text_response(self, "Forbidden", status=403)
            except Exception:
                return text_response(self, "Forbidden", status=403)
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
            # v9.80: мягкая проверка Content-Type. Принимаем только JSON;
            # отсутствие заголовка считаем за JSON для совместимости со
            # старыми клиентами, но любой другой тип — отбиваем.
            ctype = (self.headers.get("Content-Type") or "").split(";", 1)[0].strip().lower()
            if ctype and ctype != "application/json":
                return json_response(self, {"ok": False, "error": "expected application/json"}, status=415)
            body = parse_json_body(self)
            if path == "/api/online/checkin":
                payload = handle_online_checkin(body, self)
                return json_response(self, payload, status=200 if payload.get("allowed") else 429)
            if path == "/api/admin/status":
                user = _admin_user_from_body(body)
                if not user:
                    return json_response(self, {"ok": False, "error": "access denied"}, status=403)
                return json_response(self, {"ok": True, "stats": _admin_stats_snapshot(), "collector": _collector_state_copy(), "notify": dict(_notify_worker_state), "online": _online_snapshot(include_users=True), "telegram_jobs": _telegram_job_counts()})
            if path == "/api/admin/online-limit":
                user = _admin_user_from_body(body)
                if not user:
                    return json_response(self, {"ok": False, "error": "access denied"}, status=403)
                if "limit" not in (body or {}):
                    return json_response(self, {"ok": True, **_online_snapshot(include_users=True)})
                limit = _online_save_limit((body or {}).get("limit"), user.get("id"))
                _admin_log("online_limit_set", limit, user.get("id"))
                return json_response(self, {"ok": True, **_online_snapshot(include_users=True)})
            if path == "/api/subscribe":
                return json_response(self, handle_subscribe(body))
            if path == "/api/notify":
                return json_response(self, handle_notify(body))
            if path == "/api/notify/matches":
                return json_response(self, handle_notify_matches(str((body or {}).get("init_data") or "")))
            if path == "/api/goal-subscribe":
                return json_response(self, handle_goal_subscribe(body))
            if path == "/api/goal-wait-subscribe":
                return json_response(self, handle_goal_wait_subscribe(body))
            if path == "/api/notify/clear":
                return json_response(self, handle_notify_matches_clear(body))
            if path == "/api/notify/remove":
                return json_response(self, handle_notify_match_remove(body))
            return json_response(self, {"ok": False, "error": "unknown endpoint"}, status=404)
        except Exception as exc:
            traceback.print_exc()
            return json_response(self, {"ok": False, "error": f"{type(exc).__name__}: {exc}"}, status=500)


def main() -> None:
    DATA_DIR.mkdir(exist_ok=True)
    TEAM_LOGO_DIR.mkdir(exist_ok=True)
    _init_logo_db()
    _init_notify_storage()
    _init_admin_storage()
    _load_admin_state()
    start_collector_worker()
    _load_notify_subs()
    _load_notify_matches()
    cleanup_runtime_caches(force=True)
    # The worker stores found matches for the app and queues Telegram push when BOT_TOKEN is set.
    start_telegram_send_workers()
    start_notify_worker()
    start_admin_panel_worker()
    if BOT_TOKEN:
        # v9.80: не логируем хвост токена в STDOUT (на Render это попадает в журналы).
        print("[notify] worker started with BOT_TOKEN (configured)")
        if not BOT_USERNAME:
            # v9.80: notification_match_link молча возвращает "" без BOT_USERNAME —
            # кнопка "Открыть" в Telegram-сообщениях пропадает.
            print("[notify] WARNING: BOT_USERNAME is empty; notification links will be missing the inline button")
    else:
        print("[notify] BOT_TOKEN not set — Telegram push disabled, in-app/local storage only")
    server = ThreadingHTTPServer((HOST, PORT), MiniAppHandler)
    print("=" * 72)
    print("Telegram Live Matches Mini App — v9.81 stability patch (per-row save, SIGTERM, plan starter)")
    print(f"Local:    http://127.0.0.1:{PORT}")
    print(f"Host:     {HOST}:{PORT}")
    print(f"Mode:     {DATA_MODE}")
    print(f"Notify:   {'enabled (' + str(len(_notify_subs)) + ' subs)' if BOT_TOKEN else 'disabled (no BOT_TOKEN)'}")
    print(f"Storage:  notify={NOTIFY_STORAGE} live_cache=sqlite")
    print(f"Online limit: {_online_load_limit()} users, ttl={ONLINE_USER_TTL_SECONDS}s")
    # v9.80: видно сразу при старте, кто админ и какие воркеры включены.
    print(f"Admins:   {sorted(ADMIN_IDS) if ADMIN_IDS else '[]'} (panel={'on' if ADMIN_PANEL_ENABLED else 'off'}, polling={'on' if ADMIN_POLLING_ENABLED else 'off'})")
    print(f"Collector: {'on' if COLLECTOR_ENABLED else 'off'} (interval={COLLECTOR_INTERVAL}s, max={COLLECTOR_MAX_MATCHES})")
    print(f"Rate-limit: {API_RATE_LIMIT_MAX_REQUESTS}/{API_RATE_LIMIT_WINDOW_SECONDS}s")
    print("=" * 72)

    # v9.81: Render шлёт SIGTERM перед kill (обычно за 30 секунд).
    # Раньше main() ловил только KeyboardInterrupt → SIGTERM убивал процесс
    # на середине цикла, daemon-потоки обрывались, ~10-20 сообщений из
    # _telegram_send_queue терялись и не доставлялись пользователям.
    import signal
    def _on_sigterm(signum, _frame) -> None:
        print(f"\n[main] signal {signum} received, shutting down")
        # ThreadingHTTPServer.serve_forever реагирует на shutdown()
        # быстрее чем на raise — но shutdown нельзя звать из обработчика
        # сигнала того же потока, поэтому используем простой raise.
        raise KeyboardInterrupt
    try:
        signal.signal(signal.SIGTERM, _on_sigterm)
    except Exception:
        # На Windows и в некоторых embed-окружениях SIGTERM недоступен.
        pass

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped, draining telegram queue...")
    finally:
        # v9.81: даём sender-воркеру дослать накопленные сообщения.
        # 5 секунд хватает, чтобы добить очередь среднего размера (~20 шт);
        # большего не ждём, чтобы Render не убил процесс по таймауту.
        try:
            deadline = time.time() + 5.0
            while time.time() < deadline and _telegram_send_queue.qsize() > 0:
                time.sleep(0.2)
            remaining = _telegram_send_queue.qsize()
            if remaining:
                print(f"[main] {remaining} messages still queued (will resume on next start from persistent jobs)")
            else:
                print("[main] telegram queue drained")
        except Exception:
            pass
        server.server_close()


if __name__ == "__main__":
    main()
