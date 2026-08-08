#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Scores24 editorial prediction collector.

Public Scores24 pages are read without bypassing access controls. The collector
stores every discovered prediction permanently and refreshes unsettled matches.
"""
from __future__ import annotations

import hashlib
import difflib
import html as html_lib
import json
import os
import re
import threading
import time
import urllib.error
import urllib.parse
import http.cookiejar
import urllib.request
import unicodedata
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

import livezot_postgres
from zoneinfo import ZoneInfo

BASE_URL = os.environ.get("SCORES24_BASE_URL", "https://scores24.live").rstrip("/")
LANG = (os.environ.get("SCORES24_LANG", "ru") or "ru").strip().lower()
TIMEZONE_NAME = (os.environ.get("APP_TIMEZONE", "Europe/Moscow") or "Europe/Moscow").strip()
REQUEST_TIMEOUT = max(8, int(os.environ.get("SCORES24_TIMEOUT", "25")))
REFRESH_SECONDS = max(300, int(os.environ.get("SCORES24_REFRESH_SECONDS", "300")))
LIVE_REFRESH_SECONDS = max(45, int(os.environ.get("SCORES24_LIVE_REFRESH_SECONDS", "90")))
STATUS_WORKER_SECONDS = max(45, int(os.environ.get("SCORES24_STATUS_WORKER_SECONDS", "90")))
MIN_ODD = 1.0
MAX_ODD = 1000.0
MAX_PAGES_PER_SCAN = max(1, min(40, int(os.environ.get("SCORES24_MAX_PAGES_PER_SCAN", "8"))))
REQUEST_GAP_SECONDS = max(0.25, min(5.0, float(os.environ.get("SCORES24_REQUEST_GAP_SECONDS", "1.25"))))
RATE_LIMIT_BACKOFF_SECONDS = max(300, int(os.environ.get("SCORES24_RATE_LIMIT_BACKOFF_SECONDS", "300")))
INDEX_REFRESH_SECONDS = max(900, int(os.environ.get("SCORES24_INDEX_REFRESH_SECONDS", "1800")))
MAX_DISCOVERED_URLS = max(50, min(1000, int(os.environ.get("SCORES24_MAX_DISCOVERED_URLS", "500"))))
CATEGORY_REFRESH_SECONDS = max(180, int(os.environ.get("SCORES24_CATEGORY_REFRESH_SECONDS", "180")))
CATEGORY_BATCH_SIZE = max(1, min(10, int(os.environ.get("SCORES24_CATEGORY_BATCH_SIZE", "3"))))
LEAGUE_REFRESH_SECONDS = max(900, int(os.environ.get("SCORES24_LEAGUE_REFRESH_SECONDS", "1800")))
LEAGUE_BATCH_SIZE = max(1, min(12, int(os.environ.get("SCORES24_LEAGUE_BATCH_SIZE", "4"))))
MAX_DISCOVERED_LEAGUES = max(20, min(500, int(os.environ.get("SCORES24_MAX_DISCOVERED_LEAGUES", "250"))))
HISTORY_LOOKBACK_DAYS = max(1, min(31, int(os.environ.get("SCORES24_HISTORY_LOOKBACK_DAYS", "7"))))
DISCOVERY_REFRESH_SECONDS = max(180, int(os.environ.get("SCORES24_DISCOVERY_REFRESH_SECONDS", "180")))
GRAPHQL_URL = os.environ.get("SCORES24_GRAPHQL_URL", f"{BASE_URL}/graphql").strip()
GRAPHQL_CACHE_SECONDS = max(30, min(170, int(os.environ.get("SCORES24_GRAPHQL_CACHE_SECONDS", "120"))))
GRAPHQL_LEAGUE_BATCH_SIZE = max(1, min(20, int(os.environ.get("SCORES24_GRAPHQL_LEAGUE_BATCH_SIZE", "8"))))
GRAPHQL_MAX_FIRST = max(50, min(1000, int(os.environ.get("SCORES24_GRAPHQL_MAX_FIRST", "500"))))
GRAPHQL_MAX_PAGES = max(1, min(20, int(os.environ.get("SCORES24_GRAPHQL_MAX_PAGES", "8"))))
GRAPHQL_COOKIE = (os.environ.get("SCORES24_COOKIE", "") or "").strip()
GRAPHQL_TIMEZONE_OFFSET = int(os.environ.get("SCORES24_TIMEZONE_OFFSET", "300") or 300)
SCORE_FALLBACK_REFRESH_SECONDS = max(3600, int(os.environ.get("SCORES24_SCORE_FALLBACK_REFRESH_SECONDS", "3600")))
FINAL_DETAILS_REFRESH_SECONDS = max(60, int(os.environ.get("SCORES24_FINAL_DETAILS_REFRESH_SECONDS", "120")))
MAX_RESULT_PAGES_PER_SCAN = max(1, min(40, int(os.environ.get("SCORES24_MAX_RESULT_PAGES_PER_SCAN", "12"))))
MAX_API_HISTORY_DATES_PER_SCAN = max(2, min(12, int(os.environ.get("SCORES24_API_HISTORY_DATES_PER_SCAN", "4"))))
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
_storage_merge_lock = threading.RLock()
_request_lock = threading.Lock()
_fetch_cache: dict[str, tuple[float, str]] = {}
_graphql_cache: dict[str, tuple[float, dict[str, Any]]] = {}
_graphql_cookie_jar = http.cookiejar.CookieJar()
_graphql_opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(_graphql_cookie_jar))
_graphql_session_bootstrapped = False
_graphql_last_http_status = 0
_graphql_last_success_at = 0
_graphql_last_error = ""
_graphql_last_leagues = 0
_graphql_last_predictions = 0
_graphql_last_secondary_filtered = 0
_next_request_at = 0.0
_blocked_until = 0.0
_category_cursor = 0
_league_cursor = 0
_discovered_league_urls: list[str] = []
_started = False
_stop = threading.Event()
_data_dir = Path(os.environ.get("SCORES24_DATA_DIR", "")).expanduser() if os.environ.get("SCORES24_DATA_DIR") else None
_database_url = ""
_STORAGE_SOURCE = "scores24"
_DATABASE_HISTORY_RESTORE_SECONDS = 60
_api_fixture_resolver: Callable[[dict[str, Any], bool], dict[str, Any] | None] | None = None
_api_statistics_loader: Callable[[int, bool], list[dict[str, Any]]] | None = None
_igscore_snapshot_loader: Callable[[dict[str, Any], dict[str, Any] | None, bool], dict[str, Any] | None] | None = None
_date_migration_done = False
_database_date_restore_lock = threading.RLock()
_database_date_restore_at: dict[str, float] = {}
_last_discovery_at = 0
_last_score_fallback_at = 0

_FINISHED_STATUSES = {"FT", "AET", "PEN"}
_LIVE_STATUSES = {"1H", "HT", "2H", "ET", "BT", "P", "LIVE", "INT"}
_CANCELLED_STATUSES = {"CANC", "ABD", "AWD", "WO"}
_POSTPONED_STATUSES = {"PST", "SUSP"}
_TERMINAL_STATUSES = _FINISHED_STATUSES | _CANCELLED_STATUSES
_STAT_MARKETS = {"corners", "yellow_cards", "red_cards"}

# Prediction league policy requested for LiveZOT:
#   * every senior UEFA men's and women's club competition;
#   * the senior men's top domestic division of each country.
# National cups, lower divisions, friendlies, youth/reserve leagues and
# domestic women's competitions are excluded.
_DOMESTIC_TOP_DIVISION_SLUGS = {
    # Europe
    "albania-kategoria-superiore", "andorra-primera-divisio", "armenia-premier-league",
    "austria-bundesliga", "azerbaijan-premier-league", "belarus-vysshaya-liga",
    "belgium-first-division-a", "belgium-pro-league", "bosnia-herzegovina-premier-league",
    "bulgaria-first-professional-league", "croatia-1-hnl", "croatia-hnl",
    "cyprus-1st-division", "cyprus-first-division", "czech-republic-1-liga",
    "denmark-superligaen", "england-premier-league", "estonia-premium-liiga",
    "faroe-islands-premier-league", "finland-veikkausliiga", "france-ligue-1",
    "georgia-erovnuli-liga", "germany-bundesliga", "gibraltar-national-league",
    "gibraltar-premier-division", "greece-super-league", "greece-super-league-1",
    "hungary-nb-i", "iceland-urvalsdeild", "israel-premier-league", "italy-serie-a",
    "kazakhstan-premier-league", "kosovo-superliga", "latvia-virsliga",
    "lithuania-a-lyga", "luxembourg-national-division", "malta-premier-league",
    "moldova-divizia-nationala", "moldova-super-liga", "montenegro-1-cfl",
    "netherlands-eredivisie", "north-macedonia-first-league",
    "northern-ireland-premiership", "norway-eliteserien", "poland-ekstraklasa",
    "portugal-primeira-liga", "portugal-primeira-liga-1",
    "republic-of-ireland-premier-division", "romania-liga-i", "russia-premier-league",
    "san-marino-campionato-sammarinese", "scotland-premiership", "serbia-superliga",
    "slovakia-super-liga", "slovenia-prva-liga", "spain-laliga",
    "spain-primera-division", "sweden-allsvenskan", "switzerland-super-league",
    "turkey-super-lig", "ukraine-premier-league", "wales-premier-league",
    # North, Central and South America
    "argentina-primera-division", "division-profesional", "bolivia-division-profesional",
    "brazil-brasileiro-serie-a", "chile-primera-division", "colombia-primera-a",
    "costa-rica-primera-division", "ecuador-serie-a", "el-salvador-primera-division",
    "guatemala-liga-nacional", "honduras-liga-nacional", "mexico-liga-mx",
    "mexico-liga-mx-apertura", "mexico-liga-mx-clausura", "nicaragua-primera-division",
    "panama-lpf", "paraguay-primera-division", "peru-primera-division",
    "uruguay-primera-division", "venezuela-primera-division", "usa-major-league-soccer",
    "usa-mls", "united-states-major-league-soccer", "united-states-mls",
    "united-states-of-america-major-league-soccer", "us-major-league-soccer",
    "canadian-premiere-league",
    # Asia and Oceania
    "australia-a-league", "china-chinese-super-league", "hong-kong-premier-league",
    "india-indian-super-league", "indonesia-liga-1", "japan-j-league",
    "south-korea-k-league-1", "south-korea-k-league-classic", "malaysia-super-league",
    "new-zealand-national-league", "philippines-football-league", "singapore-premier-league",
    "thailand-thai-league-1", "vietnam-v-league", "uzbekistan-pfl",
    "saudi-arabia-saudi-professional-league", "qatar-stars-league",
    "united-arab-emirates-pro-league", "united-arab-emirates-arabian-gulf-league",
    "iran-pro-league", "iran-persian-gulf-pro-league", "iraq-premier-league",
    "jordan-pro-league", "bahrain-premier-league", "kuwait-premier-league",
    "oman-professional-league", "lebanon-premier-league",
    # Africa
    "algeria-ligue-1", "angola-girabola", "cameroon-elite-one", "egypt-premier-league",
    "ghana-premier-league", "ivory-coast-ligue-1", "kenya-premier-league",
    "morocco-botola", "nigeria-premier-league", "rwanda-premier-league",
    "senegal-premier-league", "south-africa-premier-soccer-league", "tanzania-premier-league",
    "tunisia-ligue-1", "uganda-premier-league", "zambia-super-league",
    "zimbabwe-premier-soccer-league",
}

# Exact league-name fallbacks for Scores24 aliases. Keys are Scores24 country
# slugs; values are normalized Russian/English names of the top men's division.
_DOMESTIC_TOP_DIVISION_NAMES = {
    "albania": {"категория супериор", "kategoria superiore"},
    "andorra": {"примера дивизио", "primera divisio"},
    "armenia": {"премьер лига", "premier league"},
    "austria": {"бундеслига", "bundesliga"},
    "azerbaijan": {"премьер лига", "premier league"},
    "belarus": {"высшая лига", "vysshaya liga"},
    "belgium": {"высшая лига", "про лига", "first division a", "pro league"},
    "bosnia-herzegovina": {"премьер лига", "premier league"},
    "bulgaria": {"первая лига", "first professional league"},
    "croatia": {"хнл", "1 hnl", "hnl"},
    "cyprus": {"первый дивизион", "first division"},
    "czech-republic": {"первая лига", "1 liga", "first league"},
    "denmark": {"суперлига", "superligaen"},
    "england": {"премьер лига", "premier league"},
    "estonia": {"премиум лига", "premium liiga"},
    "faroe-islands": {"премьер лига", "premier league"},
    "finland": {"вейккауслига", "veikkausliiga"},
    "france": {"лига 1", "ligue 1"},
    "georgia": {"эровнули лига", "erovnuli liga"},
    "germany": {"бундеслига", "bundesliga"},
    "gibraltar": {"национальная лига", "national league", "premier division"},
    "greece": {"суперлига", "super league"},
    "hungary": {"высшая лига", "nb i"},
    "iceland": {"урвалсдейлд", "urvalsdeild"},
    "israel": {"премьер лига", "premier league"},
    "italy": {"серия а", "serie a"},
    "kazakhstan": {"премьер лига", "premier league"},
    "kosovo": {"суперлига", "superliga"},
    "latvia": {"вирслига", "virsliga"},
    "lithuania": {"а лига", "a lyga"},
    "luxembourg": {"национальный дивизион", "national division"},
    "malta": {"премьер лига", "premier league"},
    "moldova": {"суперлига", "национальный дивизион", "super liga", "divizia nationala"},
    "montenegro": {"первая лига", "1 cfl"},
    "netherlands": {"эредивизи", "eredivisie"},
    "north-macedonia": {"первая лига", "first league"},
    "northern-ireland": {"премьершип", "premiership"},
    "norway": {"элитсерия", "eliteserien"},
    "poland": {"экстракласа", "ekstraklasa"},
    "portugal": {"примейра лига", "primeira liga"},
    "republic-of-ireland": {"премьер дивизион", "premier division"},
    "romania": {"лига 1", "liga i", "superliga"},
    "russia": {"премьер лига", "premier league"},
    "san-marino": {"чемпионат сан марино", "campionato sammarinese"},
    "scotland": {"премьершип", "premiership"},
    "serbia": {"суперлига", "superliga"},
    "slovakia": {"суперлига", "super liga"},
    "slovenia": {"первая лига", "prva liga"},
    "spain": {"ла лига", "laliga", "примера дивизион", "primera division"},
    "sweden": {"аллсвенскан", "allsvenskan"},
    "switzerland": {"суперлига", "super league"},
    "turkey": {"суперлига", "super lig"},
    "ukraine": {"премьер лига", "premier league"},
    "wales": {"премьер лига", "premier league"},
    "argentina": {"премьер дивизион", "primera division"},
    "bolivia": {"профессиональный дивизион", "division profesional"},
    "brazil": {"серия а", "serie a"},
    "chile": {"примера дивизион", "primera division"},
    "colombia": {"примера а", "primera a"},
    "costa-rica": {"высший дивизион", "примера дивизион", "primera division"},
    "ecuador": {"серия а", "serie a"},
    "el-salvador": {"примера дивизион", "primera division"},
    "guatemala": {"национальная лига", "liga nacional"},
    "honduras": {"национальная лига", "liga nacional"},
    "mexico": {"лига mx", "liga mx", "апертура", "клаусура"},
    "nicaragua": {"примера дивизион", "primera division"},
    "panama": {"лпф", "lpf"},
    "paraguay": {"премьер дивизион", "primera division"},
    "peru": {"примера", "лига 1", "primera", "liga 1"},
    "uruguay": {"примера дивизион", "primera division"},
    "venezuela": {"примера", "primera division"},
    "usa": {"главная футбольная лига", "major league soccer", "mls"},
    "canada": {"премьер лига", "premier league"},
    "australia": {"а лига", "a league"},
    "china": {"суперлига", "chinese super league"},
    "hong-kong": {"премьер лига", "premier league"},
    "india": {"индийская суперлига", "indian super league"},
    "indonesia": {"лига 1", "liga 1"},
    "japan": {"джей лига 1", "j league 1", "j1 league"},
    "south-korea": {"к лига 1", "k league 1", "k league classic"},
    "malaysia": {"суперлига", "super league"},
    "new-zealand": {"национальная лига", "national league"},
    "philippines": {"футбольная лига", "football league"},
    "singapore": {"премьер лига", "premier league"},
    "thailand": {"тай лига 1", "thai league 1"},
    "vietnam": {"в лига 1", "v league 1"},
    "uzbekistan": {"суперлига", "super league"},
    "saudi-arabia": {"профессиональная лига", "saudi professional league"},
    "qatar": {"лига звезд", "stars league"},
    "united-arab-emirates": {"про лига", "pro league", "arabian gulf league"},
    "iran": {"про лига", "persian gulf pro league"},
    "iraq": {"премьер лига", "premier league"},
    "jordan": {"про лига", "pro league"},
    "bahrain": {"премьер лига", "premier league"},
    "kuwait": {"премьер лига", "premier league"},
    "oman": {"профессиональная лига", "professional league"},
    "lebanon": {"премьер лига", "premier league"},
    "algeria": {"лига 1", "ligue 1"},
    "angola": {"жирабола", "girabola"},
    "cameroon": {"элит уан", "elite one"},
    "egypt": {"премьер лига", "premier league"},
    "ghana": {"премьер лига", "premier league"},
    "ivory-coast": {"лига 1", "ligue 1"},
    "kenya": {"премьер лига", "premier league"},
    "morocco": {"ботола", "botola"},
    "nigeria": {"премьер лига", "premier league"},
    "rwanda": {"премьер лига", "premier league"},
    "senegal": {"премьер лига", "premier league"},
    "south-africa": {"премьер футбольная лига", "premier soccer league"},
    "tanzania": {"премьер лига", "premier league"},
    "tunisia": {"лига 1", "ligue 1"},
    "uganda": {"премьер лига", "premier league"},
    "zambia": {"суперлига", "super league"},
    "zimbabwe": {"премьер футбольная лига", "premier soccer league"},
}

# Scores24 occasionally changes prefixes (for example ``international-clubs-``)
# while preserving the UEFA competition part. These markers intentionally do
# not include youth or national-team competitions.
_UEFA_CLUB_COMPETITION_MARKERS = (
    "uefa-champions-league", "uefa-europa-league", "uefa-europa-conference-league",
    "europa-conference-league", "uefa-super-cup", "uefa-women-s-champions-league",
    "uefa-womens-champions-league", "women-s-champions-league", "womens-champions-league",
    "uefa-women-s-europa-cup", "uefa-womens-europa-cup", "women-s-europa-cup",
    "womens-europa-cup",
)

# Direct-query aliases are deliberately limited to UEFA competitions. Adding
# every domestic league here would create hundreds of unnecessary requests;
# domestic top divisions are taken from the complete GraphQL catalogue.
_SELECTED_QUERY_LEAGUE_SLUGS = {
    "international-clubs-uefa-champions-league", "uefa-champions-league",
    "international-clubs-uefa-europa-league", "uefa-europa-league",
    "uefa-europa-conference-league", "international-clubs-uefa-europa-conference-league",
    "uefa-super-cup", "international-clubs-uefa-super-cup",
    "uefa-women-s-champions-league", "international-clubs-uefa-women-s-champions-league",
    "uefa-womens-champions-league", "uefa-women-s-europa-cup",
    "international-clubs-uefa-women-s-europa-cup", "uefa-womens-europa-cup",
}

_COUNTRY_SLUG_ALIASES = {
    # Scores24 uses more than one country slug for the United States depending
    # on the GraphQL catalogue/locale. Keep them on one canonical key so MLS
    # is always recognized as the US top division.
    "united-states": "usa",
    "united-states-of-america": "usa",
    "united-states-america": "usa",
    "us": "usa",
    "u-s-a": "usa",
}

_REGIONAL_COUNTRY_SLUGS = {
    "", "world", "europe", "asia", "africa", "oceania", "south-america",
    "north-central-america", "international", "international-clubs",
}

_DOMESTIC_EXCLUDED_MARKERS = (
    "friendly", "friendlies", "club-friendly", "international-friendly",
    "cup", "copa", "coppa", "pokal", "trophy", "shield", "super-cup", "supercup",
    "women", "woman", "female", "femenina", "feminine", "женщ",
    "youth", "junior", "u17", "u18", "u19", "u20", "u21", "u23", "юнош", "молод",
    "reserve", "reserves", "academy", "резерв", "development",
    "second", "2nd", "third", "3rd", "serie-b", "serie-c", "primera-b",
    "challenge-league", "championship", "superettan", "eerste-divisie", "erste-liga",
    "persha-liga", "pervaya-liga", "first-division-0", "k-league-challenge",
    "national-league-cup", "league-cup", "кубок", "товарищ",
)

# Safe generic suffixes for countries whose exact Scores24 alias changes.
_GENERIC_TOP_DIVISION_SUFFIXES = (
    "premier-league", "premier-division", "primera-division", "super-league",
    "superliga", "super-liga", "serie-a", "liga-1", "liga-i", "bundesliga",
    "eredivisie", "major-league-soccer", "professional-league", "pro-league",
    "national-division", "national-league", "a-league", "j-league",
)

_graphql_last_catalog_total = 0
_graphql_last_filtered_out = 0
SCORES24_BUILD_ID = "SCORES24_PRIMARY_EDITORIAL_ONLY_V37"
SETTLEMENT_RULE_VERSION = 2
IGSCORE_STATUS_RULE_VERSION = 2
MATCH_DISCOVERY_RETRY_SECONDS = max(900, int(os.environ.get("SCORES24_MATCH_DISCOVERY_RETRY_SECONDS", "1800")))
POSTPONED_RECHECK_SECONDS = max(3600, int(os.environ.get("SCORES24_POSTPONED_RECHECK_SECONDS", "21600")))
WORKER_STALE_SECONDS = max(1200, int(os.environ.get("SCORES24_WORKER_STALE_SECONDS", str(REFRESH_SECONDS * 3))))
WORKER_FAILURE_THRESHOLD = max(2, int(os.environ.get("SCORES24_WORKER_FAILURE_THRESHOLD", "3")))
_HEALTH_STATE_KEY = "scores24_worker_health_v2"
_health_lock = threading.RLock()
_health_state: dict[str, Any] = {
    "worker_started": False,
    "worker_started_at": 0,
    "heartbeat_at": 0,
    "cycle_started_at": 0,
    "cycle_finished_at": 0,
    "last_success_at": 0,
    "last_error_at": 0,
    "last_error": "",
    "consecutive_failures": 0,
    "current_action": "idle",
    "last_feed_total": 0,
    "last_checked_dates": 0,
    "last_updated_dates": 0,
    "last_cycle_seconds": 0.0,
}


def configure(*, data_dir: Path, pg_conn_factory: Callable[[], Any] | None = None, database_url: str = "") -> None:
    """Configure PostgreSQL-only storage.

    ``data_dir`` is kept for compatibility with the frozen application wrapper,
    but prediction history is never written to local files.
    """
    global _data_dir, _database_url
    _data_dir = Path(data_dir)
    _data_dir.mkdir(parents=True, exist_ok=True)
    _database_url = str(database_url or "")
    livezot_postgres.configure(database_url=_database_url, connection_factory=pg_conn_factory)
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


def configure_igscore(
    *,
    snapshot_loader: Callable[[dict[str, Any], dict[str, Any] | None, bool], dict[str, Any] | None],
) -> None:
    """Attach IGScore as the LIVE statistics/events provider.

    The callback is executed only by the background worker.  It receives the
    saved Scores24 row, the latest API-Football fixture card when available,
    and a force flag.  It returns one normalized snapshot that is persisted in
    PostgreSQL; opening the Mini App never calls IGScore directly.
    """
    global _igscore_snapshot_loader
    _igscore_snapshot_loader = snapshot_loader


def _api_football_enabled() -> bool:
    return _api_fixture_resolver is not None


def _igscore_enabled() -> bool:
    return _igscore_snapshot_loader is not None


def _final_snapshot_saved(row: dict[str, Any]) -> bool:
    # In the IGScore build a final card is frozen only after the IGScore final
    # snapshot (or the provider-neutral marker) has been persisted. Legacy
    # API-Football final flags are honoured only when IGScore is disabled, so
    # old database rows are automatically upgraded instead of keeping missing
    # statistics forever.
    if row.get("final_snapshot_saved") or row.get("igscore_final_snapshot_saved"):
        return True
    return bool(row.get("api_football_final_snapshot_saved")) and not _igscore_enabled()


def _row_needs_event_audit(row: dict[str, Any]) -> bool:
    """Return True when a finished saved card is missing goal timeline events."""
    fixture_item = row.get("fixture_item") if isinstance(row.get("fixture_item"), dict) else {}
    fixture = fixture_item.get("fixture") if isinstance(fixture_item.get("fixture"), dict) else {}
    status = str(((fixture.get("status") or {}).get("short") or "")).upper()
    if status not in {"FT", "AET", "PEN"}:
        return False
    goals = fixture_item.get("goals") if isinstance(fixture_item.get("goals"), dict) else {}
    try:
        total_goals = int(goals.get("home") or 0) + int(goals.get("away") or 0)
    except Exception:
        total_goals = 0
    if total_goals <= 0 or status == "PEN":
        return False
    events = fixture_item.get("events") if isinstance(fixture_item.get("events"), list) else []
    goal_events = sum(1 for event in events if isinstance(event, dict) and str(event.get("type") or "").lower() == "goal")
    return goal_events < total_goals


def _health_update(*, persist: bool = False, **changes: Any) -> dict[str, Any]:
    with _health_lock:
        _health_state.update(changes)
        snapshot = dict(_health_state)
    if persist:
        try:
            livezot_postgres.upsert_service_state(_HEALTH_STATE_KEY, snapshot)
        except Exception:
            # PostgreSQL health is reported separately; telemetry must never stop
            # the prediction worker itself.
            pass
    return snapshot


def _health_snapshot(*, read_shared: bool = True) -> dict[str, Any]:
    with _health_lock:
        local = dict(_health_state)
    shared: dict[str, Any] = {}
    if read_shared:
        try:
            record = livezot_postgres.get_service_state(_HEALTH_STATE_KEY) or {}
            payload = record.get("payload") or {}
            if isinstance(payload, dict):
                shared = dict(payload)
                shared["shared_updated_at"] = int(record.get("updated_at") or 0)
        except Exception:
            shared = {}
    # A standby web process has no local worker. Prefer the newest PostgreSQL
    # heartbeat written by the Redis leader.
    local_heartbeat = int(local.get("heartbeat_at") or 0)
    shared_heartbeat = int(shared.get("heartbeat_at") or 0)
    return shared if shared_heartbeat > local_heartbeat else local


def _status_short_from_row(row: dict[str, Any]) -> str:
    return str(((((row.get("fixture_item") or {}).get("fixture") or {}).get("status") or {}).get("short") or "")).upper()


def _lifecycle_state(row: dict[str, Any]) -> str:
    fixture_id = int(row.get("api_football_fixture_id") or 0)
    igscore_id = str(row.get("igscore_match_id") or "").strip()
    status = _status_short_from_row(row)
    if not fixture_id and not igscore_id:
        return "UNMATCHED"
    if status in _CANCELLED_STATUSES:
        return "CANCELLED"
    if status in _FINISHED_STATUSES:
        return "FINALIZED" if _final_snapshot_saved(row) else "FINAL_PENDING"
    if status in _POSTPONED_STATUSES:
        return "POSTPONED"
    if status in _LIVE_STATUSES:
        return "LIVE"
    if row.get("api_football_initial_snapshot_saved") or igscore_id:
        return "PREMATCH_SAVED"
    return "PREMATCH_NEW"


def _storage_mode() -> str:
    return "postgres"


def _storage_init() -> None:
    livezot_postgres.initialize()


def _storage_get(date_text: str) -> dict[str, Any] | None:
    if str(date_text or "") < HISTORY_START_DATE:
        return None
    try:
        record = livezot_postgres.get_prediction_day(_STORAGE_SOURCE, date_text)
        if not record:
            return None
        payload = record.get("feed") or record.get("summary") or {}
        if not isinstance(payload, dict):
            return None
        payload = dict(payload)
        payload["is_final"] = bool(record.get("is_final"))
        payload["updated_at"] = int(record.get("updated_at") or payload.get("updated_at") or 0)
        return payload
    except Exception as exc:
        print(f"[scores24] PostgreSQL get failed date={date_text}: {exc}")
        return None


def _storage_records_full() -> list[dict[str, Any]]:
    """Return saved payloads from PostgreSQL from the configured history start."""
    try:
        rows = livezot_postgres.list_prediction_days(
            _STORAGE_SOURCE,
            from_date=HISTORY_START_DATE,
            descending=False,
        )
    except Exception as exc:
        print(f"[scores24] PostgreSQL records failed: {exc}")
        return []
    out: list[dict[str, Any]] = []
    for row in rows:
        payload = row.get("feed") or row.get("summary") or {}
        if not isinstance(payload, dict):
            payload = {}
        payload = dict(payload)
        payload["date"] = str(row.get("date") or "")
        payload["is_final"] = bool(row.get("is_final"))
        payload["updated_at"] = int(row.get("updated_at") or payload.get("updated_at") or 0)
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
        # Rebuild the day summary from the full saved feed so databases created
        # by the former admin-selection mode immediately report all forecasts.
        stored_rows = [dict(row) for row in (payload.get("feed") or []) if isinstance(row, dict)]
        if stored_rows:
            payload = _recalculate(date_text, stored_rows, list(payload.get("warnings") or [])) | {
                "is_final": bool(payload.get("is_final"))
            }
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
    now = int(time.time())
    saved = dict(payload)
    saved["date"] = date_text
    saved["updated_at"] = now
    saved["is_final"] = bool(is_final)
    summary = dict(saved)
    summary.pop("feed", None)
    try:
        livezot_postgres.upsert_prediction_day(
            _STORAGE_SOURCE,
            date_text,
            summary=summary,
            feed=saved,
            is_final=bool(is_final),
            updated_at=now,
        )
    except Exception as exc:
        print(f"[scores24] PostgreSQL save failed date={date_text}: {exc}")
        raise


def _storage_delete_before(date_text: str = HISTORY_START_DATE) -> int:
    """Remove legacy prediction days from the unified PostgreSQL table."""
    try:
        return livezot_postgres.delete_prediction_days_before(_STORAGE_SOURCE, date_text)
    except Exception as exc:
        print(f"[scores24] PostgreSQL legacy cleanup failed: {exc}")
        return 0

def history() -> list[dict[str, Any]]:
    try:
        rows = livezot_postgres.list_prediction_days(
            _STORAGE_SOURCE,
            from_date=HISTORY_START_DATE,
            descending=True,
        )
    except Exception as exc:
        print(f"[scores24] PostgreSQL history failed: {exc}")
        rows = []
    out: list[dict[str, Any]] = []
    for row in rows:
        payload = row.get("feed") or row.get("summary") or {}
        if not isinstance(payload, dict):
            payload = {}
        payload = dict(payload)
        stored_rows = [dict(item) for item in (payload.get("feed") or []) if isinstance(item, dict)]
        if stored_rows:
            payload = _recalculate(str(row.get("date") or ""), stored_rows, list(payload.get("warnings") or []))
        out.append({
            "date": str(row.get("date") or ""),
            "predicted_total": int(payload.get("predicted_total") or 0),
            "won_total": int(payload.get("won_total") or 0),
            "lost_total": int(payload.get("lost_total") or 0),
            "hit_rate": float(payload.get("hit_rate") or 0),
            "profit_total": float(payload.get("profit_total") or 0),
            "roi_percent": float(payload.get("roi_percent") or 0),
            "is_final": bool(row.get("is_final")),
            "updated_at": int(row.get("updated_at") or 0),
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
            with _graphql_opener.open(request, timeout=REQUEST_TIMEOUT) as response:
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



_GRAPHQL_HIDDEN_QUERY = """query LeaguesPredictionHidden($sportSlug: String, $langSlug: String!, $day: DayEnum, $votesType: PredictionVotesTypeEnum, $leagues: [String], $first: Int!, $after: String, $timezoneOffset: Int, $isOpen: Boolean, $marketSlugs: [String], $withMatches: Boolean!, $subMarketSlug: String) {
  LeaguesPrediction(day: $day, sport_slug: $sportSlug, votes_type: $votesType, leagues: $leagues, first: $first, after: $after, lang: $langSlug, timezone_offset: $timezoneOffset, is_open: $isOpen, market_slugs: $marketSlugs, sub_market_slug: $subMarketSlug) {
    league { ...LeagueCacheFragment country { ...CountryFragment logo __typename } name __typename }
    items @include(if: $withMatches) {
      edges { cursor node { ...PredictionsCardFragment __typename } __typename }
      pageInfo { hasNextPage endCursor __typename }
      __typename
    }
    count
    leagueKey: league_key
    isOpen: is_open
    __typename
  }
}
fragment LeagueCacheFragment on League { slug langSlug: lang_slug sportSlug: sport_slug __typename }
fragment CountryFragment on Country { name slug iso __typename }
fragment PredictionsCardFragment on CachedPrediction {
  prediction
  predictionValue: prediction_value
  allVotesCount: all_votes_count
  agreedVotesPercent: agreed_votes_percent
  match {
    ...MatchCacheFragment
    uniqueTournament: unique_tournament { ...LeagueCacheFragment name __typename }
    country { ...CountryFragment __typename }
    teams { ...TeamCacheFragment name logo __typename }
    __typename
  }
  __typename
}
fragment MatchCacheFragment on Match { slug matchDate: match_date langSlug: lang_slug __typename }
fragment TeamCacheFragment on Team { slug langSlug: lang_slug name temporarilyQualified: temporarily_qualified __typename }"""

_GRAPHQL_OPEN_QUERY = """query LeaguesPredictionOpen($sportSlug: String, $langSlug: String!, $day: DayEnum, $votesType: PredictionVotesTypeEnum, $leagues: [String], $first: Int!, $after: String, $timezoneOffset: Int, $isOpen: Boolean, $marketSlugs: [String], $subMarketSlug: String) {
  LeaguesPrediction(day: $day, sport_slug: $sportSlug, votes_type: $votesType, leagues: $leagues, first: $first, after: $after, lang: $langSlug, timezone_offset: $timezoneOffset, is_open: $isOpen, market_slugs: $marketSlugs, sub_market_slug: $subMarketSlug) {
    league { ...LeagueCacheFragment country { ...CountryFragment logo __typename } name __typename }
    count
    isOpen: is_open
    leagueKey: league_key
    items {
      edges { cursor node { ...PredictionsCardFragment __typename } __typename }
      pageInfo { hasNextPage endCursor __typename }
      __typename
    }
    __typename
  }
}
fragment LeagueCacheFragment on League { slug langSlug: lang_slug sportSlug: sport_slug __typename }
fragment CountryFragment on Country { name slug iso __typename }
fragment PredictionsCardFragment on CachedPrediction {
  prediction
  predictionValue: prediction_value
  allVotesCount: all_votes_count
  agreedVotesPercent: agreed_votes_percent
  match {
    ...MatchCacheFragment
    uniqueTournament: unique_tournament { ...LeagueCacheFragment name __typename }
    country { ...CountryFragment __typename }
    teams { ...TeamCacheFragment name logo __typename }
    __typename
  }
  __typename
}
fragment MatchCacheFragment on Match { slug matchDate: match_date langSlug: lang_slug __typename }
fragment TeamCacheFragment on Team { slug langSlug: lang_slug name temporarilyQualified: temporarily_qualified __typename }"""


def _timezone_offset_minutes() -> int:
    # Scores24 filters the current prediction day by the client offset. The
    # browser request captured for this project uses UTC+05:00 (300 minutes).
    # Keep it configurable for Railway instead of deriving it from the server.
    try:
        return int(GRAPHQL_TIMEZONE_OFFSET)
    except Exception:
        offset = datetime.now(_tz()).utcoffset()
        return int(offset.total_seconds() // 60) if offset is not None else 0


def _graphql_timezone_candidates() -> list[int]:
    values = [_timezone_offset_minutes(), 300, 180, 0]
    return list(dict.fromkeys(int(value) for value in values))


def _graphql_headers() -> dict[str, str]:
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/graphql-response+json, application/graphql+json, application/json, text/event-stream, multipart/mixed",
        "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.7",
        "Content-Type": "application/json",
        "Origin": BASE_URL,
        "Referer": f"{BASE_URL}/{LANG}/predictions/soccer",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "DNT": "1",
        "Sec-CH-UA": '"Chromium";v="131", "Not_A Brand";v="24"',
        "Sec-CH-UA-Mobile": "?0",
        "Sec-CH-UA-Platform": '"Windows"',
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
    }
    if GRAPHQL_COOKIE:
        headers["Cookie"] = GRAPHQL_COOKIE
    return headers


def _graphql_bootstrap_session(*, force: bool = False) -> None:
    """Open the public predictions page once so Cloudflare/session cookies exist."""
    global _graphql_session_bootstrapped
    if _graphql_session_bootstrapped and not force:
        return
    request = urllib.request.Request(
        f"{BASE_URL}/{LANG}/predictions/soccer",
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.7",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Sec-CH-UA": '"Chromium";v="131", "Not_A Brand";v="24"',
            "Sec-CH-UA-Mobile": "?0",
            "Sec-CH-UA-Platform": '"Windows"',
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Upgrade-Insecure-Requests": "1",
        },
        method="GET",
    )
    try:
        with _graphql_opener.open(request, timeout=REQUEST_TIMEOUT) as response:
            response.read(256)
    except Exception:
        # The GraphQL endpoint can still be public even if the HTML page is
        # challenged, so bootstrap failure must not stop collection.
        pass
    _graphql_session_bootstrapped = True


def _graphql_post(
    operation_name: str,
    query: str,
    variables: dict[str, Any],
    *,
    force: bool = False,
    ttl: int = GRAPHQL_CACHE_SECONDS,
) -> dict[str, Any]:
    """Execute one public Scores24 GraphQL request with cache and rate limits."""
    global _next_request_at, _blocked_until
    global _graphql_last_http_status, _graphql_last_success_at, _graphql_last_error

    _graphql_bootstrap_session(force=False)
    body = {
        "operationName": operation_name,
        "query": query,
        "variables": variables,
    }
    encoded = json.dumps(body, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
    cache_key = hashlib.sha1(encoded).hexdigest()
    now = time.time()
    with _lock:
        cached = _graphql_cache.get(cache_key)
        if cached and not force and cached[0] > now:
            return dict(cached[1])
        blocked_for = int(max(0, _blocked_until - now))
        if blocked_for > 0:
            if cached:
                return dict(cached[1])
            raise Scores24RateLimited(blocked_for)

    with _request_lock:
        now = time.time()
        wait_for = max(0.0, _next_request_at - now)
        if wait_for:
            time.sleep(wait_for)
        _next_request_at = time.time() + REQUEST_GAP_SECONDS
        request = urllib.request.Request(
            GRAPHQL_URL,
            data=encoded,
            headers=_graphql_headers(),
            method="POST",
        )
        try:
            with _graphql_opener.open(request, timeout=REQUEST_TIMEOUT) as response:
                raw = response.read()
                status = int(getattr(response, "status", 200) or 200)
                encoding = response.headers.get_content_charset() or "utf-8"
                text = raw.decode(encoding, "replace")
        except urllib.error.HTTPError as exc:
            _graphql_last_http_status = int(exc.code or 0)
            if exc.code in {403, 429}:
                retry_header = str(exc.headers.get("Retry-After") or "").strip()
                retry_after = int(retry_header) if retry_header.isdigit() else RATE_LIMIT_BACKOFF_SECONDS
                retry_after = max(RATE_LIMIT_BACKOFF_SECONDS, retry_after)
                with _lock:
                    _blocked_until = max(_blocked_until, time.time() + retry_after)
                message = "Scores24 GraphQL заблокировал запросы (HTTP 403)" if exc.code == 403 else "Scores24 GraphQL ограничил запросы (HTTP 429)"
                _graphql_last_error = message
                if cached:
                    return dict(cached[1])
                raise Scores24RateLimited(retry_after) from exc
            _graphql_last_error = f"Scores24 GraphQL HTTP {exc.code}"
            if cached:
                return dict(cached[1])
            raise RuntimeError(_graphql_last_error) from exc
        except Exception as exc:
            _graphql_last_error = f"Scores24 GraphQL request failed: {exc}"
            if cached:
                return dict(cached[1])
            raise RuntimeError(_graphql_last_error) from exc

    _graphql_last_http_status = status
    try:
        payload = json.loads(text)
    except Exception as exc:
        _graphql_last_error = f"Scores24 GraphQL returned invalid JSON: {exc}"
        if cached:
            return dict(cached[1])
        raise RuntimeError(_graphql_last_error) from exc
    if not isinstance(payload, dict):
        raise RuntimeError("Scores24 GraphQL returned an invalid payload")
    errors = payload.get("errors") or []
    if errors:
        messages = []
        for item in errors:
            if isinstance(item, dict) and item.get("message"):
                messages.append(str(item.get("message")))
        _graphql_last_error = "Scores24 GraphQL: " + ("; ".join(messages) or "unknown error")
        if cached:
            return dict(cached[1])
        raise RuntimeError(_graphql_last_error)

    _graphql_last_success_at = int(time.time())
    _graphql_last_error = ""
    with _lock:
        _graphql_cache[cache_key] = (time.time() + max(15, int(ttl)), dict(payload))
        if len(_graphql_cache) > 500:
            for key in list(_graphql_cache)[:100]:
                _graphql_cache.pop(key, None)
    return payload


def _league_filter_text(value: Any) -> str:
    text = unicodedata.normalize("NFKD", str(value or "")).lower()
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.replace("ё", "е").replace("ı", "i")
    text = re.sub(r"[^a-zа-я0-9]+", " ", text, flags=re.I)
    return re.sub(r"\s+", " ", text).strip()


def _is_selected_uefa_club_competition(slug: str, name: str = "", country_slug: str = "", country_name: str = "") -> bool:
    slug = str(slug or "").strip().lower().replace("_", "-")
    if any(marker in slug for marker in _UEFA_CLUB_COMPETITION_MARKERS):
        return True
    # Name fallback is limited to leagues explicitly marked as European, which
    # prevents AFC/CAF/CONCACAF Champions League competitions from slipping in.
    country_text = _league_filter_text(country_slug or country_name)
    if country_text not in {"europe", "европа"}:
        return False
    name_text = _league_filter_text(name)
    senior_markers = (
        "лига чемпионов", "champions league",
        "лига европы", "europa league",
        "лига конференций", "conference league",
        "суперкубок уефа", "uefa super cup",
        "женский кубок европы", "women europa cup", "womens europa cup",
    )
    return any(marker in name_text for marker in senior_markers) and "юнош" not in name_text and "youth" not in name_text


def _is_selected_prediction_league(league: dict[str, Any] | None) -> bool:
    league = league if isinstance(league, dict) else {}
    slug = str(league.get("slug") or "").strip().lower().replace("_", "-")
    name = str(league.get("name") or "").strip()
    country = league.get("country") if isinstance(league.get("country"), dict) else {}
    country_slug = str((country or {}).get("slug") or "").strip().lower().replace("_", "-")
    country_name = str((country or {}).get("name") or "").strip()
    country_slug = _COUNTRY_SLUG_ALIASES.get(country_slug, country_slug)
    # Some Scores24 responses omit/alter the US country slug but still expose
    # an unmistakable MLS league slug/name. Canonicalize that case as USA.
    mls_text = _league_filter_text(f"{slug} {name}")
    if country_slug in {"", "world", "international"} and (
        "major league soccer" in mls_text or re.search(r"(^| )mls($| )", mls_text)
    ):
        country_slug = "usa"

    if _is_selected_uefa_club_competition(slug, name, country_slug, country_name):
        return True
    if country_slug in _REGIONAL_COUNTRY_SLUGS:
        return False

    combined_text = _league_filter_text(f"{slug} {name}")
    if any(_league_filter_text(marker) in combined_text for marker in _DOMESTIC_EXCLUDED_MARKERS):
        return False
    if slug in _DOMESTIC_TOP_DIVISION_SLUGS:
        return True
    if country_slug == "usa" and (slug == "mls" or slug.endswith("-mls")):
        return True

    name_text = _league_filter_text(name)
    accepted_names = _DOMESTIC_TOP_DIVISION_NAMES.get(country_slug) or set()
    if name_text in accepted_names:
        return True

    # Conservative generic fallback for standard country-prefixed top-flight
    # aliases. Ambiguous labels such as "First Division" are not accepted here.
    for suffix in _GENERIC_TOP_DIVISION_SUFFIXES:
        if slug == f"{country_slug}-{suffix}":
            return True
    return False


def _row_prediction_league(row: dict[str, Any]) -> dict[str, Any]:
    fixture_item = row.get("fixture_item") if isinstance(row.get("fixture_item"), dict) else {}
    fixture_league = fixture_item.get("league") if isinstance(fixture_item.get("league"), dict) else {}
    source_page = str(row.get("source_page") or "")
    page_match = re.search(r"/soccer/l-([^/?#]+)/predictions", source_page, re.I)
    slug = str(row.get("scores24_league_slug") or (page_match.group(1) if page_match else "")).strip().lower()
    name = str(row.get("scores24_league_name") or fixture_league.get("name") or "").strip()
    country_slug = str(row.get("scores24_country_slug") or "").strip().lower()
    country_name = str(row.get("scores24_country_name") or fixture_league.get("country") or "").strip()
    return {"slug": slug, "name": name, "country": {"slug": country_slug, "name": country_name}}


def _row_is_selected_prediction_league(row: dict[str, Any]) -> bool:
    return _is_selected_prediction_league(_row_prediction_league(row))


def _prune_unselected_active_predictions(*, confirmed_discovered_count: int = 0) -> dict[str, int]:
    """Remove every saved prediction outside the current league policy.

    This also rebuilds completed-day statistics, so wins, losses, ROI and
    counters contain only UEFA club cups and domestic top divisions.

    Destructive cleanup is forbidden unless the same GraphQL cycle confirmed
    at least one valid selected prediction. This second guard protects the
    database even if the caller is changed later.
    """
    if int(confirmed_discovered_count or 0) <= 0:
        return {"rows": 0, "dates": 0, "skipped_empty_scan": 1}
    removed_rows = changed_dates = 0
    today_text = _today()
    for payload in _storage_records_full():
        date_text = str(payload.get("date") or "")
        if not _valid_date(date_text):
            continue
        rows = [dict(row) for row in payload.get("feed") or [] if isinstance(row, dict)]
        kept = [row for row in rows if _row_is_selected_prediction_league(row)]
        if len(kept) == len(rows):
            continue
        removed_rows += len(rows) - len(kept)
        rebuilt = _recalculate(date_text, kept, list(payload.get("warnings") or []))
        rebuilt["league_filter_applied"] = True
        rebuilt["league_filter_removed"] = len(rows) - len(kept)
        is_past = date_text < today_text
        all_finished = _rows_fully_finalized(kept) if kept else False
        _storage_save(
            date_text,
            rebuilt,
            is_final=bool(is_past and all_finished and not rebuilt.get("pending_result_total")),
        )
        changed_dates += 1
    return {"rows": removed_rows, "dates": changed_dates}


def _graphql_hidden_payload(
    *,
    first: int,
    with_matches: bool,
    leagues: list[str] | None = None,
    after: str | None = None,
    timezone_offset: int | None = None,
    force: bool = False,
) -> dict[str, Any]:
    variables: dict[str, Any] = {
        "first": max(1, min(GRAPHQL_MAX_FIRST, int(first))),
        "isOpen": False,
        "langSlug": LANG,
        "sportSlug": "soccer",
        "timezoneOffset": int(_timezone_offset_minutes() if timezone_offset is None else timezone_offset),
        "withMatches": bool(with_matches),
    }
    if leagues:
        variables["leagues"] = list(dict.fromkeys(str(x).strip() for x in leagues if str(x).strip()))
    if after:
        variables["after"] = after
    return _graphql_post(
        "LeaguesPredictionHidden",
        _GRAPHQL_HIDDEN_QUERY,
        variables,
        force=force,
    )


def _graphql_league_catalog(
    *,
    force: bool = False,
    timezone_offset: int | None = None,
    with_matches: bool = True,
) -> list[dict[str, Any]]:
    # Ask the catalogue for embedded prediction items immediately. The old
    # version used withMatches=false and then needed dozens of follow-up calls;
    # Scores24 can return the full current feed in this single public query.
    payload = _graphql_hidden_payload(
        first=min(GRAPHQL_MAX_FIRST, 100),
        with_matches=with_matches,
        timezone_offset=timezone_offset,
        force=force,
    )
    values = ((payload.get("data") or {}).get("LeaguesPrediction") or [])
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for value in values if isinstance(values, list) else []:
        if not isinstance(value, dict):
            continue
        league = value.get("league") or {}
        slug = str(league.get("slug") or "").strip()
        if not slug or slug in seen:
            continue
        seen.add(slug)
        out.append({
            "slug": slug,
            "count": max(0, int(value.get("count") or 0)),
            "leagueKey": str(value.get("leagueKey") or ""),
            "league": league,
            "result": value,
        })
    return out


def _graphql_open_payload(
    slugs: list[str],
    *,
    first: int,
    after: str | None = None,
    timezone_offset: int | None = None,
    force: bool = False,
) -> dict[str, Any]:
    variables: dict[str, Any] = {
        "first": max(1, min(GRAPHQL_MAX_FIRST, int(first))),
        "isOpen": True,
        "langSlug": LANG,
        "leagues": list(dict.fromkeys(str(x).strip() for x in slugs if str(x).strip())),
        "sportSlug": "soccer",
        "timezoneOffset": int(_timezone_offset_minutes() if timezone_offset is None else timezone_offset),
    }
    if after:
        variables["after"] = after
    return _graphql_post(
        "LeaguesPredictionOpen",
        _GRAPHQL_OPEN_QUERY,
        variables,
        force=force,
    )


def _graphql_rows_from_league_result(
    result: dict[str, Any],
) -> tuple[list[dict[str, Any]], bool, str]:
    league = result.get("league") or {}
    slug = str(league.get("slug") or "").strip()
    source_page = f"{BASE_URL}/{LANG}/soccer/l-{slug}/predictions" if slug else f"{BASE_URL}/{LANG}/predictions/soccer"
    items = result.get("items") or {}
    edges = items.get("edges") or []
    rows: list[dict[str, Any]] = []
    for edge in edges if isinstance(edges, list) else []:
        node = edge.get("node") if isinstance(edge, dict) else None
        if not isinstance(node, dict):
            continue
        row = _row_from_prediction_node(node, source_page, league, "soccer")
        if row:
            rows.append(row)
    page_info = items.get("pageInfo") or {}
    return rows, bool(page_info.get("hasNextPage")), str(page_info.get("endCursor") or "")


def _graphql_discover_rows(*, force: bool = False) -> tuple[list[dict[str, Any]], int, int, list[str], bool]:
    """Load current football predictions directly from Scores24 GraphQL.

    Scores24 can temporarily report a non-zero league count while returning an
    empty ``items`` list for that same league. Such a provider inconsistency is
    treated as a soft per-league warning: predictions successfully loaded from
    the other leagues are still published. A rate-limit or a completely empty
    scan remains a hard failure and never triggers destructive database cleanup.
    """
    global _graphql_last_leagues, _graphql_last_predictions
    warnings: list[str] = []
    rate_limited = False

    catalog: list[dict[str, Any]] = []
    chosen_offset = _timezone_offset_minutes()
    best_embedded = -1
    last_error: Exception | None = None
    for offset in _graphql_timezone_candidates():
        try:
            candidate = _graphql_league_catalog(
                force=force,
                timezone_offset=offset,
                with_matches=True,
            )
        except Scores24RateLimited as exc:
            return [], 0, 0, [str(exc)], True
        except Exception as exc:
            last_error = exc
            continue
        embedded = 0
        for entry in candidate:
            rows, _has_next, _cursor = _graphql_rows_from_league_result(entry.get("result") or {})
            embedded += len(rows)
        if embedded > best_embedded or not catalog:
            catalog = candidate
            chosen_offset = offset
            best_embedded = embedded
        if embedded > 0:
            break
    if not catalog:
        return [], 0, 0, [_compact_error(last_error or RuntimeError("Scores24 GraphQL returned no leagues"))], False

    global _graphql_last_catalog_total, _graphql_last_filtered_out
    catalog = [entry for entry in catalog if int(entry.get("count") or 0) > 0]
    _graphql_last_catalog_total = len(catalog)
    catalog = [entry for entry in catalog if _is_selected_prediction_league(entry.get("league") or {})]
    _graphql_last_filtered_out = max(0, _graphql_last_catalog_total - len(catalog))

    # Hidden and Open are UI-oriented GraphQL views. A league expanded on the
    # Scores24 website may disappear from the Hidden catalogue, so query every
    # selected canonical slug directly as an optional candidate as well.
    known_slugs = {str(entry.get("slug") or "") for entry in catalog}
    for slug in sorted(_SELECTED_QUERY_LEAGUE_SLUGS - known_slugs):
        catalog.append({
            "slug": slug,
            "count": GRAPHQL_MAX_FIRST,
            "leagueKey": "",
            "league": {"slug": slug, "sportSlug": "soccer"},
            "result": {},
            "_optional_direct": True,
        })

    _graphql_last_leagues = len(catalog)
    checked_leagues = len(catalog)
    complete = True
    league_rows: dict[str, dict[str, dict[str, Any]]] = {}
    pending: list[dict[str, Any]] = []

    for entry in catalog:
        slug = str(entry.get("slug") or "")
        rows, has_next, cursor = _graphql_rows_from_league_result(entry.get("result") or {})
        bucket = league_rows.setdefault(slug, {})
        for row in rows:
            key = str(row.get("prediction_key") or "")
            if key:
                bucket[key] = row
        entry["_has_next"] = has_next
        entry["_cursor"] = cursor
        if has_next or len(bucket) < int(entry.get("count") or 0):
            pending.append(entry)

    # Exact browser-style request: one league slug per LeaguesPredictionOpen.
    for entry in pending:
        slug = str(entry.get("slug") or "")
        expected_count = max(1, int(entry.get("count") or 1))
        bucket = league_rows.setdefault(slug, {})
        payloads: list[tuple[str, dict[str, Any]]] = []
        try:
            payloads.append(("open", _graphql_open_payload(
                [slug], first=expected_count, timezone_offset=chosen_offset, force=force
            )))
        except Scores24RateLimited as exc:
            warnings.append(str(exc)); rate_limited = True; complete = False; break
        except Exception as exc:
            warnings.append(_compact_error(exc))

        # Some Railway/Cloudflare sessions return league metadata but empty
        # Open items. The Hidden operation with withMatches=true is then used
        # as a fallback. Optional alias candidates that return no league at all
        # skip this second request to avoid unnecessary load and 429 responses.
        need_hidden_fallback = not bool(entry.get("_optional_direct"))
        if entry.get("_optional_direct") and payloads:
            open_results = (((payloads[0][1].get("data") or {}).get("LeaguesPrediction")) or [])
            open_result = next((x for x in open_results if isinstance(x, dict)), None)
            if open_result and int(open_result.get("count") or 0) > 0:
                open_rows, _open_next, _open_cursor = _graphql_rows_from_league_result(open_result)
                need_hidden_fallback = not bool(open_rows)
        if need_hidden_fallback:
            try:
                payloads.append(("hidden", _graphql_hidden_payload(
                    first=expected_count, with_matches=True, leagues=[slug],
                    timezone_offset=chosen_offset, force=force
                )))
            except Scores24RateLimited as exc:
                warnings.append(str(exc)); rate_limited = True; complete = False; break
            except Exception as exc:
                warnings.append(_compact_error(exc))

        best_kind = ""
        best_result: dict[str, Any] | None = None
        best_rows: list[dict[str, Any]] = []
        best_next = False
        best_cursor = ""
        for kind, payload in payloads:
            results = ((payload.get("data") or {}).get("LeaguesPrediction") or [])
            result = next((x for x in results if isinstance(x, dict) and str(((x.get("league") or {}).get("slug") or "")) == slug), None)
            if result is None:
                result = next((x for x in results if isinstance(x, dict)), None)
            if not result:
                continue
            rows, has_next, cursor = _graphql_rows_from_league_result(result)
            if len(rows) > len(best_rows):
                best_kind, best_result, best_rows, best_next, best_cursor = kind, result, rows, has_next, cursor
        if entry.get("_optional_direct"):
            if best_result:
                entry["count"] = max(len(best_rows), int(best_result.get("count") or 0))
                league_payload = best_result.get("league") or {}
                if isinstance(league_payload, dict) and league_payload.get("slug"):
                    entry["league"] = league_payload
            else:
                # Alias not used by Scores24 today. It must not invalidate the
                # otherwise complete atomic scan.
                entry["count"] = 0

        for row in best_rows:
            key = str(row.get("prediction_key") or "")
            if key:
                bucket[key] = row

        page = 1
        has_next, cursor = best_next, best_cursor
        while has_next and cursor and page < GRAPHQL_MAX_PAGES:
            try:
                if best_kind == "hidden":
                    follow = _graphql_hidden_payload(
                        first=expected_count, with_matches=True, leagues=[slug], after=cursor,
                        timezone_offset=chosen_offset, force=force
                    )
                else:
                    follow = _graphql_open_payload(
                        [slug], first=expected_count, after=cursor,
                        timezone_offset=chosen_offset, force=force
                    )
            except Scores24RateLimited as exc:
                warnings.append(str(exc)); rate_limited = True; complete = False; break
            except Exception as exc:
                warnings.append(_compact_error(exc)); complete = False; break
            results = ((follow.get("data") or {}).get("LeaguesPrediction") or [])
            result = next((x for x in results if isinstance(x, dict)), None)
            if not result:
                complete = False
                break
            more_rows, has_next, cursor = _graphql_rows_from_league_result(result)
            for row in more_rows:
                key = str(row.get("prediction_key") or "")
                if key:
                    bucket[key] = row
            page += 1
        if len(bucket) < int(entry.get("count") or 0):
            complete = False
            warnings.append(
                f"Scores24 GraphQL: лига {slug} — получено {len(bucket)} из {int(entry.get('count') or 0)} прогнозов"
            )
        if rate_limited:
            break

    final_rows = [row for bucket in league_rows.values() for row in bucket.values()]
    # Scores24 also exposes statistical/market tips elsewhere on a match page.
    # LiveZOT must publish only the single main editorial card per match.
    raw_prediction_count = len(final_rows)
    final_rows, secondary_filtered = _select_primary_editorial_rows(final_rows)
    global _graphql_last_secondary_filtered
    _graphql_last_secondary_filtered = int(secondary_filtered)
    expected_total = sum(int(entry.get("count") or 0) for entry in catalog)
    # Completeness is checked against the raw provider rows before the
    # intentional one-main-pick-per-match collapse.
    if raw_prediction_count < expected_total:
        complete = False
        warnings.append(
            f"Scores24 GraphQL: получено {raw_prediction_count} из {expected_total} прогнозов; "
            f"embedded={max(0, best_embedded)} offset={chosen_offset}"
        )

    deduped_warnings = list(dict.fromkeys(warnings))
    # Never publish a completely empty or rate-limited scan. This protects the
    # PostgreSQL feed when Scores24 temporarily returns metadata without items.
    if rate_limited or not final_rows:
        return [], 1, checked_leagues, deduped_warnings, rate_limited

    # A few individual leagues may be temporarily incomplete even though the
    # rest of the catalogue is healthy. Keep the successfully received rows and
    # preserve existing rows from missing selected leagues via merge storage.
    _graphql_last_predictions = len(final_rows)
    return final_rows, 1, checked_leagues, deduped_warnings, False

def _primary_editorial_match_key(row: dict[str, Any]) -> str:
    """Stable match identity for the single Scores24 editorial pick.

    The unfiltered ``LeaguesPrediction`` feed is the Scores24 prediction-card
    feed.  A match can still be encountered more than once through provider
    pagination/aliases or linger in PostgreSQL from older market/category
    collectors.  LiveZOT intentionally exposes one editorial pick per match.
    """
    slug = str(row.get("source_slug") or "").strip().lower()
    if slug:
        return f"slug:{slug}"
    fixture_item = row.get("fixture_item") if isinstance(row.get("fixture_item"), dict) else {}
    fixture = fixture_item.get("fixture") if isinstance(fixture_item.get("fixture"), dict) else {}
    fixture_id = str(row.get("api_football_fixture_id") or row.get("fixture_id") or fixture.get("id") or "").strip()
    if fixture_id:
        return f"fixture:{fixture_id}"
    teams = fixture_item.get("teams") if isinstance(fixture_item.get("teams"), dict) else {}
    home = str(((teams.get("home") or {}).get("name") or "")).strip().lower()
    away = str(((teams.get("away") or {}).get("name") or "")).strip().lower()
    kickoff = str(fixture.get("date") or fixture.get("timestamp") or row.get("match_date") or "").strip()
    return f"fallback:{kickoff}:{home}:{away}"


def _select_primary_editorial_rows(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    """Keep only the first provider editorial card for each match.

    Scores24's unfiltered LeaguesPrediction order is the order used by its
    public prediction cards.  We therefore preserve the first card for a match
    and discard repeated/secondary cards.  This is deliberately *not* based on
    odds or vote percentage: those values can move after publication and are
    not a reliable definition of the editorial choice.
    """
    selected: list[dict[str, Any]] = []
    seen: set[str] = set()
    filtered = 0
    now_ts = int(time.time())
    for raw in rows or []:
        if not isinstance(raw, dict):
            continue
        row = dict(raw)
        match_key = _primary_editorial_match_key(row)
        if match_key in seen:
            filtered += 1
            continue
        seen.add(match_key)
        row["scores24_primary_editorial"] = True
        row["scores24_prediction_role"] = "editorial_main"
        row["scores24_primary_seen_at"] = now_ts
        selected.append(row)
    return selected, filtered


def _drop_legacy_secondary_rows_when_primary_known(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Remove old same-match extras once a confirmed primary row is present.

    Older builds stored market/category predictions alongside the main card.
    V24+ archives are monotonic, so removing these rows from the live feed does
    not destroy the backup copy in PostgreSQL.
    """
    groups: dict[str, list[dict[str, Any]]] = {}
    order: list[str] = []
    for raw in rows or []:
        if not isinstance(raw, dict):
            continue
        row = dict(raw)
        key = _primary_editorial_match_key(row)
        if key not in groups:
            groups[key] = []
            order.append(key)
        groups[key].append(row)

    out: list[dict[str, Any]] = []
    for key in order:
        group = groups[key]
        primary = [row for row in group if bool(row.get("scores24_primary_editorial"))]
        if primary:
            # There should normally be exactly one.  If a legacy deployment
            # marked more than one, keep the newest confirmed provider card.
            primary.sort(
                key=lambda row: (
                    int(row.get("scores24_primary_seen_at") or 0),
                    int(row.get("votes_total") or 0),
                ),
                reverse=True,
            )
            out.append(primary[0])
        else:
            # Historical rows created before V37 are left untouched unless a
            # current primary card for that exact match has been observed.
            out.extend(group)
    return out


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


def _score_pair(value: Any) -> tuple[int | None, int | None]:
    if isinstance(value, dict):
        for home_key, away_key in (
            ("home", "away"),
            ("homeScore", "awayScore"),
            ("scoreHome", "scoreAway"),
        ):
            if value.get(home_key) is not None and value.get(away_key) is not None:
                try:
                    home, away = int(value.get(home_key)), int(value.get(away_key))
                    if home >= 0 and away >= 0:
                        return home, away
                except Exception:
                    pass
        value = value.get("value") or value.get("score") or value.get("result")
    found = _SCORE_RE.search(str(value or ""))
    if not found:
        return None, None
    home, away = int(found.group(1)), int(found.group(2))
    return (home, away) if home >= 0 and away >= 0 else (None, None)


def _score_bundle_from_match(match_data: dict[str, Any]) -> dict[str, Any]:
    """Return regulation, after-extra-time and shootout scores separately.

    Standard football prediction markets are settled on 90 minutes plus added
    time. Goals scored in extra time and penalty shootouts must never change a
    regular-time prediction result.
    """
    regulation_types = {"FT", "FULLTIME", "FULL_TIME", "REGULAR", "REGULATION", "90"}
    extra_types = {"AET", "ET", "EXTRA_TIME", "EXTRATIME"}
    penalty_types = {"PEN", "PENALTY", "PENALTIES", "PSO", "SHOOTOUT"}
    regulation = extra = penalty = (None, None)
    for item in match_data.get("resultScores") or []:
        if not isinstance(item, dict):
            continue
        score_type = str(item.get("type") or item.get("name") or item.get("period") or "").upper().replace(" ", "_")
        pair = _score_pair(item.get("value") if "value" in item else item)
        if pair[0] is None:
            continue
        if score_type in regulation_types and regulation[0] is None:
            regulation = pair
        elif score_type in extra_types and extra[0] is None:
            extra = pair
        elif score_type in penalty_types and penalty[0] is None:
            penalty = pair

    aggregate = _score_pair(match_data.get("resultScore"))
    had_extra_time = extra[0] is not None
    had_penalties = penalty[0] is not None
    if regulation[0] is None and not had_extra_time and not had_penalties:
        regulation = aggregate
    final_score = extra if extra[0] is not None else aggregate if aggregate[0] is not None else regulation
    status_short = "PEN" if had_penalties else "AET" if had_extra_time else "FT"
    return {
        "regulation": regulation,
        "final": final_score,
        "extra_time": extra,
        "penalty": penalty,
        "had_extra_time": had_extra_time,
        "had_penalties": had_penalties,
        "status_short": status_short,
    }


def _score_from_match(match_data: dict[str, Any]) -> tuple[int | None, int | None]:
    """Compatibility helper: always return the 90-minute settlement score."""
    return _score_bundle_from_match(match_data)["regulation"]


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


def _pair_from_mapping(value: Any) -> tuple[int | None, int | None]:
    if not isinstance(value, dict):
        return None, None
    try:
        home, away = value.get("home"), value.get("away")
        if home is None or away is None:
            return None, None
        return int(home), int(away)
    except Exception:
        return None, None


def _regulation_score_from_events(row: dict[str, Any], final_home: int | None, final_away: int | None) -> tuple[int | None, int | None]:
    fixture_item = row.get("fixture_item") or {}
    events = fixture_item.get("events") if isinstance(fixture_item.get("events"), list) else []
    teams = fixture_item.get("teams") or {}
    home_team, away_team = teams.get("home") or {}, teams.get("away") or {}
    home_id, away_id = str(home_team.get("id") or ""), str(away_team.get("id") or "")
    home_name, away_name = str(home_team.get("name") or "").strip().lower(), str(away_team.get("name") or "").strip().lower()
    regulation_home = regulation_away = all_home = all_away = 0
    found_goals = 0
    for event in events:
        if not isinstance(event, dict) or str(event.get("type") or "").lower() != "goal":
            continue
        detail = str(event.get("detail") or "").lower()
        if "missed" in detail or "shootout" in detail:
            continue
        event_team = event.get("team") or {}
        team_id = str(event_team.get("id") or "")
        team_name = str(event_team.get("name") or "").strip().lower()
        side = ""
        if home_id and team_id == home_id:
            side = "home"
        elif away_id and team_id == away_id:
            side = "away"
        elif home_name and team_name == home_name:
            side = "home"
        elif away_name and team_name == away_name:
            side = "away"
        if not side:
            continue
        elapsed = int(((event.get("time") or {}).get("elapsed") or 0))
        extra = (event.get("time") or {}).get("extra")
        in_regulation = elapsed < 90 or elapsed == 90 or (elapsed <= 90 and extra is not None)
        if side == "home":
            all_home += 1
            if in_regulation:
                regulation_home += 1
        else:
            all_away += 1
            if in_regulation:
                regulation_away += 1
        found_goals += 1
    if not found_goals:
        return None, None
    # Only trust event reconstruction when the complete goal timeline matches
    # the provider's final non-shootout score.
    if final_home is not None and final_away is not None and (all_home, all_away) != (int(final_home), int(final_away)):
        return None, None
    return regulation_home, regulation_away


def _regulation_score_for_row(row: dict[str, Any], fallback_home: int | None, fallback_away: int | None) -> tuple[int | None, int | None]:
    for home_key, away_key in (
        ("regulation_home_score", "regulation_away_score"),
        ("settlement_home_score", "settlement_away_score"),
    ):
        if row.get(home_key) is not None and row.get(away_key) is not None:
            try:
                return int(row.get(home_key)), int(row.get(away_key))
            except Exception:
                pass

    fixture_item = row.get("fixture_item") or {}
    scores24_meta = fixture_item.get("scores24") or {}
    pair = _pair_from_mapping(scores24_meta.get("regulation_score"))
    if pair[0] is not None:
        return pair

    fixture = fixture_item.get("fixture") or {}
    status = str(((fixture.get("status") or {}).get("short") or row.get("last_status_short") or "")).upper()
    score = fixture_item.get("score") or {}
    has_extra_marker = status in {"AET", "PEN"} or _pair_from_mapping(score.get("extratime"))[0] is not None or _pair_from_mapping(score.get("penalty"))[0] is not None

    explicit = _pair_from_mapping(score.get("regular_time"))
    if explicit[0] is None:
        explicit = _pair_from_mapping(score.get("regulation"))
    if explicit[0] is not None:
        return explicit

    if has_extra_marker:
        # Older builds overwrote score.fulltime with the after-extra-time score.
        # Reconstruct from the goal timeline first when possible.
        event_pair = _regulation_score_from_events(row, fallback_home, fallback_away)
        if event_pair[0] is not None:
            return event_pair
        fulltime = _pair_from_mapping(score.get("fulltime"))
        extra_time = _pair_from_mapping(score.get("extratime"))
        if fulltime[0] is not None:
            # In legacy snapshots IGScore replaced fulltime with the final AET
            # score. When both fields equal the final score, do not guess;
            # schedule one exact provider refresh instead.
            if (
                extra_time[0] is not None
                and fulltime == extra_time
                and fallback_home is not None
                and fallback_away is not None
                and fulltime == (int(fallback_home), int(fallback_away))
            ):
                return None, None
            return fulltime
        return None, None

    fulltime = _pair_from_mapping(score.get("fulltime"))
    if fulltime[0] is not None:
        return fulltime
    if fallback_home is None or fallback_away is None:
        return None, None
    return int(fallback_home), int(fallback_away)


def _settle_row(row: dict[str, Any], home: int | None, away: int | None) -> tuple[str, bool | None, str]:
    final_home, final_away = home, away
    home, away = _regulation_score_for_row(row, home, away)
    row["settlement_rule_version"] = SETTLEMENT_RULE_VERSION
    if home is None or away is None:
        row["regulation_score_pending"] = True
        fallback = f"{final_home}:{final_away}" if final_home is not None and final_away is not None else ""
        return "", None, fallback
    row["regulation_home_score"] = int(home)
    row["regulation_away_score"] = int(away)
    row["settlement_home_score"] = int(home)
    row["settlement_away_score"] = int(away)
    row["settlement_period"] = "REGULATION_TIME"
    row.pop("regulation_score_pending", None)
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


def _scores24_status_from_match_data(match_data: dict[str, Any]) -> dict[str, Any]:
    """Build a compact Scores24 status/score snapshot from an index or match page."""
    status_data = match_data.get("status") or {}
    status_name = str(status_data.get("name") or status_data.get("title") or status_data.get("slug") or "").strip()
    status_token = " ".join(
        str(value or "").strip().lower()
        for value in (status_data.get("slug"), status_data.get("name"), status_data.get("title"), match_data.get("statusSlug"))
        if value
    )
    finished = bool(match_data.get("isFinished")) or any(token in status_token for token in ("finished", "ended", "full time", "заверш"))
    live = bool(match_data.get("isLive")) or any(token in status_token for token in ("live", "1st half", "2nd half", "half time", "идет", "идёт"))
    cancelled = any(token in status_token for token in ("cancel", "abandon", "walkover", "awarded", "отмен", "прерван"))
    postponed = any(token in status_token for token in ("postpon", "suspend", "перенес", "приостанов"))
    score_bundle = _score_bundle_from_match(match_data)
    regulation_home, regulation_away = score_bundle["regulation"]
    home_score, away_score = score_bundle["final"]
    extra_home, extra_away = score_bundle["extra_time"]
    penalty_home, penalty_away = score_bundle["penalty"]
    if cancelled:
        short, long_name = "CANC", status_name or "Матч отменён"
    elif postponed:
        short, long_name = "PST", status_name or "Матч перенесён"
    elif finished:
        short = str(score_bundle.get("status_short") or "FT")
        long_name = status_name or ("Матч завершён по пенальти" if short == "PEN" else "Матч завершён в дополнительное время" if short == "AET" else "Матч завершён")
    elif live:
        short, long_name = "LIVE", status_name or "LIVE"
    else:
        short, long_name = "NS", status_name or "Не начался"
    return {
        "finished": finished,
        "live": live,
        "cancelled": cancelled,
        "postponed": postponed,
        "status_short": short,
        "status_long": long_name,
        "home_score": home_score,
        "away_score": away_score,
        "regulation_home_score": regulation_home,
        "regulation_away_score": regulation_away,
        "extra_time_home_score": extra_home,
        "extra_time_away_score": extra_away,
        "penalty_home_score": penalty_home,
        "penalty_away_score": penalty_away,
        "had_extra_time": bool(score_bundle.get("had_extra_time")),
        "had_penalties": bool(score_bundle.get("had_penalties")),
    }


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
    status_snapshot = _scores24_status_from_match_data(match_data)
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
            "status": {
                "short": status_snapshot["status_short"],
                "long": status_snapshot["status_long"],
                "elapsed": 90 if status_snapshot["finished"] else None,
            },
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
        "goals": {"home": status_snapshot["home_score"], "away": status_snapshot["away_score"]},
        "score": {
            "fulltime": {"home": status_snapshot["regulation_home_score"], "away": status_snapshot["regulation_away_score"]},
            "extratime": {"home": status_snapshot["extra_time_home_score"], "away": status_snapshot["extra_time_away_score"]},
            "penalty": {"home": status_snapshot["penalty_home_score"], "away": status_snapshot["penalty_away_score"]},
            "regular_time": {"home": status_snapshot["regulation_home_score"], "away": status_snapshot["regulation_away_score"]},
        },
        "scores24": {
            "source_url": _absolute_prediction_url(slug),
            "slug": slug,
            "prediction_date": prediction_date,
            "listed_match_date": local_dt.isoformat(),
            "result_status": status_snapshot["status_short"],
            "result_score_source": "Scores24" if status_snapshot["home_score"] is not None else "",
            "regulation_score": {"home": status_snapshot["regulation_home_score"], "away": status_snapshot["regulation_away_score"]},
            "had_extra_time": bool(status_snapshot.get("had_extra_time")),
            "had_penalties": bool(status_snapshot.get("had_penalties")),
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
    if odd is None or odd <= 0.0:
        return None
    # GraphQL's match.uniqueTournament fragment usually contains only the
    # tournament slug/name and omits its country.  The outer league object does
    # contain the country.  Replacing the outer object with uniqueTournament
    # made every domestic top division look country-less and the league policy
    # rejected all of its prediction rows.  Merge both contexts instead.
    outer_league = league_data if isinstance(league_data, dict) else {}
    match_league = match_data.get("uniqueTournament") if isinstance(match_data.get("uniqueTournament"), dict) else {}
    merged_league = dict(outer_league)
    for key, value in match_league.items():
        if value not in (None, "", {}, []):
            merged_league[key] = value
    match_country = match_data.get("country") if isinstance(match_data.get("country"), dict) else {}
    if not isinstance(merged_league.get("country"), dict) or not merged_league.get("country"):
        if match_country:
            merged_league["country"] = dict(match_country)
        elif isinstance(outer_league.get("country"), dict):
            merged_league["country"] = dict(outer_league.get("country") or {})
    league_data = merged_league
    league_sport = str(league_data.get("sportSlug") or "").strip().lower()
    if league_sport and league_sport != "soccer":
        return None
    if not _is_selected_prediction_league(league_data):
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
    row = {
        "source": "Scores24",
        "source_url": source_url,
        "source_page": source_page,
        "source_slug": slug,
        "scores24_league_slug": str(league_data.get("slug") or "").strip().lower(),
        "scores24_league_name": str(league_data.get("name") or "").strip(),
        "scores24_country_slug": str((((match_data.get("country") or league_data.get("country") or {}).get("slug")) or "")).strip().lower(),
        "scores24_country_name": str((((match_data.get("country") or league_data.get("country") or {}).get("name")) or "")).strip(),
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
    status_snapshot = _scores24_status_from_match_data(match_data)
    if status_snapshot["status_short"] in (_LIVE_STATUSES | _TERMINAL_STATUSES | _POSTPONED_STATUSES):
        row = _apply_match_status(row, {
            **status_snapshot,
            "source_url": source_url,
            "source_slug": slug,
            "date": (fixture_item.get("fixture") or {}).get("date"),
            "timestamp": (fixture_item.get("fixture") or {}).get("timestamp"),
        })
    return row


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
    snapshot = _scores24_status_from_match_data(match_data)
    return {
        "source_url": source_url,
        "source_slug": str(match_data.get("slug") or ""),
        "date": local_dt.isoformat(),
        "timestamp": int(local_dt.timestamp()),
        **snapshot,
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
    fixture_item["score"] = {
        "fulltime": {"home": status.get("regulation_home_score"), "away": status.get("regulation_away_score")},
        "regular_time": {"home": status.get("regulation_home_score"), "away": status.get("regulation_away_score")},
        "extratime": {"home": status.get("extra_time_home_score"), "away": status.get("extra_time_away_score")},
        "penalty": {"home": status.get("penalty_home_score"), "away": status.get("penalty_away_score")},
    }
    if status.get("regulation_home_score") is not None and status.get("regulation_away_score") is not None:
        updated["regulation_home_score"] = int(status.get("regulation_home_score"))
        updated["regulation_away_score"] = int(status.get("regulation_away_score"))
    scores24_meta = dict(fixture_item.get("scores24") or {})
    scores24_meta["result_status"] = status.get("status_short") or ""
    scores24_meta["result_score_source"] = "Scores24" if home_score is not None and away_score is not None else ""
    scores24_meta["result_checked_at"] = int(time.time())
    scores24_meta["regulation_score"] = {"home": status.get("regulation_home_score"), "away": status.get("regulation_away_score")}
    scores24_meta["had_extra_time"] = bool(status.get("had_extra_time"))
    scores24_meta["had_penalties"] = bool(status.get("had_penalties"))
    fixture_item["scores24"] = scores24_meta
    updated["fixture_item"] = fixture_item
    if home_score is not None and away_score is not None:
        updated["result_score_source"] = "Scores24"
        updated["scores24_score_saved_at"] = int(time.time())
    if status.get("finished"):
        settlement, won, result_code = _settle_row(updated, home_score, away_score)
        updated["settlement"] = settlement
        updated["won"] = won
        updated["result_code"] = result_code or (f"{home_score}:{away_score}" if home_score is not None and away_score is not None else "")
        updated["result_pending"] = not bool(settlement)
        updated["scores24_final_score_saved"] = home_score is not None and away_score is not None
        if not _final_snapshot_saved(updated):
            updated["prediction_match_state"] = "FINAL_PENDING"
            updated["next_check_at"] = 0
    elif status.get("live"):
        updated["prediction_match_state"] = "LIVE"
    elif status.get("cancelled"):
        updated["prediction_match_state"] = "CANCELLED"
    elif status.get("postponed"):
        updated["prediction_match_state"] = "POSTPONED"
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
    """Persist one strict API-Football snapshot for a prediction match.

    PREMATCH metadata/statistics is saved once, LIVE is refreshed at the
    five-minute cadence, and a successful FINAL snapshot is frozen forever.
    """
    updated = dict(row)
    previous_fixture_item = updated.get("fixture_item") or {}
    previous_fixture = (previous_fixture_item.get("fixture") or {}) if isinstance(previous_fixture_item, dict) else {}
    previous_status_short = str(((previous_fixture.get("status") or {}).get("short") or "")).upper()
    previous_goals = (previous_fixture_item.get("goals") or {}) if isinstance(previous_fixture_item, dict) else {}
    previous_score_available = previous_goals.get("home") is not None and previous_goals.get("away") is not None
    previous_scores24_final = bool(
        updated.get("scores24_final_score_saved")
        or (previous_status_short in _FINISHED_STATUSES and previous_score_available and updated.get("result_score_source") == "Scores24")
    )
    preserved_scores24_final = False
    try:
        fixture_item = json.loads(json.dumps(api_item, ensure_ascii=False))
    except Exception:
        fixture_item = dict(api_item)
    fixture = fixture_item.get("fixture") or {}
    fixture_id = int(fixture.get("id") or 0)
    if not fixture_id:
        return updated
    now_ts = int(time.time())

    api_status_short = str((((fixture.get("status") or {}).get("short") or ""))).upper()
    api_goals = fixture_item.get("goals") or {}
    api_score_available = api_goals.get("home") is not None and api_goals.get("away") is not None
    if previous_scores24_final and (api_status_short not in _FINISHED_STATUSES or not api_score_available):
        # API-Football may not cover the match result yet. Keep the exact final
        # score already saved from Scores24 while retaining API teams/logos and
        # any later statistics/card data.
        fixture_item = _overlay_scores24_score(fixture_item, previous_fixture_item)
        fixture = fixture_item.get("fixture") or fixture
        preserved_scores24_final = True

    if statistics is None and isinstance(previous_fixture_item, dict):
        previous_stats = previous_fixture_item.get("statistics")
        if isinstance(previous_stats, list):
            fixture_item["statistics"] = previous_stats
        for field in ("statistics_updated_at", "statistics_snapshot_state"):
            if previous_fixture_item.get(field) is not None:
                fixture_item[field] = previous_fixture_item.get(field)

    prediction_date = _row_prediction_date(updated)
    scores24_meta = dict((previous_fixture_item.get("scores24") or {}) if isinstance(previous_fixture_item, dict) else {})
    scores24_meta.update({
        "source_url": str(updated.get("source_url") or ""),
        "slug": str(updated.get("source_slug") or ""),
        "prediction_key": str(updated.get("prediction_key") or ""),
        "prediction_date": prediction_date,
        "api_fixture_date": str(fixture.get("date") or "")[:10],
    })
    fixture_item["scores24"] = scores24_meta
    if prediction_date:
        updated["scores24_prediction_date"] = prediction_date
        updated["prediction_date"] = prediction_date
    fixture_item["data_source"] = "API-Football"
    updated["fixture_item"] = fixture_item
    updated["fixture_id"] = fixture_id
    updated["api_football_fixture_id"] = fixture_id
    updated["api_football_match_confidence"] = round(float(confidence or 0.0), 4)
    updated["api_football_matched_at"] = int(updated.get("api_football_matched_at") or now_ts)
    updated["api_football_last_fixture_refresh_at"] = now_ts
    updated["api_football_match_status"] = "score-only-fallback" if preserved_scores24_final else "matched"
    updated["match_data_source"] = "API-Football card + Scores24 score" if preserved_scores24_final else "API-Football"
    if preserved_scores24_final:
        updated["result_score_source"] = "Scores24"
        updated["scores24_final_score_saved"] = True
    updated["api_football_discovery_failures"] = 0

    status = fixture.get("status") or {}
    status_short = str(status.get("short") or "").upper()
    updated["last_status_short"] = status_short
    goals = fixture_item.get("goals") or {}
    home_score = goals.get("home")
    away_score = goals.get("away")
    api_fulltime = ((fixture_item.get("score") or {}).get("fulltime") or {})
    if status_short in _FINISHED_STATUSES and api_fulltime.get("home") is not None and api_fulltime.get("away") is not None:
        try:
            updated["regulation_home_score"] = int(api_fulltime.get("home"))
            updated["regulation_away_score"] = int(api_fulltime.get("away"))
            score_map = dict(fixture_item.get("score") or {})
            score_map["regular_time"] = {"home": updated["regulation_home_score"], "away": updated["regulation_away_score"]}
            fixture_item["score"] = score_map
        except Exception:
            pass
    kickoff = int(fixture.get("timestamp") or 0)

    if statistics is not None:
        fixture_item["statistics"] = statistics
        fixture_item["statistics_updated_at"] = now_ts
        updated["api_football_last_statistics_refresh_at"] = now_ts
        updated["api_football_statistics_failures"] = 0
        updated.pop("api_football_statistics_error", None)

    if status_short in _LIVE_STATUSES:
        updated["api_football_last_live_refresh_at"] = now_ts
        updated["live_updated_at"] = now_ts
        updated["next_check_at"] = now_ts + LIVE_REFRESH_SECONDS
        updated["prediction_match_state"] = "LIVE"
        if statistics is not None:
            fixture_item["statistics_snapshot_state"] = "live"
    elif status_short in _FINISHED_STATUSES:
        # A final snapshot is considered complete only after the final
        # statistics endpoint answered successfully (an empty list is valid).
        if statistics is not None:
            updated["api_football_final_snapshot_saved"] = True
            updated["api_football_final_snapshot_saved_at"] = now_ts
            updated["final_checked_at"] = now_ts
            updated["next_check_at"] = 0
            updated["prediction_match_state"] = "FINALIZED"
            fixture_item["statistics_snapshot_state"] = "final"
        else:
            updated["api_football_final_snapshot_saved"] = False
            updated["next_check_at"] = now_ts + FINAL_DETAILS_REFRESH_SECONDS
            updated["prediction_match_state"] = "FINAL_PENDING"
    elif status_short in _CANCELLED_STATUSES:
        updated["api_football_final_snapshot_saved"] = True
        updated["api_football_final_snapshot_saved_at"] = now_ts
        updated["final_checked_at"] = now_ts
        updated["next_check_at"] = 0
        updated["prediction_match_state"] = "CANCELLED"
        updated["settlement"] = "push"
        updated["won"] = None
        updated["result_code"] = "Матч отменён"
        updated["result_pending"] = False
    elif status_short in _POSTPONED_STATUSES:
        updated["prediction_match_state"] = "POSTPONED"
        updated["next_check_at"] = now_ts + POSTPONED_RECHECK_SECONDS
    else:
        updated["api_football_initial_snapshot_saved"] = True
        updated["api_football_initial_snapshot_saved_at"] = int(
            updated.get("api_football_initial_snapshot_saved_at") or now_ts
        )
        updated["prematch_loaded_at"] = int(updated.get("prematch_loaded_at") or now_ts)
        updated["prediction_match_state"] = "PREMATCH_SAVED"
        # No API-Football polling before kickoff. The next shared fixture-status
        # lookup becomes due at kickoff; statistics are not fetched again.
        updated["next_check_at"] = max(now_ts + LIVE_REFRESH_SECONDS, kickoff) if kickoff else now_ts + LIVE_REFRESH_SECONDS
        if statistics is not None:
            updated["api_football_initial_statistics_saved"] = True
            updated["api_football_initial_statistics_saved_at"] = int(
                updated.get("api_football_initial_statistics_saved_at") or now_ts
            )
            fixture_item["statistics_snapshot_state"] = "prematch"

    updated["fixture_item"] = fixture_item

    if status_short in _FINISHED_STATUSES:
        settlement, won, result_code = _settle_row(updated, home_score, away_score)
        if not settlement and statistics is not None:
            settlement, won, result_code = _settle_stat_market(updated, statistics)
        updated["settlement"] = settlement
        updated["won"] = won
        updated["result_code"] = result_code or (
            f"{home_score}:{away_score}" if home_score is not None and away_score is not None else ""
        )
        updated["result_pending"] = not bool(settlement)
    elif status_short not in _CANCELLED_STATUSES:
        updated["result_pending"] = False
    return updated


def _apply_igscore_snapshot(row: dict[str, Any], snapshot: dict[str, Any]) -> dict[str, Any]:
    """Merge one exact IGScore LIVE/final snapshot into a saved prediction.

    Team/league identity from API-Football (or the original Scores24 card) is
    preserved.  Only score, status, statistics and events from the resolved
    IGScore match are overlaid.  This prevents provider IDs from being mixed and
    prevents a random match timeline from appearing in a prediction card.
    """
    updated = dict(row)
    try:
        fixture_item = json.loads(json.dumps(updated.get("fixture_item") or {}, ensure_ascii=False))
    except Exception:
        fixture_item = dict(updated.get("fixture_item") or {})
    fixture = dict(fixture_item.get("fixture") or {})
    now_ts = int(snapshot.get("fetched_at") or time.time())
    match_id = str(snapshot.get("match_id") or "").strip()
    if not match_id:
        return updated

    confidence = _float(snapshot.get("confidence")) or 0.0
    updated["igscore_match_id"] = match_id
    updated["igscore_match_confidence"] = round(confidence, 4)
    updated["igscore_matched_at"] = int(updated.get("igscore_matched_at") or now_ts)
    updated["igscore_last_refresh_at"] = now_ts
    updated["igscore_status_rule_version"] = IGSCORE_STATUS_RULE_VERSION
    updated["igscore_match_status"] = "matched"
    updated["igscore_discovery_failures"] = 0
    updated.pop("igscore_error", None)

    status_short = str(snapshot.get("status_short") or "").upper()
    status_long = str(snapshot.get("status_long") or "")
    elapsed = snapshot.get("elapsed")
    current_short = str(((fixture.get("status") or {}).get("short") or "")).upper()
    status_verified_live = bool(snapshot.get("status_verified_live"))
    # A verified exact match in IGScore's current LIVE list is allowed to
    # correct a previously false terminal status. Otherwise retain the normal
    # protection against downgrading genuine completed matches.
    if status_short and (
        status_verified_live
        or status_short in (_TERMINAL_STATUSES | _POSTPONED_STATUSES)
        or current_short not in _TERMINAL_STATUSES
    ):
        fixture["status"] = {
            "short": status_short,
            "long": status_long or ("Match Finished" if status_short in _FINISHED_STATUSES else "Live" if status_short in _LIVE_STATUSES else status_short),
            "elapsed": elapsed,
        }

    home_score = snapshot.get("home_score")
    away_score = snapshot.get("away_score")
    regulation_home = snapshot.get("regulation_home_score")
    regulation_away = snapshot.get("regulation_away_score")
    if regulation_home is not None and regulation_away is not None:
        try:
            updated["regulation_home_score"] = int(regulation_home)
            updated["regulation_away_score"] = int(regulation_away)
        except Exception:
            pass
    score_available = home_score is not None and away_score is not None
    saved_scores24_final = bool(updated.get("scores24_final_score_saved"))
    snapshot_final = status_short in _FINISHED_STATUSES
    if score_available and (not saved_scores24_final or snapshot_final):
        fixture_item["goals"] = {"home": home_score, "away": away_score}
        score = dict(fixture_item.get("score") or {})
        if status_short in {"AET", "PEN"}:
            score["extratime"] = {"home": home_score, "away": away_score}
            if regulation_home is not None and regulation_away is not None:
                score["fulltime"] = {"home": regulation_home, "away": regulation_away}
                score["regular_time"] = {"home": regulation_home, "away": regulation_away}
        else:
            score["fulltime"] = {"home": home_score, "away": away_score}
            score["regular_time"] = {"home": home_score, "away": away_score}
        fixture_item["score"] = score
        updated["result_score_source"] = "IGScore" if snapshot_final or not saved_scores24_final else updated.get("result_score_source")

    fixture_item["fixture"] = fixture
    ig_meta = dict(fixture_item.get("igscore") or {})
    ig_meta.update({
        "match_id": match_id,
        "confidence": round(confidence, 4),
        "source_url": str(snapshot.get("source_url") or ""),
        "updated_at": now_ts,
    })
    fixture_item["igscore"] = ig_meta

    statistics = snapshot.get("statistics")
    statistics_success = bool(snapshot.get("statistics_success"))
    if statistics_success and isinstance(statistics, list):
        existing_stats = fixture_item.get("statistics")
        # A successful empty final response is valid, but keep the latest rich
        # LIVE snapshot instead of replacing it with an empty list.
        if statistics or not isinstance(existing_stats, list) or not existing_stats:
            fixture_item["statistics"] = statistics
        fixture_item["statistics_updated_at"] = now_ts
        fixture_item["statistics_snapshot_state"] = "final" if snapshot_final else "live"
        fixture_item["statistics_data_source"] = "IGScore"
        updated["igscore_last_statistics_refresh_at"] = now_ts
        updated["igscore_statistics_failures"] = 0
        updated.pop("igscore_statistics_error", None)

    events = snapshot.get("events")
    events_success = bool(snapshot.get("events_success"))
    if events_success and isinstance(events, list):
        existing_events = fixture_item.get("events")
        if events or not isinstance(existing_events, list) or not existing_events:
            fixture_item["events"] = events
        fixture_item["events_updated_at"] = now_ts
        fixture_item["events_data_source"] = "IGScore"
        updated["igscore_last_events_refresh_at"] = now_ts

    api_id = int(updated.get("api_football_fixture_id") or 0)
    fixture_item["data_source"] = "API-Football card + IGScore LIVE" if api_id else "Scores24 card + IGScore LIVE"
    updated["match_data_source"] = fixture_item["data_source"]
    updated["fixture_item"] = fixture_item
    updated["last_status_short"] = status_short or current_short

    if status_short in _LIVE_STATUSES:
        if status_verified_live:
            # Undo any false finalization produced by the old status mapper.
            updated["final_snapshot_saved"] = False
            updated["igscore_final_snapshot_saved"] = False
            updated["api_football_final_snapshot_saved"] = False
            updated["scores24_final_score_saved"] = False
            updated.pop("final_snapshot_source", None)
            updated.pop("igscore_final_snapshot_saved_at", None)
            updated.pop("api_football_final_snapshot_saved_at", None)
            updated["final_checked_at"] = 0
            updated["settlement"] = ""
            updated["won"] = None
            updated["result_code"] = ""
            updated["result_pending"] = True
        updated["prediction_match_state"] = "LIVE"
        updated["live_updated_at"] = now_ts
        updated["igscore_last_live_refresh_at"] = now_ts
        updated["next_check_at"] = now_ts + LIVE_REFRESH_SECONDS
    elif status_short in _FINISHED_STATUSES:
        final_complete = bool(snapshot.get("final_complete"))
        updated["igscore_final_snapshot_saved"] = final_complete
        if final_complete:
            updated["igscore_final_snapshot_saved_at"] = now_ts
            updated["final_snapshot_saved"] = True
            updated["final_snapshot_source"] = "IGScore"
            updated["final_checked_at"] = now_ts
            updated["prediction_match_state"] = "FINALIZED"
            updated["next_check_at"] = 0
        else:
            updated["prediction_match_state"] = "FINAL_PENDING"
            # The score may already be saved, but final statistics/events can
            # arrive a little later. Retry quickly so the saved prediction card
            # keeps its complete "Ход матча" instead of freezing score-only.
            updated["next_check_at"] = now_ts + FINAL_DETAILS_REFRESH_SECONDS
    elif status_short in _CANCELLED_STATUSES:
        updated["final_snapshot_saved"] = True
        updated["final_snapshot_source"] = "IGScore"
        updated["igscore_final_snapshot_saved"] = True
        updated["igscore_final_snapshot_saved_at"] = now_ts
        updated["prediction_match_state"] = "CANCELLED"
        updated["next_check_at"] = 0
        updated["settlement"] = "push"
        updated["won"] = None
        updated["result_code"] = "Матч отменён"
        updated["result_pending"] = False
    elif status_short in _POSTPONED_STATUSES:
        updated["prediction_match_state"] = "POSTPONED"
        updated["next_check_at"] = now_ts + POSTPONED_RECHECK_SECONDS

    if status_short in _FINISHED_STATUSES and score_available:
        settlement, won, result_code = _settle_row(updated, home_score, away_score)
        saved_stats = fixture_item.get("statistics") if isinstance(fixture_item.get("statistics"), list) else []
        if not settlement and saved_stats:
            settlement, won, result_code = _settle_stat_market(updated, saved_stats)
        updated["settlement"] = settlement
        updated["won"] = won
        updated["result_code"] = result_code or f"{home_score}:{away_score}"
        updated["result_pending"] = not bool(settlement)
    return updated


def _group_refresh_reason(group: list[dict[str, Any]], now_ts: int) -> str:
    """Strict provider-neutral request policy for one prediction match.

    API-Football supplies the prematch card/status. IGScore supplies LIVE/final
    statistics and events. A match without API-Football coverage may still be
    driven by its saved IGScore id.
    """
    if not group:
        return "skip"
    sample = group[0]
    fixture = ((sample.get("fixture_item") or {}).get("fixture") or {})
    status = str(((fixture.get("status") or {}).get("short") or "")).upper()
    fixture_id = int(sample.get("api_football_fixture_id") or 0)
    igscore_id = str(sample.get("igscore_match_id") or "").strip()
    kickoff = int(fixture.get("timestamp") or 0)
    next_check = min(
        [int(row.get("next_check_at") or 0) for row in group if int(row.get("next_check_at") or 0) > 0]
        or [0]
    )

    needs_regulation_audit = any(
        int(row.get("settlement_rule_version") or 0) < SETTLEMENT_RULE_VERSION
        or bool(row.get("regulation_score_pending"))
        for row in group
    )
    needs_igscore_status_audit = bool(
        igscore_id
        and any(
            int(row.get("igscore_status_rule_version") or 0) < IGSCORE_STATUS_RULE_VERSION
            for row in group
        )
    )
    needs_event_audit = bool(
        igscore_id and any(_row_needs_event_audit(row) for row in group)
    )
    # One-time audit after status-mapping changes. This corrects rows that were
    # accidentally frozen as FT while the exact IGScore match was still LIVE.
    if needs_igscore_status_audit:
        return "igscore"
    if all(_final_snapshot_saved(row) for row in group) and status in _TERMINAL_STATUSES and not needs_regulation_audit and not needs_event_audit:
        return "skip"
    if status in _CANCELLED_STATUSES:
        return "skip" if all(_final_snapshot_saved(row) for row in group) else "final_status"
    if status in _FINISHED_STATUSES:
        if all(_final_snapshot_saved(row) for row in group) and not needs_regulation_audit and not needs_event_audit:
            return "skip"
        return "skip" if next_check and now_ts < next_check else "final"
    if status in _LIVE_STATUSES:
        last_live = max(
            int(
                row.get("live_updated_at")
                or row.get("igscore_last_live_refresh_at")
                or row.get("api_football_last_live_refresh_at")
                or 0
            )
            for row in group
        )
        return "live" if now_ts - last_live >= LIVE_REFRESH_SECONDS else "skip"
    if status in _POSTPONED_STATUSES:
        return "postponed" if not next_check or now_ts >= next_check else "skip"

    if not fixture_id:
        source_date = str(
            sample.get("scores24_prediction_date")
            or sample.get("prediction_date")
            or ((sample.get("fixture_item") or {}).get("scores24") or {}).get("prediction_date")
            or fixture.get("date")
            or ""
        )[:10]
        current_day_unmatched = bool(not igscore_id and source_date == _today())
        # Old versions could postpone an unmatched current-day prediction for
        # 30 minutes after one failed API-Football lookup.  Do not let that old
        # next_check value block the lightweight IGScore LIVE linker: current
        # day rows are retried once per normal worker cycle.
        if next_check and now_ts < next_check and not current_day_unmatched:
            return "skip"
        # A previously resolved IGScore match can be refreshed independently of
        # API-Football. Otherwise try both providers during discovery.
        return "igscore" if igscore_id else "discover"

    initial_saved = all(bool(row.get("api_football_initial_snapshot_saved")) for row in group)
    initial_stats_saved = all(bool(row.get("api_football_initial_statistics_saved")) for row in group)
    if not initial_saved:
        return "prematch"
    # API-Football statistics are disabled for prediction cards after the
    # IGScore migration. Keep this retry branch only for custom deployments
    # that explicitly attach an API-Football statistics loader.
    if _api_statistics_loader is not None:
        max_stat_failures = max(int(row.get("api_football_statistics_failures") or 0) for row in group)
        if not initial_stats_saved and max_stat_failures < 2 and (not next_check or now_ts >= next_check):
            return "prematch_stats"
    if kickoff and now_ts < kickoff:
        return "skip"
    last_fixture = max(int(row.get("api_football_last_fixture_refresh_at") or 0) for row in group)
    return "kickoff" if now_ts - last_fixture >= LIVE_REFRESH_SECONDS else "skip"

def _enrich_rows_with_api(
    rows: list[dict[str, Any]],
    *,
    force: bool = False,
) -> tuple[list[dict[str, Any]], int, list[str]]:
    if not rows or (_api_fixture_resolver is None and _igscore_snapshot_loader is None):
        return rows, 0, []
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        known_api = int(row.get("api_football_fixture_id") or 0)
        known_ig = str(row.get("igscore_match_id") or "").strip()
        if known_api:
            key = f"api:{known_api}"
        elif known_ig:
            key = f"ig:{known_ig}"
        else:
            key = str(row.get("source_slug") or row.get("prediction_key") or "")
        if key:
            grouped.setdefault(key, []).append(row)

    processed = 0
    warnings: list[str] = []
    out: list[dict[str, Any]] = []
    now_ts = int(time.time())

    for key, group in grouped.items():
        sample = group[0]
        refresh_reason = _group_refresh_reason(group, now_ts)
        if refresh_reason == "skip":
            out.extend(dict(row) for row in group)
            continue

        api_item: dict[str, Any] | None = None
        api_attempted = False
        # An IGScore-only row does not need a wasted direct API-Football lookup
        # every five minutes. Discovery/kickoff/final rows still refresh the
        # shared API-Football fixture card when possible.
        if _api_fixture_resolver is not None and refresh_reason != "igscore":
            api_attempted = True
            try:
                api_item = _api_fixture_resolver(
                    sample,
                    bool(force and refresh_reason in {"discover", "live", "kickoff", "final", "final_status"}),
                )
            except Exception as exc:
                warnings.append(f"API-Football: {_compact_error(exc)}")
                api_item = None

        base_group: list[dict[str, Any]] = []
        status_short = _status_short_from_row(sample)
        if api_item:
            confidence = _float(api_item.get("_scores24_match_confidence")) or 0.0
            api_item = dict(api_item)
            api_item.pop("_scores24_match_confidence", None)
            fixture_id = int(((api_item.get("fixture") or {}).get("id") or 0))
            status_short = str((((api_item.get("fixture") or {}).get("status") or {}).get("short") or "")).upper()
            previous_initial_stats = all(bool(row.get("api_football_initial_statistics_saved")) for row in group)
            needs_api_stats = bool(
                fixture_id
                and _api_statistics_loader is not None
                and (
                    status_short in _LIVE_STATUSES
                    or status_short in _FINISHED_STATUSES
                    or (
                        refresh_reason in {"discover", "prematch", "prematch_stats"}
                        and status_short not in (_LIVE_STATUSES | _TERMINAL_STATUSES | _POSTPONED_STATUSES)
                        and not previous_initial_stats
                    )
                )
            )
            api_stats: list[dict[str, Any]] | None = None
            api_stats_error = ""
            if needs_api_stats:
                try:
                    api_stats = _api_statistics_loader(
                        fixture_id,
                        status_short in _LIVE_STATUSES or status_short in _FINISHED_STATUSES,
                    )
                    if api_stats is None:
                        api_stats = []
                except Exception as exc:
                    api_stats_error = _compact_error(exc)
                    warnings.append(f"API-Football статистика: {api_stats_error}")
                    api_stats = None

            for row in group:
                updated = _apply_api_fixture(row, api_item, statistics=api_stats, confidence=confidence)
                # When API-Football is used only for the card/status, mark the
                # prematch-statistics stage complete so it is not requested on
                # every worker cycle. LIVE statistics come from IGScore.
                if _api_statistics_loader is None and status_short not in (_LIVE_STATUSES | _FINISHED_STATUSES):
                    updated["api_football_initial_statistics_saved"] = True
                    updated["api_football_initial_statistics_saved_at"] = int(
                        updated.get("api_football_initial_statistics_saved_at") or now_ts
                    )
                if api_stats_error:
                    failures = int(updated.get("api_football_statistics_failures") or 0) + 1
                    updated["api_football_statistics_failures"] = failures
                    updated["api_football_statistics_error"] = api_stats_error
                    updated["next_check_at"] = now_ts + (
                        LIVE_REFRESH_SECONDS if status_short in (_LIVE_STATUSES | _FINISHED_STATUSES) else MATCH_DISCOVERY_RETRY_SECONDS
                    )
                base_group.append(updated)
        else:
            for row in group:
                copy = dict(row)
                if api_attempted:
                    failures = int(copy.get("api_football_discovery_failures") or 0) + 1
                    copy["api_football_discovery_attempted_at"] = now_ts
                    copy["api_football_discovery_failures"] = failures
                    if copy.get("api_football_fixture_id"):
                        copy["api_football_match_status"] = "stale"
                    else:
                        copy["api_football_match_status"] = "unmatched"
                base_group.append(copy)

        # IGScore is queried only around/after kickoff, for an already resolved
        # IGScore match, or when API/Scores24 says the match is LIVE/final. The
        # callback itself shares one cached competition list across all rows.
        ig_attempted = False
        ig_snapshot: dict[str, Any] | None = None
        ig_error = ""
        ig_sample = base_group[0] if base_group else sample
        ig_fixture = ((ig_sample.get("fixture_item") or {}).get("fixture") or {})
        ig_status = str(((ig_fixture.get("status") or {}).get("short") or status_short or "")).upper()
        kickoff = int(ig_fixture.get("timestamp") or 0)
        known_ig = str(ig_sample.get("igscore_match_id") or "").strip()
        source_date = str(
            ig_sample.get("scores24_prediction_date")
            or ig_sample.get("prediction_date")
            or ((ig_sample.get("fixture_item") or {}).get("scores24") or {}).get("prediction_date")
            or ig_fixture.get("date")
            or ""
        )[:10]
        current_day_unmatched = bool(
            not known_ig
            and source_date == _today()
        )
        should_try_ig = bool(
            _igscore_snapshot_loader is not None
            and (
                known_ig
                or current_day_unmatched
                or ig_status in (_LIVE_STATUSES | _FINISHED_STATUSES | _CANCELLED_STATUSES)
                or refresh_reason in {"live", "final", "final_status", "igscore"}
                or (kickoff and now_ts >= kickoff - 10 * 60)
            )
        )
        if should_try_ig:
            ig_attempted = True
            try:
                ig_snapshot = _igscore_snapshot_loader(ig_sample, api_item, bool(force))
            except Exception as exc:
                ig_error = _compact_error(exc)
                warnings.append(f"IGScore: {ig_error}")
                ig_snapshot = None

        if ig_snapshot:
            base_group = [_apply_igscore_snapshot(row, ig_snapshot) for row in base_group]
        elif ig_attempted:
            adjusted: list[dict[str, Any]] = []
            for row in base_group:
                copy = dict(row)
                failures = int(copy.get("igscore_discovery_failures") or 0) + 1
                copy["igscore_discovery_attempted_at"] = now_ts
                copy["igscore_discovery_failures"] = failures
                if ig_error:
                    copy["igscore_error"] = ig_error
                # Near kickoff and during LIVE/final, retry in five minutes. Far
                # prematch misses use the slower discovery backoff.
                row_fixture = ((copy.get("fixture_item") or {}).get("fixture") or {})
                row_status = str(((row_fixture.get("status") or {}).get("short") or "")).upper()
                row_kickoff = int(row_fixture.get("timestamp") or 0)
                row_source_date = str(
                    copy.get("scores24_prediction_date")
                    or copy.get("prediction_date")
                    or ((copy.get("fixture_item") or {}).get("scores24") or {}).get("prediction_date")
                    or row_fixture.get("date")
                    or ""
                )[:10]
                urgent = bool(
                    row_source_date == _today()
                    or row_status in (_LIVE_STATUSES | _FINISHED_STATUSES)
                    or (row_kickoff and now_ts >= row_kickoff - 10 * 60)
                )
                if row_status in _FINISHED_STATUSES:
                    # Final score/details must never fall back to the old hourly
                    # retry.  Recheck final status/events within about two minutes
                    # so settled predictions move to "Завершённые" promptly.
                    retry_seconds = FINAL_DETAILS_REFRESH_SECONDS
                else:
                    retry_seconds = LIVE_REFRESH_SECONDS if urgent else MATCH_DISCOVERY_RETRY_SECONDS
                copy["next_check_at"] = now_ts + retry_seconds
                if not copy.get("api_football_fixture_id") and not copy.get("igscore_match_id"):
                    copy["prediction_match_state"] = "UNMATCHED"
                    copy["match_data_source"] = "Scores24 metadata; IGScore match pending"
                adjusted.append(copy)
            base_group = adjusted

        out.extend(base_group)
        if api_attempted or ig_attempted:
            processed += 1

    grouped_ids = {id(row) for group in grouped.values() for row in group}
    out.extend(row for row in rows if id(row) not in grouped_ids)
    return out, processed, list(dict.fromkeys(warnings))[-6:]

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


def _stored_api_match_plausible(row: dict[str, Any]) -> bool:
    """Check that a stored API fixture still matches the original Scores24 slug.

    Old versions could treat a synthetic Scores24 numeric id as an API fixture id.
    Such rows must be rediscovered instead of preserving an unrelated match.
    """
    if not row.get("api_football_fixture_id"):
        return False
    fixture_item = row.get("fixture_item") or {}
    teams = fixture_item.get("teams") or {}
    home_name = str((teams.get("home") or {}).get("name") or "")
    away_name = str((teams.get("away") or {}).get("name") or "")
    source_slug = str(row.get("source_slug") or "")
    if not (source_slug and home_name and away_name):
        return False
    # Local implementation mirrors the API resolver's ordered slug check without
    # importing the web application and creating a circular dependency.
    body = re.sub(r"^(?:lm-)?\d{2}-\d{2}-\d{4}-", "", source_slug.strip().lower())
    slug_tokens = [token for token in re.sub(r"[^a-z0-9]+", " ", body).split() if token]
    if len(slug_tokens) < 2:
        return False

    def norm_tokens(value: str) -> list[str]:
        text = unicodedata.normalize("NFKD", str(value or "").lower())
        text = "".join(ch for ch in text if not unicodedata.combining(ch))
        text = re.sub(r"[^a-z0-9]+", " ", text)
        stop = {"fc", "cf", "sc", "afc", "fk", "ac", "club", "football", "futbol", "the", "de", "cd", "sd"}
        tokens = [token for token in text.split() if token not in stop]
        return tokens or text.split()

    def similarity(left: str, right: str) -> float:
        a, b = norm_tokens(left), norm_tokens(right)
        if not a or not b:
            return 0.0
        sa, sb = set(a), set(b)
        overlap = len(sa & sb)
        precision = overlap / max(1, len(sa))
        recall = overlap / max(1, len(sb))
        f1 = 2 * precision * recall / max(1e-9, precision + recall)
        seq = difflib.SequenceMatcher(None, " ".join(a), " ".join(b)).ratio()
        contains = 1.0 if " ".join(a) in " ".join(b) or " ".join(b) in " ".join(a) else 0.0
        return max(f1, seq * 0.92, contains * 0.94)

    best = 0.0
    for split in range(1, len(slug_tokens)):
        home_score = similarity(" ".join(slug_tokens[:split]), home_name)
        away_score = similarity(" ".join(slug_tokens[split:]), away_name)
        if min(home_score, away_score) < 0.35:
            continue
        best = max(best, (home_score + away_score) / 2)
    return best >= 0.62


def _overlay_scores24_score(previous_fixture: dict[str, Any], scores24_fixture: dict[str, Any]) -> dict[str, Any]:
    """Keep the rich API fixture while accepting a newer Scores24 score/status."""
    merged = dict(previous_fixture or {})
    incoming_fixture = dict((scores24_fixture or {}).get("fixture") or {})
    incoming_status = str(((incoming_fixture.get("status") or {}).get("short") or "")).upper()
    incoming_goals = dict((scores24_fixture or {}).get("goals") or {})
    home_score, away_score = incoming_goals.get("home"), incoming_goals.get("away")
    has_score = home_score is not None and away_score is not None
    if incoming_status in (_LIVE_STATUSES | _TERMINAL_STATUSES | _POSTPONED_STATUSES) or has_score:
        current_fixture = dict(merged.get("fixture") or {})
        if incoming_fixture.get("date"):
            current_fixture.setdefault("date", incoming_fixture.get("date"))
        if incoming_fixture.get("timestamp"):
            current_fixture.setdefault("timestamp", incoming_fixture.get("timestamp"))
        if incoming_status:
            current_fixture["status"] = dict(incoming_fixture.get("status") or {})
        merged["fixture"] = current_fixture
        if has_score:
            merged["goals"] = {"home": home_score, "away": away_score}
            merged_score = dict(merged.get("score") or {})
            incoming_score = (scores24_fixture or {}).get("score") or {}
            for score_key in ("fulltime", "regular_time", "regulation", "extratime", "penalty"):
                pair = incoming_score.get(score_key)
                if isinstance(pair, dict) and (pair.get("home") is not None or pair.get("away") is not None):
                    merged_score[score_key] = dict(pair)
            merged["score"] = merged_score
        meta = dict(merged.get("scores24") or {})
        meta.update(dict((scores24_fixture or {}).get("scores24") or {}))
        merged["scores24"] = meta
    return merged


def _merge_new_row(rows: dict[str, dict[str, Any]], row: dict[str, Any]) -> bool:
    key = str(row.get("prediction_key") or "")
    if not key:
        return False
    previous = rows.get(key)
    before = json.dumps(previous, ensure_ascii=False, sort_keys=True, default=str) if previous else ""
    if previous:
        row["odd"] = previous.get("odd", row.get("odd"))
        row["bookmaker"] = previous.get("bookmaker", row.get("bookmaker"))
        row["bookmaker_logo"] = previous.get("bookmaker_logo", row.get("bookmaker_logo"))
        row["published_at"] = previous.get("published_at", row.get("published_at"))
        row["global_selected"] = bool(previous.get("global_selected", row.get("global_selected", False)))
        row["global_selected_at"] = previous.get("global_selected_at", row.get("global_selected_at"))
        for field in ("scores24_prediction_date", "prediction_date"):
            if previous.get(field):
                row[field] = previous.get(field)
        old_status = ((((previous.get("fixture_item") or {}).get("fixture") or {}).get("status") or {}).get("short"))
        previous_match_plausible = bool(
            (previous.get("api_football_fixture_id") and _stored_api_match_plausible(previous))
            or (
                str(previous.get("igscore_match_id") or "").strip()
                and (_float(previous.get("igscore_match_confidence")) or 0.0) >= 0.68
            )
        )
        if previous_match_plausible:
            scores24_fixture = dict(row.get("fixture_item") or {})
            scores24_status = str(((((scores24_fixture.get("fixture") or {}).get("status") or {}).get("short") or ""))).upper()
            scores24_goals = dict(scores24_fixture.get("goals") or {})
            scores24_has_score = scores24_goals.get("home") is not None and scores24_goals.get("away") is not None
            row["fixture_item"] = _overlay_scores24_score(
                dict(previous.get("fixture_item") or {}),
                scores24_fixture,
            )
            for field in (
                "fixture_id", "api_football_fixture_id", "api_football_match_confidence",
                "api_football_matched_at", "match_data_source", "api_football_match_status",
                "api_football_last_fixture_refresh_at", "api_football_last_live_refresh_at",
                "api_football_last_statistics_refresh_at",
                "api_football_initial_snapshot_saved", "api_football_initial_snapshot_saved_at",
                "api_football_initial_statistics_saved", "api_football_initial_statistics_saved_at",
                "api_football_final_snapshot_saved", "api_football_final_snapshot_saved_at",
                "prediction_match_state", "next_check_at", "prematch_loaded_at",
                "live_updated_at", "final_checked_at", "last_status_short",
                "api_football_discovery_attempted_at", "api_football_discovery_failures",
                "igscore_match_id", "igscore_match_confidence", "igscore_matched_at",
                "igscore_last_refresh_at", "igscore_last_live_refresh_at",
                "igscore_last_statistics_refresh_at", "igscore_last_events_refresh_at",
                "igscore_final_snapshot_saved", "igscore_final_snapshot_saved_at",
                "igscore_discovery_attempted_at", "igscore_discovery_failures",
                "igscore_match_status", "igscore_error", "igscore_statistics_error",
                "igscore_status_rule_version",
                "final_snapshot_saved", "final_snapshot_source",
            ):
                if field in previous:
                    row[field] = previous.get(field)
            # The score from the current Scores24 scan is newer than an old
            # prematch/API status. Keep the API IDs and rich card, but move the
            # lifecycle to LIVE/FINAL_PENDING immediately.
            if scores24_has_score:
                row["result_score_source"] = "Scores24"
                row["scores24_score_saved_at"] = int(time.time())
            if scores24_status in _FINISHED_STATUSES:
                row["scores24_final_score_saved"] = scores24_has_score
                row["last_status_short"] = scores24_status
                if not _final_snapshot_saved(previous):
                    row["prediction_match_state"] = "FINAL_PENDING"
                    row["next_check_at"] = 0
            elif scores24_status in _LIVE_STATUSES:
                row["last_status_short"] = scores24_status
                row["prediction_match_state"] = "LIVE"
                row["next_check_at"] = 0
            elif scores24_status in _POSTPONED_STATUSES:
                row["last_status_short"] = scores24_status
                row["prediction_match_state"] = "POSTPONED"
        if old_status in {"FT", "AET", "PEN"} and previous_match_plausible:
            row["fixture_item"] = previous.get("fixture_item")
            row["settlement"] = previous.get("settlement")
            row["won"] = previous.get("won")
            row["result_code"] = previous.get("result_code")
            row["result_pending"] = previous.get("result_pending", False)
    rows[key] = row
    after = json.dumps(row, ensure_ascii=False, sort_keys=True, default=str)
    return before != after


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


def _discover_category_rows(*, force: bool = False, full_categories: bool = False) -> tuple[list[dict[str, Any]], int, int, list[str], bool]:
    """Compatibility wrapper for the direct Scores24 GraphQL collector.

    ``full_categories`` is retained because older callers pass it, but GraphQL
    already returns the complete current league catalogue in one operation.
    """
    return _graphql_discover_rows(force=force)

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
        with _storage_merge_lock:
            old = _storage_get(date_text) or {}
            merged = _existing_rows(old)
            before = len(merged)
            changed = False
            for row in new_rows:
                changed = _merge_new_row(merged, row) or changed
            if not changed and len(merged) == before and old:
                continue
            old_warnings = list(old.get("warnings") or [])
            payload = _recalculate(date_text, list(merged.values()), old_warnings + list(warnings or []))
            payload["historical_recovery"] = date_text < _today()
            payload["raw_discovery_saved_at"] = int(time.time())
            payload["raw_discovery_interval_seconds"] = DISCOVERY_REFRESH_SECONDS
            _storage_save(date_text, payload, is_final=False)
            saved_dates += 1
            saved_rows += max(0, len(merged) - before)
    return {"dates": saved_dates, "rows": saved_rows}


def _restore_saved_date_from_database(date_text: str, *, force: bool = False) -> dict[str, Any] | None:
    """Rebuild one calendar day from every Scores24 row already in PostgreSQL.

    Earlier builds sometimes saved a prediction under the date embedded in the
    Scores24 URL instead of the real local fixture day.  The Mini App asks for
    an exact calendar date, so collect all saved rows whose actual match date
    equals ``date_text``, merge them, and persist the corrected day.  No network
    request is made here.
    """
    if not _valid_date(date_text) or date_text < HISTORY_START_DATE:
        return None

    now = time.time()
    with _database_date_restore_lock:
        last = float(_database_date_restore_at.get(date_text) or 0.0)
        if not force and now - last < _DATABASE_HISTORY_RESTORE_SECONDS:
            return _storage_get(date_text)
        _database_date_restore_at[date_text] = now

    with _storage_merge_lock:
        records = _storage_records_full()
        exact_payload = next((payload for payload in records if str(payload.get("date") or "") == date_text), None)
        warnings: list[str] = []
        merged: dict[str, dict[str, Any]] = {}
        source_dates: set[str] = set()

        for payload in records:
            source_date = str(payload.get("date") or "")
            warnings.extend(payload.get("warnings") or [])
            for raw_row in payload.get("feed") or []:
                if not isinstance(raw_row, dict):
                    continue
                row = dict(raw_row)
                if _row_match_date(row) != date_text:
                    continue
                if not _row_is_selected_prediction_league(row):
                    continue
                row["match_date"] = date_text
                fixture_item = dict(row.get("fixture_item") or {})
                scores24_meta = dict(fixture_item.get("scores24") or {})
                scores24_meta["match_date"] = date_text
                fixture_item["scores24"] = scores24_meta
                row["fixture_item"] = fixture_item
                key = str(row.get("prediction_key") or row.get("source_slug") or f"row:{len(merged)}")
                previous = merged.get(key)
                if previous is None:
                    merged[key] = row
                else:
                    _merge_new_row(merged, row)
                source_dates.add(source_date)

        # Keep only rows that really belong to this calendar day.  This also
        # removes duplicates left in a wrong tab by old builds.
        rebuilt = _recalculate(date_text, list(merged.values()), warnings)
        rebuilt["database_history_restore"] = True
        rebuilt["database_history_source_dates"] = sorted(d for d in source_dates if d)
        rebuilt["database_history_rows"] = len(merged)

        old_rows = [row for row in ((exact_payload or {}).get("feed") or []) if isinstance(row, dict)]
        old_keys = {str(row.get("prediction_key") or row.get("source_slug") or "") for row in old_rows}
        new_keys = {str(row.get("prediction_key") or row.get("source_slug") or "") for row in merged.values()}
        old_wrong_date = any(_row_match_date(dict(row)) != date_text for row in old_rows)
        changed = old_keys != new_keys or old_wrong_date

        if changed:
            is_past = date_text < _today()
            all_finished = _rows_fully_finalized(rebuilt.get("feed") or [])
            _storage_save(
                date_text,
                rebuilt,
                is_final=bool(is_past and all_finished and not rebuilt.get("pending_result_total")),
            )
            print(
                f"[scores24] database history restored date={date_text} "
                f"rows={len(merged)} source_dates={','.join(sorted(source_dates)) or '-'}"
            )
            return rebuilt

        return exact_payload or rebuilt


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
        all_finished = _rows_fully_finalized(rows)
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
            needs_regulation_audit = int(row.get("settlement_rule_version") or 0) < SETTLEMENT_RULE_VERSION or bool(row.get("regulation_score_pending"))
            if not source_url or (status in {"FT", "AET", "PEN"} and not needs_regulation_audit):
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
            page_html = _fetch_text(source_url, force=False, ttl=SCORE_FALLBACK_REFRESH_SECONDS)
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
    """Process only dates that contain at least one provider match due now."""
    if _api_fixture_resolver is None and _igscore_snapshot_loader is None:
        return {"checked_dates": 0, "updated_dates": 0}
    now_ts = int(time.time())
    # A LIVE prediction can cross midnight. Prioritize dates that contain a
    # due LIVE/final IGScore refresh so a new calendar day cannot starve the
    # previous day's still-running matches. Multiple due dates are processed
    # in the same five-minute worker cycle (bounded by max_dates).
    candidates: list[tuple[int, int, str, dict[str, Any]]] = []
    urgent_reasons = {"live", "igscore", "final", "final_status"}
    for payload in _storage_records_full():
        date_text = str(payload.get("date") or "")
        if not _valid_date(date_text):
            continue
        rows = [dict(row) for row in payload.get("feed") or [] if isinstance(row, dict)]
        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            fixture_id = int(row.get("api_football_fixture_id") or 0)
            igscore_id = str(row.get("igscore_match_id") or "").strip()
            if fixture_id:
                key = f"api:{fixture_id}"
            elif igscore_id:
                key = f"ig:{igscore_id}"
            else:
                key = str(row.get("source_slug") or row.get("prediction_key") or "")
            if key:
                grouped.setdefault(key, []).append(row)
        due_times: list[int] = []
        priority = 2
        for group in grouped.values():
            reason = _group_refresh_reason(group, now_ts)
            if reason == "skip":
                continue
            if reason in urgent_reasons:
                priority = 0
            elif priority > 1:
                priority = 1
            sample = group[0]
            fixture = ((sample.get("fixture_item") or {}).get("fixture") or {})
            due_times.append(int(sample.get("next_check_at") or fixture.get("timestamp") or now_ts))
        if due_times:
            candidates.append((priority, min(due_times), date_text, payload))

    checked = updated_dates = 0
    for _, _, date_text, payload in sorted(candidates, key=lambda item: (item[0], item[1], item[2]))[:max(2, int(max_dates))]:
        rows = [dict(row) for row in payload.get("feed") or [] if isinstance(row, dict)]
        enriched, processed, warnings = _enrich_rows_with_api(rows, force=False)
        checked += 1
        if not processed:
            continue
        rebuilt = _recalculate(date_text, enriched, list(payload.get("warnings") or []) + warnings)
        is_past = date_text < _today()
        all_finished = _rows_fully_finalized(enriched)
        _storage_save(date_text, rebuilt, is_final=bool(is_past and all_finished and not rebuilt.get("pending_result_total")))
        updated_dates += 1
    return {"checked_dates": checked, "updated_dates": updated_dates}

def _rows_fully_finalized(rows: list[dict[str, Any]]) -> bool:
    if not rows:
        return False
    for row in rows:
        state = str(row.get("prediction_match_state") or _lifecycle_state(row))
        if state not in {"FINALIZED", "CANCELLED"}:
            return False
    return True


def _recalculate(date_text: str, rows: list[dict[str, Any]], warnings: list[str] | None = None) -> dict[str, Any]:
    # As soon as V37 sees the current Scores24 editorial card for a match,
    # remove old same-match secondary tips from the public/live payload.
    rows = _drop_legacy_secondary_rows_when_primary_known(rows)
    for row in rows:
        row["prediction_match_state"] = str(row.get("prediction_match_state") or _lifecycle_state(row))
        fixture_item = row.get("fixture_item") or {}
        fixture = fixture_item.get("fixture") or {}
        status = str(((fixture.get("status") or {}).get("short") or "")).upper()
        goals = fixture_item.get("goals") or {}
        if status in _FINISHED_STATUSES and str(row.get("market") or "").lower() not in _STAT_MARKETS:
            needs_audit = int(row.get("settlement_rule_version") or 0) < SETTLEMENT_RULE_VERSION or bool(row.get("regulation_score_pending"))
            if needs_audit:
                settlement, won, result_code = _settle_row(row, goals.get("home"), goals.get("away"))
                if settlement:
                    row["settlement"] = settlement
                    row["won"] = won
                    row["result_code"] = result_code
                    row["result_pending"] = False
                elif row.get("regulation_score_pending"):
                    row["settlement"] = ""
                    row["won"] = None
                    row["result_pending"] = True
    rows.sort(key=lambda row: int((((row.get("fixture_item") or {}).get("fixture") or {}).get("timestamp") or 0)))
    # Every discovered Scores24 prediction is public and participates in statistics.
    selected_rows = list(rows)
    lifecycle_counts: dict[str, int] = {}
    for row in selected_rows:
        state = str(row.get("prediction_match_state") or "UNKNOWN")
        lifecycle_counts[state] = lifecycle_counts.get(state, 0) + 1
    settled = [row for row in selected_rows if row.get("settlement") in {"won", "lost", "push"} and row.get("odd") is not None]
    won = sum(1 for row in settled if row.get("settlement") == "won")
    lost = sum(1 for row in settled if row.get("settlement") == "lost")
    push = sum(1 for row in settled if row.get("settlement") == "push")
    pending_result = sum(1 for row in selected_rows if row.get("result_pending"))
    stake = float(len(settled))
    returned = sum(float(row.get("odd") or 0) if row.get("settlement") == "won" else 1.0 if row.get("settlement") == "push" else 0.0 for row in settled)
    profit = returned - stake
    hit_rate = (won / max(1, won + lost)) * 100 if won + lost else 0.0
    roi = (profit / stake) * 100 if stake else 0.0
    market_counts: dict[str, int] = {}
    for row in selected_rows:
        label = str(row.get("market_label") or row.get("market") or "Другой рынок")
        market_counts[label] = market_counts.get(label, 0) + 1
    return {
        "date": date_text,
        "source": "Scores24",
        "source_mode": "Scores24 main editorial pick per match every 3 min + API-Football card in background + IGScore LIVE statistics",
        "market_policy": "Only the main Scores24 editorial prediction for each match is public",
        "league_policy": "Senior UEFA club cups + senior men's top domestic division of every country",
        "odds_policy": "all positive Scores24 published odds",
        "match_data_source": "API-Football card/status + IGScore LIVE statistics/events; final score fallback: Scores24",
        "fixtures_total": len({str(row.get("source_slug") or row.get("fixture_id") or "") for row in selected_rows}),
        "all_predictions_total": len(rows),
        "selected_predictions_total": len(selected_rows),
        "finished_total": len(settled),
        "predicted_total": len(settled),
        "feed_total": len(selected_rows),
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
        "lifecycle_counts": lifecycle_counts,
        "request_policy": "scores24-graphql-top-divisions-db-history-v13",
        "feed": rows,
        "errors": {},
        "warnings": list(dict.fromkeys(warnings or []))[-4:],
        "updated_at": int(time.time()),
    }


def _audit_saved_regulation_settlements() -> dict[str, int]:
    """Recalculate legacy finished predictions using regulation-time rules.

    This is a database-only startup migration. Rows that do not yet contain a
    trustworthy 90-minute score are marked pending so the normal shared worker
    performs one provider refresh; no request is made by this function.
    """
    audited_dates = corrected_rows = pending_rows = 0
    for payload in _storage_records_full():
        date_text = str(payload.get("date") or "")
        if not _valid_date(date_text):
            continue
        rows = [dict(row) for row in payload.get("feed") or [] if isinstance(row, dict)]
        candidates = [
            row for row in rows
            if str(((((row.get("fixture_item") or {}).get("fixture") or {}).get("status") or {}).get("short") or "")).upper() in _FINISHED_STATUSES
            and str(row.get("market") or "").lower() not in _STAT_MARKETS
            and (int(row.get("settlement_rule_version") or 0) < SETTLEMENT_RULE_VERSION or row.get("regulation_score_pending"))
        ]
        if not candidates:
            continue
        before = {str(row.get("prediction_key") or id(row)): (row.get("settlement"), row.get("result_code"), row.get("won")) for row in candidates}
        rebuilt = _recalculate(date_text, rows, list(payload.get("warnings") or []))
        for row in rebuilt.get("feed") or []:
            key = str(row.get("prediction_key") or id(row))
            if key in before and before[key] != (row.get("settlement"), row.get("result_code"), row.get("won")):
                corrected_rows += 1
            if key in before and row.get("regulation_score_pending"):
                pending_rows += 1
        is_past = date_text < _today()
        all_finished = _rows_fully_finalized(rebuilt.get("feed") or [])
        _storage_save(date_text, rebuilt, is_final=bool(is_past and all_finished and not rebuilt.get("pending_result_total")))
        audited_dates += 1
    return {"audited_dates": audited_dates, "corrected_rows": corrected_rows, "pending_rows": pending_rows}



def _discover_and_store_raw(*, force: bool = False) -> dict[str, Any]:
    """Find and publish Scores24 picks without waiting for API-Football.

    The complete current league catalogue is loaded through Scores24 GraphQL,
    then every league is opened in small batches. Rows are immediately stored
    with Scores24 team names, time, logos, pick and odd. API-Football and IGScore
    enrichment runs in the separate five-minute lifecycle worker afterwards.
    """
    discovered, checked_categories, checked_leagues, warnings, rate_limited = _discover_category_rows(
        force=force,
        full_categories=True,
    )
    discovered = [row for row in discovered if (_row_date(row) or "") >= HISTORY_START_DATE]

    # Critical safety rule: an empty provider scan must never erase or prune the
    # current PostgreSQL feed. Cleanup is allowed only after at least one valid
    # selected prediction was received in the same scan.
    if discovered and not rate_limited:
        saved = _store_discovered_rows(discovered, warnings)
        pruned = _prune_unselected_active_predictions(confirmed_discovered_count=len(discovered))
    else:
        saved = {"dates": 0, "rows": 0}
        pruned = {"dates": 0, "rows": 0}
        if not rate_limited:
            warnings = list(warnings or []) + [
                "Scores24 GraphQL: пустой скан сохранён не был; очистка базы отменена"
            ]
    return {
        "discovered": len(discovered),
        "saved_dates": int(saved.get("dates") or 0),
        "saved_rows": int(saved.get("rows") or 0),
        "pruned_dates": int(pruned.get("dates") or 0),
        "pruned_rows": int(pruned.get("rows") or 0),
        "empty_scan_guard": bool(not discovered and not rate_limited),
        "build_id": SCORES24_BUILD_ID,
        "catalog_total": int(_graphql_last_catalog_total),
        "filtered_out_leagues": int(_graphql_last_filtered_out),
        "secondary_predictions_filtered": int(_graphql_last_secondary_filtered),
        "checked_category_pages": checked_categories,
        "checked_league_pages": checked_leagues,
        "warnings": warnings,
        "rate_limited": rate_limited,
    }


def _discovery_worker() -> None:
    """Fast raw discovery loop; intentionally performs no API matching."""
    _stop.wait(1)
    first_cycle = True
    while not _stop.is_set():
        started = time.time()
        try:
            result = _discover_and_store_raw(force=first_cycle)
            print(
                f"[scores24] graphql discovery build={result.get('build_id') or SCORES24_BUILD_ID} "
                f"catalog_requests={result.get('checked_category_pages', 0)} "
                f"catalog_leagues={result.get('catalog_total', 0)} league_queries={result.get('checked_league_pages', 0)} "
                f"filtered_out={result.get('filtered_out_leagues', 0)} "
                f"main_only_filtered={result.get('secondary_predictions_filtered', 0)} "
                f"found={result.get('discovered', 0)} saved={result.get('saved_rows', 0)} "
                f"pruned_active={result.get('pruned_rows', 0)} "
                f"empty_scan_guard={bool(result.get('empty_scan_guard'))} "
                f"rate_limited={bool(result.get('rate_limited'))} "
                f"warnings={'; '.join((result.get('warnings') or [])[:2]) or '-'}"
            )
        except Scores24RateLimited as exc:
            print(f"[scores24] raw discovery paused: {exc}")
        except Exception as exc:
            print(f"[scores24] raw discovery failed: {_compact_error(exc)}")
        first_cycle = False
        elapsed = time.time() - started
        _stop.wait(max(5, DISCOVERY_REFRESH_SECONDS - elapsed))

def _claim_hourly_discovery(*, force: bool = False) -> bool:
    global _last_discovery_at
    now_ts = int(time.time())
    with _lock:
        if not force and _last_discovery_at and now_ts - _last_discovery_at < DISCOVERY_REFRESH_SECONDS:
            return False
        _last_discovery_at = now_ts
        return True


def _claim_hourly_score_fallback() -> bool:
    global _last_score_fallback_at
    now_ts = int(time.time())
    with _lock:
        if _last_score_fallback_at and now_ts - _last_score_fallback_at < SCORE_FALLBACK_REFRESH_SECONDS:
            return False
        _last_score_fallback_at = now_ts
        return True


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

    # Raw Scores24 discovery runs independently every three minutes and saves
    # rows before any API-Football work. This feed only enriches already saved
    # rows in the background, so missing API coverage/logo never hides a pick.
    discovery_due = False
    discovered_matched = 0
    recovery = {"dates": 0, "rows": 0}
    old = _storage_get(date_text) or old
    rows = _existing_rows(old)

    # Refresh all match metadata and results from API-Football. Its date request
    # is cached, so many predictions on the same day still consume one request.
    enriched, matched, current_api_warnings = _enrich_rows_with_api(list(rows.values()), force=force)
    warnings.extend(current_api_warnings)
    rows = {str(row.get("prediction_key") or index): row for index, row in enumerate(enriched)}

    with _storage_merge_lock:
        latest = _storage_get(date_text) or {}
        latest_rows = _existing_rows(latest)
        for key, latest_row in latest_rows.items():
            if key not in rows:
                rows[key] = latest_row
        payload = _recalculate(date_text, list(rows.values()), warnings)
        payload["scan_status"] = {
            "checked_category_pages": 0,
            "checked_league_pages": 0,
            "known_league_pages": len(_discovered_league_urls),
            "category_pages_total": len(_category_urls()),
            "category_batch_size": len(_category_urls()),
            "checked_scores24_result_pages": 0,
            "api_football_matched_matches": matched,
            "api_football_discovered_matches": discovered_matched,
            "rate_limited": False,
            "hourly_discovery_ran": False,
            "raw_discovery_worker": True,
            "raw_discovery_interval_seconds": DISCOVERY_REFRESH_SECONDS,
            "recovered_dates": int(recovery.get("dates") or 0),
            "recovered_predictions": int(recovery.get("rows") or 0),
        }
        is_past = date_text < _today()
        all_finished = _rows_fully_finalized(payload["feed"])
        _storage_save(date_text, payload, is_final=bool(is_past and all_finished and not payload.get("pending_result_total")))
    return payload


def set_global_selection(date_text: str, prediction_key: str, selected: bool) -> dict[str, Any]:
    """Compatibility endpoint from the former manual-publication mode.

    All Scores24 predictions are now public.  Keep the endpoint as a harmless
    no-op so an older cached Mini App cannot accidentally hide a card.
    """
    if not _valid_date(date_text) or not str(prediction_key or "").strip():
        raise ValueError("invalid selection")
    payload = _storage_get(date_text)
    if not isinstance(payload, dict):
        raise ValueError("prediction date not found")
    rows = [row for row in (payload.get("feed") or []) if isinstance(row, dict)]
    if not any(str(row.get("prediction_key") or "") == str(prediction_key) for row in rows):
        raise ValueError("prediction not found")
    return {
        "ok": True,
        "date": date_text,
        "prediction_key": prediction_key,
        "selected": True,
        "all_public": True,
        "selected_total": len(rows),
    }


_LEAGUE_ROI_CACHE: dict[str, Any] = {"ts": 0.0, "rows": []}
_LEAGUE_ROI_CACHE_TTL = 900.0


def _league_roi_key_part(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"^international-clubs-", "", text)
    text = re.sub(r"[^a-z0-9а-яё]+", "-", text, flags=re.IGNORECASE)
    return text.strip("-")


def league_roi_ranking(limit: int = 5) -> list[dict[str, Any]]:
    """Cached ROI ranking. Never scan PostgreSQL on every prediction-feed request."""
    now = time.time()
    cached = _LEAGUE_ROI_CACHE.get("rows") or []
    if cached and now - float(_LEAGUE_ROI_CACHE.get("ts") or 0) < _LEAGUE_ROI_CACHE_TTL:
        return [dict(x) for x in cached[:max(1, min(int(limit or 5), 200))]]
    groups: dict[str, dict[str, Any]] = {}
    try:
        payloads = _storage_records_full()
    except Exception:
        return [dict(x) for x in cached[:max(1, min(int(limit or 5), 200))]]
    for payload in payloads:
        for row in payload.get("feed") or []:
            if not isinstance(row, dict):
                continue
            settlement = str(row.get("settlement") or "").lower()
            try:
                odd = float(row.get("odd") or 0)
            except (TypeError, ValueError):
                odd = 0.0
            if settlement not in {"won", "lost", "push"} or odd <= 0:
                continue
            fixture_item = row.get("fixture_item") if isinstance(row.get("fixture_item"), dict) else {}
            league = fixture_item.get("league") if isinstance(fixture_item.get("league"), dict) else {}
            source_league_slug = _league_roi_key_part(row.get("scores24_league_slug"))
            source_country_slug = _league_roi_key_part(row.get("scores24_country_slug"))
            source_country = str(row.get("scores24_country_name") or "").strip()
            source_name = str(row.get("scores24_league_name") or "").strip()
            country = source_country or str(league.get("country") or "Мир")
            name = source_name or str(league.get("name") or "Лига")
            fallback_country = _league_roi_key_part(country)
            fallback_name = _league_roi_key_part(name)
            league_id = str(league.get("id") or "").strip()
            if source_league_slug:
                key = f"scores24:{source_country_slug or fallback_country}:{source_league_slug}"
            elif league_id:
                key = f"api:{league_id}"
            else:
                key = f"name:{fallback_country}:{fallback_name}"
            group = groups.setdefault(key, {
                "key": key, "country": country, "name": name,
                "logo": str(league.get("logo") or league.get("flag") or ""),
                "settled": 0, "won": 0, "lost": 0, "push": 0,
                "stake": 0.0, "returned": 0.0,
            })
            stake = float(row.get("stake") or 1) or 1.0
            explicit_returned = row.get("returned")
            try:
                returned = float(explicit_returned) if explicit_returned is not None else None
            except (TypeError, ValueError):
                returned = None
            if returned is None:
                returned = odd * stake if settlement == "won" else stake if settlement == "push" else 0.0
            group["settled"] += 1
            group[settlement] += 1
            group["stake"] += stake
            group["returned"] += returned
    ranking: list[dict[str, Any]] = []
    for raw in groups.values():
        group = dict(raw)
        stake = float(group.pop("stake") or 0)
        returned = float(group.pop("returned") or 0)
        profit = returned - stake
        group["profit"] = round(profit, 2)
        group["roi"] = round((profit / stake) * 100 if stake > 0 else 0.0, 2)
        ranking.append(group)
    ranking.sort(key=lambda item: (-float(item.get("roi") or 0), -int(item.get("settled") or 0), str(item.get("name") or "")))
    _LEAGUE_ROI_CACHE["ts"] = now
    _LEAGUE_ROI_CACHE["rows"] = [dict(x) for x in ranking]
    return [dict(x) for x in ranking[:max(1, min(int(limit or 5), 200))]]


def feed_for_client(date_text: str, *, include_all: bool = False) -> dict[str, Any]:
    """Return every saved prediction to every user.

    ``include_all`` is retained for API compatibility but no longer changes
    visibility; league grouping is a client-side presentation only.
    """
    payload = cached_feed(date_text)
    result = dict(payload)
    all_rows = [dict(row) for row in (payload.get("feed") or []) if isinstance(row, dict)]
    result["feed"] = all_rows
    result["feed_total"] = len(all_rows)
    result["all_predictions_total"] = len(all_rows)
    result["selected_predictions_total"] = len(all_rows)
    result["selection_admin"] = False
    result["all_predictions_public"] = True
    result["league_roi_top"] = league_roi_ranking(200)
    return result


def saved_match_by_igscore_id(match_id: str) -> dict[str, Any] | None:
    """Return the richest saved prediction row for one IGScore match.

    Used only as a read-only fallback for finished prediction cards after the
    LIVE collector has already removed the match from its short-lived cache.
    """
    wanted = str(match_id or "").strip()
    if not wanted:
        return None
    best: dict[str, Any] | None = None
    best_score = -1
    for payload in reversed(_storage_records_full()):
        for row in payload.get("feed") or []:
            if not isinstance(row, dict):
                continue
            row_match_id = str(
                row.get("igscore_match_id")
                or (((row.get("fixture_item") or {}).get("igscore") or {}).get("match_id"))
                or ""
            ).strip()
            if row_match_id != wanted:
                continue
            fixture_item = row.get("fixture_item") if isinstance(row.get("fixture_item"), dict) else {}
            events = fixture_item.get("events") if isinstance(fixture_item.get("events"), list) else []
            statistics = fixture_item.get("statistics") if isinstance(fixture_item.get("statistics"), list) else []
            lifecycle = str(row.get("prediction_match_state") or "").upper()
            score = len(events) * 10 + len(statistics) * 3
            if lifecycle in {"FINALIZED", "FINISHED", "FT", "AET", "PEN"}:
                score += 1000
            if row.get("final_snapshot_saved"):
                score += 500
            if score > best_score:
                best = dict(row)
                best_score = score
    return best


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
    # Historical tabs are rebuilt from the complete PostgreSQL history before
    # they are returned. This makes 05/06 August appear from the database even
    # when an older build stored those rows under a neighbouring date.
    payload = _restore_saved_date_from_database(date_text) if date_text < _today() else _storage_get(date_text)
    if payload is None:
        result = _recalculate(date_text, [], ["Фоновое обновление ещё не сохранило прогнозы за эту дату."])
        result["served_from_cache"] = True
        result["warming_up"] = True
        result["cache_age_seconds"] = None
        return result
    result = dict(payload)
    updated_at = int(result.get("updated_at") or 0)
    result["served_from_cache"] = True
    result["cache_age_seconds"] = max(0, int(time.time()) - updated_at) if updated_at else None
    return result


def _worker() -> None:
    _health_update(
        persist=True,
        worker_started=True,
        worker_started_at=int(time.time()),
        heartbeat_at=int(time.time()),
        current_action="startup-delay",
    )
    _stop.wait(8)
    try:
        audit = _audit_saved_regulation_settlements()
        if int(audit.get("audited_dates") or 0):
            print(
                f"[scores24] regulation-time audit dates={audit.get('audited_dates', 0)} "
                f"corrected={audit.get('corrected_rows', 0)} pending={audit.get('pending_rows', 0)}"
            )
    except Exception as exc:
        print(f"[scores24] regulation-time audit failed: {_compact_error(exc)}")
    cycle = 0
    while not _stop.is_set():
        started = time.time()
        if cycle % 10 == 0:
            try:
                livezot_postgres.ensure_daily_prediction_archive_backup(reason="scores24-worker")
            except Exception as exc:
                print(f"[prediction-backup] daily archive failed: {_compact_error(exc)}")
        now_ts = int(started)
        _health_update(persist=True, heartbeat_at=now_ts, cycle_started_at=now_ts, current_action="scores24-scan")
        cycle_failed = False
        last_feed_total = 0
        checked_dates = updated_dates = 0
        try:
            today = datetime.now(_tz()).date()
            offsets = [0]
            if cycle % 3 == 0:
                offsets.append(1 + ((cycle // 3) % 3))
            for offset in offsets:
                date_text = (today + timedelta(days=offset)).isoformat()
                try:
                    _health_update(heartbeat_at=int(time.time()), current_action=f"feed:{date_text}")
                    result = feed(date_text, force=False)
                    last_feed_total = int(result.get("feed_total") or 0)
                    print(
                        f"[scores24] refresh date={date_text} feed={last_feed_total} "
                        f"settled={result.get('predicted_total', 0)} warnings={len(result.get('warnings') or [])}"
                    )
                except Scores24RateLimited as exc:
                    print(f"[scores24] refresh paused: {exc}")
                    cycle_failed = True
                    _health_update(last_error_at=int(time.time()), last_error=str(exc))
                    break
                except Exception as exc:
                    cycle_failed = True
                    error = _compact_error(exc)
                    print(f"[scores24] refresh failed date={date_text}: {error}")
                    _health_update(last_error_at=int(time.time()), last_error=error)
            if _claim_hourly_score_fallback():
                _health_update(heartbeat_at=int(time.time()), current_action="scores24-score-fallback")
                score_refresh = _refresh_unsettled_history(MAX_RESULT_PAGES_PER_SCAN)
                if int(score_refresh.get("checked_pages") or 0):
                    print(
                        f"[scores24] score fallback pages={score_refresh.get('checked_pages', 0)} "
                        f"updated_dates={score_refresh.get('updated_dates', 0)}"
                    )
            _health_update(heartbeat_at=int(time.time()), current_action="api-history")
            history_refresh = _refresh_unsettled_history_api(MAX_API_HISTORY_DATES_PER_SCAN)
            checked_dates = int(history_refresh.get("checked_dates") or 0)
            updated_dates = int(history_refresh.get("updated_dates") or 0)
            if checked_dates:
                print(
                    f"[scores24] API-Football history dates={checked_dates} "
                    f"updated_dates={updated_dates}"
                )
        except Exception as exc:
            cycle_failed = True
            error = _compact_error(exc)
            print(f"[scores24] worker error: {error}")
            _health_update(last_error_at=int(time.time()), last_error=error)

        finished = int(time.time())
        with _health_lock:
            previous_failures = int(_health_state.get("consecutive_failures") or 0)
        final_health = {
            "heartbeat_at": finished,
            "cycle_finished_at": finished,
            "consecutive_failures": (previous_failures + 1) if cycle_failed else 0,
            "current_action": "sleep",
            "last_feed_total": last_feed_total,
            "last_checked_dates": checked_dates,
            "last_updated_dates": updated_dates,
            "last_cycle_seconds": round(time.time() - started, 3),
        }
        if not cycle_failed:
            final_health["last_success_at"] = finished
            final_health["last_error"] = ""
        _health_update(persist=True, **final_health)
        cycle += 1
        elapsed = time.time() - started
        # Match state is lightweight and must not wait five minutes after FT.
        # Raw Scores24 discovery still runs in its own 3-minute thread.
        _stop.wait(max(5, STATUS_WORKER_SECONDS - elapsed))


def start_worker() -> None:
    global _started
    with _lock:
        if _started:
            return
        _started = True
    # Protect every prediction day before any startup cleanup. The archive is
    # stored in PostgreSQL, so it survives Railway container redeploys.
    try:
        livezot_postgres.ensure_daily_prediction_archive_backup(reason="startup-before-cleanup")
    except Exception as exc:
        print(f"[prediction-backup] startup archive failed: {_compact_error(exc)}")
    deleted = _storage_delete_before(HISTORY_START_DATE)
    if deleted:
        print(f"[scores24] removed legacy dates before {HISTORY_START_DATE}: {deleted}")
    try:
        migrated = _migrate_saved_prediction_dates()
        if int(migrated.get("moved") or 0):
            print(
                f"[scores24] database date restore moved={migrated.get('moved', 0)} "
                f"dates={migrated.get('dates', 0)}"
            )
    except Exception as exc:
        print(f"[scores24] database date restore failed: {_compact_error(exc)}")
    _health_update(persist=True, worker_started=True, worker_started_at=int(time.time()), heartbeat_at=int(time.time()))
    threading.Thread(target=_discovery_worker, name="scores24-raw-discovery", daemon=True).start()
    threading.Thread(target=_worker, name="scores24-predictions", daemon=True).start()


def health(*, database_health: dict[str, Any] | None = None) -> dict[str, Any]:
    database = database_health if isinstance(database_health, dict) else livezot_postgres.health()
    runtime = _health_snapshot(read_shared=bool(database.get("ok")))
    now_ts = int(time.time())
    heartbeat = int(runtime.get("heartbeat_at") or 0)
    started_at = int(runtime.get("worker_started_at") or 0)
    heartbeat_age = max(0, now_ts - heartbeat) if heartbeat else None
    within_startup_grace = bool(started_at and now_ts - started_at < max(90, REFRESH_SECONDS + 30))
    worker_stale = bool(
        runtime.get("worker_started")
        and heartbeat_age is not None
        and heartbeat_age > WORKER_STALE_SECONDS
        and not within_startup_grace
    )
    too_many_failures = int(runtime.get("consecutive_failures") or 0) >= WORKER_FAILURE_THRESHOLD
    worker_ok = bool(runtime.get("worker_started")) and not worker_stale and not too_many_failures
    return {
        "source": "Scores24",
        "ok": bool(database.get("ok")) and worker_ok,
        "database": database,
        "worker": {
            **runtime,
            "heartbeat_age_seconds": heartbeat_age,
            "stale_after_seconds": WORKER_STALE_SECONDS,
            "stale": worker_stale,
            "failure_threshold": WORKER_FAILURE_THRESHOLD,
            "too_many_failures": too_many_failures,
            "ok": worker_ok,
        },
        "base_url": BASE_URL,
        "refresh_seconds": REFRESH_SECONDS,
        "live_refresh_seconds": LIVE_REFRESH_SECONDS,
        "request_policy": "scores24-graphql-top-divisions-db-history-v13",
        "discovery_refresh_seconds": DISCOVERY_REFRESH_SECONDS,
        "score_fallback_refresh_seconds": SCORE_FALLBACK_REFRESH_SECONDS,
        "category_refresh_seconds": CATEGORY_REFRESH_SECONDS,
        "category_batch_size": CATEGORY_BATCH_SIZE,
        "category_pages": len(_category_urls()),
        "graphql_url": GRAPHQL_URL,
        "graphql_league_batch_size": GRAPHQL_LEAGUE_BATCH_SIZE,
        "graphql_cache_seconds": GRAPHQL_CACHE_SECONDS,
        "graphql_last_http_status": _graphql_last_http_status,
        "graphql_last_success_at": _graphql_last_success_at,
        "graphql_last_error": _graphql_last_error,
        "graphql_last_leagues": _graphql_last_leagues,
        "graphql_last_predictions": _graphql_last_predictions,
        "graphql_secondary_predictions_filtered": _graphql_last_secondary_filtered,
        "league_batch_size": LEAGUE_BATCH_SIZE,
        "known_league_pages": len(_discovered_league_urls),
        "history_lookback_days": HISTORY_LOOKBACK_DAYS,
        "history_start_date": HISTORY_START_DATE,
        "date_source": "actual fixture date (API-Football / Scores24 matchDate)",
        "max_result_pages_per_scan": MAX_RESULT_PAGES_PER_SCAN,
        "api_history_dates_per_scan": MAX_API_HISTORY_DATES_PER_SCAN,
        "prediction_source": "Scores24 GraphQL filtered: UEFA club cups + 15 men's top divisions",
        "odds_source": "Scores24",
        "match_data_source": "API-Football card/status + IGScore LIVE statistics/events; final score fallback: Scores24",
        "api_football_provider_attached": _api_football_enabled(),
        "igscore_provider_attached": _igscore_enabled(),
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
