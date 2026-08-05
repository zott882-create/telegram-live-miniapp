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
USER_AGENT = os.environ.get(
    "SCORES24_USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
).strip()

_REACT_STATE_RE = re.compile(r'window\.__REACT_QUERY_STATE__\s*=\s*JSON\.parse\("(.*?)"\);', re.S)
_PREDICTION_LINK_RE = re.compile(
    r'(?:https?://scores24\.live)?/(?P<lang>[a-z]{2})/soccer/m-(?P<slug>[^"\'<>?#]+?)-prediction',
    re.I,
)
_SCORE_RE = re.compile(r'(-?\d+)\s*[:\-]\s*(-?\d+)')
_TAG_RE = re.compile(r'<[^>]+>')

_lock = threading.RLock()
_request_lock = threading.Lock()
_fetch_cache: dict[str, tuple[float, str]] = {}
_next_request_at = 0.0
_blocked_until = 0.0
_started = False
_stop = threading.Event()
_data_dir = Path(os.environ.get("SCORES24_DATA_DIR", "")).expanduser() if os.environ.get("SCORES24_DATA_DIR") else None
_db_path: Path | None = None
_pg_conn_factory: Callable[[], Any] | None = None
_database_url = ""


def configure(*, data_dir: Path, pg_conn_factory: Callable[[], Any] | None = None, database_url: str = "") -> None:
    global _data_dir, _db_path, _pg_conn_factory, _database_url
    _data_dir = Path(data_dir)
    _data_dir.mkdir(parents=True, exist_ok=True)
    _db_path = _data_dir / "scores24_prediction_history.sqlite3"
    _pg_conn_factory = pg_conn_factory
    _database_url = str(database_url or "")
    _storage_init()


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


def _storage_save(date_text: str, payload: dict[str, Any], *, is_final: bool = False) -> None:
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


def history() -> list[dict[str, Any]]:
    _storage_init()
    rows: list[Any] = []
    try:
        if _storage_mode() == "postgres":
            with _pg_conn_factory() as conn:  # type: ignore[misc]
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT stat_date,payload_json,is_final,updated_at FROM scores24_prediction_daily_history ORDER BY stat_date DESC"
                    )
                    rows = cur.fetchall() or []
        elif _db_path is not None:
            with sqlite3.connect(str(_db_path), timeout=30) as conn:
                rows = conn.execute(
                    "SELECT stat_date,payload_json,is_final,updated_at FROM scores24_prediction_daily_history ORDER BY stat_date DESC"
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
    if clean.endswith("-prediction"):
        clean = clean[:-11]
    return f"{BASE_URL}/{LANG}/soccer/m-{clean}-prediction"


def _url_near_date(url: str, date_text: str, tolerance_days: int = 2) -> bool:
    match = re.search(r"/m-(\d{2})-(\d{2})-(\d{4})-", str(url or ""))
    if not match:
        return True
    try:
        slug_date = datetime(int(match.group(3)), int(match.group(2)), int(match.group(1))).date()
        requested = datetime.strptime(date_text, "%Y-%m-%d").date()
        return abs((slug_date - requested).days) <= max(0, tolerance_days)
    except Exception:
        return True


def _merge_urls(*groups: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for group in groups:
        for value in group or []:
            url = str(value or "").strip()
            if not url or url in seen:
                continue
            seen.add(url)
            out.append(url)
            if len(out) >= MAX_DISCOVERED_URLS:
                return out
    return out


def _extract_prediction_urls(page_html: str) -> list[str]:
    normalized = html_lib.unescape((page_html or "").replace("\\/", "/"))
    seen: set[str] = set()
    out: list[str] = []
    for match in _PREDICTION_LINK_RE.finditer(normalized):
        slug = match.group("slug").strip("/")
        url = _absolute_prediction_url(slug)
        if url not in seen:
            seen.add(url)
            out.append(url)
    return out


def _react_queries(page_html: str) -> list[dict[str, Any]]:
    match = _REACT_STATE_RE.search(page_html or "")
    if not match:
        return []
    escaped = match.group(1)
    try:
        decoded = json.loads('"' + escaped + '"')
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


def _clean_text(value: Any) -> str:
    text = _TAG_RE.sub(" ", str(value or ""))
    text = html_lib.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def _float(value: Any) -> float | None:
    try:
        return float(str(value).strip().replace(",", "."))
    except Exception:
        return None


def _parse_iso(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None


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
    # Stays below JavaScript's safe integer range.
    return int(hashlib.sha256(slug.encode("utf-8")).hexdigest()[:12], 16)


def _pick_label(market: str, code: str, teams: list[dict[str, Any]], variables: Any = None) -> str:
    home = str((teams[0] if len(teams) > 0 else {}).get("name") or "Хозяева")
    away = str((teams[1] if len(teams) > 1 else {}).get("name") or "Гости")
    market = market.lower()
    code = code.lower()
    if market == "one_x_two":
        return {"w1": f"Победа {home}", "x": "Ничья", "w2": f"Победа {away}"}.get(code, code.upper())
    if market in {"both_to_score", "both_teams_to_score"}:
        return "Обе забьют — Да" if code in {"yes", "y"} else "Обе забьют — Нет"
    if "total" in market:
        line = ""
        if isinstance(variables, list) and variables:
            line = str(variables[0])
        line = line or code.replace("total_over", "").replace("total_under", "").replace("_", ".").strip(" .")
        return ("ТБ " if "over" in code else "ТМ ") + (line or "")
    return code.replace("_", " ").upper()


def _settle(market: str, code: str, home: int | None, away: int | None, variables: Any = None) -> tuple[str, bool | None, str]:
    if home is None or away is None:
        return "", None, ""
    result_code = "1" if home > away else "2" if away > home else "X"
    market = market.lower()
    code = code.lower()
    if market == "one_x_two":
        expected = {"w1": "1", "x": "X", "w2": "2"}.get(code)
        won = expected == result_code
        return ("won" if won else "lost"), won, result_code
    if market in {"both_to_score", "both_teams_to_score"}:
        yes = home > 0 and away > 0
        expected_yes = code in {"yes", "y"}
        won = yes == expected_yes
        return ("won" if won else "lost"), won, result_code
    return "", None, result_code


def _parse_prediction_page(page_html: str, source_url: str) -> tuple[dict[str, Any] | None, list[str]]:
    queries = _react_queries(page_html)
    match_data = _query_data(queries, "matchShow") or {}
    prediction_data = _query_data(queries, "matchPrediction") or {}
    other = _query_data(queries, "matchOtherMatches") or []
    discovered: list[str] = []
    for item in other if isinstance(other, list) else []:
        if not isinstance(item, dict) or item.get("sportSlug") != "soccer" or not item.get("slug"):
            continue
        discovered.append(_absolute_prediction_url(str(item.get("slug"))))
    if not isinstance(match_data, dict) or not isinstance(prediction_data, dict):
        return None, discovered
    prediction = prediction_data.get("prediction") or []
    if not isinstance(prediction, list) or len(prediction) < 2:
        return None, discovered
    market, code = str(prediction[0] or ""), str(prediction[1] or "")
    # Preserve the user's outcomes-only rule: editorial P1 / X / P2 only.
    if market != "one_x_two" or code not in {"w1", "x", "w2"}:
        return None, discovered
    odd = _float(prediction_data.get("predictionValue"))
    if odd is None or odd < MIN_ODD or odd > MAX_ODD:
        return None, discovered
    teams = match_data.get("teams") or []
    if not isinstance(teams, list) or len(teams) < 2:
        return None, discovered
    match_dt = _parse_iso(match_data.get("matchDate"))
    if match_dt is None:
        return None, discovered
    local_dt = match_dt.astimezone(_tz()) if match_dt.tzinfo else match_dt.replace(tzinfo=timezone.utc).astimezone(_tz())
    finished = bool(match_data.get("isFinished"))
    live = bool(match_data.get("isLive"))
    home_score, away_score = _score_from_match(match_data) if (finished or live) else (None, None)
    status_short = "FT" if finished else "LIVE" if live else "NS"
    status_long = "Матч завершён" if finished else "LIVE" if live else "Не начался"
    settlement, won, result_code = _settle(market, code, home_score, away_score, prediction_data.get("predictionVars")) if finished else ("", None, "")
    slug = str(match_data.get("slug") or source_url.rsplit("/m-", 1)[-1].removesuffix("-prediction"))
    fixture_id = _stable_id(slug)
    league = match_data.get("uniqueTournament") or {}
    country = match_data.get("country") or {}
    author = prediction_data.get("author") or {}
    bookmaker = prediction_data.get("bookmaker") or {}
    pick_label = _pick_label(market, code, teams, prediction_data.get("predictionVars"))
    row = {
        "source": "Scores24",
        "source_url": source_url,
        "source_slug": slug,
        "fixture_id": fixture_id,
        "prediction_key": f"scores24:{slug}:{market}:{code}",
        "market": "1x2",
        "source_market": market,
        "pick_code": {"w1": "1", "x": "X", "w2": "2"}.get(code, code),
        "source_pick_code": code,
        "pick_label": pick_label,
        "odd": round(odd, 3),
        "probability": None,
        "bookmaker": str(bookmaker.get("name") or bookmaker.get("slug") or "Scores24"),
        "bookmaker_logo": bookmaker.get("logo") or bookmaker.get("favicon") or "",
        "author": str(author.get("name") or "Редакция Scores24"),
        "author_avatar": author.get("avatar") or "",
        "published_at": prediction_data.get("createdDate"),
        "modified_at": prediction_data.get("modifiedDate"),
        "editorial_text": _clean_text((prediction_data.get("text") or {}).get("prediction")),
        "handwritten": bool(prediction_data.get("handwritten")),
        "settlement": settlement,
        "won": won,
        "result_code": result_code,
        "fixture_item": {
            "fixture": {
                "id": fixture_id,
                "date": local_dt.isoformat(),
                "timestamp": int(local_dt.timestamp()),
                "timezone": TIMEZONE_NAME,
                "status": {"short": status_short, "long": status_long, "elapsed": 90 if finished else None},
            },
            "league": {
                "id": _stable_id(str(league.get("slug") or match_data.get("leagueSlug") or "scores24")),
                "name": league.get("name") or match_data.get("uniqueTournamentName") or "Scores24",
                "country": country.get("name") or "Мир",
                "logo": league.get("logo") or "",
                "round": "Редакционный прогноз Scores24",
            },
            "teams": {
                "home": {"id": teams[0].get("id"), "name": teams[0].get("name"), "logo": teams[0].get("logo") or ""},
                "away": {"id": teams[1].get("id"), "name": teams[1].get("name"), "logo": teams[1].get("logo") or ""},
            },
            "goals": {"home": home_score, "away": away_score},
            "score": {"fulltime": {"home": home_score, "away": away_score}},
            "scores24": {"source_url": source_url, "slug": slug},
        },
    }
    return row, discovered


def _candidate_index_urls(date_text: str) -> list[str]:
    today = datetime.strptime(_today(), "%Y-%m-%d").date()
    requested = datetime.strptime(date_text, "%Y-%m-%d").date()
    delta = (requested - today).days
    urls = [f"{BASE_URL}/{LANG}/predictions/soccer"]
    if delta == 0:
        urls.insert(0, f"{BASE_URL}/{LANG}/predictions/soccer/today")
    elif delta == 1:
        urls.insert(0, f"{BASE_URL}/{LANG}/predictions/soccer/tomorrow")
    # These variants are harmless if unsupported and let the collector work if
    # Scores24 exposes an explicit date route later.
    return list(dict.fromkeys(urls))


def _existing_rows(payload: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    rows = {}
    for row in (payload or {}).get("feed") or []:
        if isinstance(row, dict) and row.get("prediction_key"):
            rows[str(row["prediction_key"])] = dict(row)
    return rows


def _recalculate(date_text: str, rows: list[dict[str, Any]], errors: list[str] | None = None) -> dict[str, Any]:
    rows.sort(key=lambda row: int((((row.get("fixture_item") or {}).get("fixture") or {}).get("timestamp") or 0)))
    settled = [row for row in rows if row.get("settlement") in {"won", "lost", "push"} and row.get("odd") is not None]
    won = sum(1 for row in settled if row.get("settlement") == "won")
    lost = sum(1 for row in settled if row.get("settlement") == "lost")
    push = sum(1 for row in settled if row.get("settlement") == "push")
    stake = float(len(settled))
    returned = sum(float(row.get("odd") or 0) if row.get("settlement") == "won" else 1.0 if row.get("settlement") == "push" else 0.0 for row in settled)
    profit = returned - stake
    hit_rate = (won / max(1, won + lost)) * 100 if won + lost else 0.0
    roi = (profit / stake) * 100 if stake else 0.0
    now = int(time.time())
    return {
        "date": date_text,
        "source": "Scores24",
        "source_mode": "editorial-direct",
        "market_policy": "Scores24 editorial 1X2 only",
        "odds_policy": f"Scores24 published odd {MIN_ODD:.2f}-{MAX_ODD:.2f}",
        "fixtures_total": len(rows),
        "finished_total": len(settled),
        "predicted_total": len(settled),
        "feed_total": len(rows),
        "won_total": won,
        "lost_total": lost,
        "push_total": push,
        "hit_rate": round(hit_rate, 2),
        "stake_total": round(stake, 2),
        "returned_total": round(returned, 2),
        "profit_total": round(profit, 2),
        "roi_percent": round(roi, 2),
        "odds_total": len(settled),
        "feed": rows,
        "errors": {},
        "warnings": list(dict.fromkeys(errors or []))[-4:],
        "updated_at": now,
    }


def feed(date_text: str, *, force: bool = False) -> dict[str, Any]:
    if not _valid_date(date_text):
        raise ValueError("invalid date")

    old = _storage_get(date_text)
    rows = _existing_rows(old)
    warnings: list[str] = []
    checked_pages = 0
    rate_limited = False

    stored_candidates = [
        str(url) for url in ((old or {}).get("_candidate_urls") or [])
        if isinstance(url, str) and _url_near_date(url, date_text)
    ]
    discovered_now: list[str] = []

    # Refresh the prediction index no more than once every 30 minutes. A manual
    # refresh does not bypass page caches for every match.
    for index_url in _candidate_index_urls(date_text):
        try:
            index_html = _fetch_text(index_url, force=bool(force and not old), ttl=INDEX_REFRESH_SECONDS)
            discovered_now.extend(
                url for url in _extract_prediction_urls(index_html)
                if _url_near_date(url, date_text)
            )
        except Scores24RateLimited as exc:
            warnings.append(str(exc))
            rate_limited = True
            break
        except Exception as exc:
            warnings.append(_compact_error(exc))

    candidates = _merge_urls(stored_candidates, discovered_now)

    # Stored unsettled matches get priority, especially after kickoff.
    now_ts = int(time.time())
    unsettled: list[tuple[int, str]] = []
    for row in rows.values():
        fixture = ((row.get("fixture_item") or {}).get("fixture") or {})
        status = str(((fixture.get("status") or {}).get("short") or ""))
        source_url = str(row.get("source_url") or "")
        if status in {"FT", "AET", "PEN"} or not source_url:
            continue
        kickoff = int(fixture.get("timestamp") or 0)
        priority = 0 if kickoff and kickoff <= now_ts + 3 * 3600 else 1
        unsettled.append((priority * 10**12 + kickoff, source_url))
    unsettled_urls = [url for _, url in sorted(unsettled, key=lambda item: item[0])]

    cursor = int((old or {}).get("_scan_cursor") or 0)
    if candidates:
        cursor %= len(candidates)
        rotating = candidates[cursor:] + candidates[:cursor]
    else:
        rotating = []
    queue = _merge_urls(unsettled_urls, rotating)

    processed_candidate_urls: set[str] = set()
    for url in queue:
        if checked_pages >= MAX_PAGES_PER_SCAN or rate_limited:
            break
        try:
            # Only the explicit refresh button may bypass a cached stored match.
            # New discovery pages always respect the cache.
            page_force = bool(force and url in unsettled_urls and checked_pages == 0)
            page_html = _fetch_text(url, force=page_force, ttl=REFRESH_SECONDS)
            checked_pages += 1
            processed_candidate_urls.add(url)
            row, discovered = _parse_prediction_page(page_html, url)
            for extra in discovered:
                if _url_near_date(extra, date_text):
                    candidates = _merge_urls(candidates, [extra])
            if not row:
                continue
            fixture_date = str((((row.get("fixture_item") or {}).get("fixture") or {}).get("date") or ""))[:10]
            if fixture_date != date_text:
                continue
            key = str(row["prediction_key"])
            previous = rows.get(key)
            if previous:
                # The coefficient is fixed when the prediction first appears;
                # later refreshes update only the match status and score.
                row["odd"] = previous.get("odd", row.get("odd"))
                row["bookmaker"] = previous.get("bookmaker", row.get("bookmaker"))
                row["bookmaker_logo"] = previous.get("bookmaker_logo", row.get("bookmaker_logo"))
                row["published_at"] = previous.get("published_at", row.get("published_at"))
            for old_key, old_row in list(rows.items()):
                if old_key != key and str(old_row.get("source_slug") or "") == str(row.get("source_slug") or ""):
                    rows.pop(old_key, None)
            rows[key] = row
        except Scores24RateLimited as exc:
            warnings.append(str(exc))
            rate_limited = True
            break
        except Exception as exc:
            warnings.append(_compact_error(exc))

    if candidates:
        advanced = sum(1 for url in rotating if url in processed_candidate_urls)
        next_cursor = (cursor + advanced) % len(candidates)
    else:
        next_cursor = 0

    payload = _recalculate(date_text, list(rows.values()), warnings)
    payload.update({
        "_candidate_urls": candidates[:MAX_DISCOVERED_URLS],
        "_scan_cursor": next_cursor,
        "scan_status": {
            "checked_pages": checked_pages,
            "candidate_pages": len(candidates),
            "max_pages_per_scan": MAX_PAGES_PER_SCAN,
            "rate_limited": rate_limited,
        },
    })
    is_past = date_text < _today()
    all_settled = bool(payload["feed"]) and all(
        str(((((row.get("fixture_item") or {}).get("fixture") or {}).get("status") or {}).get("short") or "")) in {"FT", "AET", "PEN"}
        for row in payload["feed"]
    )
    _storage_save(date_text, payload, is_final=bool(is_past and all_settled))
    return payload


def _worker() -> None:
    # Give the HTTP server a moment to start. Today is checked every five
    # minutes. Future dates are rotated once every fifteen minutes, so one
    # instance never opens four full date scans in a burst.
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
    threading.Thread(target=_worker, name="scores24-predictions", daemon=True).start()


def health() -> dict[str, Any]:
    return {
        "source": "Scores24",
        "base_url": BASE_URL,
        "refresh_seconds": REFRESH_SECONDS,
        "min_odd": MIN_ODD,
        "max_odd": MAX_ODD,
        "max_pages_per_scan": MAX_PAGES_PER_SCAN,
        "request_gap_seconds": REQUEST_GAP_SECONDS,
        "rate_limit_backoff_seconds": RATE_LIMIT_BACKOFF_SECONDS,
        "storage": _storage_mode(),
    }


# Test helper kept private from WSGI routes.
def _parse_file(path: str) -> dict[str, Any] | None:
    text = Path(path).read_text("utf-8", errors="ignore")
    row, _ = _parse_prediction_page(text, "https://scores24.live/ru/soccer/m-test-prediction")
    return row
