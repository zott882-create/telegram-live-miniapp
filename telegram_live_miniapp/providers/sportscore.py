#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""SportScore data provider.

Uses SportScore's public JSON widget API where possible and the public match
HTML as an enrichment source for pre-match odds, H2H, standings and lineups.
No browser cookies or Cloudflare clearance tokens are required.
"""
from __future__ import annotations

import datetime as dt
import concurrent.futures
import gzip
import hashlib
import json
import os
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import unicodedata
from dataclasses import dataclass
from typing import Any

from bs4 import BeautifulSoup


BASE_URL = os.environ.get("SPORTSCORE_BASE_URL", "https://sportscore.com").rstrip("/")
SOURCE_TAG = os.environ.get("SPORTSCORE_SOURCE_TAG", "telegram-live-miniapp").strip() or "telegram-live-miniapp"
HTTP_TIMEOUT = max(3.0, min(15.0, float(os.environ.get("SPORTSCORE_HTTP_TIMEOUT", "8"))))
CACHE_SECONDS = max(30, int(os.environ.get("SPORTSCORE_CACHE_SECONDS", "180")))
# SportScore caches public API responses for roughly 60 seconds at the edge.
LIVE_CACHE_SECONDS = max(55, int(os.environ.get("SPORTSCORE_LIVE_CACHE_SECONDS", "60")))
LIVE_HTML_CACHE_SECONDS = max(55, int(os.environ.get("SPORTSCORE_LIVE_HTML_CACHE_SECONDS", "60")))
DETAIL_CACHE_SECONDS = max(15, int(os.environ.get("SPORTSCORE_DETAIL_CACHE_SECONDS", "60")))
TEAM_CACHE_SECONDS = max(60, int(os.environ.get("SPORTSCORE_TEAM_CACHE_SECONDS", "600")))
COMPETITION_CACHE_SECONDS = max(60, int(os.environ.get("SPORTSCORE_COMPETITION_CACHE_SECONDS", "600")))
MAX_MATCHES = max(1, min(50, int(os.environ.get("SPORTSCORE_MAX_MATCHES", "50"))))
PREMATCH_DAYS_AHEAD = max(2, min(7, int(os.environ.get("SPORTSCORE_PREMATCH_DAYS_AHEAD", "3"))))
PREMATCH_WORKERS = max(1, min(5, int(os.environ.get("SPORTSCORE_PREMATCH_WORKERS", "3"))))
PREMATCH_START_GRACE_SECONDS = max(0, min(300, int(os.environ.get("SPORTSCORE_PREMATCH_START_GRACE_SECONDS", "60"))))

_HEADERS = {
    "Accept": "application/json, text/plain, text/html, */*",
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "ru,en;q=0.9",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36"
    ),
    "Referer": BASE_URL + "/football/",
}

_CACHE_LOCK = threading.RLock()
_CACHE: dict[str, tuple[float, Any]] = {}


def _cached(key: str, ttl: int, loader):
    now = time.time()
    with _CACHE_LOCK:
        item = _CACHE.get(key)
        if item and now - item[0] < ttl:
            return item[1]
    value = loader()
    with _CACHE_LOCK:
        _CACHE[key] = (now, value)
        if len(_CACHE) > 1000:
            oldest = sorted(_CACHE.items(), key=lambda kv: kv[1][0])[:250]
            for k, _ in oldest:
                _CACHE.pop(k, None)
    return value


def _read_response(resp) -> bytes:
    data = resp.read()
    enc = str(resp.headers.get("Content-Encoding") or "").lower()
    if enc == "gzip":
        data = gzip.decompress(data)
    return data


def _request(path: str, params: dict[str, Any] | None = None, *, want_json: bool = False) -> Any:
    if not path.startswith("http"):
        url = BASE_URL + (path if path.startswith("/") else "/" + path)
    else:
        url = path
    if params:
        query = urllib.parse.urlencode({k: v for k, v in params.items() if v not in (None, "")})
        url += ("&" if "?" in url else "?") + query
    req = urllib.request.Request(url, headers=_HEADERS, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
            raw = _read_response(resp)
            ctype = str(resp.headers.get("Content-Type") or "")
    except urllib.error.HTTPError as exc:
        body = b""
        try:
            body = exc.read()[:500]
        except Exception:
            pass
        raise RuntimeError(f"SportScore HTTP {exc.code}: {url}: {body.decode('utf-8','replace')}") from exc
    except Exception as exc:
        raise RuntimeError(f"SportScore request failed: {url}: {type(exc).__name__}: {exc}") from exc

    text = raw.decode("utf-8", "replace")
    if want_json or "json" in ctype.lower():
        try:
            return json.loads(text)
        except Exception as exc:
            raise RuntimeError(f"SportScore invalid JSON: {url}: {text[:300]}") from exc
    return text


def _json(path: str, params: dict[str, Any], ttl: int = CACHE_SECONDS) -> Any:
    key = "json:" + path + "?" + urllib.parse.urlencode(sorted(params.items()))
    return _cached(key, ttl, lambda: _request(path, params, want_json=True))


def _html(path: str, ttl: int = CACHE_SECONDS) -> str:
    return str(_cached("html:" + path, ttl, lambda: _request(path)))


def _dig(obj: Any, *paths: str, default: Any = None) -> Any:
    for path in paths:
        cur = obj
        ok = True
        for part in path.split("."):
            if isinstance(cur, dict) and part in cur:
                cur = cur[part]
            else:
                ok = False
                break
        if ok and cur not in (None, ""):
            return cur
    return default


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(str(value).replace("%", "").strip()))
    except Exception:
        return default


def _to_float(value: Any, default: float | None = None) -> float | None:
    try:
        return float(str(value).replace(",", ".").strip())
    except Exception:
        return default


def _parse_iso(value: Any) -> dt.datetime | None:
    if value in (None, ""):
        return None
    # Some widget versions return Unix seconds/milliseconds.
    try:
        if isinstance(value, (int, float)) or re.fullmatch(r"\d{10,13}", str(value).strip()):
            raw = float(value)
            if raw > 10_000_000_000:
                raw /= 1000.0
            return dt.datetime.fromtimestamp(raw, tz=dt.timezone.utc)
    except Exception:
        pass
    text = str(value or "").strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        out = dt.datetime.fromisoformat(text)
        if out.tzinfo is None:
            out = out.replace(tzinfo=dt.timezone.utc)
        return out.astimezone(dt.timezone.utc)
    except Exception:
        return None


def _slug_from_url(value: Any) -> str:
    text = str(value or "").strip()
    m = re.search(r"/(?:football|basketball|tennis)/match/([^/?#]+)/?", text, re.I)
    if m:
        return m.group(1)
    text = text.strip("/")
    if "/" not in text and text:
        return text
    return ""


def _entity_name(value: Any) -> str:
    if isinstance(value, dict):
        return str(value.get("name") or value.get("title") or value.get("short_name") or "").strip()
    return str(value or "").strip()


def _team_slug(value: Any) -> str:
    if isinstance(value, dict):
        return _slug_generic(value.get("url") or value.get("slug") or value.get("id"), "team")
    return ""


def _slug_generic(value: Any, kind: str) -> str:
    text = str(value or "").strip()
    if kind == "competition":
        # Competition URLs contain both country and competition slugs:
        # /football/competition/brazil/brazilian-serie-a/<id>/
        m = re.search(r"/football/competition/[^/?#]+/([^/?#]+)", text)
    else:
        if kind == "team":
            m = re.search(r"/(?:football|basketball|tennis)/team/([^/?#]+)", text, re.I)
        else:
            m = re.search(rf"/(?:football|basketball|tennis)/{re.escape(kind)}/([^/?#]+)", text, re.I)
    if m:
        return m.group(1)
    if text and "/" not in text and "#" not in text:
        return text
    return ""


def _competition_identity(value: Any) -> dict[str, str]:
    """Return a validated internal identity for a SportScore competition URL."""
    text = str(value or "").strip()
    if not text:
        return {"path": "", "country_slug": "", "slug": "", "id": ""}
    try:
        parsed = urllib.parse.urlparse(text if text.startswith("http") else BASE_URL + (text if text.startswith("/") else "/" + text))
        path = parsed.path
    except Exception:
        path = text
    m = re.fullmatch(r"/football/competition/([a-z0-9-]+)/([a-z0-9-]+)/([a-z0-9]+)/?", path, re.I)
    if not m:
        return {"path": "", "country_slug": "", "slug": "", "id": ""}
    country_slug, slug, comp_id = (x.lower() for x in m.groups())
    return {
        "path": f"/football/competition/{country_slug}/{slug}/{comp_id}/",
        "country_slug": country_slug,
        "slug": slug,
        "id": comp_id,
    }


def _validated_competition_path(value: Any) -> str:
    return _competition_identity(value).get("path") or ""


def _status_info(item: dict[str, Any]) -> tuple[str, bool, bool, bool]:
    raw = str(_dig(item, "status", "status_text", "eventStatus", "state", "match_status", "phase", default="") or "").lower()
    raw = raw.rsplit("/", 1)[-1]
    explicit_live = bool(_dig(item, "is_live", "live", "in_play", default=False))
    explicit_finished = bool(_dig(item, "finished", "is_finished", "completed", default=False))
    explicit_scheduled = bool(_dig(item, "scheduled", "is_scheduled", default=False))
    live = explicit_live or any(x in raw for x in (
        "live", "inplay", "in_play", "in progress", "in_progress", "playing",
        "1st", "2nd", "1h", "2h", "half", "halftime", "half-time", "interval",
        "extra time", "extra_time", "penalties",
        "q1", "q2", "q3", "q4", "quarter", "ot", "overtime",
        "set 1", "set 2", "set 3", "set 4", "set 5", "set1", "set2", "set3", "set4", "set5",
    ))
    finished = explicit_finished or any(x in raw for x in (
        "finished", "full", "full-time", "full time", "ended", "complete", "completed",
        "cancelled", "postponed", "abandoned", "after extra time", "after penalties",
    ))
    scheduled = explicit_scheduled or any(x in raw for x in (
        "scheduled", "upcoming", "not_started", "not started", "notstarted", "fixture", "pre",
    ))
    if not (live or finished or scheduled):
        hs = _dig(item, "home_score", "score.home", "scores.home")
        ast = _dig(item, "away_score", "score.away", "scores.away")
        scheduled = hs in (None, "") and ast in (None, "")
    status = "live" if live else "finished" if finished else "scheduled" if scheduled else (raw or "unknown")
    return status, live, finished, scheduled


def _flat_stats(raw: Any) -> dict[str, int]:
    out = {
        "possession_home": 0, "possession_away": 0,
        "shots_home": 0, "shots_away": 0,
        "on_target_home": 0, "on_target_away": 0,
        "off_target_home": 0, "off_target_away": 0,
        "dangerous_home": 0, "dangerous_away": 0,
        "attacks_home": 0, "attacks_away": 0,
        "corners_home": 0, "corners_away": 0,
        "yellow_cards_home": 0, "yellow_cards_away": 0,
        "red_cards_home": 0, "red_cards_away": 0,
    }
    if not isinstance(raw, (dict, list)):
        return out

    label_map = {
        "ball possession": "possession", "possession": "possession",
        "shots": "shots", "total shots": "shots",
        "shots on target": "on_target", "on target": "on_target",
        "shots off target": "off_target", "off target": "off_target",
        "dangerous attacks": "dangerous", "attacks": "attacks",
        "corner kicks": "corners", "corners": "corners",
        "yellow cards": "yellow_cards", "red cards": "red_cards",
    }

    def set_pair(label: str, home: Any, away: Any) -> None:
        key = label_map.get(re.sub(r"\s+", " ", label.lower()).strip())
        if not key:
            return
        out[f"{key}_home"] = _to_int(home)
        out[f"{key}_away"] = _to_int(away)

    if isinstance(raw, dict):
        for label, value in raw.items():
            if isinstance(value, dict):
                set_pair(str(label), _dig(value, "home", "home_value", "team1"), _dig(value, "away", "away_value", "team2"))
            elif isinstance(value, (list, tuple)) and len(value) >= 2:
                set_pair(str(label), value[0], value[1])
        for prefix in ("", "stats.", "statistics."):
            for label, key in label_map.items():
                h = _dig(raw, prefix + key + "_home", prefix + key + ".home")
                a = _dig(raw, prefix + key + "_away", prefix + key + ".away")
                if h is not None or a is not None:
                    out[f"{key}_home"] = _to_int(h)
                    out[f"{key}_away"] = _to_int(a)
    else:
        for row in raw:
            if not isinstance(row, dict):
                continue
            set_pair(str(_dig(row, "name", "label", "type", default="")), _dig(row, "home", "home_value", "team1"), _dig(row, "away", "away_value", "team2"))

    for key in ("shots", "on_target", "off_target", "dangerous", "attacks", "corners", "yellow_cards", "red_cards"):
        out[f"{key}_total"] = out[f"{key}_home"] + out[f"{key}_away"]
    return out


def _sport_name(value: Any) -> str:
    sport = str(value or "football").strip().lower()
    return sport if sport in {"football", "basketball", "tennis"} else "football"


def _participant_obj(item: dict[str, Any], side: str) -> Any:
    names = (
        ("homeTeam", "home_team", "homeParticipant", "home_participant", "player1", "participant1")
        if side == "home" else
        ("awayTeam", "away_team", "awayParticipant", "away_participant", "player2", "participant2")
    )
    return _dig(item, *names, default={})


def _side_score(item: dict[str, Any], side: str) -> int:
    if side == "home":
        value = _dig(item, "home_score", "homeScore", "score.home", "scores.home", "score1", "participant1_score", "home_points")
        index = 0
    else:
        value = _dig(item, "away_score", "awayScore", "score.away", "scores.away", "score2", "participant2_score", "away_points")
        index = 1
    if value not in (None, ""):
        return _to_int(value, 0)
    # The official client examples expose a compact `score` field. It can be a
    # string ("83-79", "2:1") or a two-item array.
    compact = item.get("score")
    if isinstance(compact, (list, tuple)) and len(compact) >= 2:
        return _to_int(compact[index], 0)
    if isinstance(compact, str):
        nums = re.findall(r"\d+", compact)
        if len(nums) >= 2:
            return _to_int(nums[index], 0)
    return 0


def _score_parts(item: dict[str, Any], sport: str) -> list[dict[str, Any]]:
    """Best-effort period/set normalisation across SportScore payload versions."""
    labels = ["Q1", "Q2", "Q3", "Q4", "OT1", "OT2"] if sport == "basketball" else ["S1", "S2", "S3", "S4", "S5"]
    out: list[dict[str, Any]] = []

    def add(label: Any, home: Any, away: Any) -> None:
        if home in (None, "") and away in (None, ""):
            return
        out.append({"label": str(label or labels[len(out)] if len(out) < len(labels) else label or ""), "home": _to_int(home), "away": _to_int(away)})

    raw = _dig(item, "score_parts", "periods", "quarters", "sets", "scores.periods", "scores.quarters", "scores.sets", "period_scores", "set_scores", default=None)
    if isinstance(raw, list):
        for i, row in enumerate(raw[:8]):
            if isinstance(row, dict):
                add(_dig(row, "label", "name", "period", "set", default=(labels[i] if i < len(labels) else str(i + 1))), _dig(row, "home", "home_score", "score1", "participant1"), _dig(row, "away", "away_score", "score2", "participant2"))
            elif isinstance(row, (list, tuple)) and len(row) >= 2:
                add(labels[i] if i < len(labels) else str(i + 1), row[0], row[1])
    elif isinstance(raw, dict):
        for key, value in raw.items():
            if isinstance(value, dict):
                add(str(key).upper(), _dig(value, "home", "home_score", "score1", "participant1"), _dig(value, "away", "away_score", "score2", "participant2"))
            elif isinstance(value, (list, tuple)) and len(value) >= 2:
                add(str(key).upper(), value[0], value[1])

    if not out:
        home_values = _dig(item, "home_period_scores", "home_set_scores", "home_scores", "scores.home_periods", "scores.home_sets", default=[])
        away_values = _dig(item, "away_period_scores", "away_set_scores", "away_scores", "scores.away_periods", "scores.away_sets", default=[])
        if isinstance(home_values, list) or isinstance(away_values, list):
            home_values = home_values if isinstance(home_values, list) else []
            away_values = away_values if isinstance(away_values, list) else []
            for i in range(min(8, max(len(home_values), len(away_values)))):
                add(labels[i] if i < len(labels) else str(i + 1), home_values[i] if i < len(home_values) else None, away_values[i] if i < len(away_values) else None)

    if not out:
        keys = labels
        for label in keys:
            low = label.lower()
            add(label, _dig(item, f"{low}_home", f"scores.{low}.home", f"home_{low}"), _dig(item, f"{low}_away", f"scores.{low}.away", f"away_{low}"))
    return out


def _generic_sport_stats(raw: Any) -> list[dict[str, Any]]:
    """Flatten nested basketball/tennis statistics into home/away rows.

    SportScore has used several envelopes (groups/items/statistics and direct
    dictionaries). This recursive parser accepts all of them and de-duplicates
    labels while preserving the first useful value pair.
    """
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()

    def clean_label(value: Any) -> str:
        label = re.sub(r"[_-]+", " ", str(value or ""))
        label = re.sub(r"\s+", " ", label).strip()
        return label

    def add(label: Any, home: Any, away: Any) -> None:
        label_text = clean_label(label)
        if not label_text or (home in (None, "") and away in (None, "")):
            return
        key = label_text.casefold()
        blocked = {"match", "game", "home", "away", "logo", "image", "id", "slug", "url", "link", "name", "team", "participant", "country", "league", "status", "time", "date", "value", "score"}
        if key in blocked or any(key.endswith(" " + x) for x in ("logo", "image", "id", "slug", "url", "link")):
            return
        def stat_like(value: Any) -> bool:
            if isinstance(value, (int, float)):
                return True
            return bool(re.search(r"\d", str(value or "")))
        if not (stat_like(home) or stat_like(away)):
            return
        if key in seen:
            return
        seen.add(key)
        rows.append({"label": label_text, "home": home, "away": away})

    def paired_keys(node: dict[str, Any]) -> None:
        pairs: dict[str, dict[str, Any]] = {}
        for key, value in node.items():
            key_text = str(key)
            low = key_text.lower()
            side = ""
            base = ""
            for prefix in ("home_", "home", "team1_", "participant1_"):
                if low.startswith(prefix):
                    side, base = "home", key_text[len(prefix):]
                    break
            if not side:
                for prefix in ("away_", "away", "team2_", "participant2_"):
                    if low.startswith(prefix):
                        side, base = "away", key_text[len(prefix):]
                        break
            if side and base and not isinstance(value, (dict, list)):
                pairs.setdefault(base, {})[side] = value
        for base, pair in pairs.items():
            if "home" in pair or "away" in pair:
                add(base, pair.get("home"), pair.get("away"))

    def walk(node: Any, inherited: str = "") -> None:
        if isinstance(node, dict):
            label = clean_label(_dig(node, "label", "name", "type", "title", "stat", "key", default=inherited))
            home = _dig(node, "home", "home_value", "homeValue", "participant1", "team1", "value1")
            away = _dig(node, "away", "away_value", "awayValue", "participant2", "team2", "value2")
            if home not in (None, "") or away not in (None, ""):
                if not isinstance(home, (dict, list)) and not isinstance(away, (dict, list)):
                    add(label, home, away)
            paired_keys(node)
            for key, value in node.items():
                if key in {"home", "away", "home_value", "away_value", "homeValue", "awayValue", "participant1", "participant2", "team1", "team2", "value1", "value2"}:
                    continue
                child_label = label if str(key).lower() in {"items", "rows", "stats", "statistics", "groups", "data", "game", "match"} else clean_label(key)
                walk(value, child_label)
        elif isinstance(node, list):
            for value in node:
                walk(value, inherited)

    walk(raw)
    return rows[:80]


def _participant_slug(item: dict[str, Any], obj: Any, side: str, sport: str, name: str) -> str:
    """Resolve the public team slug expected by ``/api/widget/team/``.

    SportScore often exposes both an opaque internal ID and a URL slug. The
    team endpoint expects the readable URL slug, so IDs must not win over URL
    or name-based slugs.
    """
    side = "away" if side == "away" else "home"
    idx = "2" if side == "away" else "1"

    for value in (
        _dig(obj, "url", "link", "@id", default=""),
        _dig(obj, "slug", default=""),
        _dig(item, f"{side}_slug", f"{side}Slug", f"{side}_team_slug", f"{side}TeamSlug", f"participant{idx}_slug", f"participant{idx}Slug", f"player{idx}_slug", f"player{idx}Slug", default=""),
    ):
        slug = _slug_generic(value, "team") or _slug_from_url(value)
        if slug:
            return slug

    # The documented team endpoint is slug-based. A name-derived slug is more
    # useful than an opaque provider ID such as ``2y8m4pt3w1dml07``.
    ascii_name = unicodedata.normalize("NFKD", str(name or "")).encode("ascii", "ignore").decode("ascii")
    name_slug = re.sub(r"[^a-z0-9]+", "-", ascii_name.lower()).strip("-")
    if name_slug:
        return name_slug

    value = _dig(obj, "id", default="") or _dig(
        item,
        f"{side}_id", f"{side}Id", f"{side}_team_id", f"{side}TeamId",
        f"participant{idx}_id", f"player{idx}_id",
        default="",
    )
    return re.sub(r"[^a-z0-9-]+", "", str(value or "").lower())


def normalize_match(item: dict[str, Any], *, source: str = "sportscore", sport: str = "football") -> dict[str, Any]:
    item = item if isinstance(item, dict) else {}
    sport = _sport_name(sport or item.get("sport"))
    home_obj = _participant_obj(item, "home")
    away_obj = _participant_obj(item, "away")
    home = str(_dig(item, "home", "home_name", "team1", "player1_name", "participant1_name", default="") or _entity_name(home_obj) or "Home").strip()
    away = str(_dig(item, "away", "away_name", "team2", "player2_name", "participant2_name", default="") or _entity_name(away_obj) or "Away").strip()
    url = str(_dig(item, "url", "link", "match_url", "@id", default="") or "")
    slug = str(_dig(item, "slug", "match_slug", default="") or _slug_from_url(url)).strip()
    if not slug:
        slug = re.sub(r"[^a-z0-9]+", "-", f"{home}-vs-{away}".lower()).strip("-")
    provider_raw = str(_dig(item, "provider_match_id", "match_id", "matchId", "event_id", "eventId", "id", default="") or "").strip()
    provider_match_id = provider_raw if re.fullmatch(r"[A-Za-z0-9_-]{2,80}", provider_raw) else ""
    identity = provider_match_id or slug
    mid = "ss:" + identity if sport == "football" else f"ss:{sport}:{identity}"
    kickoff = _parse_iso(_dig(item, "time", "startDate", "start_time", "start_at", "scheduled_at", "kickoff", "kickoff_at", "date"))
    status, is_live, finished, scheduled = _status_info(item)
    hs = _side_score(item, "home")
    ast = _side_score(item, "away")
    status_text = str(_dig(item, "status_text", "statusText", "minute", "clock", "period", "current_period", "phase", "status", default="") or "").strip()
    minute_match = re.search(r"\d+", status_text)
    minute = _to_int(minute_match.group(0) if minute_match else 0)
    competition_obj = _dig(item, "competition", "league", "tournament", "superEvent", "organizer", default={})
    league_raw = _dig(item, "competition_name", "league_name", "tournament_name", default="")
    league = _entity_name(league_raw) or _entity_name(competition_obj) or "Без лиги"
    competition_identity = _competition_identity(_dig(competition_obj, "url", "link", "@id", default="")) if sport == "football" else {"slug": str(_dig(competition_obj, "slug", "id", default="") or ""), "id": "", "path": "", "country_slug": ""}
    country_raw = _dig(item, "country_name", "location.address.addressCountry", "addressCountry", "country", default="")
    country = _entity_name(country_raw) or "Без страны"
    home_logo = str(_dig(item, "home_logo", "homeLogo", "home_team_logo", "homeTeam.logo", "home_team.logo", "participant1.logo", "player1.logo", default="") or "")
    away_logo = str(_dig(item, "away_logo", "awayLogo", "away_team_logo", "awayTeam.logo", "away_team.logo", "participant2.logo", "player2.logo", default="") or "")
    home_link = _absolute_url(_dig(home_obj, "url", "link", "@id", default=""))
    away_link = _absolute_url(_dig(away_obj, "url", "link", "@id", default=""))
    images = item.get("image")
    if isinstance(images, list):
        if not home_logo and len(images) > 0: home_logo = str(images[0] or "")
        if not away_logo and len(images) > 1: away_logo = str(images[1] or "")
    raw_stats = _dig(item, "stats", "statistics", default={})
    stats = _flat_stats(raw_stats) if sport == "football" else {}
    odds = item.get("odds") if isinstance(item.get("odds"), dict) else {}
    parts = _score_parts(item, sport) if sport != "football" else []
    path = f"/{sport}/match/{slug}/" if sport != "football" else f"/football/match/{slug}/"
    return {
        "id": mid, "provider_match_id": provider_match_id, "slug": slug, "sport": sport,
        "home": home, "away": away,
        "home_id": _participant_slug(item, home_obj, "home", sport, home),
        "away_id": _participant_slug(item, away_obj, "away", sport, away),
        "home_link": home_link, "away_link": away_link, "home_logo": home_logo, "away_logo": away_logo,
        "score_home": hs, "score_away": ast, "score": f"{hs}-{ast}", "score_parts": parts,
        "minute": minute,
        "minute_text": status_text or ("LIVE" if is_live else "FT" if finished else (kickoff.strftime("%H:%M") if kickoff else "СКОРО")),
        "period": status_text or ("LIVE" if is_live else "FT" if finished else "PRE"),
        "status": status, "is_live": is_live, "finished": finished, "scheduled": scheduled,
        "kickoff_at": int(kickoff.timestamp()) if kickoff else 0, "kickoff_iso": kickoff.isoformat() if kickoff else "",
        "date_text": kickoff.strftime("%d.%m.%Y") if kickoff else "", "time_text": kickoff.strftime("%H:%M") if kickoff else "",
        "country": country, "country_code": "", "league": league,
        "league_logo": str(_dig(competition_obj, "logo", "image", default="") or ""),
        "competition_slug": competition_identity.get("slug") or str(_dig(competition_obj, "slug", "id", default="") or ""),
        "competition_id": competition_identity.get("id") or "", "competition_path": competition_identity.get("path") or "",
        "competition_country_slug": competition_identity.get("country_slug") or "", "link": BASE_URL + path,
        "stats": stats, "sport_stats": _generic_sport_stats(raw_stats) if sport != "football" else [],
        "odds": odds, "has_odds": bool(odds), "source": source,
    }


def _iter_jsonld(soup: BeautifulSoup):
    for tag in soup.select('script[type="application/ld+json"]'):
        text = tag.string or tag.get_text("", strip=True)
        if not text:
            continue
        try:
            yield json.loads(text)
        except Exception:
            continue


def _walk_json(obj: Any):
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from _walk_json(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from _walk_json(v)


def _page_scheduled_total(text: str) -> int:
    """Best-effort scheduled counter printed in the SportScore page header."""
    plain = BeautifulSoup(text or "", "html.parser").get_text(" ", strip=True)
    patterns = (
        r"\bScheduled\s+fixtures\s*\(?\s*(\d{1,5})\s*\)?",
        r"\bScheduled\s*\(?\s*(\d{1,5})\s*\)?",
    )
    for pattern in patterns:
        m = re.search(pattern, plain, re.I)
        if m:
            return _to_int(m.group(1), 0)
    return 0


def _candidate_match_slug(node: Any) -> str:
    if node is None:
        return ""
    attrs = getattr(node, "attrs", {}) or {}
    for key in ("href", "data-href", "data-url", "data-match-url", "data-fixture-url"):
        slug = _slug_from_url(attrs.get(key))
        if slug:
            return slug
    for key in ("data-fixture-slug", "data-match-slug", "data-slug"):
        value = str(attrs.get(key) or "").strip()
        if value:
            return _slug_from_url(value) or value.strip("/")
    onclick = str(attrs.get("onclick") or "")
    return _slug_from_url(onclick)


def _match_container(node: Any) -> Any:
    """Find the smallest ancestor that contains one fixture and both teams.

    SportScore has changed the row class names a few times. Relying only on
    ``football-match-table-container`` caused the bot to collect only the SEO
    JSON-LD sample (usually 15-35 fixtures). This structural search is tolerant
    to desktop/mobile markup and Unpoly wrappers.
    """
    best = None
    cur = node
    for depth in range(11):
        if cur is None or not hasattr(cur, "select"):
            break
        team_links = cur.select('a[href*="/football/team/"]')
        match_nodes = cur.select(
            'a[href*="/football/match/"], [data-match-url*="/football/match/"], '
            '[data-url*="/football/match/"], [data-fixture-slug], [data-match-slug]'
        )
        classes = " ".join(cur.get("class") or []) if hasattr(cur, "get") else ""
        looks_row = any(token in classes.lower() for token in ("match", "fixture", "event", "game", "row"))
        if len(team_links) >= 2 and len(match_nodes) <= 5:
            best = cur
            if looks_row or depth <= 3:
                return cur
        cur = getattr(cur, "parent", None)
    return best or getattr(node, "parent", None)


def _context_link(container: Any, selector: str) -> Any:
    cur = container
    fallback = None
    for _ in range(7):
        if cur is None or not hasattr(cur, "select_one"):
            break
        found = cur.select_one(selector)
        if found:
            if fallback is None:
                fallback = found
            # Prefer a context block which does not include many fixtures.
            fixture_count = len(cur.select('a[href*="/football/match/"], [data-fixture-slug], [data-match-slug]'))
            if fixture_count <= 8:
                return found
        cur = getattr(cur, "parent", None)
    return fallback


def _extract_team_names(container: Any, match_node: Any) -> list[str]:
    names: list[str] = []
    if container is not None and hasattr(container, "select"):
        for a in container.select('a[href*="/football/team/"]'):
            name = " ".join(a.get_text(" ", strip=True).split())
            if name and name not in names:
                names.append(name)
        if len(names) < 2:
            for el in container.select(
                '.home-name, .away-name, .team-name, .football-match-team-name, '
                '[data-home-team], [data-away-team]'
            ):
                name = str(el.get("data-home-team") or el.get("data-away-team") or el.get_text(" ", strip=True)).strip()
                name = " ".join(name.split())
                if name and name not in names:
                    names.append(name)
        if len(names) < 2:
            for img in container.select('img[alt]'):
                name = re.sub(r"\s+(?:team\s+)?logo$", "", str(img.get("alt") or ""), flags=re.I).strip()
                if name and "flag" not in name.lower() and name not in names:
                    names.append(name)
    if len(names) < 2 and match_node is not None:
        aria = str(match_node.get("aria-label") or match_node.get("title") or "")
        parts = re.split(r"\s+vs\.?\s+", aria, maxsplit=1, flags=re.I)
        if len(parts) == 2:
            names = [parts[0].strip(), re.split(r"\s+[—|]\s+", parts[1])[0].strip()]
    return names[:2]


def _extract_kickoff(container: Any, default_date: dt.date | None) -> str:
    if container is None:
        return ""
    time_el = container.select_one("time[datetime]") if hasattr(container, "select_one") else None
    if time_el:
        value = str(time_el.get("datetime") or "").strip()
        if value:
            return value
    attrs_to_check = ("data-start-date", "data-kickoff", "data-start", "data-time", "data-datetime", "data-timestamp")
    candidates = [container]
    if hasattr(container, "select"):
        candidates += list(container.select("[data-start-date], [data-kickoff], [data-start], [data-time], [data-datetime], [data-timestamp]"))
    for el in candidates:
        attrs = getattr(el, "attrs", {}) or {}
        for key in attrs_to_check:
            value = attrs.get(key)
            if value in (None, ""):
                continue
            text = str(value).strip()
            if re.fullmatch(r"\d{10,13}", text):
                stamp = int(text)
                if stamp > 10**12:
                    stamp //= 1000
                return dt.datetime.fromtimestamp(stamp, tz=dt.timezone.utc).isoformat()
            if _parse_iso(text):
                return text
    raw = " ".join(container.get_text(" ", strip=True).split()) if hasattr(container, "get_text") else ""
    hm = re.search(r"\b([01]?\d|2[0-3]):[0-5]\d\b", raw)
    if hm and default_date:
        return f"{default_date.isoformat()}T{hm.group(0)}:00+00:00"
    return ""


def _absolute_url(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return urllib.parse.urljoin(BASE_URL + "/", text)


def _node_text(node: Any) -> str:
    if node is None or not hasattr(node, "get_text"):
        return ""
    return " ".join(node.get_text(" ", strip=True).split())


def _market_values(container: Any, market: str) -> list[float | None]:
    if container is None or not hasattr(container, "select_one"):
        return []
    root = container.select_one(f'[data-market="{market}"]')
    if root is None:
        return []
    values: list[float | None] = []
    for el in root.select('.football-odd-cell, [data-odd], [data-odds-value]'):
        raw = el.get("data-odd") or el.get("data-odds-value") or _node_text(el)
        values.append(_to_float(raw))
    return values


def _extract_row_odds(container: Any) -> dict[str, Any]:
    """Read each SportScore market separately.

    The old parser flattened all odd cells from the row. SportScore renders four
    markets in parallel, so flattening shifted Asian handicap/total/corners
    values into the wrong fields.
    """
    eu = _market_values(container, "tab-1")
    asia = _market_values(container, "tab-2")
    totals = _market_values(container, "tab-3")
    corners = _market_values(container, "tab-4")
    if not any(any(v is not None for v in values) for values in (eu, asia, totals, corners)):
        return {}
    eu += [None] * (3 - len(eu))
    asia += [None] * (3 - len(asia))
    totals += [None] * (3 - len(totals))
    corners += [None] * (3 - len(corners))
    return {
        "eu": {"1": eu[0], "X": eu[1], "2": eu[2]},
        "asia": {"home": asia[0], "line": asia[1], "away": asia[2]},
        "bs": {"over": totals[0], "line": totals[1], "under": totals[2]},
        "corners": {"over": corners[0], "line": corners[1], "under": corners[2]},
        "kind": "prematch",
    }


def _header_context(node: Any) -> dict[str, str]:
    comp = node.select_one('a[href*="/football/competition/"]') if hasattr(node, "select_one") else None
    country = node.select_one('a[href*="/football/country/"]') if hasattr(node, "select_one") else None
    flag = node.select_one('img[src]') if hasattr(node, "select_one") else None
    competition_url = _absolute_url(comp.get("href")) if comp else ""
    competition_identity = _competition_identity(competition_url)
    return {
        "league": _node_text(comp) or "Без лиги",
        "competition_url": competition_url,
        "competition_path": competition_identity.get("path") or "",
        "competition_id": competition_identity.get("id") or "",
        "competition_slug": competition_identity.get("slug") or "",
        "country": _node_text(country) or "Без страны",
        "country_url": _absolute_url(country.get("href")) if country else "",
        "league_logo": _absolute_url(flag.get("src")) if flag else "",
    }


def _row_fixture(row: Any, context: dict[str, str], default_date: dt.date | None) -> dict[str, Any] | None:
    provider_match_id = str(row.get("data-match-id") or "").strip()
    # Desktop and mobile markup are duplicated. Parse desktop only when present.
    body = row.select_one('.d-none.d-md-flex') or row.select_one('.football-match-table-container') or row
    match_link = body.select_one('a.sc-stretched-link[href*="/football/match/"]') or body.select_one('a[href*="/football/match/"]')
    if match_link is None:
        return None
    href = _absolute_url(match_link.get("href") or match_link.get("data-url") or match_link.get("data-match-url"))
    slug = _slug_from_url(href)
    if not slug:
        return None

    team_links = []
    for link in body.select('a.sc-team-link[href*="/football/team/"], a[href*="/football/team/"]'):
        name = _node_text(link)
        if name and all(_absolute_url(link.get("href")) != x[1] for x in team_links):
            team_links.append((name, _absolute_url(link.get("href"))))
        if len(team_links) == 2:
            break
    if len(team_links) < 2:
        return None

    utc_el = body.select_one('[data-utc]')
    kickoff = str(utc_el.get("data-utc") or "").strip() if utc_el else _extract_kickoff(body, default_date)
    parsed_kickoff = _parse_iso(kickoff)
    if parsed_kickoff is None:
        return None

    status_text = _node_text(body.select_one('[data-live="status"]'))
    row_text = _node_text(body)
    status_probe = f"{status_text} {row_text}".lower()
    if re.search(r'(?:^|\s)(?:ft|aet|pen|ht)(?:\s|$)', status_probe, re.I) or any(x in status_probe for x in ("finished", "заверш", "live", "перерыв")):
        return None

    team_logos: list[str] = []
    for img in body.select('.football-match-table-box img[src], img.icon-circuit[src], a[href*="/football/team/"] img[src]'):
        src = _absolute_url(img.get("src"))
        if src and "star" not in src.lower() and src not in team_logos:
            team_logos.append(src)
        if len(team_logos) == 2:
            break

    obj: dict[str, Any] = {
        "home": team_links[0][0],
        "away": team_links[1][0],
        "homeTeam": {"name": team_links[0][0], "url": team_links[0][1]},
        "awayTeam": {"name": team_links[1][0], "url": team_links[1][1]},
        "url": href,
        "slug": slug,
        "startDate": parsed_kickoff.isoformat(),
        "eventStatus": "EventScheduled",
        "country": context.get("country") or "Без страны",
        "competition": {
            "name": context.get("league") or "Без лиги",
            "url": context.get("competition_url") or context.get("competition_path") or "",
        },
        "home_logo": team_logos[0] if len(team_logos) > 0 else "",
        "away_logo": team_logos[1] if len(team_logos) > 1 else "",
    }
    item = normalize_match(obj, source="sportscore_html")
    # data-match-id is the unique fixture identity. The slug is only a route and
    # can be reused for another meeting between the same teams.
    if provider_match_id:
        item["provider_match_id"] = provider_match_id
        item["id"] = "ss:" + provider_match_id
    item["slug"] = slug
    item["link"] = href
    item["league_logo"] = context.get("league_logo") or item.get("league_logo") or ""
    item["competition_path"] = context.get("competition_path") or item.get("competition_path") or ""
    item["competition_id"] = context.get("competition_id") or item.get("competition_id") or ""
    item["competition_slug"] = context.get("competition_slug") or item.get("competition_slug") or ""
    item["country_url"] = context.get("country_url") or ""
    odds = _extract_row_odds(body)
    if odds:
        item["odds"] = odds
        item["has_odds"] = True
    return item


def _legacy_fixture_rows(soup: BeautifulSoup, default_date: dt.date | None) -> list[dict[str, Any]]:
    """Compatibility parser for old fixtures/tests without data-match-id."""
    out: list[dict[str, Any]] = []
    for body in soup.select('.football-match-table-container.sc-row-stretched'):
        if body.find_parent(attrs={"data-live-row": True}) is not None:
            continue
        match_link = body.select_one('a.sc-stretched-link[href*="/football/match/"]') or body.select_one('a[href*="/football/match/"]')
        if match_link is None:
            continue
        pseudo = BeautifulSoup('<div data-live-row=""></div>', 'html.parser').div
        pseudo.append(body.__copy__())
        context = _header_context(body)
        item = _row_fixture(pseudo, context, default_date)
        if item:
            out.append(item)
    return out


def parse_upcoming_html(text: str, default_date: dt.date | None = None) -> list[dict[str, Any]]:
    """Parse every rendered SportScore fixture in linear time.

    SportScore ships scheduled rows in the HTML document. We walk direct
    children of each match-state section so competition context is updated once
    per league instead of repeatedly searching the entire page for every match.
    """
    soup = BeautifulSoup(text or "", "html.parser")
    out: dict[str, dict[str, Any]] = {}

    sections = soup.select('section.match-state-section')
    for section in sections:
        context = {"league": "Без лиги", "country": "Без страны", "competition_url": "", "competition_path": "", "competition_id": "", "competition_slug": "", "country_url": "", "league_logo": ""}
        for child in section.find_all(recursive=False):
            classes = set(child.get("class") or [])
            if "table-active" in classes:
                context = _header_context(child)
                continue
            if not child.has_attr("data-live-row") or not child.get("data-match-id"):
                continue
            item = _row_fixture(child, context, default_date)
            if not item:
                continue
            key = str(item.get("id") or f'{item.get("slug")}@{item.get("kickoff_at")}')
            out[key] = item

    # Older saved fixtures and unit tests do not have the new row attributes.
    if not out:
        for item in _legacy_fixture_rows(soup, default_date):
            key = str(item.get("id") or f'{item.get("slug")}@{item.get("kickoff_at")}')
            out[key] = item

    # JSON-LD is a small SEO sample. Use it only if no rendered rows exist.
    if not out:
        for root in _iter_jsonld(soup):
            for obj in _walk_json(root):
                if str(obj.get("@type") or "") != "SportsEvent":
                    continue
                item = normalize_match(obj, source="sportscore_jsonld")
                if item.get("scheduled") or item.get("status") == "scheduled":
                    key = str(item.get("id") or f'{item.get("slug")}@{item.get("kickoff_at")}')
                    out[key] = item

    return sorted(out.values(), key=lambda x: (x.get("kickoff_at") or 2**62, x.get("league") or "", x.get("home") or ""))

def _sports_event_from_html(soup: BeautifulSoup) -> dict[str, Any]:
    for root in _iter_jsonld(soup):
        for obj in _walk_json(root):
            if str(obj.get("@type") or "") == "SportsEvent" and "/football/match/" in str(obj.get("url") or obj.get("@id") or ""):
                return obj
    return {}


def _parse_odds(soup: BeautifulSoup) -> dict[str, Any]:
    for th in soup.find_all("th"):
        if "pre-match odds" not in th.get_text(" ", strip=True).lower():
            continue
        row = th.find_parent("tr")
        vals = [_to_float(td.get_text(" ", strip=True)) for td in row.find_all("td")]
        vals += [None] * (11 - len(vals))
        eu = {"1": vals[0], "X": vals[1], "2": vals[2]}
        asia = {"home": vals[3], "away": vals[4]}
        bs = {"line": vals[5], "over": vals[6], "under": vals[7]}
        corners = {"line": vals[8], "over": vals[9], "under": vals[10]}
        return {"eu": eu, "asia": asia, "bs": bs, "corners": corners, "kind": "prematch"}
    return {}


def _parse_h2h(soup: BeautifulSoup) -> dict[str, Any]:
    root = soup.select_one("#h2h")
    if not root:
        root = soup.find(class_=lambda c: c and "h2h" in str(c).lower())
    if not root:
        return {"total": 0, "home_wins": 0, "draws": 0, "away_wins": 0, "meetings": []}
    total = _to_int((root.select_one(".meetings-total strong") or root.select_one(".meetings-total") or {}).get_text(" ", strip=True) if root.select_one(".meetings-total") else 0)
    home_wins = _to_int((root.select_one(".num-cell .n.home") or {}).get_text(" ", strip=True) if root.select_one(".num-cell .n.home") else 0)
    away_wins = _to_int((root.select_one(".num-cell .n.away") or {}).get_text(" ", strip=True) if root.select_one(".num-cell .n.away") else 0)
    draws = 0
    for el in root.select(".h2h-bar-labels span"):
        m = re.search(r"(\d+)\s+draw", el.get_text(" ", strip=True), re.I)
        if m:
            draws = int(m.group(1))
            break
    meetings = []
    for li in root.select(".h2h-meetings-list li"):
        score_text = (li.select_one(".score") or {}).get_text(" ", strip=True) if li.select_one(".score") else ""
        sm = re.search(r"(\d+)\s*[-:]\s*(\d+)", score_text)
        home_name = (li.select_one(".home-name") or {}).get_text(" ", strip=True) if li.select_one(".home-name") else ""
        away_name = (li.select_one(".away-name") or {}).get_text(" ", strip=True) if li.select_one(".away-name") else ""
        date = (li.select_one(".date") or {}).get_text(" ", strip=True) if li.select_one(".date") else ""
        result = (li.select_one(".result-pill") or {}).get_text(" ", strip=True) if li.select_one(".result-pill") else ""
        meetings.append({
            "date": date, "home": home_name, "away": away_name,
            "score_home": int(sm.group(1)) if sm else 0,
            "score_away": int(sm.group(2)) if sm else 0,
            "score": score_text, "result": result,
        })
    if total == 0:
        total = home_wins + draws + away_wins or len(meetings)
    return {"total": total, "home_wins": home_wins, "draws": draws, "away_wins": away_wins, "meetings": meetings}


def _parse_standings(soup: BeautifulSoup) -> list[dict[str, Any]]:
    table = soup.select_one("#standings table.standings-table") or soup.select_one("table.standings-table")
    if not table:
        return []
    out = []
    for tr in table.select("tbody tr"):
        cells = tr.find_all("td")
        if len(cells) < 8:
            continue
        team_cell = cells[1]
        out.append({
            "position": _to_int(cells[0].get_text(" ", strip=True)),
            "team": " ".join(team_cell.get_text(" ", strip=True).split()),
            "team_slug": _slug_generic((team_cell.find("a") or {}).get("href") if team_cell.find("a") else "", "team"),
            "logo": (team_cell.find("img") or {}).get("src", "") if team_cell.find("img") else "",
            "played": _to_int(cells[2].get_text(" ", strip=True)),
            "wins": _to_int(cells[3].get_text(" ", strip=True)),
            "draws": _to_int(cells[4].get_text(" ", strip=True)),
            "losses": _to_int(cells[5].get_text(" ", strip=True)),
            "goal_difference": cells[6].get_text(" ", strip=True),
            "points": _to_int(cells[7].get_text(" ", strip=True)),
            "form": [x.get_text(" ", strip=True).upper() for x in tr.select(".form-pill")],
            "is_home": "is-home" in (tr.get("class") or []),
            "is_away": "is-away" in (tr.get("class") or []),
        })
    return out


def _parse_lineups(soup: BeautifulSoup) -> dict[str, Any]:
    root = soup.select_one("#lineups")
    if not root:
        return {"announced": False, "message": "Составы пока недоступны", "home": [], "away": []}
    empty = root.select_one(".lineups-empty")
    if empty:
        msg = root.select_one(".lineups-empty .msg")
        sub = root.select_one(".lineups-empty .sub")
        return {
            "announced": False,
            "message": " ".join((msg.get_text(" ", strip=True) if msg else "Составы ещё не объявлены").split()),
            "submessage": " ".join((sub.get_text(" ", strip=True) if sub else "").split()),
            "home": [], "away": [],
        }

    head = root.select_one(".lineups-head")
    formations = []
    if head:
        formations = [x.get_text(" ", strip=True) for x in head.select(".formation")]

    def players_for(side: str) -> list[dict[str, Any]]:
        container = root.select_one(f".lineups-grid .side.{side}") or root.select_one(f".lineup-player-list.{side}")
        if not container:
            return []
        players = []
        for row in container.select("li, .player, .lineup-player"):
            name_el = row.select_one(".name, .player-name")
            name = " ".join((name_el.get_text(" ", strip=True) if name_el else row.get_text(" ", strip=True)).split())
            if not name:
                continue
            number_el = row.select_one(".number, .shirt-number")
            rating_el = row.select_one(".rating")
            players.append({
                "name": name,
                "number": _to_int(number_el.get_text(" ", strip=True) if number_el else 0),
                "rating": _to_float(rating_el.get_text(" ", strip=True) if rating_el else None),
            })
        return players

    coaches = [x.get_text(" ", strip=True) for x in root.select(".lineups-coach .coach-name")]
    return {
        "announced": True,
        "confirmed": bool(root.select_one(".lineup-confirmed-badge:not(.provisional)")),
        "home_formation": formations[0] if formations else "",
        "away_formation": formations[1] if len(formations) > 1 else "",
        "home_coach": coaches[0] if coaches else "",
        "away_coach": coaches[1] if len(coaches) > 1 else "",
        "home": players_for("home"),
        "away": players_for("away"),
    }


def _parse_html_stats(soup: BeautifulSoup) -> dict[str, int]:
    root = soup.select_one("#stats") or soup.select_one(".fb-stats")
    if not root:
        return _flat_stats({})
    rows = []
    for tr in root.select("tr"):
        cells = tr.find_all(["th", "td"])
        if len(cells) >= 3:
            rows.append({"name": cells[1].get_text(" ", strip=True), "home": cells[0].get_text(" ", strip=True), "away": cells[-1].get_text(" ", strip=True)})
    for block in root.select(".stat-row, .stat-bar, .v-stat-row"):
        label = block.select_one(".label, .stat-bar-label, .v-stat-row__label")
        vals = block.select(".value, .stat-bar-value, .v-stat__value")
        if label and len(vals) >= 2:
            rows.append({"name": label.get_text(" ", strip=True), "home": vals[0].get_text(" ", strip=True), "away": vals[-1].get_text(" ", strip=True)})
    return _flat_stats(rows)


def parse_match_html(text: str, slug: str = "") -> dict[str, Any]:
    soup = BeautifulSoup(text or "", "html.parser")
    event = _sports_event_from_html(soup)
    match = normalize_match(event, source="sportscore_html") if event else normalize_match({"slug": slug, "url": f"/football/match/{slug}/"}, source="sportscore_html")
    if slug:
        match["slug"] = slug
        match["id"] = "ss:" + slug
        match["link"] = BASE_URL + f"/football/match/{slug}/"
    odds = _parse_odds(soup)
    match["odds"] = odds
    match["has_odds"] = bool(odds)
    location = event.get("location") if isinstance(event, dict) and isinstance(event.get("location"), dict) else {}
    address = location.get("address") if isinstance(location.get("address"), dict) else {}
    venue = {
        "name": str(location.get("name") or ""),
        "city": str(address.get("addressLocality") or ""),
        "country": str(address.get("addressCountry") or ""),
        "capacity": _to_int(location.get("maximumAttendeeCapacity"), 0),
    }
    return {
        "match": match,
        "odds": odds,
        "h2h": _parse_h2h(soup),
        "standings": _parse_standings(soup),
        "lineups": _parse_lineups(soup),
        "stats_flat": _parse_html_stats(soup),
        "venue": venue,
    }



def _score_text(node: Any) -> str:
    if node is None:
        return ""
    text = _node_text(node)
    if text:
        return text
    for key in ("data-score", "data-value", "value"):
        value = str(node.get(key) or "").strip() if hasattr(node, "get") else ""
        if value:
            return value
    return ""


def _row_live_fixture(row: Any, context: dict[str, str]) -> dict[str, Any] | None:
    """Parse one rendered live row from SportScore's football page."""
    provider_match_id = str(row.get("data-match-id") or "").strip()
    body = row.select_one('.d-none.d-md-flex') or row.select_one('.football-match-table-container') or row
    match_link = body.select_one('a.sc-stretched-link[href*="/football/match/"]') or body.select_one('a[href*="/football/match/"]')
    if match_link is None:
        return None
    href = _absolute_url(match_link.get("href") or match_link.get("data-url") or match_link.get("data-match-url"))
    slug = _slug_from_url(href)
    if not slug:
        return None

    team_links: list[tuple[str, str]] = []
    for link in body.select('a.sc-team-link[href*="/football/team/"], a[href*="/football/team/"]'):
        name = _node_text(link)
        url = _absolute_url(link.get("href"))
        if name and url and all(url != x[1] for x in team_links):
            team_links.append((name, url))
        if len(team_links) == 2:
            break
    if len(team_links) < 2:
        return None

    status_text = _node_text(body.select_one('[data-live="status"]'))
    status_probe = status_text.casefold()
    home_score_el = body.select_one('[data-live="home-score"]')
    away_score_el = body.select_one('[data-live="away-score"]')
    home_score_text = _score_text(home_score_el)
    away_score_text = _score_text(away_score_el)
    score_visible = bool(
        (home_score_el is not None and not home_score_el.has_attr("hidden"))
        or (away_score_el is not None and not away_score_el.has_attr("hidden"))
        or home_score_text or away_score_text
    )
    finished = bool(re.search(r"(?:^|\s)(?:ft|aet|pen)(?:\s|$)", status_probe)) or any(
        x in status_probe for x in ("finished", "заверш", "окончен")
    )
    live = not finished and (
        score_visible
        or bool(re.search(r"\d{1,3}(?:\+\d{1,2})?\s*['’]?", status_text))
        or any(x in status_probe for x in ("live", "ht", "1st", "2nd", "half", "перерыв"))
    )
    if not live:
        return None

    utc_el = body.select_one('[data-utc]')
    kickoff = str(utc_el.get("data-utc") or "").strip() if utc_el else ""
    team_logos: list[str] = []
    for img in body.select('.football-match-table-box img[src], img.icon-circuit[src], a[href*="/football/team/"] img[src]'):
        src = _absolute_url(img.get("src"))
        if src and "star" not in src.lower() and src not in team_logos:
            team_logos.append(src)
        if len(team_logos) == 2:
            break

    obj: dict[str, Any] = {
        "id": provider_match_id,
        "home": team_links[0][0],
        "away": team_links[1][0],
        "homeTeam": {"name": team_links[0][0], "url": team_links[0][1]},
        "awayTeam": {"name": team_links[1][0], "url": team_links[1][1]},
        "url": href,
        "slug": slug,
        "time": kickoff,
        "status": "live",
        "status_text": status_text or "LIVE",
        "home_score": _to_int(home_score_text, 0),
        "away_score": _to_int(away_score_text, 0),
        "country": context.get("country") or "Без страны",
        "competition": {
            "name": context.get("league") or "Без лиги",
            "url": context.get("competition_url") or context.get("competition_path") or "",
        },
        "home_logo": team_logos[0] if len(team_logos) > 0 else "",
        "away_logo": team_logos[1] if len(team_logos) > 1 else "",
    }
    item = normalize_match(obj, source="sportscore_html_live")
    if provider_match_id:
        item["provider_match_id"] = provider_match_id
        item["id"] = "ss:" + provider_match_id
    item["slug"] = slug
    item["link"] = href
    item["league_logo"] = context.get("league_logo") or item.get("league_logo") or ""
    item["competition_path"] = context.get("competition_path") or item.get("competition_path") or ""
    item["competition_id"] = context.get("competition_id") or item.get("competition_id") or ""
    item["competition_slug"] = context.get("competition_slug") or item.get("competition_slug") or ""
    odds = _extract_row_odds(body)
    if odds:
        item["odds"] = odds
        item["has_odds"] = True
    return item


def parse_live_html(text: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(text or "", "html.parser")
    out: dict[str, dict[str, Any]] = {}
    sections = soup.select('section.match-state-section')
    roots = sections or [soup]
    for root in roots:
        context = {"league": "Без лиги", "country": "Без страны", "competition_url": "", "competition_path": "", "competition_id": "", "competition_slug": "", "country_url": "", "league_logo": ""}
        children = root.find_all(recursive=False) if root is not soup else root.find_all(["div", "section"], recursive=False)
        for child in children:
            classes = set(child.get("class") or [])
            if "table-active" in classes or child.select_one('a[href*="/football/competition/"]'):
                header = _header_context(child)
                if header.get("league") and header.get("league") != "Без лиги":
                    context = header
            rows = [child] if child.has_attr("data-live-row") else child.select("[data-live-row][data-match-id]")
            for row in rows:
                item = _row_live_fixture(row, context)
                if item:
                    out[str(item.get("id") or item.get("slug"))] = item
    if not out:
        for row in soup.select("[data-live-row][data-match-id]"):
            item = _row_live_fixture(row, _header_context(row))
            if item:
                out[str(item.get("id") or item.get("slug"))] = item
    return list(out.values())


def _normalize_event_type(value: Any) -> str:
    text = str(value or "").casefold()
    if "goal" in text or "penalty scored" in text or "гол" in text:
        return "goal"
    if "yellow" in text or "жёлт" in text or "желт" in text:
        return "yellow"
    if "red" in text or "красн" in text:
        return "red"
    return ""


def _normalize_events(raw: Any, match: dict[str, Any]) -> list[dict[str, Any]]:
    rows = raw if isinstance(raw, list) else []
    out: list[dict[str, Any]] = []
    home_id = str(match.get("home_id") or "")
    away_id = str(match.get("away_id") or "")
    home_name = _normalized_team_name(match.get("home"))
    away_name = _normalized_team_name(match.get("away"))
    for row in rows:
        if not isinstance(row, dict):
            continue
        kind = _normalize_event_type(_dig(row, "type", "event_type", "eventType", "incidentType", "name", "label", default=""))
        if not kind:
            continue
        minute_raw = str(_dig(row, "minute_text", "time", "minute", "clock", default="") or "").strip()
        minute_match = re.search(r"\d{1,3}", minute_raw)
        minute = _to_int(minute_match.group(0) if minute_match else 0)
        team_obj = _dig(row, "team", "participant", default={})
        team_id = str(_dig(row, "team_id", "teamId", "participant_id", "participantId", default="") or _dig(team_obj, "id", "slug", default="") or "")
        team_name = _normalized_team_name(_dig(row, "team_name", "teamName", default="") or _entity_name(team_obj))
        side = str(_dig(row, "side", "team_side", default="") or "").lower()
        if side not in {"home", "away"}:
            if team_id and home_id and team_id == home_id:
                side = "home"
            elif team_id and away_id and team_id == away_id:
                side = "away"
            elif team_name and team_name == home_name:
                side = "home"
            elif team_name and team_name == away_name:
                side = "away"
            else:
                side = "neutral"
        player_obj = _dig(row, "player", "scorer", default={})
        player = str(_dig(row, "player_name", "playerName", "name", default="") or _entity_name(player_obj) or "").strip()
        score = str(_dig(row, "score", "result", default="") or "").replace(":", "-").strip()
        out.append({
            "type": kind,
            "minute": minute,
            "minute_text": minute_raw or (f"{minute}'" if minute else "—"),
            "side": side,
            "player": player,
            "score": score,
        })
    out.sort(key=lambda x: (_to_int(x.get("minute"), 0), 0 if x.get("type") == "goal" else 1))
    return out


def _extract_matches_envelope(payload: Any) -> list[dict[str, Any]]:
    """Return match rows from every documented SportScore envelope shape.

    The official Python client examples use ``response["data"]``.  Older
    widget responses used ``matches``. Supporting both is required because
    football and the generic sports are not always served by the same backend.
    """
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if not isinstance(payload, dict):
        return []
    preferred = ("matches", "fixtures", "results", "recent", "past", "completed", "upcoming", "schedule", "games", "events", "items", "data")
    for key in preferred:
        value = payload.get(key)
        if isinstance(value, list):
            rows = [x for x in value if isinstance(x, dict)]
            if rows:
                return rows
        if isinstance(value, dict):
            nested = _extract_matches_envelope(value)
            if nested:
                return nested
    # Last-resort recursive scan for provider envelope changes. Only accept
    # lists whose rows look like fixtures, not players or generic metadata.
    for value in payload.values():
        if isinstance(value, list):
            rows = [x for x in value if isinstance(x, dict)]
            if rows and any(any(k in row for k in ("home", "away", "homeTeam", "awayTeam", "participant1", "participant2", "startDate", "match_id", "event_id")) for row in rows):
                return rows
        elif isinstance(value, dict):
            nested = _extract_matches_envelope(value)
            if nested:
                return nested
    return []


def _generic_match_slug(href: Any, sport: str) -> str:
    text = str(href or "").strip()
    try:
        path = urllib.parse.urlparse(text).path
    except Exception:
        path = text
    patterns = (
        rf"/{re.escape(sport)}/match/([^/?#]+)",
        rf"/{re.escape(sport)}/([^/?#]+)/[A-Za-z0-9_-]+/?$",
    )
    for pattern in patterns:
        m = re.search(pattern, path, re.I)
        if m:
            slug = m.group(1).strip()
            if slug not in {"competition", "team", "player", "country", "matches"}:
                return slug
    return ""


def _generic_row_match(row: Any, sport: str, default_date: dt.date | None = None) -> dict[str, Any] | None:
    sport = _sport_name(sport)
    if row is None or not hasattr(row, "select"):
        return None
    href = ""
    slug = ""
    link_node = None
    for link in row.select("a[href]"):
        candidate = str(link.get("href") or "")
        candidate_slug = _generic_match_slug(candidate, sport)
        if candidate_slug:
            href = _absolute_url(candidate)
            slug = candidate_slug
            link_node = link
            break
    if not slug:
        for key in ("data-match-url", "data-url", "data-href", "data-slug", "data-match-slug"):
            candidate = str(row.get(key) or "") if hasattr(row, "get") else ""
            slug = _generic_match_slug(candidate, sport) or (candidate.strip("/") if key in {"data-slug", "data-match-slug"} else "")
            if slug:
                href = _absolute_url(candidate) if "/" in candidate else BASE_URL + f"/{sport}/match/{slug}/"
                break
    if not slug:
        return None

    participant_selector = (
        f'a[href*="/{sport}/team/"], a[href*="/{sport}/player/"], '
        '.home-name, .away-name, .team-name, .player-name, .participant-name, '
        '[data-home-team], [data-away-team], [data-home-player], [data-away-player]'
    )
    names: list[str] = []
    participant_nodes: list[Any] = []
    for node in row.select(participant_selector):
        attrs = getattr(node, "attrs", {}) or {}
        name = str(attrs.get("data-home-team") or attrs.get("data-away-team") or attrs.get("data-home-player") or attrs.get("data-away-player") or _node_text(node)).strip()
        if name and name not in names:
            names.append(name)
            participant_nodes.append(node)
        if len(names) >= 2:
            break
    if len(names) < 2:
        for img in row.select("img[alt]"):
            name = re.sub(r"\s+(?:team|player)?\s*logo$", "", str(img.get("alt") or ""), flags=re.I).strip()
            if name and "flag" not in name.lower() and name not in names:
                names.append(name)
            if len(names) >= 2:
                break
    if len(names) < 2:
        aria = str((link_node or row).get("aria-label") or (link_node or row).get("title") or "")
        parts = re.split(r"\s+vs\.?\s+", aria, maxsplit=1, flags=re.I)
        if len(parts) == 2:
            names = [parts[0].strip(), parts[1].strip()]
    if len(names) < 2:
        return None

    kickoff_text = _extract_kickoff(row, default_date)
    status_node = row.select_one('[data-live="status"], .match-status, .status, [data-status]')
    status_text = _node_text(status_node) or str(row.get("data-status") or "")
    home_score_node = row.select_one('[data-live="home-score"], .home-score, [data-home-score]')
    away_score_node = row.select_one('[data-live="away-score"], .away-score, [data-away-score]')
    home_score = _score_text(home_score_node) or str(row.get("data-home-score") or "")
    away_score = _score_text(away_score_node) or str(row.get("data-away-score") or "")
    provider_id = str(row.get("data-match-id") or row.get("data-event-id") or "").strip()

    competition_link = row.find_previous("a", href=re.compile(rf"/{re.escape(sport)}/competition/", re.I))
    league = _node_text(competition_link) or "Без лиги"
    country = "Без страны"
    if competition_link:
        try:
            path_parts = urllib.parse.urlparse(str(competition_link.get("href") or "")).path.strip("/").split("/")
            if "competition" in path_parts:
                i = path_parts.index("competition")
                if len(path_parts) > i + 1:
                    country = path_parts[i + 1].replace("-", " ").title()
        except Exception:
            pass

    obj = {
        "id": provider_id,
        "slug": slug,
        "url": href,
        "home": names[0],
        "away": names[1],
        "home_score": home_score,
        "away_score": away_score,
        "status": status_text or ("scheduled" if kickoff_text else "unknown"),
        "status_text": status_text,
        "time": kickoff_text,
        "country": country,
        "competition_name": league,
        "sport": sport,
    }
    logos: list[str] = []
    for img in row.select("img[src]"):
        src = _absolute_url(img.get("src"))
        if src and "flag" not in src.lower() and "star" not in src.lower() and src not in logos:
            logos.append(src)
        if len(logos) == 2:
            break
    if logos:
        obj["home_logo"] = logos[0]
    if len(logos) > 1:
        obj["away_logo"] = logos[1]
    return normalize_match(obj, source="sportscore_html", sport=sport)


def parse_generic_sport_html(text: str, sport: str, default_date: dt.date | None = None) -> list[dict[str, Any]]:
    sport = _sport_name(sport)
    soup = BeautifulSoup(text or "", "html.parser")
    out: dict[str, dict[str, Any]] = {}
    # Rendered rows provide the full page rather than the limited SEO sample.
    candidates: list[Any] = list(soup.select("[data-match-id], [data-event-id], [data-live-row]"))

    # Basketball/tennis pages do not always expose the data attributes used by
    # football. Start from match links and climb to the smallest row containing
    # the participants and the score/time.
    seen_nodes: set[int] = {id(x) for x in candidates}
    for link in soup.select(f'a[href*="/{sport}/match/"]'):
        row = link
        best = None
        for _ in range(9):
            row = getattr(row, "parent", None)
            if row is None or not hasattr(row, "select"):
                break
            participant_links = row.select(f'a[href*="/{sport}/team/"], a[href*="/{sport}/player/"]')
            match_links = row.select(f'a[href*="/{sport}/match/"]')
            classes = " ".join(row.get("class") or []).lower() if hasattr(row, "get") else ""
            if len(match_links) <= 4 and (len(participant_links) >= 2 or any(k in classes for k in ("match", "fixture", "event", "game"))):
                best = row
                if len(participant_links) >= 2:
                    break
        if best is not None and id(best) not in seen_nodes:
            seen_nodes.add(id(best))
            candidates.append(best)

    for row in candidates:
        item = _generic_row_match(row, sport, default_date)
        if item:
            out[str(item.get("id") or item.get("slug"))] = item
    # JSON-LD fallback is valuable if SportScore changes CSS classes.
    for obj in _iter_jsonld(soup):
        for node in _walk_json(obj):
            if not isinstance(node, dict) or str(node.get("@type") or "").lower() != "sportsevent":
                continue
            item = normalize_match(node, source="sportscore_jsonld", sport=sport)
            if item.get("home") and item.get("away"):
                out.setdefault(str(item.get("id") or item.get("slug")), item)
    return list(out.values())



def list_api_matches(force: bool = False, sport: str = "football") -> list[dict[str, Any]]:
    sport = str(sport or "football").strip().lower()
    if sport not in {"football", "basketball", "tennis"}:
        sport = "football"
    params = {"sport": sport, "limit": MAX_MATCHES, "src": SOURCE_TAG}
    if force:
        params["_t"] = int(time.time())
    payload = _json("/api/widget/matches/", params, ttl=10 if force else LIVE_CACHE_SECONDS)
    rows = [normalize_match(x, source="sportscore_api", sport=sport) for x in _extract_matches_envelope(payload)]
    for item in rows:
        item["sport"] = sport
    return rows


def list_live(force: bool = False, sport: str = "football") -> list[dict[str, Any]]:
    """Return SportScore live matches only.

    JSON is authoritative for current score/status. The SportScore live HTML
    page supplements league, country, logos and rows beyond the API limit of 50.
    """
    sport = str(sport or "football").strip().lower()
    if sport not in {"football", "basketball", "tennis"}:
        sport = "football"
    errors: list[str] = []
    api_rows: list[dict[str, Any]] = []
    html_rows: list[dict[str, Any]] = []
    try:
        api_rows = [x for x in list_api_matches(force=force, sport=sport) if x.get("is_live") and not x.get("finished")]
    except Exception as exc:
        errors.append(f"api: {type(exc).__name__}: {exc}")
    try:
        if sport == "football":
            path = "/football/?filter=live"
            html_text = str(_cached("live-html:" + path, 10 if force else LIVE_HTML_CACHE_SECONDS, lambda: _request(path)))
            html_rows = parse_live_html(html_text)
        else:
            path = f"/{sport}/?filter=live"
            html_text = str(_cached("live-html:" + path, 10 if force else LIVE_HTML_CACHE_SECONDS, lambda: _request(path)))
            html_rows = [x for x in parse_generic_sport_html(html_text, sport) if x.get("is_live") and not x.get("finished")]
    except Exception as exc:
        errors.append(f"html: {type(exc).__name__}: {exc}")

    by_slug: dict[str, dict[str, Any]] = {}
    for item in html_rows:
        slug = str(item.get("slug") or item.get("id") or "")
        if slug:
            by_slug[slug] = dict(item)
    for item in api_rows:
        slug = str(item.get("slug") or item.get("id") or "")
        if not slug:
            continue
        base = by_slug.get(slug, {})
        merged = dict(item)
        for key, value in base.items():
            if key in {"score_home", "score_away", "score", "minute", "minute_text", "period", "status", "is_live", "finished"}:
                continue
            if value not in (None, "", {}, []):
                merged[key] = value
        if base.get("provider_match_id"):
            merged["provider_match_id"] = base["provider_match_id"]
            merged["id"] = base.get("id") or merged.get("id")
        merged["source"] = "sportscore"
        merged["sport"] = sport
        by_slug[slug] = merged

    out = [x for x in by_slug.values() if x.get("is_live") and not x.get("finished")]
    if not out and errors:
        raise RuntimeError("SportScore live unavailable; " + " | ".join(errors[:2]))
    out.sort(key=lambda x: (str(x.get("country") or ""), str(x.get("league") or ""), str(x.get("home") or "")))
    return out


_LAST_UPCOMING_DIAGNOSTICS: dict[str, Any] = {}


def list_upcoming(force: bool = False, sport: str = "football") -> list[dict[str, Any]]:
    """Load only future fixtures from dated SportScore pages.

    Dated pages are fetched concurrently to reduce wall-clock time. The widget
    API is used only when all HTML pages fail because it is capped and may mix
    live/recent events with scheduled fixtures.
    """
    global _LAST_UPCOMING_DIAGNOSTICS
    sport = str(sport or "football").strip().lower()
    if sport not in {"football", "basketball", "tennis"}:
        sport = "football"
    if sport != "football":
        today = dt.datetime.now(dt.timezone.utc).date()
        combined: dict[str, dict[str, Any]] = {}
        errors: list[str] = []
        date_paths = [
            (f"/{sport}/?date={(today + dt.timedelta(days=offset)).isoformat()}&filter=upcoming", today + dt.timedelta(days=offset))
            for offset in range(PREMATCH_DAYS_AHEAD)
        ]
        def load_generic(spec: tuple[str, dt.date]) -> list[dict[str, Any]]:
            path, page_date = spec
            page = _html(path, ttl=10 if force else CACHE_SECONDS)
            return parse_generic_sport_html(page, sport, default_date=page_date)
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(PREMATCH_WORKERS, len(date_paths))) as pool:
            futures = [pool.submit(load_generic, spec) for spec in date_paths]
            for future in futures:
                try:
                    for item in future.result():
                        if item.get("scheduled") and not item.get("is_live") and not item.get("finished"):
                            item["sport"] = sport
                            combined[str(item.get("id") or item.get("slug"))] = item
                except Exception as exc:
                    errors.append(f"{type(exc).__name__}: {exc}")
        # API fallback/supplement; endpoint may include near-future fixtures.
        try:
            for item in list_api_matches(force=force, sport=sport):
                if item.get("scheduled") and not item.get("is_live") and not item.get("finished"):
                    item["sport"] = sport
                    combined.setdefault(str(item.get("id") or item.get("slug")), item)
        except Exception as exc:
            errors.append(f"api: {type(exc).__name__}: {exc}")
        rows = list(combined.values())
        rows.sort(key=lambda x: (_to_int(x.get("kickoff_at"), 2**62), str(x.get("league") or ""), str(x.get("home") or "")))
        if not rows and errors:
            raise RuntimeError(f"SportScore {sport} upcoming unavailable; " + " | ".join(errors[:2]))
        return rows
    started = time.monotonic()
    combined: dict[str, dict[str, Any]] = {}
    errors: list[str] = []
    pages: list[dict[str, Any]] = []

    today = dt.datetime.now(dt.timezone.utc).date()
    date_paths = [
        (f"/football/?date={(today + dt.timedelta(days=offset)).isoformat()}&filter=upcoming", today + dt.timedelta(days=offset))
        for offset in range(PREMATCH_DAYS_AHEAD)
    ]

    def load_page(spec: tuple[str, dt.date]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        path, page_date = spec
        text = _html(path, ttl=10 if force else CACHE_SECONDS)
        rows = parse_upcoming_html(text, default_date=page_date)
        return ({
            "date": page_date.isoformat(),
            "expected": _page_scheduled_total(text),
            "parsed": len(rows),
            "path": path,
        }, rows)

    workers = min(PREMATCH_WORKERS, max(1, len(date_paths)))
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers, thread_name_prefix="sportscore-prematch") as pool:
        future_map = {pool.submit(load_page, spec): spec for spec in date_paths}
        for future in concurrent.futures.as_completed(future_map):
            path, page_date = future_map[future]
            try:
                info, rows = future.result()
                pages.append(info)
                for item in rows:
                    merge_key = str(item.get("id") or f'{item.get("slug")}@{item.get("kickoff_at")}')
                    current = combined.get(merge_key, {})
                    combined[merge_key] = {
                        **current,
                        **{k: v for k, v in item.items() if v not in (None, "", {}, [])},
                    }
            except Exception as exc:
                errors.append(f"{path}: {type(exc).__name__}: {exc}")

    # Legacy pages are a fallback only when every dated page produced no rows.
    if not combined:
        legacy = [
            ("/football/?filter=upcoming", today),
            ("/football/tomorrow/", today + dt.timedelta(days=1)),
        ]
        for path, page_date in legacy:
            try:
                info, rows = load_page((path, page_date))
                pages.append(info)
                for item in rows:
                    merge_key = str(item.get("id") or f'{item.get("slug")}@{item.get("kickoff_at")}')
                    current = combined.get(merge_key, {})
                    combined[merge_key] = {
                        **current,
                        **{k: v for k, v in item.items() if v not in (None, "", {}, [])},
                    }
            except Exception as exc:
                errors.append(f"{path}: {type(exc).__name__}: {exc}")

    api_fallback_used = False
    if not combined:
        try:
            api_fallback_used = True
            for item in list_api_matches(force=force):
                if item.get("scheduled") and not item.get("is_live") and not item.get("finished"):
                    merge_key = str(item.get("id") or f'{item.get("slug")}@{item.get("kickoff_at")}')
                    combined[merge_key] = item
        except Exception as exc:
            errors.append(f"widget API: {type(exc).__name__}: {exc}")

    if not combined and errors:
        raise RuntimeError("SportScore sources unavailable; " + " | ".join(errors[:3]))

    cutoff = int(time.time()) - PREMATCH_START_GRACE_SECONDS
    discarded = {"live": 0, "finished": 0, "not_scheduled": 0, "past": 0, "no_kickoff": 0}
    out: list[dict[str, Any]] = []
    for item in combined.values():
        status = str(item.get("status") or "").strip().lower()
        period = str(item.get("period") or "").strip().upper()
        if item.get("is_live") or status in {"live", "inplay", "in_play", "started"} or period in {"LIVE", "HT", "1H", "2H"}:
            discarded["live"] += 1
            continue
        if item.get("finished") or status in {"finished", "ended", "completed", "cancelled", "postponed", "abandoned"} or period in {"FT", "AET", "PEN"}:
            discarded["finished"] += 1
            continue
        if not item.get("scheduled"):
            discarded["not_scheduled"] += 1
            continue
        kickoff = _to_int(item.get("kickoff_at"), 0)
        if kickoff <= 0:
            discarded["no_kickoff"] += 1
            continue
        if kickoff < cutoff:
            discarded["past"] += 1
            continue
        item = dict(item)
        item["period"] = "PRE"
        item["status"] = "scheduled"
        item["scheduled"] = True
        item["is_live"] = False
        item["finished"] = False
        out.append(item)

    out.sort(key=lambda x: (int(x.get("kickoff_at") or 2**62), x.get("league") or "", x.get("home") or ""))
    pages.sort(key=lambda x: (x.get("date") or "", x.get("path") or ""))
    _LAST_UPCOMING_DIAGNOSTICS = {
        "days_ahead": PREMATCH_DAYS_AHEAD,
        "pages": pages,
        "total_unique": len(out),
        "raw_unique": len(combined),
        "discarded": discarded,
        "api_fallback_used": api_fallback_used,
        "parallel_workers": workers,
        "load_seconds": round(time.monotonic() - started, 3),
        "partial_pages": [p for p in pages if p.get("expected") and p.get("parsed", 0) < p.get("expected", 0)],
        "errors": errors[:8],
        "updated_at": int(time.time()),
    }
    return out


def upcoming_diagnostics() -> dict[str, Any]:
    return dict(_LAST_UPCOMING_DIAGNOSTICS)

def _team_matches(slug: str, limit: int = 10, sport: str = "football") -> list[dict[str, Any]]:
    if not slug:
        return []
    sport = _sport_name(sport)
    payload = _json("/api/widget/team/", {"sport": sport, "slug": slug, "limit": max(1, min(50, limit)), "src": SOURCE_TAG}, ttl=TEAM_CACHE_SECONDS)
    return [normalize_match(x, source="sportscore_team", sport=sport) for x in _extract_matches_envelope(payload)]


def _recent_summary(team_slug: str, team_name: str) -> dict[str, Any]:
    matches = [x for x in _team_matches(team_slug, 15) if x.get("finished")][:10]
    rows = []
    total_goals = scored = conceded = wins = draws = losses = 0
    for m in matches:
        is_home = str(m.get("home") or "").lower() == str(team_name or "").lower()
        sh, sa = int(m.get("score_home") or 0), int(m.get("score_away") or 0)
        gf, ga = (sh, sa) if is_home else (sa, sh)
        result = "W" if gf > ga else "D" if gf == ga else "L"
        wins += result == "W"
        draws += result == "D"
        losses += result == "L"
        total_goals += sh + sa
        scored += gf
        conceded += ga
        rows.append({
            "id": m.get("id"), "date": m.get("date_text"), "home": m.get("home"), "away": m.get("away"),
            "score_home": sh, "score_away": sa, "total": sh + sa, "result": result,
            "side": "home" if is_home else "away", "league": m.get("league"), "country": m.get("country"),
        })
    count = len(rows)
    return {
        "count": count,
        "avg": round(total_goals / count, 2) if count else None,
        "total_avg": round(total_goals / count, 2) if count else None,
        "scored_avg": round(scored / count, 2) if count else None,
        "conceded_avg": round(conceded / count, 2) if count else None,
        "wins": wins, "draws": draws, "losses": losses,
        "matches": rows,
    }


def _pct(part: int, total: int) -> int:
    if total <= 0:
        return 0
    return int(round((part * 100.0) / total))


def _team_side(match: dict[str, Any], team_slug: str, team_name: str) -> str:
    slug = str(team_slug or "").strip()
    name = _normalized_team_name(team_name)
    home_id = str(match.get("home_id") or "").strip()
    away_id = str(match.get("away_id") or "").strip()
    if slug and home_id == slug:
        return "home"
    if slug and away_id == slug:
        return "away"
    home_name = _normalized_team_name(match.get("home"))
    away_name = _normalized_team_name(match.get("away"))
    if name and home_name == name:
        return "home"
    if name and away_name == name:
        return "away"
    return ""


def _team_match_row(match: dict[str, Any], team_slug: str, team_name: str) -> dict[str, Any] | None:
    side = _team_side(match, team_slug, team_name)
    if not side:
        return None
    is_home = side == "home"
    sh = _to_int(match.get("score_home"), 0)
    sa = _to_int(match.get("score_away"), 0)
    gf, ga = (sh, sa) if is_home else (sa, sh)
    finished = bool(match.get("finished"))
    result = ""
    if finished:
        result = "W" if gf > ga else "D" if gf == ga else "L"
    opponent = str((match.get("away") if is_home else match.get("home")) or "").strip()
    return {
        "id": str(match.get("id") or ""),
        "sport": str(match.get("sport") or "football"),
        "date": str(match.get("date_text") or ""),
        "time": str(match.get("time_text") or ""),
        "kickoff_at": _to_int(match.get("kickoff_at"), 0),
        "home": str(match.get("home") or ""),
        "away": str(match.get("away") or ""),
        "home_logo": str(match.get("home_logo") or ""),
        "away_logo": str(match.get("away_logo") or ""),
        "home_id": str(match.get("home_id") or ""),
        "away_id": str(match.get("away_id") or ""),
        "competition_path": str(match.get("competition_path") or ""),
        "competition_id": str(match.get("competition_id") or ""),
        "competition_slug": str(match.get("competition_slug") or ""),
        "score_home": sh,
        "score_away": sa,
        "score": f"{sh}-{sa}",
        "side": side,
        "opponent": opponent,
        "goals_for": gf,
        "goals_against": ga,
        "result": result,
        "league": str(match.get("league") or ""),
        "country": str(match.get("country") or ""),
        "status": str(match.get("status") or ""),
        "scheduled": bool(match.get("scheduled")),
        "is_live": bool(match.get("is_live")),
        "finished": finished,
    }


def _team_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    finished = [r for r in rows if r.get("finished")]
    count = len(finished)
    wins = sum(1 for r in finished if r.get("result") == "W")
    draws = sum(1 for r in finished if r.get("result") == "D")
    losses = sum(1 for r in finished if r.get("result") == "L")
    goals_for = sum(_to_int(r.get("goals_for"), 0) for r in finished)
    goals_against = sum(_to_int(r.get("goals_against"), 0) for r in finished)
    total_goals = goals_for + goals_against
    points = wins * 3 + draws

    def hits(predicate: Any) -> int:
        return sum(1 for r in finished if predicate(r))

    def current_streak(predicate: Any) -> int:
        value = 0
        for row in finished:  # rows are newest first
            if not predicate(row):
                break
            value += 1
        return value

    def longest_streak(predicate: Any) -> int:
        best = current = 0
        for row in finished:
            if predicate(row):
                current += 1
                best = max(best, current)
            else:
                current = 0
        return best

    def gf(row: dict[str, Any]) -> int:
        return _to_int(row.get("goals_for"), 0)

    def ga(row: dict[str, Any]) -> int:
        return _to_int(row.get("goals_against"), 0)

    def total(row: dict[str, Any]) -> int:
        return gf(row) + ga(row)

    clean_sheets = hits(lambda r: ga(r) == 0)
    failed_to_score = hits(lambda r: gf(r) == 0)
    scored_matches = hits(lambda r: gf(r) > 0)
    conceded_matches = hits(lambda r: ga(r) > 0)
    btts = hits(lambda r: gf(r) > 0 and ga(r) > 0)
    over_0_5 = hits(lambda r: total(r) >= 1)
    over_1_5 = hits(lambda r: total(r) >= 2)
    over_2_5 = hits(lambda r: total(r) >= 3)
    over_3_5 = hits(lambda r: total(r) >= 4)
    over_4_5 = hits(lambda r: total(r) >= 5)
    under_1_5 = hits(lambda r: total(r) <= 1)
    under_2_5 = hits(lambda r: total(r) <= 2)
    under_3_5 = hits(lambda r: total(r) <= 3)
    under_4_5 = hits(lambda r: total(r) <= 4)
    team_over_0_5 = hits(lambda r: gf(r) >= 1)
    team_over_1_5 = hits(lambda r: gf(r) >= 2)
    team_over_2_5 = hits(lambda r: gf(r) >= 3)
    conceded_over_0_5 = hits(lambda r: ga(r) >= 1)
    conceded_over_1_5 = hits(lambda r: ga(r) >= 2)
    conceded_over_2_5 = hits(lambda r: ga(r) >= 3)
    totals = [total(r) for r in finished]
    scored_values = [gf(r) for r in finished]
    conceded_values = [ga(r) for r in finished]
    win_margins = [gf(r) - ga(r) for r in finished if gf(r) > ga(r)]
    loss_margins = [ga(r) - gf(r) for r in finished if gf(r) < ga(r)]

    return {
        "count": count,
        "wins": wins,
        "draws": draws,
        "losses": losses,
        "win_percent": _pct(wins, count),
        "draw_percent": _pct(draws, count),
        "loss_percent": _pct(losses, count),
        "points": points,
        "points_per_match": round(points / count, 2) if count else None,
        "goals_for": goals_for,
        "goals_against": goals_against,
        "goal_difference": goals_for - goals_against,
        "points_for": goals_for,
        "points_against": goals_against,
        "scored_avg": round(goals_for / count, 2) if count else None,
        "conceded_avg": round(goals_against / count, 2) if count else None,
        "total_avg": round(total_goals / count, 2) if count else None,
        "scored_matches": scored_matches,
        "scored_matches_percent": _pct(scored_matches, count),
        "conceded_matches": conceded_matches,
        "conceded_matches_percent": _pct(conceded_matches, count),
        "clean_sheets": clean_sheets,
        "clean_sheets_percent": _pct(clean_sheets, count),
        "failed_to_score": failed_to_score,
        "failed_to_score_percent": _pct(failed_to_score, count),
        "btts": btts,
        "btts_percent": _pct(btts, count),
        "no_btts": count - btts,
        "no_btts_percent": _pct(count - btts, count),
        "over_0_5": over_0_5,
        "over_0_5_percent": _pct(over_0_5, count),
        "over_1_5": over_1_5,
        "over_1_5_percent": _pct(over_1_5, count),
        "over_2_5": over_2_5,
        "over_2_5_percent": _pct(over_2_5, count),
        "over_3_5": over_3_5,
        "over_3_5_percent": _pct(over_3_5, count),
        "over_4_5": over_4_5,
        "over_4_5_percent": _pct(over_4_5, count),
        "under_1_5": under_1_5,
        "under_1_5_percent": _pct(under_1_5, count),
        "under_2_5": under_2_5,
        "under_2_5_percent": _pct(under_2_5, count),
        "under_3_5": under_3_5,
        "under_3_5_percent": _pct(under_3_5, count),
        "under_4_5": under_4_5,
        "under_4_5_percent": _pct(under_4_5, count),
        "team_over_0_5": team_over_0_5,
        "team_over_0_5_percent": _pct(team_over_0_5, count),
        "team_over_1_5": team_over_1_5,
        "team_over_1_5_percent": _pct(team_over_1_5, count),
        "team_over_2_5": team_over_2_5,
        "team_over_2_5_percent": _pct(team_over_2_5, count),
        "conceded_over_0_5": conceded_over_0_5,
        "conceded_over_0_5_percent": _pct(conceded_over_0_5, count),
        "conceded_over_1_5": conceded_over_1_5,
        "conceded_over_1_5_percent": _pct(conceded_over_1_5, count),
        "conceded_over_2_5": conceded_over_2_5,
        "conceded_over_2_5_percent": _pct(conceded_over_2_5, count),
        "max_scored": max(scored_values) if scored_values else 0,
        "max_conceded": max(conceded_values) if conceded_values else 0,
        "max_total": max(totals) if totals else 0,
        "min_total": min(totals) if totals else 0,
        "avg_win_margin": round(sum(win_margins) / len(win_margins), 2) if win_margins else None,
        "avg_loss_margin": round(sum(loss_margins) / len(loss_margins), 2) if loss_margins else None,
        "current_streaks": {
            "wins": current_streak(lambda r: r.get("result") == "W"),
            "unbeaten": current_streak(lambda r: r.get("result") != "L"),
            "winless": current_streak(lambda r: r.get("result") != "W"),
            "scoring": current_streak(lambda r: gf(r) > 0),
            "conceding": current_streak(lambda r: ga(r) > 0),
            "clean_sheets": current_streak(lambda r: ga(r) == 0),
            "failed_to_score": current_streak(lambda r: gf(r) == 0),
            "btts": current_streak(lambda r: gf(r) > 0 and ga(r) > 0),
            "over_2_5": current_streak(lambda r: total(r) >= 3),
            "under_2_5": current_streak(lambda r: total(r) <= 2),
        },
        "longest_streaks": {
            "wins": longest_streak(lambda r: r.get("result") == "W"),
            "unbeaten": longest_streak(lambda r: r.get("result") != "L"),
            "scoring": longest_streak(lambda r: gf(r) > 0),
            "clean_sheets": longest_streak(lambda r: ga(r) == 0),
            "btts": longest_streak(lambda r: gf(r) > 0 and ga(r) > 0),
            "over_2_5": longest_streak(lambda r: total(r) >= 3),
        },
        "form": [str(r.get("result") or "") for r in finished[:10] if r.get("result")],
    }


def team_detail(team_slug: str, team_name: str = "", force: bool = False, sport: str = "football") -> dict[str, Any]:
    """Return an internal team-statistics payload without external page links."""
    sport = _sport_name(sport)
    slug = str(team_slug or "").strip()
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,119}", slug):
        return {"ok": False, "error": "invalid_team_slug"}

    def load() -> dict[str, Any]:
        raw_matches = _team_matches(slug, 40, sport=sport)
        detected_name = str(team_name or "").strip()
        if not detected_name:
            for m in raw_matches:
                side = _team_side(m, slug, "")
                if side:
                    detected_name = str(m.get(side) or "").strip()
                    break
        rows: list[dict[str, Any]] = []
        logo = ""
        country = ""
        league = ""
        competition_path = ""
        competition_id = ""
        competition_slug = ""
        for m in raw_matches:
            row = _team_match_row(m, slug, detected_name)
            if not row:
                continue
            rows.append(row)
            side = str(row.get("side") or "")
            if not logo:
                logo = str(m.get(f"{side}_logo") or "")
            if not country:
                country = str(m.get("country") or "")
            if not league:
                league = str(m.get("league") or "")
            if not competition_path:
                competition_path = str(m.get("competition_path") or "")
            if not competition_id:
                competition_id = str(m.get("competition_id") or "")
            if not competition_slug:
                competition_slug = str(m.get("competition_slug") or "")

        finished = sorted(
            [r for r in rows if r.get("finished")],
            key=lambda r: _to_int(r.get("kickoff_at"), 0),
            reverse=True,
        )[:20]
        now = int(time.time())
        upcoming = sorted(
            [
                r for r in rows
                if not r.get("finished") and not r.get("is_live")
                and _to_int(r.get("kickoff_at"), 0) >= now - PREMATCH_START_GRACE_SECONDS
            ],
            key=lambda r: _to_int(r.get("kickoff_at"), 2**62),
        )[:12]
        recent_for_stats = finished[:10]
        home_rows = [r for r in recent_for_stats if r.get("side") == "home"]
        away_rows = [r for r in recent_for_stats if r.get("side") == "away"]
        return {
            "ok": True,
            "provider": "sportscore",
            "team": {
                "id": slug,
                "sport": sport,
                "name": detected_name or slug.replace("-", " ").title(),
                "logo": logo,
                "country": country,
                "league": league,
                "competition_path": competition_path,
                "competition_id": competition_id,
                "competition_slug": competition_slug,
            },
            "summary": _team_summary(recent_for_stats),
            "splits": {
                "home": _team_summary(home_rows),
                "away": _team_summary(away_rows),
            },
            "recent": recent_for_stats,
            "upcoming": upcoming,
            "updated_at": int(time.time()),
        }

    if force:
        return load()
    return _cached("team-detail:" + sport + ":" + slug + ":" + _normalized_team_name(team_name), TEAM_CACHE_SECONDS, load)


def _parse_standings_table(table: Any) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    if table is None:
        return out
    for tr in table.select("tbody tr"):
        cells = tr.find_all("td")
        if len(cells) < 6:
            continue
        team_cell = cells[1] if len(cells) > 1 else cells[0]
        link = team_cell.find("a")
        team_name = " ".join(team_cell.get_text(" ", strip=True).split())
        if not team_name:
            continue
        # Most SportScore tables use Pos, Team, P, W, D, L, GD, Pts.
        out.append({
            "position": _to_int(cells[0].get_text(" ", strip=True)),
            "team": team_name,
            "team_id": _slug_generic(link.get("href") if link else "", "team"),
            "logo": _absolute_url((team_cell.find("img") or {}).get("src", "")) if team_cell.find("img") else "",
            "played": _to_int(cells[2].get_text(" ", strip=True)) if len(cells) > 2 else 0,
            "wins": _to_int(cells[3].get_text(" ", strip=True)) if len(cells) > 3 else 0,
            "draws": _to_int(cells[4].get_text(" ", strip=True)) if len(cells) > 4 else 0,
            "losses": _to_int(cells[5].get_text(" ", strip=True)) if len(cells) > 5 else 0,
            "goal_difference": cells[6].get_text(" ", strip=True) if len(cells) > 6 else "",
            "points": _to_int(cells[7].get_text(" ", strip=True)) if len(cells) > 7 else _to_int(cells[-1].get_text(" ", strip=True)),
            "form": [x.get_text(" ", strip=True).upper() for x in tr.select(".form-pill")],
        })
    return out


def _nearest_heading_text(node: Any) -> str:
    if node is None:
        return ""
    for prev in node.find_all_previous(["h1", "h2", "h3", "h4"], limit=12):
        text = " ".join(prev.get_text(" ", strip=True).split())
        if text:
            return text
    return ""


def _parse_competition_standings(soup: BeautifulSoup) -> list[dict[str, Any]]:
    groups: list[dict[str, Any]] = []
    seen: set[str] = set()
    for table in soup.select("table.standings-table, #standings table, table[class*='standing']"):
        rows = _parse_standings_table(table)
        if not rows:
            continue
        name = _nearest_heading_text(table)
        if not name or "standing" in name.lower() or "табли" in name.lower():
            name = "Общая таблица" if not groups else f"Группа {len(groups) + 1}"
        key = name + ":" + ",".join(str(x.get("team_id") or x.get("team")) for x in rows[:4])
        if key in seen:
            continue
        seen.add(key)
        groups.append({"name": name, "rows": rows})
    return groups


def _competition_match_row(row: Any, context: dict[str, str], round_label: str = "") -> dict[str, Any] | None:
    provider_match_id = str(row.get("data-match-id") or "").strip()
    body = row.select_one(".d-none.d-md-flex") or row.select_one(".football-match-table-container") or row
    match_link = body.select_one('a.sc-stretched-link[href*="/football/match/"]') or body.select_one('a[href*="/football/match/"]')
    if match_link is None:
        return None
    href = _absolute_url(match_link.get("href") or match_link.get("data-url") or "")
    slug = _slug_from_url(href)
    if not slug:
        return None

    team_links: list[tuple[str, str]] = []
    for link in body.select('a.sc-team-link[href*="/football/team/"], a[href*="/football/team/"]'):
        name = _node_text(link)
        url = _absolute_url(link.get("href"))
        if name and url and all(url != x[1] for x in team_links):
            team_links.append((name, url))
        if len(team_links) == 2:
            break
    if len(team_links) < 2:
        return None

    utc_el = body.select_one("[data-utc]")
    kickoff_text = str(utc_el.get("data-utc") or "").strip() if utc_el else ""
    kickoff = _parse_iso(kickoff_text)
    status_text = _node_text(body.select_one('[data-live="status"]'))
    home_score_el = body.select_one('[data-live="home-score"] b') or body.select_one('[data-live="home-score"]')
    away_score_el = body.select_one('[data-live="away-score"] b') or body.select_one('[data-live="away-score"]')
    home_score_text = _node_text(home_score_el)
    away_score_text = _node_text(away_score_el)
    row_text = _node_text(body)
    probe = f"{status_text} {row_text}".lower()
    finished = bool(re.search(r"(?:^|\s)(?:ft|aet|pen)(?:\s|$)", probe, re.I) or any(x in probe for x in ("finished", "заверш")))
    live = bool(any(x in probe for x in ("live", "1st half", "2nd half", "перерыв")) or re.fullmatch(r"\d{1,3}(?:'| min)?", status_text.strip(), re.I))
    postponed = any(x in probe for x in ("postponed", "отлож", "cancelled", "canceled", "abandoned"))
    event_status = "EventCompleted" if finished else "EventInProgress" if live else "EventScheduled"

    logos: list[str] = []
    for img in body.select('.football-match-table-box img[src], img.icon-circuit[src], a[href*="/football/team/"] img[src]'):
        src = _absolute_url(img.get("src"))
        if src and "star" not in src.lower() and src not in logos:
            logos.append(src)
        if len(logos) == 2:
            break

    obj: dict[str, Any] = {
        "home": team_links[0][0],
        "away": team_links[1][0],
        "homeTeam": {"name": team_links[0][0], "url": team_links[0][1]},
        "awayTeam": {"name": team_links[1][0], "url": team_links[1][1]},
        "url": href,
        "slug": slug,
        "startDate": kickoff.isoformat() if kickoff else "",
        "eventStatus": event_status,
        "status": "live" if live else "finished" if finished else "postponed" if postponed else "scheduled",
        "status_text": status_text,
        "home_score": _to_int(home_score_text, 0),
        "away_score": _to_int(away_score_text, 0),
        "country": context.get("country") or "Без страны",
        "competition": {"name": context.get("league") or "Без лиги", "url": context.get("competition_path") or context.get("competition_url") or ""},
        "home_logo": logos[0] if len(logos) > 0 else "",
        "away_logo": logos[1] if len(logos) > 1 else "",
    }
    item = normalize_match(obj, source="sportscore_competition")
    if provider_match_id:
        item["provider_match_id"] = provider_match_id
        item["id"] = "ss:" + provider_match_id
    item["slug"] = slug
    item["round"] = round_label
    item["postponed"] = postponed
    item["competition_path"] = context.get("competition_path") or item.get("competition_path") or ""
    item["competition_id"] = context.get("competition_id") or item.get("competition_id") or ""
    item["competition_slug"] = context.get("competition_slug") or item.get("competition_slug") or ""
    item["league_logo"] = context.get("league_logo") or item.get("league_logo") or ""
    return item


def _parse_competition_matches(soup: BeautifulSoup, context: dict[str, str]) -> list[dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    sections = soup.select("section.match-state-section")
    roots = sections or [soup]
    round_re = re.compile(r"^(?:round|matchday|тур|раунд|группа)\s*[A-Za-zА-Яа-я0-9-]+$", re.I)
    for root in roots:
        current = dict(context)
        round_label = ""
        children = root.find_all(recursive=False) if root is not soup else root.find_all(["div", "section"], recursive=False)
        for child in children:
            classes = set(child.get("class") or [])
            if "table-active" in classes or child.select_one('a[href*="/football/competition/"]'):
                header = _header_context(child)
                if header.get("league") and header.get("league") != "Без лиги":
                    current.update(header)
                current["competition_path"] = context.get("competition_path") or current.get("competition_path") or ""
                current["competition_id"] = context.get("competition_id") or current.get("competition_id") or ""
                current["competition_slug"] = context.get("competition_slug") or current.get("competition_slug") or ""
            text = _node_text(child)
            if text and len(text) < 50 and round_re.fullmatch(text):
                round_label = text
            rows = [child] if child.has_attr("data-live-row") else child.select("[data-live-row][data-match-id]")
            for row in rows:
                item = _competition_match_row(row, current, round_label)
                if item:
                    out[str(item.get("id") or f"{item.get('slug')}@{item.get('kickoff_at')}")] = item

    if not out:
        for row in soup.select("[data-live-row][data-match-id]"):
            round_label = ""
            for prev in row.find_all_previous(["h2", "h3", "h4", "div"], limit=16):
                txt = _node_text(prev)
                if txt and len(txt) < 50 and round_re.fullmatch(txt):
                    round_label = txt
                    break
                if prev.has_attr("data-live-row"):
                    break
            item = _competition_match_row(row, context, round_label)
            if item:
                out[str(item.get("id") or f"{item.get('slug')}@{item.get('kickoff_at')}")] = item
    return sorted(out.values(), key=lambda x: (_to_int(x.get("kickoff_at"), 2**62), str(x.get("round") or ""), str(x.get("home") or "")))


def _parse_competition_seasons(soup: BeautifulSoup, current_path: str) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    seen: set[str] = set()
    for select in soup.select("select"):
        probe = " ".join([str(select.get("id") or ""), str(select.get("name") or ""), " ".join(select.get("class") or [])]).lower()
        if "season" not in probe and "сезон" not in probe:
            continue
        for opt in select.select("option"):
            label = _node_text(opt)
            value = str(opt.get("value") or "").strip()
            if not label:
                continue
            identity = _competition_identity(value)
            key = identity.get("path") or value or label
            if key in seen:
                continue
            seen.add(key)
            out.append({
                "label": label,
                "value": value,
                "path": identity.get("path") or current_path,
                "selected": "1" if opt.has_attr("selected") else "0",
            })
    for link in soup.select('a[href*="season="], a[href*="/football/competition/"]'):
        label = _node_text(link)
        href = str(link.get("href") or "").strip()
        if not label or not re.search(r"\b20\d{2}(?:/\d{2})?\b", label):
            continue
        identity = _competition_identity(href)
        parsed = urllib.parse.urlparse(href)
        season = urllib.parse.parse_qs(parsed.query).get("season", [""])[0]
        key = identity.get("path") or season or label
        if key in seen:
            continue
        seen.add(key)
        out.append({"label": label, "value": season, "path": identity.get("path") or current_path, "selected": "0"})
    return out[:30]


def _parse_competition_round_options(soup: BeautifulSoup, matches: list[dict[str, Any]]) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    seen: set[str] = set()
    pattern = re.compile(r"(?:Round|Matchday|Тур|Раунд)\s*([A-Za-zА-Яа-я0-9-]+)", re.I)
    for el in soup.select("button, a, option"):
        label = _node_text(el)
        match = pattern.fullmatch(label)
        if not match:
            continue
        href = str(el.get("href") or el.get("value") or "").strip()
        query = urllib.parse.parse_qs(urllib.parse.urlparse(href).query) if href else {}
        value = str(el.get("data-round") or el.get("data-matchday") or el.get("data-value") or (query.get("round") or query.get("matchday") or [""])[0] or match.group(1)).strip()
        key = value or label
        if key in seen:
            continue
        seen.add(key)
        out.append({"label": label, "value": value})
    for item in matches:
        label = str(item.get("round") or "").strip()
        match = pattern.fullmatch(label)
        value = match.group(1) if match else label
        key = value or label
        if label and key not in seen:
            seen.add(key)
            out.append({"label": label, "value": value})
    return out[:80]


def _parse_competition_rounds(soup: BeautifulSoup, matches: list[dict[str, Any]]) -> list[str]:
    return [x.get("label") or "" for x in _parse_competition_round_options(soup, matches) if x.get("label")]


def _player_stat_kind(text: str) -> str:
    probe = str(text or "").lower()
    if any(x in probe for x in ("assist", "ассист", "передач")):
        return "assists"
    if any(x in probe for x in ("rating", "рейтинг", "rated")):
        return "ratings"
    if any(x in probe for x in ("scorer", "goals", "бомбард", "голы")):
        return "scorers"
    return ""


def _parse_player_stat_rows(container: Any, kind: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    candidates = container.select("tbody tr, li, .player-row, .top-player-row, .stat-row") if container is not None else []
    for row in candidates:
        player_link = row.select_one('a[href*="/football/player/"], a[href*="/football/"]:not([href*="/team/"]):not([href*="/competition/"]):not([href*="/match/"]):not([href*="/country/"])')
        if player_link is None:
            continue
        name = _node_text(player_link)
        if not name:
            continue
        team_link = row.select_one('a[href*="/football/team/"]')
        team_name = _node_text(team_link)
        text = _node_text(row)
        nums = re.findall(r"(?<!\d)(\d+(?:[.,]\d+)?)(?!\d)", text)
        if not nums:
            continue
        value_text = nums[-1].replace(",", ".")
        value: int | float = float(value_text) if "." in value_text else int(value_text)
        player_id = _slug_generic(player_link.get("href"), "player")
        key = player_id or name.casefold()
        if key in seen:
            continue
        seen.add(key)
        img = row.select_one("img[src]")
        rows.append({
            "position": len(rows) + 1,
            "name": name,
            "player_id": player_id,
            "team": team_name,
            "team_id": _slug_generic(team_link.get("href") if team_link else "", "team"),
            "image": _absolute_url(img.get("src")) if img else "",
            "value": value,
            "kind": kind,
        })
    return rows[:50]


def _parse_competition_players(soup: BeautifulSoup) -> dict[str, list[dict[str, Any]]]:
    out = {"scorers": [], "assists": [], "ratings": []}
    for heading in soup.find_all(["h2", "h3", "h4"]):
        kind = _player_stat_kind(_node_text(heading))
        if not kind:
            continue
        container = heading.find_parent("section") or heading.parent
        rows = _parse_player_stat_rows(container, kind)
        if rows and len(rows) > len(out[kind]):
            out[kind] = rows
    for table in soup.select("table"):
        kind = _player_stat_kind(_nearest_heading_text(table) + " " + _node_text(table.select_one("thead")))
        if kind:
            rows = _parse_player_stat_rows(table, kind)
            if rows and len(rows) > len(out[kind]):
                out[kind] = rows
    return out


def _parse_competition_teams(standings_groups: list[dict[str, Any]], matches: list[dict[str, Any]]) -> list[dict[str, Any]]:
    teams: dict[str, dict[str, Any]] = {}
    for group in standings_groups:
        for row in group.get("rows") or []:
            key = str(row.get("team_id") or row.get("team") or "").strip()
            if key:
                teams[key] = {"id": str(row.get("team_id") or ""), "name": str(row.get("team") or ""), "logo": str(row.get("logo") or ""), "position": _to_int(row.get("position"), 0), "points": _to_int(row.get("points"), 0), "form": row.get("form") or []}
    for match in matches:
        for side in ("home", "away"):
            key = str(match.get(f"{side}_id") or match.get(side) or "").strip()
            if not key:
                continue
            teams.setdefault(key, {"id": str(match.get(f"{side}_id") or ""), "name": str(match.get(side) or ""), "logo": str(match.get(f"{side}_logo") or ""), "position": 0, "points": 0, "form": []})
    return sorted(teams.values(), key=lambda x: (_to_int(x.get("position"), 999) or 999, str(x.get("name") or "")))


def _parse_competition_champions(soup: BeautifulSoup) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for heading in soup.find_all(["h2", "h3", "h4"]):
        probe = _node_text(heading).lower()
        if not any(x in probe for x in ("champion", "победител", "чемпион")):
            continue
        container = heading.find_parent("section") or heading.parent
        for row in container.select("li, tr, .champion-row"):
            team_link = row.select_one('a[href*="/football/team/"]')
            if not team_link:
                continue
            text = _node_text(row)
            year_match = re.search(r"\b(19|20)\d{2}\b", text)
            out.append({"season": year_match.group(0) if year_match else "", "team": _node_text(team_link), "team_id": _slug_generic(team_link.get("href"), "team")})
        if out:
            break
    return out[:50]


def _parse_competition_bracket(soup: BeautifulSoup, context: dict[str, str]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for root in soup.select('[class*="bracket"], [class*="knockout"], #bracket, #playoffs'):
        stage = _nearest_heading_text(root) or _node_text(root.select_one("h2,h3,h4")) or "Плей-офф"
        for row in root.select("[data-live-row][data-match-id]"):
            item = _competition_match_row(row, context, stage)
            if item:
                out.append(item)
    unique: dict[str, dict[str, Any]] = {str(x.get("id") or i): x for i, x in enumerate(out)}
    return list(unique.values())[:100]


def _competition_header(soup: BeautifulSoup, path: str) -> dict[str, Any]:
    identity = _competition_identity(path)
    name = _node_text(soup.select_one("h1"))
    name = re.sub(r"\s+[—-]\s+.*$", "", name).strip() or identity.get("slug", "").replace("-", " ").title()
    country_link = soup.select_one('a[href*="/football/country/"]')
    country = _node_text(country_link)
    logo = ""
    for img in soup.select('img[src*="/football/competition/"], h1 img, .competition-logo img, .comp-hero img'):
        logo = _absolute_url(img.get("src"))
        if logo:
            break
    page_text = _node_text(soup)
    teams_match = re.search(r"(\d+)\s+teams?", page_text, re.I)
    matches_match = re.search(r"(\d+)\s+matches?", page_text, re.I)
    rounds_match = re.search(r"(\d+)\s+rounds?", page_text, re.I)
    defending = ""
    defending_id = ""
    for el in soup.find_all(string=re.compile(r"defending champion|действующ", re.I)):
        parent = el.parent.parent if getattr(el, "parent", None) and getattr(el.parent, "parent", None) else el.parent
        link = parent.select_one('a[href*="/football/team/"]') if parent else None
        if link:
            defending = _node_text(link)
            defending_id = _slug_generic(link.get("href"), "team")
            break
    return {
        "id": identity.get("id") or "",
        "slug": identity.get("slug") or "",
        "path": identity.get("path") or "",
        "name": name,
        "country": country,
        "country_slug": identity.get("country_slug") or "",
        "logo": logo,
        "teams_count": _to_int(teams_match.group(1) if teams_match else 0),
        "matches_count": _to_int(matches_match.group(1) if matches_match else 0),
        "rounds_count": _to_int(rounds_match.group(1) if rounds_match else 0),
        "defending_champion": defending,
        "defending_champion_id": defending_id,
    }


def competition_detail(competition_path: str, season: str = "", round_value: str = "", force: bool = False) -> dict[str, Any]:
    """Build the internal competition centre from the public competition HTML."""
    path = _validated_competition_path(competition_path)
    if not path:
        return {"ok": False, "error": "invalid_competition_path"}
    season_value = str(season or "").strip()[:80]
    round_filter = str(round_value or "").strip()[:40]
    if round_filter and not re.fullmatch(r"[A-Za-zА-Яа-я0-9_.-]+", round_filter):
        return {"ok": False, "error": "invalid_round"}

    def load() -> dict[str, Any]:
        request_path = path
        query_params: dict[str, str] = {}
        if season_value:
            query_params["season"] = season_value
        if round_filter:
            query_params["round"] = round_filter
        if query_params:
            request_path += ("&" if "?" in request_path else "?") + urllib.parse.urlencode(query_params)
        html_text = _request(request_path)
        soup = BeautifulSoup(str(html_text or ""), "html.parser")
        competition = _competition_header(soup, path)
        context = {
            "league": competition.get("name") or "Без лиги",
            "country": competition.get("country") or "Без страны",
            "competition_path": path,
            "competition_id": competition.get("id") or "",
            "competition_slug": competition.get("slug") or "",
            "competition_url": BASE_URL + path,
            "league_logo": competition.get("logo") or "",
            "country_url": "",
        }
        matches = _parse_competition_matches(soup, context)
        standings_groups = _parse_competition_standings(soup)
        players = _parse_competition_players(soup)
        teams = _parse_competition_teams(standings_groups, matches)
        finished = [x for x in matches if x.get("finished")]
        upcoming = [x for x in matches if not x.get("finished") and not x.get("is_live")]
        live = [x for x in matches if x.get("is_live")]
        total_goals = sum(_to_int(x.get("score_home"), 0) + _to_int(x.get("score_away"), 0) for x in finished)
        competition["teams_count"] = competition.get("teams_count") or len(teams)
        stats = {
            "teams": len(teams),
            "matches": len(matches),
            "finished": len(finished),
            "upcoming": len(upcoming),
            "live": len(live),
            "goals": total_goals,
            "goals_per_match": round(total_goals / len(finished), 2) if finished else None,
        }
        return {
            "ok": True,
            "provider": "sportscore",
            "competition": competition,
            "season": season_value,
            "seasons": _parse_competition_seasons(soup, path),
            "rounds": _parse_competition_rounds(soup, matches),
            "round_options": _parse_competition_round_options(soup, matches),
            "round_filter": round_filter,
            "matches": matches,
            "upcoming": upcoming,
            "results": sorted(finished, key=lambda x: _to_int(x.get("kickoff_at"), 0), reverse=True),
            "live": live,
            "standings_groups": standings_groups,
            "standings": (standings_groups[0].get("rows") if standings_groups else []),
            "players": players,
            "teams": teams,
            "stats": stats,
            "bracket": _parse_competition_bracket(soup, context),
            "champions": _parse_competition_champions(soup),
            "updated_at": int(time.time()),
        }

    if force:
        return load()
    cache_key = "competition:" + hashlib.sha1((path + "|" + season_value + "|" + round_filter).encode("utf-8")).hexdigest()
    return _cached(cache_key, COMPETITION_CACHE_SECONDS, load)

def _pair_stats(flat: dict[str, int]) -> dict[str, dict[str, int]]:
    mapping = {
        "possession": "possession", "shots": "shots", "on_target": "on_target", "off_target": "off_target",
        "dangerous": "dangerous", "attacks": "attacks", "corners": "corners",
        "yellow_cards": "yellow_cards", "red_cards": "red_cards",
    }
    out = {}
    for key, prefix in mapping.items():
        out[key] = {"home": int(flat.get(prefix + "_home") or 0), "away": int(flat.get(prefix + "_away") or 0)}
    return out


def _normalized_team_name(value: Any) -> str:
    text = str(value or "").casefold()
    text = re.sub(r"[^a-z0-9а-яё]+", " ", text, flags=re.I)
    return " ".join(text.split())


def _same_team_pair(left: dict[str, Any], right: dict[str, Any]) -> bool:
    a = {_normalized_team_name(left.get("home")), _normalized_team_name(left.get("away"))}
    b = {_normalized_team_name(right.get("home")), _normalized_team_name(right.get("away"))}
    return "" not in a and "" not in b and a == b


def _detail_mismatch(actual: dict[str, Any], expected: dict[str, Any]) -> tuple[bool, str]:
    if not _same_team_pair(actual, expected):
        return True, "team_pair"
    actual_ts = _to_int(actual.get("kickoff_at"), 0)
    expected_ts = _to_int(expected.get("kickoff_at"), 0)
    if expected_ts and not actual_ts:
        return True, "missing_kickoff"
    # Slug routes can point to another meeting of the same teams. Date/time is
    # therefore part of the fixture identity, not only the team pair.
    if actual_ts and expected_ts and abs(actual_ts - expected_ts) > 12 * 3600:
        return True, "kickoff"
    if expected_ts > int(time.time()) - PREMATCH_START_GRACE_SECONDS and (actual.get("finished") or str(actual.get("period") or "").upper() in {"FT", "AET", "PEN"}):
        return True, "finished_old_fixture"
    return False, ""


def _expected_match_overlay(actual: dict[str, Any], expected: dict[str, Any]) -> dict[str, Any]:
    """Keep exact fixture identity without overwriting fresh live values."""
    out = dict(actual or {})
    expected_live = bool(expected.get("is_live")) or str(expected.get("status") or "").lower() == "live"
    identity_keys = (
        "id", "provider_match_id", "slug", "link", "home", "away", "home_id", "away_id",
        "home_logo", "away_logo", "country", "country_code", "league", "league_logo",
        "competition_path", "competition_id", "competition_slug", "kickoff_at", "kickoff_iso",
    )
    if expected_live:
        for key in identity_keys:
            value = expected.get(key)
            if value not in (None, "", {}, []):
                out[key] = value
        # Use list values only when the detail endpoint omitted them.
        for key in ("score_home", "score_away", "score", "minute", "minute_text", "period"):
            if out.get(key) in (None, ""):
                value = expected.get(key)
                if value not in (None, ""):
                    out[key] = value
        out["status"] = "live"
        out["scheduled"] = False
        out["is_live"] = True
        out["finished"] = False
        out["period"] = str(out.get("period") or expected.get("period") or "LIVE")
        out["minute_text"] = str(out.get("minute_text") or expected.get("minute_text") or "LIVE")
        return out

    # Scheduled fixture list row is authoritative because slug routes may point
    # to a previous meeting of the same two teams.
    for key, value in expected.items():
        if value not in (None, "", {}, []):
            out[key] = value
    out["id"] = str(expected.get("id") or out.get("id") or "")
    out["provider_match_id"] = str(expected.get("provider_match_id") or out.get("provider_match_id") or "")
    out["slug"] = str(expected.get("slug") or out.get("slug") or "")
    out["link"] = str(expected.get("link") or out.get("link") or "")
    out["period"] = "PRE"
    out["status"] = "scheduled"
    out["scheduled"] = True
    out["is_live"] = False
    out["finished"] = False
    out["minute"] = 0
    out["minute_text"] = str(expected.get("time_text") or out.get("time_text") or "СКОРО")
    out["score_home"] = 0
    out["score_away"] = 0
    out["score"] = "0-0"
    return out


def _safe_recent_summary(team_slug: str, team_name: str) -> dict[str, Any]:
    try:
        return _recent_summary(team_slug, team_name)
    except Exception as exc:
        return {
            "count": 0, "avg": None, "total_avg": None, "scored_avg": None,
            "conceded_avg": None, "wins": 0, "draws": 0, "losses": 0,
            "matches": [], "error": f"{type(exc).__name__}: {exc}",
        }


def _merge_sport_stat_rows(*collections: Any) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for collection in collections:
        for row in collection if isinstance(collection, list) else []:
            if not isinstance(row, dict):
                continue
            label = re.sub(r"\s+", " ", str(row.get("label") or "")).strip()
            if not label:
                continue
            key = label.casefold()
            canonical = re.sub(r"\s+(points|games)$", "", key).strip()
            if canonical in seen:
                continue
            if row.get("home") in (None, "") and row.get("away") in (None, ""):
                continue
            seen.add(canonical)
            out.append({"label": label, "home": row.get("home"), "away": row.get("away")})
    return out[:80]


def _period_stat_rows(match: dict[str, Any], sport: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if sport == "basketball":
        rows.append({"label": "Total Points", "home": _to_int(match.get("score_home")), "away": _to_int(match.get("score_away"))})
    elif sport == "tennis":
        rows.append({"label": "Sets Won", "home": _to_int(match.get("score_home")), "away": _to_int(match.get("score_away"))})
    for part in match.get("score_parts") or []:
        if not isinstance(part, dict):
            continue
        label = str(part.get("label") or "").strip().upper()
        if not label:
            continue
        suffix = " Points" if sport == "basketball" else " Games"
        rows.append({"label": label + suffix, "home": part.get("home"), "away": part.get("away")})
    return rows


def _parse_generic_match_html_stats(text: str, sport: str, expected: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Best-effort extraction from the public match page when the widget detail is sparse."""
    soup = BeautifulSoup(str(text or ""), "html.parser")
    stats: list[dict[str, Any]] = []
    parts: list[dict[str, Any]] = []

    # Embedded JSON often contains richer statistics than the public widget envelope.
    for script in soup.select('script[type="application/json"], script[type="application/ld+json"], script#__NEXT_DATA__'):
        raw = script.string or script.get_text("", strip=True)
        if not raw:
            continue
        try:
            obj = json.loads(raw)
        except Exception:
            continue
        stats.extend(_generic_sport_stats(obj))
        if not parts:
            for node in _walk_json(obj):
                if isinstance(node, dict):
                    candidate = _score_parts(node, sport)
                    if candidate:
                        parts = candidate
                        break

    # Quarter/set score table: Team | Q1..Q4 | Total / Team | S1.. | Total.
    for table in soup.select("table"):
        headers = [_node_text(x).upper() for x in table.select("thead th, thead td")]
        header_probe = " ".join(headers)
        wanted = ("Q1" in header_probe or "QUARTER" in header_probe) if sport == "basketball" else ("S1" in header_probe or "SET" in header_probe)
        body_rows = table.select("tbody tr") or table.select("tr")
        if wanted and len(body_rows) >= 2:
            numeric_rows: list[list[int]] = []
            for tr in body_rows[-2:]:
                values = [_to_int(x) for x in re.findall(r"-?\d+", _node_text(tr))]
                if values:
                    numeric_rows.append(values)
            if len(numeric_rows) >= 2:
                labels = [h for h in headers if re.fullmatch(r"(?:Q|S)\d+|OT\d*", h)]
                max_parts = min(len(labels), max(0, min(len(numeric_rows[0]), len(numeric_rows[1])) - 1))
                for i in range(max_parts):
                    parts.append({"label": labels[i], "home": numeric_rows[0][i], "away": numeric_rows[1][i]})

    # Parse compact three-column stat rows under Stats/Game sections.
    roots: list[Any] = []
    for heading in soup.find_all(["h2", "h3", "h4", "button"]):
        if re.search(r"\b(stats?|statistics|game stats|статист)\b", _node_text(heading), re.I):
            parent = heading.find_parent(["section", "div", "article"])
            if parent is not None:
                roots.append(parent)
    roots.extend(soup.select("#stats, [data-tab='stats'], [data-tab-content='stats'], .match-stats, .game-stats, [class*='match-stat']"))
    seen_nodes: set[int] = set()
    for root in roots[:12]:
        if id(root) in seen_nodes:
            continue
        seen_nodes.add(id(root))
        for row in root.select("tr, .stat-row, [class*='stat-row'], [class*='stats-row']"):
            cells = row.find_all(["th", "td"], recursive=False) or row.find_all(["span", "div"], recursive=False)
            texts = [_node_text(x) for x in cells if _node_text(x)]
            if len(texts) < 3:
                continue
            nums = [i for i, value in enumerate(texts) if re.fullmatch(r"[+-]?\d+(?:[.,]\d+)?%?", value.replace(" ", ""))]
            if len(nums) < 2:
                continue
            left_i, right_i = nums[0], nums[-1]
            between = [x for i, x in enumerate(texts) if i not in {left_i, right_i} and not re.fullmatch(r"[+-]?\d+(?:[.,]\d+)?%?", x.replace(" ", ""))]
            label = max(between, key=len) if between else ""
            if label:
                stats.append({"label": label, "home": texts[left_i], "away": texts[right_i]})
    return _merge_sport_stat_rows(stats), parts


def _generic_sport_detail(slug: str, sport: str, force: bool = False, expected_match: dict[str, Any] | None = None) -> dict[str, Any]:
    sport = _sport_name(sport)
    expected = dict(expected_match or {})
    slug = str(slug or expected.get("slug") or "").strip().removeprefix(f"{sport}:")
    if not slug:
        return {"ok": False, "error": "missing_slug", "sport": sport}

    def load() -> dict[str, Any]:
        params = {"sport": sport, "slug": slug, "src": SOURCE_TAG}
        if force:
            params["_t"] = int(time.time())
        payload = _json("/api/widget/match/", params, ttl=10 if force else DETAIL_CACHE_SECONDS)
        root = payload if isinstance(payload, dict) else {}
        candidate = _dig(root, "match", "data.match", "data", default=root)
        api_obj = dict(candidate) if isinstance(candidate, dict) else {}
        # Some payload versions keep period/set scores and stats beside match.
        for key in ("score_parts", "periods", "quarters", "sets", "period_scores", "set_scores", "home_period_scores", "away_period_scores", "home_set_scores", "away_set_scores", "home_scores", "away_scores", "stats", "statistics", "odds"):
            if key not in api_obj and key in root:
                api_obj[key] = root[key]
        match = normalize_match(api_obj or {"slug": slug}, source="sportscore_api_detail", sport=sport)
        mismatch = False
        mismatch_reason = ""
        if expected:
            mismatch, mismatch_reason = _detail_mismatch(match, expected)
            # Identity and list status are authoritative; API detail supplies fresh scores.
            identity_keys = (
                "id", "provider_match_id", "slug", "home", "away", "home_id", "away_id",
                "home_logo", "away_logo", "country", "country_code", "league", "league_logo",
                "kickoff_at", "kickoff_iso", "sport", "competition_slug", "competition_id", "competition_path",
            )
            for key in identity_keys:
                value = expected.get(key)
                if value not in (None, "", {}, []):
                    match[key] = value
            if expected.get("is_live"):
                match["is_live"] = True
                match["finished"] = False
                match["scheduled"] = False
                match["status"] = "live"
            elif expected.get("finished"):
                match["finished"] = True
                match["is_live"] = False
                match["scheduled"] = False
                match["status"] = "finished"
            elif expected.get("scheduled"):
                match["scheduled"] = True
                match["is_live"] = False
                match["finished"] = False
                match["status"] = "scheduled"
                match["score_home"] = 0
                match["score_away"] = 0
                match["score"] = "0-0"
            if not match.get("score_parts") and expected.get("score_parts"):
                match["score_parts"] = expected.get("score_parts")
        raw_stats = _dig(root, "stats", "statistics", "data.stats", "data.statistics", default=_dig(api_obj, "stats", "statistics", default={}))
        sport_stats = _merge_sport_stat_rows(
            _generic_sport_stats(raw_stats),
            _generic_sport_stats(root),
            _generic_sport_stats(api_obj),
            _period_stat_rows(match, sport),
        )
        # Basketball widget responses can be intentionally compact. The public
        # match page contains the same SportScore data and often exposes quarter
        # rows and additional game stats, so use it as a cached fallback.
        html_parts: list[dict[str, Any]] = []
        try:
            html_text = _request(f"/{sport}/match/{urllib.parse.quote(slug)}/")
            html_stats, html_parts = _parse_generic_match_html_stats(str(html_text), sport, expected)
            sport_stats = _merge_sport_stat_rows(sport_stats, html_stats)
            if not match.get("score_parts") and html_parts:
                match["score_parts"] = html_parts
        except Exception:
            pass
        events_raw = _dig(root, "timeline.events", "events", "timeline", "data.events", default=[])
        odds = _dig(root, "odds", "data.odds", default=api_obj.get("odds") if isinstance(api_obj.get("odds"), dict) else {})
        expected_prematch_odds = expected.get("odds") if isinstance(expected.get("odds"), dict) else {}
        tracker_data: dict[str, Any] = {}
        tracker_id = str(match.get("provider_match_id") or expected.get("provider_match_id") or "").strip()
        if match.get("is_live") and tracker_id:
            try:
                tracker_data = tracker(tracker_id, sport=sport)
            except Exception:
                tracker_data = {}
        if tracker_data:
            sport_stats = _merge_sport_stat_rows(sport_stats, _generic_sport_stats(tracker_data))
            if not match.get("score_parts"):
                tracker_parts = _score_parts(tracker_data, sport)
                if tracker_parts:
                    match["score_parts"] = tracker_parts
        return {
            "ok": True,
            "provider": "sportscore",
            "sport": sport,
            "prematch": bool(match.get("scheduled")),
            "match": match,
            "score_parts": match.get("score_parts") or [],
            "sport_stats": sport_stats,
            "stats": {},
            "stats_flat": {},
            "events": events_raw if isinstance(events_raw, list) else [],
            "odds": (odds if isinstance(odds, dict) and odds else expected_prematch_odds),
            "live_odds": (odds if bool(match.get("is_live")) and isinstance(odds, dict) else {}),
            "prematch_odds": expected_prematch_odds,
            "lineups": _dig(root, "lineups", "data.lineups", default={}) or {},
            "tracker": tracker_data,
            "finished": bool(match.get("finished")),
            "detail_mismatch": mismatch,
            "detail_mismatch_reason": mismatch_reason,
        }

    identity = str(expected.get("id") or slug)
    return _cached(f"detail:{sport}:{identity}:{slug}", 10 if force else DETAIL_CACHE_SECONDS, load)


def detail(slug: str, force: bool = False, expected_match: dict[str, Any] | None = None, sport: str = "football") -> dict[str, Any]:
    sport = _sport_name(sport or (expected_match or {}).get("sport"))
    if sport != "football":
        return _generic_sport_detail(slug, sport, force=force, expected_match=expected_match)
    expected = dict(expected_match or {})
    slug = _slug_from_url(slug) or str(slug or "").removeprefix("ss:").strip()
    if not slug:
        slug = str(expected.get("slug") or "").strip()
    if not slug:
        return {"ok": False, "error": "missing_slug"}

    expected_id = str(expected.get("id") or expected.get("provider_match_id") or "").strip()

    def load():
        page_error = ""
        try:
            html_text = _request(f"/football/match/{urllib.parse.quote(slug)}/")
            parsed = parse_match_html(str(html_text), slug)
        except Exception as exc:
            page_error = f"{type(exc).__name__}: {exc}"
            if not expected:
                raise
            fallback_match = normalize_match({"slug": slug, "url": f"/football/match/{slug}/"}, source="sportscore_detail_fallback")
            if expected:
                fallback_match.update({k: v for k, v in expected.items() if v not in (None, "", {}, [])})
            parsed = {
                "match": fallback_match,
                "odds": {}, "h2h": {}, "standings": [], "lineups": {"announced": False},
                "stats_flat": {}, "venue": {}, "events": [],
            }

        # API detail is optional. It is never allowed to change the unique
        # prematch identity selected in the list.
        try:
            api = _request("/api/widget/match/", {"sport": "football", "slug": slug, "src": SOURCE_TAG}, want_json=True)
            api_obj = {}
            if isinstance(api, dict):
                candidate = _dig(api, "match", "data.match", "data", default=api)
                api_obj = candidate if isinstance(candidate, dict) else api
            api_match = normalize_match(api_obj, source="sportscore_api_detail") if api_obj else {}
            for key in (
                "home", "away", "home_id", "away_id", "home_logo", "away_logo", "provider_match_id",
                "score_home", "score_away", "score", "minute", "minute_text", "period", "status",
                "is_live", "finished", "kickoff_at", "kickoff_iso", "country", "league",
                "competition_path", "competition_id", "competition_slug",
            ):
                if api_match.get(key) not in (None, "", 0) or key in ("score_home", "score_away"):
                    parsed["match"][key] = api_match.get(key)
            api_stats = _flat_stats(_dig(api_obj, "stats", "statistics", default={}))
            if any(api_stats.values()):
                parsed["stats_flat"] = api_stats
            events = _dig(api_obj, "timeline.events", "events", "timeline", default=[])
            parsed["events"] = _normalize_events(events, api_match or parsed.get("match") or {})
            api_lineups = _dig(api_obj, "lineups", default=None)
            if api_lineups and not (parsed.get("lineups") or {}).get("announced"):
                parsed["lineups_raw"] = api_lineups
            api_odds = _dig(api, "odds", "data.odds", default=_dig(api_obj, "odds", default={})) if isinstance(api, dict) else {}
            if isinstance(api_odds, dict) and api_odds:
                parsed["api_odds"] = api_odds
        except Exception:
            parsed.setdefault("events", [])

        mismatch = False
        mismatch_reason = ""
        actual_match = dict(parsed.get("match") or {})
        if expected:
            mismatch, mismatch_reason = _detail_mismatch(actual_match, expected)
            parsed["match"] = _expected_match_overlay(actual_match, expected)
            expected_odds = expected.get("odds") if isinstance(expected.get("odds"), dict) else {}
            if expected_odds:
                parsed.setdefault("odds", expected_odds)
                parsed["prematch_odds"] = expected_odds
                parsed["match"]["has_odds"] = True
            if mismatch:
                # Do not show score, timeline, lineups or stats from an older
                # fixture. H2H remains valid because the team pair is checked.
                parsed["events"] = []
                parsed["stats_flat"] = {}
                parsed["lineups"] = {"announced": False, "message": "Составы ещё не подтверждены"}
                parsed["standings"] = []
                parsed["venue"] = {}

        match = parsed["match"]
        prematch_odds = parsed.get("prematch_odds") or parsed.get("odds") or {}
        api_live_odds = parsed.get("api_odds") if isinstance(parsed.get("api_odds"), dict) else {}
        current_odds = api_live_odds if bool(match.get("is_live")) and api_live_odds else prematch_odds
        tracker_data: dict[str, Any] = {}
        tracker_id = str(match.get("provider_match_id") or expected.get("provider_match_id") or "").strip()
        if match.get("is_live") and tracker_id:
            try:
                tracker_data = tracker(tracker_id)
            except Exception:
                tracker_data = {}
        home_recent = _safe_recent_summary(str(match.get("home_id") or ""), str(match.get("home") or ""))
        away_recent = _safe_recent_summary(str(match.get("away_id") or ""), str(match.get("away") or ""))
        return {
            "ok": True,
            "provider": "sportscore",
            "prematch": not bool(match.get("is_live") or match.get("finished")),
            "match": match,
            "stats": _pair_stats(parsed.get("stats_flat") or {}),
            "stats_flat": parsed.get("stats_flat") or {},
            "events": parsed.get("events") or [],
            "odds": current_odds,
            "live_odds": api_live_odds if bool(match.get("is_live")) else {},
            "prematch_odds": prematch_odds,
            "h2h": parsed.get("h2h") or {},
            "standings": parsed.get("standings") or [],
            "lineups": parsed.get("lineups") or {},
            "venue": parsed.get("venue") or {},
            "avg": {"home": home_recent, "away": away_recent},
            "tracker": tracker_data,
            "pressure": {"available": False, "reason": "sportscore_no_history" if match.get("is_live") else "prematch"},
            "pressure_chart": {"available": False, "reason": "sportscore_no_history" if match.get("is_live") else "prematch"},
            "finished": bool(match.get("finished")),
            "detail_mismatch": mismatch,
            "detail_mismatch_reason": mismatch_reason,
            "detail_page_error": page_error,
        }

    cache_identity = expected_id or slug
    return _cached("detail:" + cache_identity + ":" + slug, 10 if force else DETAIL_CACHE_SECONDS, load)


def tracker(match_id: str, force: bool = False, sport: str = "football") -> dict[str, Any]:
    match_id = str(match_id or "").strip()
    if not match_id:
        return {}
    sport = _sport_name(sport)
    params = {"sport": sport, "id": match_id, "src": SOURCE_TAG}
    if force:
        params["_t"] = int(time.time())
    payload = _json("/api/widget/tracker/", params, ttl=10 if force else LIVE_CACHE_SECONDS)
    return payload if isinstance(payload, dict) else {}


def avg_for_slug(slug: str, expected_match: dict[str, Any] | None = None) -> dict[str, Any]:
    data = detail(slug, expected_match=expected_match)
    return {"ok": bool(data.get("ok")), "avg": data.get("avg") or {"home": {}, "away": {}}}

def source_health() -> dict[str, Any]:
    return {
        "base_url": BASE_URL,
        "source_tag": SOURCE_TAG,
        "cache_seconds": CACHE_SECONDS,
        "live_cache_seconds": LIVE_CACHE_SECONDS,
        "live_html_cache_seconds": LIVE_HTML_CACHE_SECONDS,
        "max_matches": MAX_MATCHES,
        "prematch_days_ahead": PREMATCH_DAYS_AHEAD,
        "upcoming": upcoming_diagnostics(),
    }
