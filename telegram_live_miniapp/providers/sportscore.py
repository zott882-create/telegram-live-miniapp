#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""SportScore data provider.

Uses SportScore's public JSON widget API where possible and the public match
HTML as an enrichment source for pre-match odds, H2H, standings and lineups.
No browser cookies or Cloudflare clearance tokens are required.
"""
from __future__ import annotations

import datetime as dt
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
from dataclasses import dataclass
from typing import Any

from bs4 import BeautifulSoup


BASE_URL = os.environ.get("SPORTSCORE_BASE_URL", "https://sportscore.com").rstrip("/")
SOURCE_TAG = os.environ.get("SPORTSCORE_SOURCE_TAG", "telegram-live-miniapp").strip() or "telegram-live-miniapp"
HTTP_TIMEOUT = float(os.environ.get("SPORTSCORE_HTTP_TIMEOUT", "20"))
CACHE_SECONDS = max(10, int(os.environ.get("SPORTSCORE_CACHE_SECONDS", "60")))
DETAIL_CACHE_SECONDS = max(15, int(os.environ.get("SPORTSCORE_DETAIL_CACHE_SECONDS", "90")))
TEAM_CACHE_SECONDS = max(60, int(os.environ.get("SPORTSCORE_TEAM_CACHE_SECONDS", "600")))
MAX_MATCHES = max(1, min(50, int(os.environ.get("SPORTSCORE_MAX_MATCHES", "50"))))
PREMATCH_DAYS_AHEAD = max(2, min(7, int(os.environ.get("SPORTSCORE_PREMATCH_DAYS_AHEAD", "3"))))

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
    m = re.search(r"/football/match/([^/?#]+)/?", text)
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
        m = re.search(rf"/football/{re.escape(kind)}/([^/?#]+)", text)
    if m:
        return m.group(1)
    if text and "/" not in text and "#" not in text:
        return text
    return ""


def _status_info(item: dict[str, Any]) -> tuple[str, bool, bool, bool]:
    raw = str(_dig(item, "status", "status_text", "eventStatus", "state", default="") or "").lower()
    raw = raw.rsplit("/", 1)[-1]
    live = any(x in raw for x in ("live", "inplay", "in_play", "1st", "2nd", "half", "interval"))
    finished = any(x in raw for x in ("finished", "full", "ended", "complete", "cancelled", "postponed", "abandoned"))
    scheduled = any(x in raw for x in ("scheduled", "upcoming", "not_started", "not started", "fixture", "pre"))
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


def normalize_match(item: dict[str, Any], *, source: str = "sportscore") -> dict[str, Any]:
    item = item if isinstance(item, dict) else {}
    home_obj = _dig(item, "homeTeam", "home_team", default={})
    away_obj = _dig(item, "awayTeam", "away_team", default={})
    home = str(_dig(item, "home", "home_name", "team1", default="") or _entity_name(home_obj) or "Home").strip()
    away = str(_dig(item, "away", "away_name", "team2", default="") or _entity_name(away_obj) or "Away").strip()
    url = str(_dig(item, "url", "link", "@id", default="") or "")
    slug = str(_dig(item, "slug", default="") or _slug_from_url(url)).strip()
    if not slug:
        slug = re.sub(r"[^a-z0-9]+", "-", f"{home}-vs-{away}".lower()).strip("-")
    mid = "ss:" + slug
    kickoff = _parse_iso(_dig(item, "time", "startDate", "start_time", "kickoff", "date"))
    status, is_live, finished, scheduled = _status_info(item)
    hs = _to_int(_dig(item, "home_score", "score.home", "scores.home", "homeScore"), 0)
    ast = _to_int(_dig(item, "away_score", "score.away", "scores.away", "awayScore"), 0)
    status_text = str(_dig(item, "status_text", "statusText", "minute", "clock", default="") or "").strip()
    minute = _to_int(re.search(r"\d+", status_text).group(0) if re.search(r"\d+", status_text) else 0)
    competition_obj = _dig(item, "competition", "league", "superEvent", "organizer", default={})
    league = str(_dig(item, "competition_name", "league_name", default="") or _entity_name(competition_obj) or "Без лиги").strip()
    country = str(_dig(item, "country", "location.address.addressCountry", "addressCountry", default="") or "Без страны").strip()
    home_logo = str(_dig(item, "home_logo", "homeLogo", "homeTeam.logo", "home_team.logo", default="") or "")
    away_logo = str(_dig(item, "away_logo", "awayLogo", "awayTeam.logo", "away_team.logo", default="") or "")
    images = item.get("image")
    if isinstance(images, list):
        if not home_logo and len(images) > 0:
            home_logo = str(images[0] or "")
        if not away_logo and len(images) > 1:
            away_logo = str(images[1] or "")
    raw_stats = _dig(item, "stats", "statistics", default={})
    stats = _flat_stats(raw_stats)
    odds = item.get("odds") if isinstance(item.get("odds"), dict) else {}
    return {
        "id": mid,
        "slug": slug,
        "home": home,
        "away": away,
        "home_id": _slug_generic(_dig(home_obj, "url", "slug", "@id"), "team"),
        "away_id": _slug_generic(_dig(away_obj, "url", "slug", "@id"), "team"),
        "home_logo": home_logo,
        "away_logo": away_logo,
        "score_home": hs,
        "score_away": ast,
        "score": f"{hs}-{ast}",
        "minute": minute,
        "minute_text": status_text or ("LIVE" if is_live else "FT" if finished else (kickoff.strftime("%H:%M") if kickoff else "СКОРО")),
        "period": "LIVE" if is_live else "FT" if finished else "PRE",
        "status": status,
        "is_live": is_live,
        "finished": finished,
        "scheduled": scheduled,
        "kickoff_at": int(kickoff.timestamp()) if kickoff else 0,
        "kickoff_iso": kickoff.isoformat() if kickoff else "",
        "date_text": kickoff.strftime("%d.%m.%Y") if kickoff else "",
        "time_text": kickoff.strftime("%H:%M") if kickoff else "",
        "country": country,
        "country_code": "",
        "league": league,
        "league_logo": str(_dig(competition_obj, "logo", "image", default="") or ""),
        "competition_slug": _slug_generic(_dig(competition_obj, "url", "slug", "@id"), "competition"),
        "link": BASE_URL + f"/football/match/{slug}/",
        "stats": stats,
        "odds": odds,
        "has_odds": bool(odds),
        "source": source,
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


def _extract_row_odds(container: Any) -> dict[str, Any]:
    if container is None or not hasattr(container, "select"):
        return {}
    values: list[float | None] = []
    for el in container.select('.football-odd-cell, [data-odd], [data-odds-value]'):
        raw = el.get("data-odd") or el.get("data-odds-value") or el.get_text(" ", strip=True)
        val = _to_float(raw)
        if val is not None:
            values.append(val)
    if len(values) < 3:
        return {}
    values += [None] * (11 - len(values))
    return {
        "eu": {"1": values[0], "X": values[1], "2": values[2]},
        "asia": {"home": values[3], "away": values[4]},
        "bs": {"line": values[5], "over": values[6], "under": values[7]},
        "corners": {"line": values[8], "over": values[9], "under": values[10]},
        "kind": "prematch",
    }


def parse_upcoming_html(text: str, default_date: dt.date | None = None) -> list[dict[str, Any]]:
    soup = BeautifulSoup(text or "", "html.parser")
    out: dict[str, dict[str, Any]] = {}

    # SEO JSON-LD is reliable but deliberately contains only a small sample.
    for root in _iter_jsonld(soup):
        for obj in _walk_json(root):
            if str(obj.get("@type") or "") != "SportsEvent":
                continue
            item = normalize_match(obj, source="sportscore_html")
            if item.get("scheduled") or item.get("status") == "scheduled":
                out[item["slug"]] = item

    # Parse every rendered fixture row. Match URLs may live in href, data-url,
    # data-match-url or a slug attribute depending on the current template.
    nodes: list[Any] = list(soup.select(
        'a[href*="/football/match/"], [data-match-url*="/football/match/"], '
        '[data-url*="/football/match/"], [data-fixture-url*="/football/match/"], '
        '[data-fixture-slug], [data-match-slug]'
    ))
    seen_nodes: set[int] = set()
    for node in nodes:
        if id(node) in seen_nodes:
            continue
        seen_nodes.add(id(node))
        slug = _candidate_match_slug(node)
        if not slug:
            continue
        container = _match_container(node)
        if container is None:
            continue
        row_text = " ".join(container.get_text(" ", strip=True).split())
        # Skip rows clearly marked as already live or finished.
        if re.search(r"(?:^|\s)(?:FT|AET|PEN|HT)(?:\s|$)", row_text, re.I) or re.search(r"\b(?:live|finished)\b", row_text, re.I):
            continue
        names = _extract_team_names(container, node)
        if len(names) < 2:
            continue
        kickoff = _extract_kickoff(container, default_date)
        comp_link = _context_link(container, 'a[href*="/football/competition/"]')
        country_link = _context_link(container, 'a[href*="/football/country/"]')
        league = " ".join(comp_link.get_text(" ", strip=True).split()) if comp_link else "Без лиги"
        country = " ".join(country_link.get_text(" ", strip=True).split()) if country_link else "Без страны"
        logos: list[str] = []
        if hasattr(container, "select"):
            for img in container.select('img[src]'):
                alt = str(img.get("alt") or "")
                if "flag" in alt.lower() or "competition" in str(img.get("src") or ""):
                    continue
                src = str(img.get("src") or "")
                if src and src not in logos:
                    logos.append(src)
        href = str(node.get("href") or node.get("data-match-url") or node.get("data-url") or f"/football/match/{slug}/")
        obj: dict[str, Any] = {
            "home": names[0],
            "away": names[1],
            "url": href,
            "startDate": kickoff,
            "eventStatus": "EventScheduled",
            "country": country,
            "competition": {"name": league, "url": comp_link.get("href") if comp_link else ""},
            "home_logo": logos[0] if len(logos) > 0 else "",
            "away_logo": logos[1] if len(logos) > 1 else "",
        }
        item = normalize_match(obj, source="sportscore_html")
        odds = _extract_row_odds(container)
        if odds:
            item["odds"] = odds
            item["has_odds"] = True
        current = out.get(slug, {})
        # DOM rows usually contain richer league/country/odds data; keep
        # JSON-LD home/away/start values when the row lacks them.
        merged = {**current, **{k: v for k, v in item.items() if v not in (None, "", {}, [])}}
        out[slug] = merged

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


def _extract_matches_envelope(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if not isinstance(payload, dict):
        return []
    for key in ("matches", "fixtures", "results", "items"):
        value = payload.get(key)
        if isinstance(value, list):
            return [x for x in value if isinstance(x, dict)]
    data = payload.get("data")
    if isinstance(data, dict):
        return _extract_matches_envelope(data)
    return []


def list_api_matches(force: bool = False) -> list[dict[str, Any]]:
    params = {"sport": "football", "limit": MAX_MATCHES, "src": SOURCE_TAG}
    if force:
        params["_t"] = int(time.time())
    payload = _json("/api/widget/matches/", params, ttl=10 if force else CACHE_SECONDS)
    return [normalize_match(x, source="sportscore_api") for x in _extract_matches_envelope(payload)]


def list_live(force: bool = False) -> list[dict[str, Any]]:
    return [x for x in list_api_matches(force=force) if x.get("is_live") and not x.get("finished")]


_LAST_UPCOMING_DIAGNOSTICS: dict[str, Any] = {}


def list_upcoming(force: bool = False) -> list[dict[str, Any]]:
    global _LAST_UPCOMING_DIAGNOSTICS
    combined: dict[str, dict[str, Any]] = {}
    source_succeeded = False
    errors: list[str] = []
    pages: list[dict[str, Any]] = []

    # The public widget endpoint is officially capped at 50 and is described as
    # "live + recent". Keep it only as enrichment/fallback, not as the primary
    # source for the full prematch calendar.
    try:
        api_rows = list_api_matches(force=force)
        source_succeeded = True
        for item in api_rows:
            if item.get("scheduled") and not item.get("finished"):
                combined[item["slug"]] = item
    except Exception as exc:
        errors.append(f"widget API: {type(exc).__name__}: {exc}")

    today = dt.datetime.now(dt.timezone.utc).date()
    date_paths = [
        (f"/football/?date={(today + dt.timedelta(days=offset)).isoformat()}&filter=upcoming", today + dt.timedelta(days=offset))
        for offset in range(PREMATCH_DAYS_AHEAD)
    ]
    for path, page_date in date_paths:
        try:
            text = _html(path, ttl=10 if force else CACHE_SECONDS)
            rows = parse_upcoming_html(text, default_date=page_date)
            expected = _page_scheduled_total(text)
            pages.append({"date": page_date.isoformat(), "expected": expected, "parsed": len(rows), "path": path})
            source_succeeded = True
            for item in rows:
                current = combined.get(item["slug"], {})
                combined[item["slug"]] = {**item, **{k: v for k, v in current.items() if v not in (None, "", {}, [])}}
        except Exception as exc:
            errors.append(f"{path}: {type(exc).__name__}: {exc}")

    # Legacy pages are useful if a dated route is temporarily unavailable.
    if not any(p.get("parsed") for p in pages):
        for path, page_date in (("/football/?filter=upcoming", today), ("/football/tomorrow/", today + dt.timedelta(days=1))):
            try:
                text = _html(path, ttl=10 if force else CACHE_SECONDS)
                rows = parse_upcoming_html(text, default_date=page_date)
                pages.append({"date": page_date.isoformat(), "expected": _page_scheduled_total(text), "parsed": len(rows), "path": path})
                source_succeeded = True
                for item in rows:
                    current = combined.get(item["slug"], {})
                    combined[item["slug"]] = {**item, **{k: v for k, v in current.items() if v not in (None, "", {}, [])}}
            except Exception as exc:
                errors.append(f"{path}: {type(exc).__name__}: {exc}")

    if not source_succeeded:
        raise RuntimeError("SportScore sources unavailable; " + " | ".join(errors[:3]))
    now = int(time.time()) - 3 * 3600
    out = [x for x in combined.values() if not x.get("finished") and (not x.get("kickoff_at") or int(x.get("kickoff_at") or 0) >= now)]
    out = sorted(out, key=lambda x: (int(x.get("kickoff_at") or 2**62), x.get("league") or "", x.get("home") or ""))
    _LAST_UPCOMING_DIAGNOSTICS = {
        "days_ahead": PREMATCH_DAYS_AHEAD,
        "pages": pages,
        "total_unique": len(out),
        "partial_pages": [p for p in pages if p.get("expected") and p.get("parsed", 0) < p.get("expected", 0)],
        "errors": errors[:5],
        "updated_at": int(time.time()),
    }
    return out


def upcoming_diagnostics() -> dict[str, Any]:
    return dict(_LAST_UPCOMING_DIAGNOSTICS)

def _team_matches(slug: str, limit: int = 10) -> list[dict[str, Any]]:
    if not slug:
        return []
    payload = _json("/api/widget/team/", {"sport": "football", "slug": slug, "limit": max(1, min(30, limit)), "src": SOURCE_TAG}, ttl=TEAM_CACHE_SECONDS)
    return [normalize_match(x, source="sportscore_team") for x in _extract_matches_envelope(payload)]


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


def detail(slug: str, force: bool = False) -> dict[str, Any]:
    slug = _slug_from_url(slug) or str(slug or "").removeprefix("ss:").strip()
    if not slug:
        return {"ok": False, "error": "missing_slug"}

    def load():
        html_text = _request(f"/football/match/{urllib.parse.quote(slug)}/")
        parsed = parse_match_html(str(html_text), slug)
        # Merge public API detail when available; HTML remains authoritative for
        # home/away because some slugs are not ordered like the fixture.
        try:
            api = _request("/api/widget/match/", {"sport": "football", "slug": slug, "src": SOURCE_TAG}, want_json=True)
            api_obj = api.get("match") if isinstance(api, dict) and isinstance(api.get("match"), dict) else api if isinstance(api, dict) else {}
            api_match = normalize_match(api_obj, source="sportscore_api_detail") if api_obj else {}
            for key in ("score_home", "score_away", "score", "minute", "minute_text", "period", "status", "is_live", "finished"):
                if api_match.get(key) not in (None, "", 0) or key in ("score_home", "score_away"):
                    parsed["match"][key] = api_match.get(key)
            api_stats = _flat_stats(_dig(api_obj, "stats", "statistics", default={}))
            if any(api_stats.values()):
                parsed["stats_flat"] = api_stats
            events = _dig(api_obj, "timeline", "events", default=[])
            parsed["events"] = events if isinstance(events, list) else []
            api_lineups = _dig(api_obj, "lineups", default=None)
            if api_lineups and not parsed["lineups"].get("announced"):
                parsed["lineups_raw"] = api_lineups
        except Exception:
            parsed.setdefault("events", [])

        match = parsed["match"]
        home_recent = _recent_summary(str(match.get("home_id") or ""), str(match.get("home") or ""))
        away_recent = _recent_summary(str(match.get("away_id") or ""), str(match.get("away") or ""))
        return {
            "ok": True,
            "provider": "sportscore",
            "prematch": not bool(match.get("is_live") or match.get("finished")),
            "match": match,
            "stats": _pair_stats(parsed.get("stats_flat") or {}),
            "stats_flat": parsed.get("stats_flat") or {},
            "events": parsed.get("events") or [],
            "odds": parsed.get("odds") or {},
            "h2h": parsed.get("h2h") or {},
            "standings": parsed.get("standings") or [],
            "lineups": parsed.get("lineups") or {},
            "venue": parsed.get("venue") or {},
            "avg": {"home": home_recent, "away": away_recent},
            "pressure": {"available": False, "reason": "prematch"},
            "pressure_chart": {"available": False, "reason": "prematch"},
            "finished": bool(match.get("finished")),
        }

    return _cached("detail:" + slug, 10 if force else DETAIL_CACHE_SECONDS, load)


def avg_for_slug(slug: str) -> dict[str, Any]:
    data = detail(slug)
    return {"ok": bool(data.get("ok")), "avg": data.get("avg") or {"home": {}, "away": {}}}


def source_health() -> dict[str, Any]:
    return {
        "base_url": BASE_URL,
        "source_tag": SOURCE_TAG,
        "cache_seconds": CACHE_SECONDS,
        "max_matches": MAX_MATCHES,
        "prematch_days_ahead": PREMATCH_DAYS_AHEAD,
        "upcoming": upcoming_diagnostics(),
    }
