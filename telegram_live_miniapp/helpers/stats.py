"""v9.76: чистые функции работы со статистикой матча и расчётом давления.

Эти функции принимают словарь stats и возвращают результат. Не лазят
ни в IGScore, ни в SQLite. Удобно тестировать отдельно.
"""

from __future__ import annotations

from typing import Any


# IGScore type map:
#   25 possession, 2 corners, 3 yellow cards, 4 red cards,
#   21 shots on target, 22 shots off target, 23 attacks, 24 dangerous attacks.
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


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(str(value).strip()))
    except Exception:
        return int(default)


def stat_total_from_match_stats(stats: dict[str, Any], key: str) -> int:
    total = stats.get(f"{key}_total")
    if total not in (None, ""):
        return _safe_int(total)
    nested = stats.get(key)
    if isinstance(nested, dict):
        return _safe_int(nested.get("home")) + _safe_int(nested.get("away"))
    return _safe_int(stats.get(f"{key}_home")) + _safe_int(stats.get(f"{key}_away"))


def stat_side_from_match_stats(stats: dict[str, Any], key: str, side: str) -> int:
    nested = stats.get(key)
    if isinstance(nested, dict):
        return _safe_int(nested.get(side))
    return _safe_int(stats.get(f"{key}_{side}"))


def pressure_power_from_stats(stats: dict[str, Any], side: str) -> float:
    """Взвешенная "сила давления" одной команды.

    Веса: атаки×0.6, опасные×3, удары×4, в створ×6, угловые×5.
    Те же значения используются в pressure_chart_from_history.
    """
    attacks = stat_side_from_match_stats(stats, "attacks", side)
    dangerous = stat_side_from_match_stats(stats, "dangerous", side)
    shots = stat_side_from_match_stats(stats, "shots", side)
    on_target = stat_side_from_match_stats(stats, "on_target", side)
    corners = stat_side_from_match_stats(stats, "corners", side)
    return (attacks * 0.6) + (dangerous * 3.0) + (shots * 4.0) + (on_target * 6.0) + (corners * 5.0)


def flat_stats(stats: dict[str, Any]) -> dict[str, int]:
    """Из вложенной структуры {key: {home, away}} в плоскую {key_home, key_away, key_total}.

    Используется коллектором для записи в SQLite (одна колонка на счётчик).
    """
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


def stat_pairs_from_response(resp: dict[str, Any]) -> dict[str, dict[str, int]]:
    """Парсит IGScore /match/statistics в дерево {key: {home, away}}.

    type_id маппинг: 21 = on_target, 22 = off_target, 25 = possession, и т.д.
    Всего ударов = on_target + off_target.
    """
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
    possession = by_type.get(25, (50, 50))

    return {
        "possession": {"home": possession[0], "away": possession[1]},
        "shots": {"home": shots_home, "away": shots_away},
        "on_target": {"home": on_target[0], "away": on_target[1]},
        "off_target": {"home": off_target[0], "away": off_target[1]},
        "attacks": {"home": by_type.get(23, (0, 0))[0], "away": by_type.get(23, (0, 0))[1]},
        "dangerous": {"home": by_type.get(24, (0, 0))[0], "away": by_type.get(24, (0, 0))[1]},
        "corners": {"home": by_type.get(2, (0, 0))[0], "away": by_type.get(2, (0, 0))[1]},
        "yellow_cards": {"home": by_type.get(3, (0, 0))[0], "away": by_type.get(3, (0, 0))[1]},
        "red_cards": {"home": by_type.get(4, (0, 0))[0], "away": by_type.get(4, (0, 0))[1]},
    }
