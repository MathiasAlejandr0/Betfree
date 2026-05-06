"""Extrae cuotas 1X2 decimales del JSON de GET /odds (API-Football v3)."""

from __future__ import annotations

import re
from typing import Any


def _to_float_odd(raw: Any) -> float | None:
    if raw is None:
        return None
    s = str(raw).strip().replace(",", ".")
    if not s:
        return None
    try:
        v = float(s)
        return v if v > 1.0 else None
    except ValueError:
        return None


def _norm_label(s: str) -> str:
    t = s.strip().lower()
    return re.sub(r"\s+", " ", t)


def _classify_outcome_label(label: str) -> str | None:
    n = _norm_label(label)
    if n in ("1", "home", "local", "h"):
        return "H"
    if n in ("x", "draw", "empate", "d"):
        return "D"
    if n in ("2", "away", "visit", "visita", "a"):
        return "A"
    if "home" in n and len(n) < 24:
        return "H"
    if "away" in n and len(n) < 24:
        return "A"
    if "draw" in n or "empate" in n:
        return "D"
    return None


def extract_first_bookmaker_1x2_decimals(body: dict[str, Any]) -> tuple[float, float, float] | None:
    """
    Primer mercado tipo Match Winner / 1X2 con tres selecciones → (home, draw, away) en decimales.
    Recorre todos los bloques `response[]` y bookmakers hasta encontrar un mercado completo.
    """
    resp = body.get("response")
    if not isinstance(resp, list) or not resp:
        return None
    keywords = (
        "winner",
        "match result",
        "full time result",
        "1x2",
        "3way",
        "three way",
        "match outcome",
    )
    for item in resp:
        if not isinstance(item, dict):
            continue
        bookmakers = item.get("bookmakers")
        if not isinstance(bookmakers, list):
            continue
        for bm in bookmakers:
            if not isinstance(bm, dict):
                continue
            bets = bm.get("bets")
            if not isinstance(bets, list):
                continue
            for bet in bets:
                if not isinstance(bet, dict):
                    continue
                name = str(bet.get("name", "")).strip().lower()
                if not name or not any(k in name for k in keywords):
                    continue
                vals = bet.get("values")
                if not isinstance(vals, list) or len(vals) < 3:
                    continue
                h = d = a = None
                for v in vals:
                    if not isinstance(v, dict):
                        continue
                    side = _classify_outcome_label(str(v.get("value", "")))
                    odd = _to_float_odd(v.get("odd"))
                    if side is None or odd is None:
                        continue
                    if side == "H":
                        h = odd
                    elif side == "D":
                        d = odd
                    elif side == "A":
                        a = odd
                if h is not None and d is not None and a is not None:
                    return (h, d, a)
    return None
