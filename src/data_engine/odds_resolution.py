"""
Resolución de cuotas multi-fuente (API-Football vs The Odds API).

Los IDs de partido no son intercambiables entre proveedores: este módulo documenta
estrategias y helpers; el digest ESPN no trae `fixture` API-Football.

Convención de mercados en `odds_snapshot`:
  - sufijo `:open`  — línea al capturar (apertura / snapshot)
  - sufijo `:closing` — segunda escritura cerca del pitazo (requiere job dedicado)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class OddsLineSnapshot:
    """Una línea 1X2 decimal implícita (probabilidades implícitas 1/odds)."""

    source: str
    home_decimal: float | None
    draw_decimal: float | None
    away_decimal: float | None
    raw_note: str = ""


def market_key_1x2(suffix: str = "open") -> tuple[str, str, str]:
    suf = suffix.strip() or "open"
    return (f"1X2_HOME:{suf}", f"1X2_DRAW:{suf}", f"1X2_AWAY:{suf}")


def implied_probs_from_decimals(h: float, d: float, a: float) -> tuple[float, float, float] | None:
    """Normaliza probabilidades implícitas 1/h + 1/d + 1/a (sobreround removido por normalización)."""
    try:
        ih, id_, ia = 1.0 / float(h), 1.0 / float(d), 1.0 / float(a)
    except (TypeError, ValueError, ZeroDivisionError):
        return None
    s = ih + id_ + ia
    if s <= 0:
        return None
    return ih / s, id_ / s, ia / s


def api_football_odds_result_count(body: dict[str, Any]) -> int:
    """Cuenta entradas en `response` típico de GET /odds."""
    if not isinstance(body, dict):
        return 0
    r = body.get("response")
    if isinstance(r, list):
        return len(r)
    return 0


def describe_odds_sources_for_fixture(fixture_id: int) -> str:
    """Texto operativo para logs / UI interna."""
    return (
        f"Fixture API-Football {fixture_id}: usá GET /odds con x-apisports-key. "
        "The Odds API usa slugs de deporte/liga, no este fixture_id."
    )
