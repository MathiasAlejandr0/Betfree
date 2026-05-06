"""Mapea slug de agenda (ESPN / TheSportsDB) a código `competition` del CSV historical_robust (Football-Data).

Solo incluye ligas que existen en el CSV robusto típico (E0, SP1, I1, D1, F1, P1, N1, T1, G1, SC0).
Sin mapeo → Poisson+Elo **global** (mezcla todas las filas del histórico).
"""

from __future__ import annotations

# Códigos verificados en historical_robust del repo.
_ESPN_SLUG_TO_HIST: dict[str, str] = {
    "eng.1": "E0",
    "esp.1": "SP1",
    "ita.1": "I1",
    "ger.1": "D1",
    "fra.1": "F1",
    "por.1": "P1",
    "ned.1": "N1",
    "tur.1": "T1",
    "gre.1": "G1",
    "sco.1": "SC0",
}

# IDs TheSportsDB alineados con src/data_engine/thesportsdb_fixtures.py (1ª división EU).
_TSDB_ID_TO_HIST: dict[str, str] = {
    "4328": "E0",
    "4335": "SP1",
    "4334": "F1",
    "4332": "I1",
    "4331": "D1",
    "4344": "P1",
    "4337": "N1",
    "4336": "G1",
    "4330": "SC0",
}


def hist_competition_for_digest_slug(slug: str) -> str | None:
    """Devuelve código CSV o None → se usa estado global."""
    s = (slug or "").strip().lower()
    if not s:
        return None
    if s.startswith("tsdb."):
        lid = s.removeprefix("tsdb.").strip()
        return _TSDB_ID_TO_HIST.get(lid)
    return _ESPN_SLUG_TO_HIST.get(s)
