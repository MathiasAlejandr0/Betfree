"""Partido unificado para agenda (ESPN + otras fuentes)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timezone
from typing import Iterable

from src.data_engine.espn_free_fixtures import EspnFixtureRow
from src.predictor.csv_roll_state import norm_team


def _digest_merge_key(row: DigestFixtureRow) -> tuple[str, str, int]:
    """Equipos + ventana UTC 15 min (ESPN vs TheSportsDB suelen coincidir en hora, no en nombre de liga)."""
    if row.kickoff_utc is None:
        raise ValueError("merge_key requiere kickoff_utc")
    k = row.kickoff_utc.astimezone(timezone.utc)
    slot = int(k.timestamp() // 900)
    return (norm_team(row.home_name), norm_team(row.away_name), slot)


@dataclass(frozen=True)
class DigestFixtureRow:
    """Una fila de agenda con id estable para Telegram / SQLite."""

    event_id: int
    source: str  # "espn" | "tsdb"
    league_name: str
    slug: str
    home_name: str
    away_name: str
    kickoff_utc: datetime | None

    @staticmethod
    def from_espn(r: EspnFixtureRow) -> DigestFixtureRow:
        return DigestFixtureRow(
            event_id=int(r.event_id),
            source="espn",
            league_name=(r.league_name or "").strip() or r.slug,
            slug=r.slug,
            home_name=r.home_name,
            away_name=r.away_name,
            kickoff_utc=r.kickoff_utc,
        )


def merge_digest_fixtures(espn_rows: Iterable[DigestFixtureRow], extra_rows: Iterable[DigestFixtureRow]) -> list[DigestFixtureRow]:
    """ESPN gana en empate (misma fecha + equipos + liga); el resto rellena huecos."""
    out: dict[tuple[str, str, date, str], DigestFixtureRow] = {}
    for row in espn_rows:
        if row.kickoff_utc is None:
            continue
        out[_digest_merge_key(row)] = row
    for row in extra_rows:
        if row.kickoff_utc is None:
            continue
        k = _digest_merge_key(row)
        if k not in out:
            out[k] = row
    return list(out.values())


def is_chile_primera_row(row: DigestFixtureRow) -> bool:
    if row.slug == "chi.1":
        return True
    ln = (row.league_name or "").casefold()
    return "chile" in ln and ("primera" in ln or "chilean" in ln)
