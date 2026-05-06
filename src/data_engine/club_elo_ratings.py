"""Ratings externos ClubElo (api.clubelo.com) para enriquecer exports estáticos y análisis.

Documentación públic: ranking por fecha en CSV Rank,Club,Country,Level,Elo,From,To.
Sin API key. Respetá la carga—cache local en disco.
"""

from __future__ import annotations

import csv
import difflib
import logging
from dataclasses import dataclass
from datetime import date, timedelta
from io import StringIO
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen

from src.predictor.csv_roll_state import norm_team
from src.predictor.digest_hist_league_map import hist_competition_for_digest_slug

LOG = logging.getLogger(__name__)

CLUBELO_BASE = "http://api.clubelo.com"

_HIST_TO_CELO_COUNTRY: dict[str, str] = {
    "E0": "ENG",
    "SP1": "ESP",
    "I1": "ITA",
    "D1": "GER",
    "F1": "FRA",
    "P1": "POR",
    "N1": "NED",
    "T1": "TUR",
    "G1": "GRE",
    "SC0": "SCO",
}

# Nombre digest/display → canonical Club muy frecuentes en el ranking.
_ALIAS_TO_CLUBELO_NORM: dict[str, str] = {
    "liverpool fc": "liverpool",
    "manchester united": "man united",
    "man utd": "man united",
    "manchester city": "man city",
    "tottenham hotspur": "tottenham",
    "tottenham hospur": "tottenham",
    "sporting cp": "sporting",
    "paris saint germain": "paris sg",
    "atlético madrid": "atletico",
    "atletico de madrid": "atletico",
}


@dataclass(frozen=True)
class ClubEloRow:
    rank: int
    club: str
    country: str
    elo: float


class ClubEloRanking:
    """Índices por nombre normalizado; desambiguación con código país ClubElo (ENG, GER, …)."""

    __slots__ = ("as_of", "_by_norm_keys", "_lists")

    def __init__(self, rows: list[ClubEloRow], *, as_of: date) -> None:
        self.as_of = as_of
        self._lists: dict[str, list[ClubEloRow]] = {}
        for r in rows:
            k = norm_team(r.club)
            self._lists.setdefault(k, []).append(r)
        self._by_norm_keys = sorted(self._lists.keys())

    def resolve(self, raw_name: str, country_hint: str | None) -> tuple[ClubEloRow, str] | None:
        nt0 = norm_team(raw_name)
        if not nt0:
            return None
        nt = _ALIAS_TO_CLUBELO_NORM.get(nt0, nt0)

        direct = self._lists.get(nt, [])
        row_pick = ClubEloRanking._pick_with_country(direct, country_hint)
        if row_pick is not None:
            if country_hint and len(direct) > 1 and row_pick.country == country_hint:
                kind = "exact_country"
            else:
                kind = "exact"
            return row_pick, kind

        fuzz = difflib.get_close_matches(nt, self._by_norm_keys, n=3, cutoff=0.74)
        for fk in fuzz:
            group = self._lists.get(fk, [])
            row_pick2 = ClubEloRanking._pick_with_country(group, country_hint)
            if row_pick2 is None:
                continue
            return row_pick2, "fuzzy"
        return None

    @staticmethod
    def _pick_with_country(
        rows: list[ClubEloRow], country_hint: str | None
    ) -> ClubEloRow | None:
        if not rows:
            return None
        if country_hint:
            hinted = [r for r in rows if r.country == country_hint]
            if len(hinted) == 1:
                return hinted[0]
            if not hinted and len(rows) == 1:
                return rows[0]
            return None
        return rows[0] if len(rows) == 1 else None


def clubelo_country_hint_for_digest_slug(slug: str) -> str | None:
    """ClubElo usa códigos de país tres letras tipo ENG/ESP sobre todo para 1ª división."""
    hc = hist_competition_for_digest_slug(slug)
    if not hc:
        return None
    return _HIST_TO_CELO_COUNTRY.get(hc)


def parse_clubelo_csv(text: str) -> list[ClubEloRow]:
    rows: list[ClubEloRow] = []
    f = StringIO(text.strip())
    sample = text[:4096]
    delim = ";" if sample.count(";") > sample.count(",") else ","
    rdr = csv.DictReader(f, delimiter=delim)
    fieldmap = {k.strip(): k for k in (rdr.fieldnames or [])}
    rk = fieldmap.get("Rank") or "Rank"
    ck = fieldmap.get("Club") or "Club"
    cok = fieldmap.get("Country") or "Country"
    elok = fieldmap.get("Elo") or "Elo"

    if not all(x in fieldmap for x in (rk, ck, cok, elok)):
        return rows

    for line in rdr:
        try:
            rank = int(float(str(line.get(rk, "")).strip() or "0"))
            club = str(line.get(ck, "")).strip()
            country = str(line.get(cok, "")).strip()
            elo = float(str(line.get(elok, "")).strip())
        except (TypeError, ValueError):
            continue
        if not club:
            continue
        rows.append(ClubEloRow(rank=rank, club=club, country=country, elo=elo))
    return rows


def _http_fetch_csv(day: date, *, timeout: float = 25.0) -> str | None:
    url = f"{CLUBELO_BASE}/{day.isoformat()}"
    req = Request(url, headers={"User-Agent": "Betfree/1 (+https://github.com/MathiasAlejandr0/Betfree)"})
    try:
        with urlopen(req, timeout=timeout) as resp:
            return resp.read().decode("utf-8", errors="replace")
    except URLError as exc:
        LOG.debug("ClubElo HTTP %s: %s", url, exc)
        return None
    except Exception as exc:
        LOG.debug("ClubElo error %s: %s", url, exc)
        return None


def load_club_elo_ranking(
    fixture_local_day: date,
    cache_root: Path,
    *,
    allow_network: bool = True,
    max_calendar_lookback: int = 28,
) -> ClubEloRanking | None:
    """Descarga/carga ranking ClubElo anclado <= hoy con lookback ante huecos/redes.

    Cache: ``cache_root / YYYY-MM-DD.csv``.
    """
    cache_root.mkdir(parents=True, exist_ok=True)
    anchor = fixture_local_day
    today = date.today()
    if anchor > today:
        anchor = today

    for off in range(0, max(1, max_calendar_lookback)):
        cand = anchor - timedelta(days=off)
        path = cache_root / f"{cand.isoformat()}.csv"
        body: str | None = None
        if path.is_file():
            try:
                body = path.read_text(encoding="utf-8", errors="replace")
            except OSError:
                body = None
        if body is None and allow_network:
            fetched = _http_fetch_csv(cand)
            if fetched and "Rank" in fetched and "Elo" in fetched:
                try:
                    path.write_text(fetched, encoding="utf-8")
                except OSError:
                    pass
                body = fetched
        if body:
            elo_rows = parse_clubelo_csv(body)
            if elo_rows:
                return ClubEloRanking(elo_rows, as_of=cand)
    return None
