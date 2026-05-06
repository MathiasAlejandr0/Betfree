"""TheSportsDB — agenda amplia (primera división por metadatos de liga, sin API de pago)."""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

import requests

from src.config import Settings
from src.data_engine.digest_fixture import DigestFixtureRow

LOG = logging.getLogger(__name__)

# La API pública a veces devuelve pocas ligas; unimos IDs conocidos de 1ª (TheSportsDB / all_leagues).
_THESPORTSDB_STATIC_DIV1_IDS: frozenset[str] = frozenset(
    {
        "4328",  # English Premier League
        "4330",  # Scottish Premiership
        "4331",  # Bundesliga
        "4332",  # Serie A
        "4334",  # Ligue 1
        "4335",  # La Liga
        "4336",  # Greek Super League
        "4337",  # Eredivisie
        "4338",  # Belgian Pro League
    }
)

# Copas internacionales de clubes (TheSportsDB idLeague; no entran en search intDivision=1).
_THESPORTSDB_INTL_CUP_LEAGUE_IDS: frozenset[str] = frozenset(
    {
        "4480",  # UEFA Champions League
        "4481",  # UEFA Europa League
        "5071",  # UEFA Conference League
        "4512",  # UEFA Super Cup
        "4490",  # UEFA Nations League
        "4501",  # Copa Libertadores
        "4724",  # Copa Sudamericana
        "5665",  # Recopa Sudamericana
        "4719",  # AFC Champions League Elite
        "4720",  # CAF Champions League
        "4721",  # CONCACAF Champions Cup
        "4503",  # FIFA Club World Cup
        "4429",  # FIFA World Cup (selecciones; global)
    }
)
# Digest solo Europa + Sudamérica: excluye AFC/CAF/CONCACAF de la agenda TSDB.
_THESPORTSDB_INTL_CUP_NON_EU_SA_LEAGUE_IDS: frozenset[str] = frozenset({"4719", "4720", "4721"})
_THESPORTSDB_INTL_CUP_EU_SA_LEAGUE_IDS: frozenset[str] = _THESPORTSDB_INTL_CUP_LEAGUE_IDS - _THESPORTSDB_INTL_CUP_NON_EU_SA_LEAGUE_IDS

_TSDB_EUROPE_STR_COUNTRY: frozenset[str] = frozenset(
    {
        "england",
        "scotland",
        "wales",
        "northern ireland",
        "ireland",
        "spain",
        "italy",
        "germany",
        "france",
        "portugal",
        "netherlands",
        "belgium",
        "turkey",
        "greece",
        "austria",
        "switzerland",
        "sweden",
        "denmark",
        "norway",
        "finland",
        "iceland",
        "poland",
        "czech republic",
        "croatia",
        "serbia",
        "romania",
        "bulgaria",
        "ukraine",
        "hungary",
        "slovenia",
        "slovakia",
        "cyprus",
        "israel",
        "russia",
        "belarus",
        "moldova",
        "estonia",
        "latvia",
        "lithuania",
        "albania",
        "bosnia and herzegovina",
        "north macedonia",
        "montenegro",
        "kosovo",
        "luxembourg",
        "malta",
        "andorra",
        "san marino",
        "liechtenstein",
        "faroe islands",
        "gibraltar",
        "kazakhstan",
        "georgia",
        "armenia",
        "azerbaijan",
        "monaco",
        "united kingdom",
    }
)

_TSDB_SOUTH_AMERICA_STR_COUNTRY: frozenset[str] = frozenset(
    {
        "argentina",
        "bolivia",
        "brazil",
        "chile",
        "colombia",
        "ecuador",
        "guyana",
        "paraguay",
        "peru",
        "suriname",
        "uruguay",
        "venezuela",
    }
)


def _tsdb_country_europe_or_south_america(raw: str) -> bool:
    c = (raw or "").strip().casefold()
    return c in _TSDB_EUROPE_STR_COUNTRY or c in _TSDB_SOUTH_AMERICA_STR_COUNTRY

# Hora naive de strTimestamp suele ser local al país del evento; UTC solo si no hay mapa.
_COUNTRY_MAIN_TZ: dict[str, str] = {
    "England": "Europe/London",
    "Scotland": "Europe/London",
    "Wales": "Europe/London",
    "Northern Ireland": "Europe/London",
    "Italy": "Europe/Rome",
    "Spain": "Europe/Madrid",
    "Germany": "Europe/Berlin",
    "France": "Europe/Paris",
    "Portugal": "Europe/Lisbon",
    "Netherlands": "Europe/Amsterdam",
    "Belgium": "Europe/Brussels",
    "Turkey": "Europe/Istanbul",
    "Greece": "Europe/Athens",
    "Austria": "Europe/Vienna",
    "Switzerland": "Europe/Zurich",
    "Poland": "Europe/Warsaw",
    "Czech Republic": "Europe/Prague",
    "Croatia": "Europe/Zagreb",
    "Serbia": "Europe/Belgrade",
    "Romania": "Europe/Bucharest",
    "Hungary": "Europe/Budapest",
    "Ukraine": "Europe/Kyiv",
    "Russia": "Europe/Moscow",
    "Sweden": "Europe/Stockholm",
    "Norway": "Europe/Oslo",
    "Denmark": "Europe/Copenhagen",
    "Finland": "Europe/Helsinki",
    "Ireland": "Europe/Dublin",
    "Brazil": "America/Sao_Paulo",
    "Argentina": "America/Argentina/Buenos_Aires",
    "Chile": "America/Santiago",
    "Colombia": "America/Bogota",
    "Peru": "America/Lima",
    "Uruguay": "America/Montevideo",
    "Paraguay": "America/Asuncion",
    "Ecuador": "America/Guayaquil",
    "Bolivia": "America/La_Paz",
    "Venezuela": "America/Caracas",
    "Mexico": "America/Mexico_City",
    "United States": "America/New_York",
    "Canada": "America/Toronto",
    "Japan": "Asia/Tokyo",
    "South Korea": "Asia/Seoul",
    "China": "Asia/Shanghai",
    "Australia": "Australia/Sydney",
    "New Zealand": "Pacific/Auckland",
    "South Africa": "Africa/Johannesburg",
    "Egypt": "Africa/Cairo",
    "Morocco": "Africa/Casablanca",
    "Algeria": "Africa/Algiers",
    "Tunisia": "Africa/Tunis",
    "Israel": "Asia/Jerusalem",
    "Saudi Arabia": "Asia/Riyadh",
    "United Arab Emirates": "Asia/Dubai",
    "Qatar": "Asia/Qatar",
    "Iran": "Asia/Tehran",
    "India": "Asia/Kolkata",
    "Thailand": "Asia/Bangkok",
    "Indonesia": "Asia/Jakarta",
    "Malaysia": "Asia/Kuala_Lumpur",
    "Costa Rica": "America/Costa_Rica",
    "Jamaica": "America/Jamaica",
    "Worldwide": "UTC",
    "World": "UTC",
    "Europe": "Europe/Paris",
}


def _soccer_first_tier_league_ids(settings: Settings) -> frozenset[str]:
    key = (settings.thesportsdb_api_key or "").strip() or "3"
    url = f"https://www.thesportsdb.com/api/v1/json/{key}/search_all_leagues.php"
    r = requests.get(url, params={"s": "Soccer"}, timeout=float(settings.request_timeout_seconds), headers={"User-Agent": "BetfreeDigest/1.0"})
    r.raise_for_status()
    data = r.json()
    rows = data.get("countries") or data.get("leagues") or []
    out: set[str] = set()
    for it in rows:
        if not isinstance(it, dict):
            continue
        if str(it.get("strSport") or "").strip() != "Soccer":
            continue
        if str(it.get("intDivision") or "").strip() != "1":
            continue
        if str(it.get("idCup") or "0").strip() != "0":
            continue
        lid = str(it.get("idLeague") or "").strip()
        if lid:
            out.add(lid)
    return frozenset(out)


def _parse_tsdb_kickoff(ev: dict[str, Any]) -> datetime | None:
    raw_ts = str(ev.get("strTimestamp") or "").strip()
    country = str(ev.get("strCountry") or "").strip()
    date_ev = str(ev.get("dateEvent") or "").strip()
    str_time = str(ev.get("strTime") or "").strip()

    if raw_ts:
        try:
            dt = datetime.fromisoformat(raw_ts.replace("Z", "+00:00"))
            if dt.tzinfo is not None:
                return dt.astimezone(timezone.utc)
        except ValueError:
            pass
        if "T" in raw_ts:
            try:
                naive = datetime.fromisoformat(raw_ts[:19])
            except ValueError:
                naive = None
            if naive is not None:
                tzname = _COUNTRY_MAIN_TZ.get(country, "UTC")
                try:
                    zi = ZoneInfo(tzname)
                except Exception:
                    return naive.replace(tzinfo=timezone.utc)
                return naive.replace(tzinfo=zi).astimezone(timezone.utc)

    if date_ev and str_time and len(str_time) >= 5:
        try:
            naive = datetime.fromisoformat(f"{date_ev}T{str_time[:8]}")
        except ValueError:
            return None
        tzname = _COUNTRY_MAIN_TZ.get(country, "UTC")
        try:
            zi = ZoneInfo(tzname)
        except Exception:
            return naive.replace(tzinfo=timezone.utc)
        return naive.replace(tzinfo=zi).astimezone(timezone.utc)
    return None


def fetch_thesportsdb_first_tier_day(settings: Settings, day: date, league_ids: frozenset[str]) -> list[DigestFixtureRow]:
    if not league_ids:
        return []
    key = (settings.thesportsdb_api_key or "").strip() or "3"
    ds = day.strftime("%Y-%m-%d")
    url = f"https://www.thesportsdb.com/api/v1/json/{key}/eventsday.php"
    r = requests.get(
        url,
        params={"d": ds, "s": "Soccer"},
        timeout=float(settings.request_timeout_seconds),
        headers={"User-Agent": "BetfreeDigest/1.0"},
    )
    r.raise_for_status()
    data = r.json()
    events = data.get("events")
    if not isinstance(events, list):
        return []
    out: list[DigestFixtureRow] = []
    for ev in events:
        if not isinstance(ev, dict):
            continue
        lid = str(ev.get("idLeague") or "").strip()
        if lid not in league_ids:
            continue
        if str(ev.get("strPostponed") or "").strip().lower() == "yes":
            continue
        if settings.digest_tsdb_eu_sa_only:
            if lid in _THESPORTSDB_INTL_CUP_NON_EU_SA_LEAGUE_IDS:
                continue
            if lid not in _THESPORTSDB_INTL_CUP_EU_SA_LEAGUE_IDS:
                ctry = str(ev.get("strCountry") or "").strip()
                if not _tsdb_country_europe_or_south_america(ctry):
                    continue
        h = str(ev.get("strHomeTeam") or "").strip()
        a = str(ev.get("strAwayTeam") or "").strip()
        if not h or not a:
            continue
        kick = _parse_tsdb_kickoff(ev)
        if kick is None:
            continue
        league_name = str(ev.get("strLeague") or "").strip() or lid
        try:
            eid = int(str(ev.get("idEvent") or "0"))
        except ValueError:
            continue
        if eid <= 0:
            continue
        out.append(
            DigestFixtureRow(
                event_id=eid,
                source="tsdb",
                league_name=league_name,
                slug=f"tsdb.{lid}",
                home_name=h,
                away_name=a,
                kickoff_utc=kick,
            )
        )
    return out


_league_ids_day_cache: tuple[date, frozenset[str]] | None = None


def load_first_tier_league_ids_for_day(settings: Settings, day: date) -> frozenset[str]:
    """IDs de ligas con intDivision=1 e idCup=0 (TheSportsDB). Cache por día civil."""
    global _league_ids_day_cache
    if _league_ids_day_cache is not None and _league_ids_day_cache[0] == day:
        return _league_ids_day_cache[1]
    try:
        intl = (
            _THESPORTSDB_INTL_CUP_EU_SA_LEAGUE_IDS
            if settings.digest_tsdb_eu_sa_only
            else _THESPORTSDB_INTL_CUP_LEAGUE_IDS
        )
        ids = frozenset(_soccer_first_tier_league_ids(settings) | _THESPORTSDB_STATIC_DIV1_IDS | intl)
        LOG.info("TheSportsDB: %s ids de liga 1ª (API + respaldo fijo).", len(ids))
        _league_ids_day_cache = (day, ids)
        return ids
    except Exception as exc:
        LOG.warning("TheSportsDB search_all_leagues: %s", exc)
        return frozenset()
