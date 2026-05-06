"""Fixtures ESPN (sin API key)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import requests

from src.config import Settings


@dataclass(frozen=True)
class EspnFixtureRow:
    event_id: int
    league_name: str
    slug: str
    home_name: str
    away_name: str
    kickoff_utc: datetime | None
    status_state: str
    details: tuple[dict[str, Any], ...]
    home_team_id: int | None
    away_team_id: int | None
    stats_home: dict[str, str]
    stats_away: dict[str, str]


def _dt(raw: str | None) -> datetime | None:
    if not raw:
        return None
    try:
        return datetime.fromisoformat(str(raw).replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


def _stats(comp: dict[str, Any]) -> dict[str, str]:
    o: dict[str, str] = {}
    for row in comp.get("statistics") or []:
        if isinstance(row, dict) and row.get("name"):
            o[str(row["name"])] = str(row.get("displayValue") or "")
    return o


def card_counts_for_fixture(row: EspnFixtureRow) -> tuple[int, int, int, int]:
    yh = rh = ya = ra = 0
    for d in row.details:
        tm = d.get("team")
        tid = tm.get("id") if isinstance(tm, dict) else None
        try:
            ti = int(tid) if tid is not None else None
        except (TypeError, ValueError):
            ti = None
        if ti is None or row.home_team_id is None or row.away_team_id is None:
            continue
        if d.get("yellowCard"):
            if ti == row.home_team_id:
                yh += 1
            elif ti == row.away_team_id:
                ya += 1
        if d.get("redCard"):
            if ti == row.home_team_id:
                rh += 1
            elif ti == row.away_team_id:
                ra += 1
    return yh, ya, rh, ra


def fetch_fixtures_day(settings: Settings, slug: str, day) -> tuple[list[EspnFixtureRow], str]:
    ds = day.strftime("%Y%m%d")
    url = f"https://site.api.espn.com/apis/site/v2/sports/soccer/{slug.strip()}/scoreboard"
    r = requests.get(
        url,
        params={"dates": ds},
        timeout=float(settings.request_timeout_seconds),
        headers={"User-Agent": "BetfreeDigest/1.0"},
    )
    r.raise_for_status()
    data = r.json()
    lg = data.get("leagues") or []
    lg_name = str((lg[0] or {}).get("name") or slug).strip()
    out: list[EspnFixtureRow] = []
    for ev in data.get("events") or []:
        try:
            eid = int(ev.get("id"))
        except (TypeError, ValueError):
            continue
        comps = ev.get("competitions") or []
        if not comps:
            continue
        c0 = comps[0]
        cl = c0.get("competitors") or []
        hc = ac = None
        for co in cl:
            if co.get("homeAway") == "home":
                hc = co
            elif co.get("homeAway") == "away":
                ac = co
        if not hc or not ac:
            continue
        ht, at = hc.get("team") or {}, ac.get("team") or {}
        hn = str(ht.get("displayName") or ht.get("name") or "").strip()
        an = str(at.get("displayName") or at.get("name") or "").strip()
        if not hn or not an:
            continue
        try:
            hid = int(ht["id"]) if ht.get("id") is not None else None
        except (TypeError, ValueError, KeyError):
            hid = None
        try:
            aid = int(at["id"]) if at.get("id") is not None else None
        except (TypeError, ValueError, KeyError):
            aid = None
        st = ((c0.get("status") or {}).get("type") or {}).get("state") or "pre"
        # Varios payloads ESPN traen la hora en startDate, en competition.date o solo en event.date.
        kick = _dt(c0.get("startDate")) or _dt(c0.get("date")) or _dt(ev.get("date"))
        dr = [x for x in (c0.get("details") or []) if isinstance(x, dict)]
        out.append(
            EspnFixtureRow(
                event_id=eid,
                league_name=lg_name,
                slug=slug.strip(),
                home_name=hn,
                away_name=an,
                kickoff_utc=kick,
                status_state=str(st),
                details=tuple(dr),
                home_team_id=hid,
                away_team_id=aid,
                stats_home=_stats(hc),
                stats_away=_stats(ac),
            )
        )
    return out, lg_name
