"""Empareja partidos del digest (ESPN/TSDB event_id + equipos) con fixture id API-Football por fecha."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone

from src.predictor.csv_roll_state import norm_team
from src.storage.database import get_connection


@dataclass(frozen=True)
class DigestRow:
    event_id: int
    home_team: str
    away_team: str


def _soft_same(na: str, nb: str) -> bool:
    """Igualdad de nombres normalizados o prefijo largo (p. ej. arsenal vs arsenal fc)."""
    if na == nb:
        return True
    if len(na) < 4 or len(nb) < 4:
        return False
    shorter, longer = (na, nb) if len(na) <= len(nb) else (nb, na)
    return longer.startswith(shorter + " ") or longer == shorter


def _teams_match(d_home: str, d_away: str, api_home: str, api_away: str) -> bool:
    a, b = norm_team(d_home), norm_team(d_away)
    x, y = norm_team(api_home), norm_team(api_away)
    fwd = _soft_same(a, x) and _soft_same(b, y)
    rev = _soft_same(a, y) and _soft_same(b, x)
    return fwd or rev


def _iter_api_fixtures(body: dict[str, Any]) -> list[tuple[int, str, str]]:
    out: list[tuple[int, str, str]] = []
    resp = body.get("response")
    if not isinstance(resp, list):
        return out
    for item in resp:
        if not isinstance(item, dict):
            continue
        fx = item.get("fixture")
        teams = item.get("teams")
        if not isinstance(fx, dict) or not isinstance(teams, dict):
            continue
        try:
            fid = int(fx.get("id"))
        except (TypeError, ValueError):
            continue
        h = teams.get("home") or {}
        a = teams.get("away") or {}
        if not isinstance(h, dict) or not isinstance(a, dict):
            continue
        hn = str(h.get("name", "")).strip()
        an = str(a.get("name", "")).strip()
        if fid and hn and an:
            out.append((fid, hn, an))
    return out


def load_digest_rows_for_date(db_path: str, local_date_iso: str) -> list[DigestRow]:
    conn = get_connection(db_path)
    try:
        cur = conn.execute(
            """
            SELECT event_id, home_team, away_team
            FROM (
                SELECT event_id, home_team, away_team,
                       ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY id DESC) AS rn
                FROM digest_prediction_audit
                WHERE local_date_iso = ?
            ) t
            WHERE rn = 1
            ORDER BY event_id
            """,
            (local_date_iso,),
        )
        rows = [DigestRow(int(r["event_id"]), str(r["home_team"]), str(r["away_team"])) for r in cur.fetchall()]
        return rows
    finally:
        conn.close()


def match_digest_to_api_football(
    digest_rows: list[DigestRow],
    api_body: dict[str, Any],
) -> tuple[list[tuple[int, int]], list[dict[str, Any]]]:
    """
    Devuelve (lista (digest_event_id, api_football_fixture_id), ambigüedades logueables).
    """
    api_fixtures = _iter_api_fixtures(api_body)
    matches: list[tuple[int, int]] = []
    issues: list[dict[str, Any]] = []
    used_af_ids: set[int] = set()

    for dr in digest_rows:
        candidates: list[int] = []
        for fid, hn, an in api_fixtures:
            if _teams_match(dr.home_team, dr.away_team, hn, an):
                candidates.append(fid)
        cand = sorted(set(candidates))
        if len(cand) == 1:
            af = cand[0]
            if af in used_af_ids:
                issues.append(
                    {
                        "type": "api_fixture_reused",
                        "digest_event_id": dr.event_id,
                        "api_football_fixture_id": af,
                    }
                )
                continue
            used_af_ids.add(af)
            matches.append((dr.event_id, af))
        elif len(cand) == 0:
            issues.append(
                {
                    "type": "no_match",
                    "digest_event_id": dr.event_id,
                    "home": dr.home_team,
                    "away": dr.away_team,
                }
            )
        else:
            issues.append(
                {
                    "type": "ambiguous",
                    "digest_event_id": dr.event_id,
                    "candidates": cand[:8],
                }
            )
    return matches, issues


def run_bridge_for_date(
    *,
    db_path: str,
    local_date_iso: str,
    api_key: str,
    daily_cap: int,
    upsert_fn: Callable[[int, str, str, str, int], None],
) -> dict[str, Any]:
    """upsert_fn(digest_event_id, local_date_iso, home, away, api_football_fixture_id)"""
    rows = load_digest_rows_for_date(db_path, local_date_iso)
    if not rows:
        return {
            "schema": "betfree.digest_af_fixture_bridge.v1",
            "local_date_iso": local_date_iso,
            "digest_rows": 0,
            "matches": 0,
            "note": "Sin filas en digest_prediction_audit para esa fecha.",
        }

    from src.data_engine.api_football_fixtures_client import api_football_fixtures_get_by_date

    body = api_football_fixtures_get_by_date(local_date_iso, api_key=api_key, daily_cap=daily_cap)
    pairs, issues = match_digest_to_api_football(rows, body)
    by_eid = {dr.event_id: dr for dr in rows}
    for eid, afid in pairs:
        dr = by_eid.get(eid)
        if dr:
            upsert_fn(eid, local_date_iso, dr.home_team, dr.away_team, afid)

    return {
        "schema": "betfree.digest_af_fixture_bridge.v1",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "local_date_iso": local_date_iso,
        "digest_rows": len(rows),
        "matched": len(pairs),
        "pairs": [{"digest_event_id": a, "api_football_fixture_id": b} for a, b in pairs],
        "issues": issues,
        "api_fixtures_count": len(_iter_api_fixtures(body)),
    }
