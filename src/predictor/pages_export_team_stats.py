"""Estadísticas por equipo para export estático GitHub Pages (desde CSV histórico + heurísticas del modelo)."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

from src.data_engine.club_elo_ratings import ClubEloRanking, ClubEloRow
from src.predictor.csv_roll_state import (
    estimated_match_cards_split,
    estimated_match_corners_split,
)
from src.predictor.digest_hist_league_map import hist_competition_for_digest_slug
from src.predictor.digest_roll_context import DigestRollContext


def _avg_recent_goals(state, team: str, window: int) -> dict[str, Any]:
    """Promedios de goles marcados/recibidos en los últimos `window` partidos observados antes del día del partido."""
    gf, ga, n = state.avg_goals_recent(team, window)
    if n <= 0 or gf is None or ga is None:
        return {"n_matches": 0, "gf_avg": None, "ga_avg": None}
    return {
        "n_matches": int(n),
        "gf_avg": round(float(gf), 3),
        "ga_avg": round(float(ga), 3),
    }


def _club_elo_side_block(res: tuple[ClubEloRow, str] | None) -> dict[str, Any]:
    if res is None:
        return {"rating": None, "rank": None, "matched_as": None, "country": None, "match": "none"}
    row, kind = res
    return {
        "rating": round(float(row.elo), 1),
        "rank": int(row.rank),
        "matched_as": row.club,
        "country": row.country,
        "match": kind,
    }


def build_pages_team_stats_from_context(
    roll_ctx: DigestRollContext,
    *,
    digest_slug: str,
    home_team: str,
    away_team: str,
    recent_matches: int = 5,
    club_elo_ranking: ClubEloRanking | None = None,
    club_elo_country_hint: str | None = None,
    head_to_head: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Calcula estadísticas con un DigestRollContext ya construido (p. ej. cacheado por día)."""
    hist = hist_competition_for_digest_slug(digest_slug)
    st = roll_ctx.pick_state(hist)

    h_block = _avg_recent_goals(st, home_team, recent_matches)
    a_block = _avg_recent_goals(st, away_team, recent_matches)

    fh, fd, fa, xh, xa = st.poisson_probs(home_team, away_team)
    tot_goals_exp = float(xh + xa)
    ch_y, ca_y, cy_tot = estimated_match_cards_split(st, home_team, away_team, tot_goals_exp, xh, xa)
    co_h, co_a, co_tot = estimated_match_corners_split(st, home_team, away_team, tot_goals_exp, xh, xa)

    out: dict[str, Any] = {
        "schema": "betfree.pages_team_stats.v1",
        "recent_matches": int(recent_matches),
        "hist_competition_hint": hist,
        "home": {
            **h_block,
            "name": home_team,
        },
        "away": {
            **a_block,
            "name": away_team,
        },
        "match_model_estimate": {
            "label_es": "Heurística del modelo para este encuentro",
            "p_home": round(float(fh), 4),
            "p_draw": round(float(fd), 4),
            "p_away": round(float(fa), 4),
            "x_goals_home": round(float(xh), 3),
            "x_goals_away": round(float(xa), 3),
            "yellow_cards_home": round(float(ch_y), 2),
            "yellow_cards_away": round(float(ca_y), 2),
            "yellow_cards_total": round(float(cy_tot), 2),
            "corners_home": round(float(co_h), 2),
            "corners_away": round(float(co_a), 2),
            "corners_total": round(float(co_tot), 2),
        },
    }

    if club_elo_ranking is not None:
        rh = club_elo_ranking.resolve(home_team, club_elo_country_hint)
        ra = club_elo_ranking.resolve(away_team, club_elo_country_hint)
        out["club_elo"] = {
            "schema": "betfree.club_elo_snapshot.v1",
            "ranking_date_iso": club_elo_ranking.as_of.isoformat(),
            "hint_country": club_elo_country_hint,
            "source_note_es": (
                "Ratings Elo públicos api.clubelo.com para clubes europeos (y sus ligas típicas). "
                "Nombres se cruzan con heurísticas; puede faltar Copa/Sudamericana."
            ),
            "home": _club_elo_side_block(rh),
            "away": _club_elo_side_block(ra),
        }

    if head_to_head is not None:
        out["h2h"] = head_to_head

    return out


def build_pages_team_stats(
    historical_csv_path: str,
    *,
    before_local_date_iso: str,
    digest_slug: str,
    home_team: str,
    away_team: str,
    recent_matches: int = 5,
) -> dict[str, Any] | None:
    """Arma bloque serializable para `betfree_predictions.json`."""
    raw = (historical_csv_path or "").strip()
    pth = Path(raw)
    if not pth.is_file():
        return None
    iso = (before_local_date_iso or "").strip()
    try:
        bday = date.fromisoformat(iso)
    except ValueError:
        return None

    ctx = DigestRollContext.from_csv(str(pth.resolve()), before_day=bday)
    return build_pages_team_stats_from_context(
        ctx,
        digest_slug=digest_slug,
        home_team=home_team,
        away_team=away_team,
        recent_matches=recent_matches,
        club_elo_ranking=None,
        club_elo_country_hint=None,
        head_to_head=None,
    )

