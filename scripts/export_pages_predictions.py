"""
Exporta predicciones recientes del digest a docs/betfree_predictions.json para GitHub Pages.

  python scripts/export_pages_predictions.py

Lee SQLITE_DB_PATH (o betfree.db) y la tabla digest_prediction_audit.
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"
OUT_JSON = DOCS / "betfree_predictions.json"


def main() -> None:
    sys.path.insert(0, str(ROOT))
    from src.config import bootstrap_dotenv, get_settings

    bootstrap_dotenv()
    settings = get_settings()
    hist_csv = settings.historical_csv_path
    if hist_csv and not Path(hist_csv).is_absolute():
        hist_csv = str((ROOT / hist_csv).resolve())
    db_path = settings.sqlite_db_path
    if not Path(db_path).is_absolute():
        db_path = str((ROOT / db_path).resolve())

    payload: dict = {
        "schema": "betfree.pages_predictions.v2",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "db_path_hint": Path(db_path).name,
        "items": [],
        "live_eval": None,
        "historical_csv_hint": None,
        "stats_note_es": (
            "Goles (últ. N): medias desde el CSV histórico antes del día del partido. "
            "Corners y tarjetas: estimación heurística del modelo para ESTE enfrentamiento, "
            "no promedios reales por equipo en el CSV. "
            "ClubElo (opcional): rating público europeo desde api.clubelo.com por nombre+Liga cuando hay RED al export."
        ),
    }

    skip_club_elo_env = (
        str(os.getenv("EXPORT_PAGES_SKIP_CLUBELO", "")).strip().lower()
        in ("1", "true", "yes", "on")
    )

    ev_path = ROOT / "data" / "digest_live_evaluation.json"
    if ev_path.is_file():
        try:
            payload["live_eval"] = json.loads(ev_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            payload["live_eval"] = {"error": "no se pudo leer digest_live_evaluation.json"}

    payload["historical_csv_hint"] = Path(hist_csv).name if hist_csv else None

    if not Path(db_path).is_file():
        payload["note"] = "Sin base SQLite: generá predicciones con el digest y volvé a ejecutar este script."
        DOCS.mkdir(parents=True, exist_ok=True)
        OUT_JSON.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"Escrito (vacío): {OUT_JSON}")
        return

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    hist_ok = bool(hist_csv and Path(hist_csv).is_file())

    try:
        cur = conn.execute(
            """
            SELECT a.event_id, a.local_date_iso, a.digest_slug, a.home_team, a.away_team,
                   a.ph, a.pd, a.pa, a.used_ml, a.blend_ml_w, a.ts_utc
            FROM digest_prediction_audit a
            INNER JOIN (
                SELECT event_id, MAX(id) AS mid
                FROM digest_prediction_audit
                GROUP BY event_id
            ) z ON a.id = z.mid
            ORDER BY a.local_date_iso DESC, a.ts_utc DESC
            LIMIT 120
            """
        )
        from src.data_engine.club_elo_ratings import (
            ClubEloRanking,
            clubelo_country_hint_for_digest_slug,
            load_club_elo_ranking,
        )
        from src.predictor.digest_roll_context import DigestRollContext
        from src.predictor.pages_export_team_stats import build_pages_team_stats_from_context

        audit_rows = cur.fetchall()
        ctx_by_day: dict[date, DigestRollContext] = {}
        elo_cache_root = ROOT / "data" / "cache" / "club_elo"
        elo_cache: dict[date, ClubEloRanking | None] = {}

        def roll_ctx_for(day_iso: str) -> DigestRollContext | None:
            if not hist_ok:
                return None
            try:
                d_key = date.fromisoformat(day_iso.strip())
            except ValueError:
                return None
            if d_key not in ctx_by_day:
                ctx_by_day[d_key] = DigestRollContext.from_csv(str(Path(hist_csv).resolve()), before_day=d_key)
            return ctx_by_day[d_key]

        def club_elo_for(day_iso: str) -> ClubEloRanking | None:
            if skip_club_elo_env:
                return None
            try:
                dk = date.fromisoformat(day_iso.strip())
            except ValueError:
                return None
            if dk not in elo_cache:
                elo_cache[dk] = load_club_elo_ranking(dk, elo_cache_root, allow_network=True)
            return elo_cache[dk]

        for r in audit_rows:
            row_home = str(r["home_team"])
            row_away = str(r["away_team"])
            row_slug = str(r["digest_slug"] or "")
            row_date = str(r["local_date_iso"])
            roller = roll_ctx_for(row_date)
            ce = club_elo_for(row_date)
            cc = clubelo_country_hint_for_digest_slug(row_slug)
            stats = (
                build_pages_team_stats_from_context(
                    roller,
                    digest_slug=row_slug,
                    home_team=row_home,
                    away_team=row_away,
                    recent_matches=5,
                    club_elo_ranking=ce,
                    club_elo_country_hint=cc,
                )
                if roller is not None
                else None
            )
            payload["items"].append(
                {
                    "event_id": int(r["event_id"]),
                    "local_date_iso": row_date,
                    "slug": row_slug,
                    "home_team": row_home,
                    "away_team": row_away,
                    "p_home": round(float(r["ph"]), 4),
                    "p_draw": round(float(r["pd"]), 4),
                    "p_away": round(float(r["pa"]), 4),
                    "used_ml": bool(int(r["used_ml"])),
                    "blend_ml_w": round(float(r["blend_ml_w"]), 3),
                    "ts_utc": str(r["ts_utc"]),
                    "team_stats": stats,
                }
            )
    finally:
        conn.close()

    DOCS.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Escrito: {OUT_JSON} ({len(payload['items'])} partidos)")


if __name__ == "__main__":
    main()
