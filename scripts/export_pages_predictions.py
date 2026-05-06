"""
Exporta predicciones recientes del digest a docs/betfree_predictions.json para GitHub Pages.

  python scripts/export_pages_predictions.py

Lee SQLITE_DB_PATH (o betfree.db) y la tabla digest_prediction_audit.
"""

from __future__ import annotations

import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"
OUT_JSON = DOCS / "betfree_predictions.json"


def main() -> None:
    sys.path.insert(0, str(ROOT))
    from src.config import bootstrap_dotenv, get_settings

    bootstrap_dotenv()
    settings = get_settings()
    db_path = settings.sqlite_db_path
    if not Path(db_path).is_absolute():
        db_path = str((ROOT / db_path).resolve())

    payload: dict = {
        "schema": "betfree.pages_predictions.v1",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "db_path_hint": Path(db_path).name,
        "items": [],
        "live_eval": None,
    }

    ev_path = ROOT / "data" / "digest_live_evaluation.json"
    if ev_path.is_file():
        try:
            payload["live_eval"] = json.loads(ev_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            payload["live_eval"] = {"error": "no se pudo leer digest_live_evaluation.json"}

    if not Path(db_path).is_file():
        payload["note"] = "Sin base SQLite: generá predicciones con el digest y volvé a ejecutar este script."
        DOCS.mkdir(parents=True, exist_ok=True)
        OUT_JSON.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"Escrito (vacío): {OUT_JSON}")
        return

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
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
        for r in cur.fetchall():
            payload["items"].append(
                {
                    "event_id": int(r["event_id"]),
                    "local_date_iso": str(r["local_date_iso"]),
                    "slug": str(r["digest_slug"] or ""),
                    "home_team": str(r["home_team"]),
                    "away_team": str(r["away_team"]),
                    "p_home": round(float(r["ph"]), 4),
                    "p_draw": round(float(r["pd"]), 4),
                    "p_away": round(float(r["pa"]), 4),
                    "used_ml": bool(int(r["used_ml"])),
                    "blend_ml_w": round(float(r["blend_ml_w"]), 3),
                    "ts_utc": str(r["ts_utc"]),
                }
            )
    finally:
        conn.close()

    DOCS.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Escrito: {OUT_JSON} ({len(payload['items'])} partidos)")


if __name__ == "__main__":
    main()
