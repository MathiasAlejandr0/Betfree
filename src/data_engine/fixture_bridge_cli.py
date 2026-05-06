"""Puente digest ↔ API-Football: rellena digest_api_football_fixture_map por fecha local."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

from src.config import bootstrap_dotenv, get_settings, repo_root
from src.data_engine.digest_af_fixture_bridge import run_bridge_for_date
from src.storage.database import init_db
from src.storage.repository import TimeSeriesRepository


def main() -> None:
    bootstrap_dotenv()
    p = argparse.ArgumentParser(description="Mapear event_id digest → fixture API-Football (GET /fixtures?date=)")
    p.add_argument("--date", default="", help="YYYY-MM-DD (local). Vacío = hoy local.")
    p.add_argument("--db-path", default="")
    args = p.parse_args()

    s = get_settings()
    if not s.api_football_key:
        print("API_FOOTBALL_KEY no configurado.", file=sys.stderr)
        sys.exit(2)

    root = repo_root()
    raw_db = (args.db_path or "").strip() or s.sqlite_db_path
    db = raw_db if Path(raw_db).is_absolute() else str((root / raw_db).resolve())
    init_db(db)
    repo = TimeSeriesRepository(db)

    d_iso = (args.date or "").strip() or date.today().isoformat()

    def _upsert(eid: int, dloc: str, h: str, a: str, af: int) -> None:
        repo.upsert_digest_api_football_map(eid, dloc, h, a, af)

    rep = run_bridge_for_date(
        db_path=db,
        local_date_iso=d_iso,
        api_key=s.api_football_key,
        daily_cap=s.api_football_fixtures_daily_cap,
        upsert_fn=_upsert,
    )
    print(json.dumps(rep, indent=2, ensure_ascii=False))
    if rep.get("digest_rows", 0) and rep.get("matched", 0) == 0 and not rep.get("note"):
        sys.exit(3)


if __name__ == "__main__":
    main()
