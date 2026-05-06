"""Cuotas 1X2 :closing para todos los partidos mapeados digest↔API-Football en una fecha."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import date
from pathlib import Path

from src.config import bootstrap_dotenv, get_settings, repo_root
from src.data_engine.api_football_odds_client import fetch_odds_by_fixture
from src.data_engine.api_football_odds_extract import extract_first_bookmaker_1x2_decimals
from src.data_engine.odds_resolution import market_key_1x2
from src.storage.database import init_db
from src.storage.repository import TimeSeriesRepository

LOG = logging.getLogger(__name__)


def main() -> None:
    bootstrap_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
    p = argparse.ArgumentParser(
        description="GET /odds por api_football_fixture_id y guarda 1X2*:closing bajo digest_event_id",
    )
    p.add_argument("--date", default="", help="local_date_iso en el mapa (default: hoy)")
    p.add_argument("--db-path", default="")
    p.add_argument("--daily-cap-odds", type=int, default=int(os.getenv("API_FOOTBALL_ODDS_DAILY_CAP", "30")))
    p.add_argument("--dry-run", action="store_true")
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
    pairs = repo.list_af_map_for_date(d_iso)
    if not pairs:
        print(json.dumps({"date": d_iso, "ok": 0, "skipped": 0, "note": "Sin mapas para esa fecha."}, indent=2))
        sys.exit(0)

    kh, kd, ka = market_key_1x2("closing")
    ok = skip = 0
    summary: list[dict[str, int | str]] = []
    for digest_eid, af_fid in pairs:
        try:
            body = fetch_odds_by_fixture(int(af_fid), api_key=s.api_football_key, daily_cap=int(args.daily_cap_odds))
            trip = extract_first_bookmaker_1x2_decimals(body)
            if trip is None:
                LOG.info("sin 1X2 digest_event_id=%s af_fixture=%s", digest_eid, af_fid)
                skip += 1
                summary.append({"digest_event_id": digest_eid, "api_football_fixture_id": af_fid, "status": "no_odds"})
                continue
            markets = {kh: trip[0], kd: trip[1], ka: trip[2]}
            if not args.dry_run:
                repo.save_odds_snapshot(int(digest_eid), markets)
            ok += 1
            summary.append({"digest_event_id": digest_eid, "api_football_fixture_id": af_fid, "status": "saved"})
        except Exception as exc:
            LOG.warning("digest_event_id=%s af=%s: %s", digest_eid, af_fid, exc)
            skip += 1
            summary.append({"digest_event_id": digest_eid, "api_football_fixture_id": af_fid, "status": f"error:{exc}"})

    rep = {"date": d_iso, "ok": ok, "skipped": skip, "dry_run": args.dry_run, "items": summary[:80]}
    print(json.dumps(rep, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
