"""Captura cuotas 1X2 vía API-Football y opcionalmente las guarda en SQLite con sufijo :closing."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from src.config import bootstrap_dotenv, get_settings, repo_root
from src.data_engine.api_football_odds_client import fetch_odds_by_fixture
from src.data_engine.api_football_odds_extract import extract_first_bookmaker_1x2_decimals
from src.data_engine.odds_resolution import market_key_1x2
from src.storage.database import init_db
from src.storage.repository import TimeSeriesRepository


def main() -> None:
    bootstrap_dotenv()
    p = argparse.ArgumentParser(description="Cuotas 1X2 de cierre (API-Football) → JSON o SQLite")
    p.add_argument("--fixture", type=int, required=True, help="ID de partido API-Football (no ESPN)")
    p.add_argument(
        "--digest-event-id",
        type=int,
        default=0,
        help="Si persistís: guardar filas bajo este fixture_id (event_id digest). Default: mismo que --fixture.",
    )
    p.add_argument("--daily-cap", type=int, default=int(os.getenv("API_FOOTBALL_ODDS_DAILY_CAP", "30")))
    p.add_argument("--persist-db", action="store_true", help="Guardar en odds_snapshot con claves ...:closing")
    p.add_argument("--db-path", default="", help="Por defecto SQLITE_DB_PATH del .env")
    args = p.parse_args()

    s = get_settings()
    if not s.api_football_key:
        print("API_FOOTBALL_KEY no configurado.", file=sys.stderr)
        sys.exit(2)

    body = fetch_odds_by_fixture(args.fixture, api_key=s.api_football_key, daily_cap=int(args.daily_cap))
    trip = extract_first_bookmaker_1x2_decimals(body)
    if trip is None:
        print(
            json.dumps(
                {"error": "no_1x2_odds", "hint": "La API devolvió vacío o sin mercado Match Winner/1X2 reconocible."},
                ensure_ascii=False,
            ),
            file=sys.stderr,
        )
        sys.exit(3)

    kh, kd, ka = market_key_1x2("closing")
    markets = {kh: trip[0], kd: trip[1], ka: trip[2]}
    out = {"fixture_id": int(args.fixture), "source": "api_football", "suffix": "closing", "markets": markets}
    print(json.dumps(out, indent=2, ensure_ascii=False))

    if args.persist_db:
        root = repo_root()
        raw_db = (args.db_path or "").strip() or s.sqlite_db_path
        db = raw_db if Path(raw_db).is_absolute() else str((root / raw_db).resolve())
        init_db(db)
        repo = TimeSeriesRepository(db)
        store_id = int(args.digest_event_id) if int(args.digest_event_id) > 0 else int(args.fixture)
        repo.save_odds_snapshot(store_id, markets)
        print(f"SQLite odds_snapshot fixture_id={store_id} → {db}", file=sys.stderr)


if __name__ == "__main__":
    main()
