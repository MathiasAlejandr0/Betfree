"""CLI: cuotas por fixture (API-Football) con cupo diario + caché."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys

from src.config import bootstrap_dotenv, get_settings
from src.data_engine.api_football_odds_client import fetch_odds_by_fixture


def main() -> None:
    bootstrap_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
    p = argparse.ArgumentParser(description="API-Football odds (fixture)")
    p.add_argument("--fixture", type=int, required=True)
    p.add_argument("--daily-cap", type=int, default=int(os.getenv("API_FOOTBALL_ODDS_DAILY_CAP", "30")))
    args = p.parse_args()
    s = get_settings()
    if not s.api_football_key:
        from src.config import repo_root

        odds_hint = ""
        if (os.getenv("THE_ODDS_API_KEY", "") or "").strip():
            odds_hint = (
                "\nNota: THE_ODDS_API_KEY es otro proveedor (the-odds-api.com).\n"
                "  Para eso usá: python -m src.data_engine.the_odds_api_cli --sport soccer_epl\n"
            )

        print(
            "API_FOOTBALL_KEY no configurado (clave distinta a The Odds API).\n"
            f"  1) Registro / clave: https://dashboard.api-football.com/\n"
            f"  2) En {repo_root() / '.env'}: API_FOOTBALL_KEY=tu_clave (no dejes la línea vacía)\n"
            "  (opcional) API_FOOTBALL_ODDS_DAILY_CAP=30"
            + odds_hint,
            file=sys.stderr,
        )
        sys.exit(2)
    out = fetch_odds_by_fixture(args.fixture, api_key=s.api_football_key, daily_cap=int(args.daily_cap))
    print(json.dumps(out, indent=2, ensure_ascii=False)[:12000])


if __name__ == "__main__":
    main()
