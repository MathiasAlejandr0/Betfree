"""CLI mínimo The Odds API (deportes / odds) con cupo mensual + caché."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys

from src.config import bootstrap_dotenv
from src.data_engine.the_odds_api_client import the_odds_api_get

LOG = logging.getLogger(__name__)


def main() -> None:
    bootstrap_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
    p = argparse.ArgumentParser(description="The Odds API (cupo mensual + caché)")
    p.add_argument("--sports", action="store_true", help="GET /sports")
    p.add_argument("--sport", type=str, default="soccer_epl", help="slug deporte p.ej. soccer_epl")
    p.add_argument("--regions", type=str, default="eu", help="regiones odds")
    p.add_argument("--markets", type=str, default="h2h", help="mercados")
    p.add_argument("--monthly-cap", type=int, default=int(os.getenv("THE_ODDS_API_MONTHLY_CAP", "450")))
    args = p.parse_args()

    key = os.getenv("THE_ODDS_API_KEY", "").strip()
    if not key:
        print("THE_ODDS_API_KEY no configurado.", file=sys.stderr)
        sys.exit(2)
    cap = max(0, int(args.monthly_cap))

    if args.sports:
        out = the_odds_api_get("sports", api_key=key, monthly_cap=cap, cache_ttl_seconds=86400)
        print(json.dumps(out, indent=2, ensure_ascii=False)[:8000])
        return

    path = f"sports/{args.sport.strip()}/odds"
    out = the_odds_api_get(
        path,
        api_key=key,
        query={"regions": args.regions.strip(), "markets": args.markets.strip()},
        monthly_cap=cap,
        cache_ttl_seconds=600,
    )
    print(json.dumps(out, indent=2, ensure_ascii=False)[:12000])


if __name__ == "__main__":
    main()
