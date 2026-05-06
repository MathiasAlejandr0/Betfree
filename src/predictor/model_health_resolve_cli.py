"""Marca filas de model_health_alerts como resueltas (resolved=1)."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from src.config import bootstrap_dotenv, get_settings, repo_root
from src.storage.repository import TimeSeriesRepository


def main() -> None:
    bootstrap_dotenv()
    p = argparse.ArgumentParser(description="Resolver alertas model_health_alerts")
    p.add_argument("--db-path", default="", help="Por defecto SQLITE_DB_PATH")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--id", type=int, help="PK de la alerta")
    g.add_argument("--all-open", action="store_true", help="Todas las alertas con resolved=0")
    p.add_argument(
        "--model-used",
        default="",
        help="Con --all-open: limitar a este model_used (ej. digest_live_evaluation)",
    )
    p.add_argument("--yes", action="store_true", help="Obligatorio con --all-open")
    args = p.parse_args()

    settings = get_settings()
    root = repo_root()
    raw = (args.db_path or "").strip() or settings.sqlite_db_path
    db = raw if Path(raw).is_absolute() else str((root / raw).resolve())
    repo = TimeSeriesRepository(db)

    if args.id is not None:
        n = repo.resolve_model_health_alert_by_id(int(args.id))
        print(f"actualizadas={n} (id={args.id})")
        sys.exit(0 if n else 4)

    if not args.yes:
        print("Refiná: --all-open requiere --yes para evitar borrados accidentales.", file=sys.stderr)
        sys.exit(2)
    mu = (args.model_used or "").strip() or None
    n = repo.resolve_model_health_alerts_open(model_used=mu)
    print(f"actualizadas={n}" + (f" model_used={mu!r}" if mu else ""))


if __name__ == "__main__":
    main()
