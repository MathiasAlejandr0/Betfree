"""Reporte CLI de alertas de salud del modelo."""

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

from src.config import repo_root


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Model health alerts report")
    parser.add_argument("--db-path", default="betfree.db")
    parser.add_argument("--only-open", action="store_true")
    parser.add_argument("--limit", type=int, default=50)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    raw = (args.db_path or "").strip() or "betfree.db"
    db = raw if Path(raw).is_absolute() else str((repo_root() / raw).resolve())
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    where = "WHERE resolved = 0" if args.only_open else ""
    rows = conn.execute(
        f"""
        SELECT id, ts_utc, day_utc, model_used, severity, resolved, roi
        FROM model_health_alerts
        {where}
        ORDER BY id DESC
        LIMIT ?
        """,
        (args.limit,),
    ).fetchall()
    conn.close()
    print(json.dumps([dict(r) for r in rows], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
