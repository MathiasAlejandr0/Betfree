"""
Descarga CSV gratuitos de Football-Data.co.uk (cuotas + resultados).

  python -m src.data_engine.fd_co_uk_downloader --divs E0,SP1 --seasons 2425,2324

Cupo diario (UTC) vía FD_CO_UK_DOWNLOAD_DAILY_CAP (default 40) para no martillar el servidor.
Si el archivo ya existe, se omite (--force para sobrescribir).
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

import requests

from src.config import bootstrap_dotenv, repo_root
from src.data_engine.fd_co_uk_urls import fd_co_uk_csv_url
from src.data_engine.http_quota import DailyQuotaConfig, quota_try_acquire

LOG = logging.getLogger(__name__)

USER_AGENT = "Betfree/1.0 (+https://github.com) educational odds CSV fetch"


def download_fd_csv(
    *,
    season_folder: str,
    division: str,
    out_dir: Path,
    daily_cap: int,
    force: bool,
    min_interval_s: float = 0.45,
) -> Path | None:
    url = fd_co_uk_csv_url(season_folder=season_folder, division=division.upper())
    out_dir.mkdir(parents=True, exist_ok=True)
    dest = out_dir / f"{division.upper()}_{season_folder}.csv"
    if dest.is_file() and not force:
        LOG.info("Ya existe (omitir): %s", dest)
        return dest

    state_path = repo_root() / "data" / ".http_daily_quota.json"
    cfg = DailyQuotaConfig(state_path=state_path, namespace="fd_co_uk_csv", limit=max(0, int(daily_cap)))
    if not quota_try_acquire(cfg):
        raise RuntimeError("Cupo diario fd_co_uk_csv agotado (UTC). Mañana o sube FD_CO_UK_DOWNLOAD_DAILY_CAP.")

    r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=45.0)
    if r.status_code == 404:
        LOG.warning("404 %s", url)
        return None
    r.raise_for_status()
    dest.write_bytes(r.content)
    LOG.info("Guardado %s <- %s", dest, url)
    time.sleep(min_interval_s)
    return dest


def main() -> None:
    bootstrap_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
    import os

    p = argparse.ArgumentParser(description="Descarga CSV Football-Data.co.uk (gratis)")
    p.add_argument("--divs", type=str, default="E0,SP1", help="divisiones coma: E0,SP1,D1,...")
    p.add_argument("--seasons", type=str, default="2425,2324,2223", help="carpetas temporada: 2425,2324,...")
    p.add_argument("--out", type=str, default="data/open_datasets/fd_co_uk", help="directorio salida (relativo al repo)")
    p.add_argument("--force", action="store_true", help="sobrescribir CSV existentes")
    p.add_argument("--daily-cap", type=int, default=int(os.getenv("FD_CO_UK_DOWNLOAD_DAILY_CAP", "40")))
    args = p.parse_args()

    root = repo_root()
    out = Path(args.out.strip())
    if not out.is_absolute():
        out = root / out

    divs = [x.strip().upper() for x in args.divs.split(",") if x.strip()]
    seasons = [x.strip() for x in args.seasons.split(",") if x.strip()]
    if not divs or not seasons:
        print("Especifica --divs y --seasons", file=sys.stderr)
        sys.exit(2)

    ok = 0
    for sf in seasons:
        for d in divs:
            try:
                path = download_fd_csv(
                    season_folder=sf,
                    division=d,
                    out_dir=out / sf,
                    daily_cap=int(args.daily_cap),
                    force=bool(args.force),
                )
                if path:
                    ok += 1
            except Exception as exc:
                LOG.error("%s %s: %s", sf, d, exc)
                print(f"ERROR {sf} {d}: {exc}", file=sys.stderr)
    print(f"Listo: {ok} archivos bajo {out}")


if __name__ == "__main__":
    main()
