"""Cliente mínimo api.football-data.org/v4 con caché en disco y cupo diario (UTC).

Uso típico (manual o job de ingesta):
  python -m src.data_engine.football_data_client --areas

Requiere FOOTBALL_DATA_TOKEN en .env. Ajusta FOOTBALL_DATA_DAILY_CAP al límite de tu plan.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import sys
import time
from pathlib import Path
from urllib.parse import urlencode

import requests

from src.config import bootstrap_dotenv, get_settings, repo_root
from src.data_engine.http_quota import DailyQuotaConfig, quota_current, quota_try_acquire

LOG = logging.getLogger(__name__)

API_BASE = "https://api.football-data.org/v4"


def _cache_path(cache_dir: Path, method: str, url: str) -> Path:
    h = hashlib.sha256(f"{method}\n{url}".encode("utf-8")).hexdigest()
    return cache_dir / f"{h}.json"


def football_data_request(
    path: str,
    *,
    query: dict[str, str] | None = None,
    token: str | None = None,
    daily_cap: int = 90,
    cache_ttl_seconds: int = 86400,
    timeout: float = 25.0,
) -> dict:
    """
    GET JSON bajo /v4/{path}. Respeta cupo diario (incrementa solo en cache-miss de red).
    """
    tok = (token or "").strip()
    if not tok:
        raise ValueError("football_data_request: token vacío (FOOTBALL_DATA_TOKEN)")
    path = path.lstrip("/")
    q = f"?{urlencode(query)}" if query else ""
    url = f"{API_BASE}/{path}{q}"
    root = repo_root()
    cache_dir = root / "data" / "cache" / "football_data"
    cache_dir.mkdir(parents=True, exist_ok=True)
    cfile = _cache_path(cache_dir, "GET", url)
    now = time.time()
    if cfile.is_file():
        try:
            wrap = json.loads(cfile.read_text(encoding="utf-8"))
            ts = float(wrap.get("_cached_at", 0))
            if now - ts <= cache_ttl_seconds:
                return wrap.get("body") if isinstance(wrap.get("body"), dict) else {}
        except (OSError, json.JSONDecodeError, TypeError, ValueError):
            pass

    state_path = root / "data" / ".http_daily_quota.json"
    cfg = DailyQuotaConfig(state_path=state_path, namespace="football_data", limit=max(0, int(daily_cap)))
    if not quota_try_acquire(cfg):
        day, used, lim = quota_current(cfg)
        raise RuntimeError(f"football-data: cupo diario agotado (UTC {day}: {used}/{lim})")

    r = requests.get(
        url,
        headers={"X-Auth-Token": tok, "User-Agent": "Betfree/1.0"},
        timeout=timeout,
    )
    if r.status_code == 429:
        LOG.error("football-data 429: demasiadas peticiones; revisa FOOTBALL_DATA_DAILY_CAP o espaciado.")
    r.raise_for_status()
    body = r.json()
    if not isinstance(body, dict):
        body = {"_raw": body}
    cfile.write_text(
        json.dumps({"_cached_at": now, "body": body}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return body


def main() -> None:
    bootstrap_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
    p = argparse.ArgumentParser(description="Prueba Football-Data.org v4 (cupo + caché)")
    p.add_argument("--areas", action="store_true", help="GET /areas")
    p.add_argument("--competitions", action="store_true", help="GET /competitions (sin filtro)")
    p.add_argument("--ttl", type=int, default=86400, help="TTL caché segundos")
    args = p.parse_args()
    s = get_settings()
    if not s.football_data_token:
        print("FOOTBALL_DATA_TOKEN no configurado.", file=sys.stderr)
        sys.exit(2)
    cap = max(0, s.football_data_daily_cap)
    if args.areas:
        out = football_data_request("areas", token=s.football_data_token, daily_cap=cap, cache_ttl_seconds=args.ttl)
        print(json.dumps(out, indent=2, ensure_ascii=False)[:4000])
        return
    if args.competitions:
        out = football_data_request("competitions", token=s.football_data_token, daily_cap=cap, cache_ttl_seconds=args.ttl)
        print(json.dumps(out, indent=2, ensure_ascii=False)[:4000])
        return
    p.print_help()


if __name__ == "__main__":
    main()
