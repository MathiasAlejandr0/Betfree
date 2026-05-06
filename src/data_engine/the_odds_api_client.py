"""
The Odds API (https://the-odds-api.com/) — plan gratuito con cupo mensual.

Variables:
  THE_ODDS_API_KEY   — obligatorio para llamadas
  THE_ODDS_API_MONTHLY_CAP — default 450 (dejar margen bajo 500/mes del plan free típico)

Caché en disco (TTL corto) para no repetir la misma URL.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from pathlib import Path
from urllib.parse import urlencode

import requests

from src.config import repo_root
from src.data_engine.http_quota_monthly import MonthlyQuotaConfig, monthly_quota_try_acquire

LOG = logging.getLogger(__name__)

API = "https://api.the-odds-api.com/v4"


def the_odds_api_get(
    path: str,
    *,
    api_key: str,
    query: dict[str, str] | None = None,
    monthly_cap: int = 450,
    cache_ttl_seconds: int = 900,
    timeout: float = 25.0,
) -> dict | list:
    key = (api_key or "").strip()
    if not key:
        raise ValueError("THE_ODDS_API_KEY vacío")
    path = path.lstrip("/")
    q = dict(query or {})
    q["apiKey"] = key
    url = f"{API}/{path}?{urlencode(q)}"

    root = repo_root()
    cache_dir = root / "data" / "cache" / "the_odds_api"
    cache_dir.mkdir(parents=True, exist_ok=True)
    h = hashlib.sha256(url.encode("utf-8")).hexdigest()
    cfile = cache_dir / f"{h}.json"
    now = time.time()
    if cfile.is_file():
        try:
            wrap = json.loads(cfile.read_text(encoding="utf-8"))
            ts = float(wrap.get("_cached_at", 0))
            if now - ts <= cache_ttl_seconds:
                return wrap.get("body") if "body" in wrap else wrap
        except (OSError, json.JSONDecodeError, TypeError, ValueError):
            pass

    state_path = root / "data" / ".http_monthly_quota.json"
    cfg = MonthlyQuotaConfig(state_path=state_path, namespace="the_odds_api", limit=max(0, int(monthly_cap)))
    if not monthly_quota_try_acquire(cfg):
        raise RuntimeError("Cupo mensual The Odds API agotado (UTC). Espera al próximo mes o sube el plan.")

    r = requests.get(url, headers={"User-Agent": "Betfree/1.0"}, timeout=timeout)
    r.raise_for_status()
    body = r.json()
    cfile.write_text(json.dumps({"_cached_at": now, "body": body}, ensure_ascii=False), encoding="utf-8")
    return body
