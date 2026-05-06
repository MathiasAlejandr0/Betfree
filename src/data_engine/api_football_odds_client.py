"""
API-Football (api-sports.io) — endpoint de cuotas por fixture.

  API_FOOTBALL_KEY en .env (cabecera x-apisports-key)
  API_FOOTBALL_ODDS_DAILY_CAP — default 30 (cupo diario UTC, además de caché)

Documentación: https://www.api-football.com/documentation-v3#tag/Odds
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
from src.data_engine.http_quota import DailyQuotaConfig, quota_try_acquire

LOG = logging.getLogger(__name__)

BASE = "https://v3.football.api-sports.io"


def api_football_odds_get(
    path: str,
    *,
    api_key: str,
    query: dict[str, str | int] | None = None,
    daily_cap: int = 30,
    cache_ttl_seconds: int = 3600,
    timeout: float = 25.0,
) -> dict:
    key = (api_key or "").strip()
    if not key:
        raise ValueError("API_FOOTBALL_KEY vacío")
    path = path.lstrip("/")
    q = urlencode({k: str(v) for k, v in (query or {}).items()})
    url = f"{BASE}/{path}?{q}" if q else f"{BASE}/{path}"

    root = repo_root()
    cache_dir = root / "data" / "cache" / "api_football"
    cache_dir.mkdir(parents=True, exist_ok=True)
    h = hashlib.sha256(f"GET\n{url}".encode("utf-8")).hexdigest()
    cfile = cache_dir / f"{h}.json"
    now = time.time()
    if cfile.is_file():
        try:
            wrap = json.loads(cfile.read_text(encoding="utf-8"))
            ts = float(wrap.get("_cached_at", 0))
            if now - ts <= cache_ttl_seconds:
                b = wrap.get("body")
                return b if isinstance(b, dict) else {}
        except (OSError, json.JSONDecodeError, TypeError, ValueError):
            pass

    state_path = root / "data" / ".http_daily_quota.json"
    cfg = DailyQuotaConfig(state_path=state_path, namespace="api_football_odds", limit=max(0, int(daily_cap)))
    if not quota_try_acquire(cfg):
        raise RuntimeError("Cupo diario API-Football odds agotado (UTC).")

    r = requests.get(url, headers={"x-apisports-key": key, "User-Agent": "Betfree/1.0"}, timeout=timeout)
    r.raise_for_status()
    body = r.json()
    if not isinstance(body, dict):
        body = {"_raw": body}
    cfile.write_text(
        json.dumps({"_cached_at": now, "body": body}, ensure_ascii=False),
        encoding="utf-8",
    )
    return body


def fetch_odds_by_fixture(fixture_id: int, *, api_key: str, daily_cap: int = 30) -> dict:
    return api_football_odds_get("odds", api_key=api_key, query={"fixture": int(fixture_id)}, daily_cap=daily_cap)
