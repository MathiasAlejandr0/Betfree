"""API-Football GET /fixtures (por fecha) con caché y cupo diario UTC."""

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


def api_football_fixtures_get_by_date(
    date_iso: str,
    *,
    api_key: str,
    daily_cap: int = 25,
    cache_ttl_seconds: int = 7200,
    timeout: float = 35.0,
) -> dict:
    """date_iso = YYYY-MM-DD (API). Respuesta típica con lista `response`."""
    key = (api_key or "").strip()
    if not key:
        raise ValueError("API_FOOTBALL_KEY vacío")
    q = urlencode({"date": date_iso.strip()})
    url = f"{BASE}/fixtures?{q}"

    root = repo_root()
    cache_dir = root / "data" / "cache" / "api_football"
    cache_dir.mkdir(parents=True, exist_ok=True)
    h = hashlib.sha256(f"GET\n{url}".encode("utf-8")).hexdigest()
    cfile = cache_dir / f"fixtures_{h}.json"
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
    cfg = DailyQuotaConfig(state_path=state_path, namespace="api_football_fixtures", limit=max(0, int(daily_cap)))
    if not quota_try_acquire(cfg):
        raise RuntimeError("Cupo diario API-Football fixtures agotado (UTC).")

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


def default_fixtures_daily_cap() -> int:
    return max(0, int(os.getenv("API_FOOTBALL_FIXTURES_DAILY_CAP", "25")))
