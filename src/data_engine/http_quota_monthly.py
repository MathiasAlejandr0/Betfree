"""Cupo mensual UTC (persistente) para APIs con límite por mes (ej. The Odds API free)."""

from __future__ import annotations

import json
import logging
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

LOG = logging.getLogger(__name__)
_lock = threading.Lock()


def _utc_month() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m")


@dataclass(frozen=True)
class MonthlyQuotaConfig:
    state_path: Path
    namespace: str
    limit: int


def monthly_quota_try_acquire(cfg: MonthlyQuotaConfig) -> bool:
    if cfg.limit <= 0:
        return False
    cfg.state_path.parent.mkdir(parents=True, exist_ok=True)
    with _lock:
        data: dict = {}
        if cfg.state_path.is_file():
            try:
                data = json.loads(cfg.state_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                data = {}
        month = _utc_month()
        block = data.get(cfg.namespace) or {}
        if block.get("month") != month:
            block = {"month": month, "count": 0}
        n = int(block.get("count", 0))
        if n >= cfg.limit:
            LOG.warning("Cupo mensual agotado (%s): %s/%s (UTC %s)", cfg.namespace, n, cfg.limit, month)
            return False
        block["count"] = n + 1
        data[cfg.namespace] = block
        cfg.state_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
        return True
