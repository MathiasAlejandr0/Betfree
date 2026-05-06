"""Cupo diario persistente (UTC) para acotar llamadas HTTP a APIs con límite por día."""

from __future__ import annotations

import json
import logging
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

LOG = logging.getLogger(__name__)

_lock = threading.Lock()


def _utc_day() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


@dataclass(frozen=True)
class DailyQuotaConfig:
    """namespace: clave lógica (ej. football_data). limit: máximo incrementos por día UTC."""

    state_path: Path
    namespace: str
    limit: int


def quota_try_acquire(cfg: DailyQuotaConfig) -> bool:
    """
    Si queda cupo para hoy (UTC), incrementa el contador y devuelve True.
    Si no hay cupo o limit<=0, devuelve False sin incrementar.
    """
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
        day = _utc_day()
        key = cfg.namespace
        block = data.get(key) or {}
        if block.get("day") != day:
            block = {"day": day, "count": 0}
        n = int(block.get("count", 0))
        if n >= cfg.limit:
            LOG.warning("Cupo diario agotado (%s): %s/%s (UTC %s)", key, n, cfg.limit, day)
            return False
        block["count"] = n + 1
        data[key] = block
        cfg.state_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
        return True


def quota_current(cfg: DailyQuotaConfig) -> tuple[str, int, int]:
    """(día UTC, usado, límite) sin modificar."""
    if cfg.limit <= 0:
        return _utc_day(), 0, cfg.limit
    data: dict = {}
    if cfg.state_path.is_file():
        try:
            data = json.loads(cfg.state_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            pass
        block = data.get(cfg.namespace) or {}
        day = _utc_day()
        if block.get("day") == day:
            return day, int(block.get("count", 0)), cfg.limit
    return _utc_day(), 0, cfg.limit
