"""Persistencia ligera para menú Telegram: callback_data sólo lleva ID (Telegram máx 64 bytes)."""

from __future__ import annotations

import json
import secrets
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.config import repo_root

MENU_CALLBACK_PREFIX = "bf"
_PAYLOAD_VERSION = 1
_MAX_AGE_DAYS_CLEANUP = 7


def _cache_dir() -> Path:
    p = repo_root() / "data" / "cache" / "telegram_menu"
    p.mkdir(parents=True, exist_ok=True)
    return p


def _prune_old() -> None:
    import time

    cutoff = time.time() - (_MAX_AGE_DAYS_CLEANUP * 86400)
    d = _cache_dir()
    for f in d.glob("*.json"):
        try:
            if f.stat().st_mtime < cutoff:
                f.unlink(missing_ok=True)
        except OSError:
            pass


@dataclass(frozen=True)
class DigestMenuPayload:
    chat_id: str
    date_iso: str
    sections: dict[str, dict[str, Any]]  # sec -> {header, cont?, blocks: list[str]}


def save_digest_menu_payload(payload: DigestMenuPayload) -> str:
    _prune_old()
    menu_id = secrets.token_hex(5)
    path = _cache_dir() / f"{menu_id}.json"
    path.write_text(
        json.dumps(
            {
                "v": _PAYLOAD_VERSION,
                "chat_id": payload.chat_id,
                "date_iso": payload.date_iso,
                "sections": payload.sections,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return menu_id


def load_digest_menu_payload(menu_id: str) -> DigestMenuPayload | None:
    raw = (menu_id or "").strip()
    if not raw or any(c not in "0123456789abcdefABCDEF" for c in raw):
        return None
    path = _cache_dir() / f"{raw.lower()}.json"
    if not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None
    if int(data.get("v", 0)) != _PAYLOAD_VERSION:
        return None
    sections = data.get("sections")
    if not isinstance(sections, dict):
        return None
    return DigestMenuPayload(
        chat_id=str(data.get("chat_id", "")),
        date_iso=str(data.get("date_iso", "")),
        sections={str(k): v for k, v in sections.items() if isinstance(v, dict)},
    )


def format_callback(menu_id: str, section: str) -> str:
    """bf|{menu_id}|{section} — mantener bajo 64 caracteres."""
    return f"{MENU_CALLBACK_PREFIX}|{menu_id}|{section}"


def parse_callback(data: str) -> tuple[str, str] | None:
    if not data or not data.startswith(f"{MENU_CALLBACK_PREFIX}|"):
        return None
    parts = data.split("|", 2)
    if len(parts) != 3:
        return None
    return parts[1], parts[2]
