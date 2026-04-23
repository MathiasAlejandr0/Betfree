"""Centralized configuration from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    """Immutable runtime settings and secrets."""

    api_football_key: str
    api_football_host: str = "api-football-v1.p.rapidapi.com"
    api_football_base_url: str = "https://api-football-v1.p.rapidapi.com/v3"
    football_data_token: str = ""
    football_data_base_url: str = "https://api.football-data.org/v4"
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""
    sqlite_db_path: str = "betfree.db"
    request_timeout_seconds: int = 20
    min_request_interval_seconds: float = 1.2


def _get_required_env(name: str) -> str:
    """Read mandatory env var with explicit error."""
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def get_settings() -> Settings:
    """Build settings from environment variables."""
    return Settings(
        api_football_key=os.getenv("API_FOOTBALL_KEY", "").strip(),
        football_data_token=os.getenv("FOOTBALL_DATA_TOKEN", "").strip(),
        telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", "").strip(),
        telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID", "").strip(),
        sqlite_db_path=os.getenv("SQLITE_DB_PATH", "betfree.db").strip() or "betfree.db",
        request_timeout_seconds=int(os.getenv("REQUEST_TIMEOUT_SECONDS", "20")),
        min_request_interval_seconds=float(
            os.getenv("MIN_REQUEST_INTERVAL_SECONDS", "1.2")
        ),
    )


def validate_provider_token(settings: Settings, provider: str) -> None:
    """Validate required token according to selected API provider."""
    provider_l = provider.strip().lower()
    if provider_l == "api-football" and not settings.api_football_key:
        raise RuntimeError("API_FOOTBALL_KEY is required for provider=api-football")
    if provider_l == "football-data" and not settings.football_data_token:
        raise RuntimeError("FOOTBALL_DATA_TOKEN is required for provider=football-data")

