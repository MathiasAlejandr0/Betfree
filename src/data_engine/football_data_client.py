"""football-data.org client (v4) including GET /matches endpoint."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import time
from typing import Any

import requests
from requests.exceptions import RequestException

from src.config import Settings, get_settings


class FootballDataError(Exception):
    """Controlled football-data.org integration error."""


@dataclass
class FootballDataClient:
    """HTTP client for football-data.org v4 API."""

    settings: Settings
    session: requests.Session | None = None

    def __post_init__(self) -> None:
        self._session = self.session or requests.Session()
        self._session.headers.update({"X-Auth-Token": self.settings.football_data_token})
        self._last_request_ts: float = 0.0

    def _respect_rate_limit(self) -> None:
        elapsed = time.monotonic() - self._last_request_ts
        wait_time = self.settings.min_request_interval_seconds - elapsed
        if wait_time > 0:
            time.sleep(wait_time)

    def _request(self, endpoint: str, params: dict[str, Any]) -> dict[str, Any]:
        self._respect_rate_limit()
        url = f"{self.settings.football_data_base_url}/{endpoint}"
        try:
            response = self._session.get(
                url, params=params, timeout=self.settings.request_timeout_seconds
            )
            self._last_request_ts = time.monotonic()
            response.raise_for_status()
            return response.json()
        except RequestException as exc:
            raise FootballDataError(f"Connection error: {exc}") from exc
        except ValueError as exc:
            raise FootballDataError("Invalid JSON response.") from exc

    def get_matches(
        self,
        date_from: str | None = None,
        date_to: str | None = None,
        status: str | None = None,
        competitions: str | None = None,
    ) -> list[dict[str, Any]]:
        """Implements GET https://api.football-data.org/v4/matches."""
        params: dict[str, Any] = {}
        if date_from:
            params["dateFrom"] = date_from
        if date_to:
            params["dateTo"] = date_to
        if status:
            params["status"] = status
        if competitions:
            params["competitions"] = competitions
        payload = self._request("matches", params)
        return payload.get("matches", [])

    def get_today_matches(self) -> list[dict[str, Any]]:
        """Fetch today's matches from football-data.org."""
        today = date.today().isoformat()
        return self.get_matches(date_from=today, date_to=today)


def build_default_football_data_client() -> FootballDataClient:
    """Build football-data.org client from environment settings."""
    return FootballDataClient(settings=get_settings())

