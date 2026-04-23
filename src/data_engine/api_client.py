"""API-Football client with basic rate limiting."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import time
from typing import Any

import requests
from requests.exceptions import RequestException

from src.config import Settings, get_settings


class ApiFootballError(Exception):
    """Controlled API-Football integration error."""


@dataclass
class ApiFootballClient:
    """HTTP client for fixtures, H2H and odds endpoints."""

    settings: Settings
    session: requests.Session | None = None

    def __post_init__(self) -> None:
        self._session = self.session or requests.Session()
        self._session.headers.update(
            {
                "x-rapidapi-key": self.settings.api_football_key,
                "x-rapidapi-host": self.settings.api_football_host,
            }
        )
        self._last_request_ts: float = 0.0

    def _respect_rate_limit(self) -> None:
        elapsed = time.monotonic() - self._last_request_ts
        wait_time = self.settings.min_request_interval_seconds - elapsed
        if wait_time > 0:
            time.sleep(wait_time)

    def _request(self, endpoint: str, params: dict[str, Any]) -> dict[str, Any]:
        self._respect_rate_limit()
        url = f"{self.settings.api_football_base_url}/{endpoint}"
        try:
            response = self._session.get(
                url, params=params, timeout=self.settings.request_timeout_seconds
            )
            self._last_request_ts = time.monotonic()
            response.raise_for_status()
            payload = response.json()
        except RequestException as exc:
            raise ApiFootballError(f"Connection error: {exc}") from exc
        except ValueError as exc:
            raise ApiFootballError("Invalid JSON response.") from exc

        if payload.get("errors"):
            raise ApiFootballError(f"API returned errors: {payload['errors']}")
        return payload

    def get_today_fixtures(self, timezone: str = "America/Santiago") -> list[dict[str, Any]]:
        """Get today's fixtures."""
        payload = self._request(
            "fixtures", {"date": date.today().isoformat(), "timezone": timezone}
        )
        return payload.get("response", [])

    def get_h2h_stats(
        self, home_team_id: int, away_team_id: int, last_matches: int = 10
    ) -> list[dict[str, Any]]:
        """Get historical head-to-head fixtures."""
        payload = self._request(
            "fixtures/headtohead",
            {"h2h": f"{home_team_id}-{away_team_id}", "last": last_matches},
        )
        return payload.get("response", [])

    def get_current_odds(
        self,
        fixture_id: int | None = None,
        league_id: int | None = None,
        season: int | None = None,
    ) -> list[dict[str, Any]]:
        """Get current odds by fixture or league/season."""
        params: dict[str, Any] = {}
        if fixture_id is not None:
            params["fixture"] = fixture_id
        if league_id is not None:
            params["league"] = league_id
        if season is not None:
            params["season"] = season
        if not params:
            raise ValueError("Provide fixture_id or league_id/season to fetch odds.")
        payload = self._request("odds", params)
        return payload.get("response", [])


def build_default_api_football_client() -> ApiFootballClient:
    """Build API-Football client from environment settings."""
    return ApiFootballClient(settings=get_settings())

