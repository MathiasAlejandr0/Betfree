"""Repository layer for snapshots and deduped alert persistence."""

from __future__ import annotations

import json
import sqlite3
from typing import Any

from src.storage.database import managed_connection


class TimeSeriesRepository:
    """SQLite repository used by the prediction pipeline."""

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path

    def save_fixture_snapshot(
        self,
        fixture_id: int,
        source_provider: str,
        home_team: str,
        away_team: str,
        fixture_payload: dict[str, Any],
    ) -> None:
        """Persist raw fixture payload snapshot."""
        with managed_connection(self._db_path) as conn:
            conn.execute(
                """
                INSERT INTO fixtures_snapshot (
                    fixture_id, source_provider, home_team, away_team, fixture_payload_json
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    fixture_id,
                    source_provider,
                    home_team,
                    away_team,
                    json.dumps(fixture_payload, ensure_ascii=True),
                ),
            )

    def save_odds_snapshot(self, fixture_id: int, market_odds: dict[str, float]) -> None:
        """Persist odds values by market."""
        with managed_connection(self._db_path) as conn:
            for market_key, odds_value in market_odds.items():
                conn.execute(
                    """
                    INSERT INTO odds_snapshot (fixture_id, market_key, odds_value)
                    VALUES (?, ?, ?)
                    """,
                    (fixture_id, market_key, odds_value),
                )

    def save_prediction_snapshot(
        self,
        fixture_id: int,
        market_key: str,
        probability: float,
        edge: float,
        ev: float,
        expected_home_goals: float,
        expected_away_goals: float,
    ) -> None:
        """Persist quantitative prediction result."""
        with managed_connection(self._db_path) as conn:
            conn.execute(
                """
                INSERT INTO predictions_snapshot (
                    fixture_id, market_key, probability, edge, ev, expected_home_goals, expected_away_goals
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    fixture_id,
                    market_key,
                    probability,
                    edge,
                    ev,
                    expected_home_goals,
                    expected_away_goals,
                ),
            )

    def save_alert_sent(
        self, fixture_id: int, market_key: str, odds_value: float, stake: float, message: str
    ) -> bool:
        """Insert sent alert if not duplicated by fixture+market."""
        with managed_connection(self._db_path) as conn:
            try:
                conn.execute(
                    """
                    INSERT INTO alerts_sent (fixture_id, market_key, odds_value, stake, message)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (fixture_id, market_key, odds_value, stake, message),
                )
                return True
            except sqlite3.IntegrityError:
                return False

