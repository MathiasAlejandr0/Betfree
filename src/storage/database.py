"""SQLite bootstrap and connection helpers."""

from __future__ import annotations

from contextlib import contextmanager
import sqlite3
from typing import Iterator


def get_connection(db_path: str) -> sqlite3.Connection:
    """Create SQLite connection with named-row access."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


@contextmanager
def managed_connection(db_path: str) -> Iterator[sqlite3.Connection]:
    """Managed connection with commit/rollback behavior."""
    conn = get_connection(db_path)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db(db_path: str) -> None:
    """Initialize tables for snapshots and sent alerts."""
    with managed_connection(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS fixtures_snapshot (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts_utc TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                fixture_id INTEGER NOT NULL,
                source_provider TEXT NOT NULL,
                home_team TEXT NOT NULL,
                away_team TEXT NOT NULL,
                fixture_payload_json TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_fixtures_snapshot_fixture_ts
            ON fixtures_snapshot (fixture_id, ts_utc);

            CREATE TABLE IF NOT EXISTS odds_snapshot (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts_utc TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                fixture_id INTEGER NOT NULL,
                market_key TEXT NOT NULL,
                odds_value REAL NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_odds_snapshot_fixture_market_ts
            ON odds_snapshot (fixture_id, market_key, ts_utc);

            CREATE TABLE IF NOT EXISTS predictions_snapshot (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts_utc TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                fixture_id INTEGER NOT NULL,
                market_key TEXT NOT NULL,
                probability REAL NOT NULL,
                edge REAL NOT NULL,
                ev REAL NOT NULL,
                expected_home_goals REAL NOT NULL,
                expected_away_goals REAL NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_predictions_snapshot_fixture_market_ts
            ON predictions_snapshot (fixture_id, market_key, ts_utc);

            CREATE TABLE IF NOT EXISTS alerts_sent (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts_utc TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                fixture_id INTEGER NOT NULL,
                market_key TEXT NOT NULL,
                odds_value REAL NOT NULL,
                stake REAL NOT NULL,
                message TEXT NOT NULL,
                UNIQUE (fixture_id, market_key)
            );
            """
        )

