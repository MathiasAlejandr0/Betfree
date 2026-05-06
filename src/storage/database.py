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

            CREATE TABLE IF NOT EXISTS alert_settlements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts_utc TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                fixture_id INTEGER NOT NULL,
                market_key TEXT NOT NULL,
                stake REAL NOT NULL,
                odds_value REAL NOT NULL,
                won INTEGER NOT NULL,
                pnl REAL NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_alert_settlements_ts
            ON alert_settlements (ts_utc);

            CREATE TABLE IF NOT EXISTS model_health_alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts_utc TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                day_utc TEXT NOT NULL,
                model_used TEXT NOT NULL DEFAULT '',
                severity TEXT NOT NULL,
                resolved INTEGER NOT NULL DEFAULT 0,
                roi REAL
            );

            CREATE INDEX IF NOT EXISTS idx_model_health_alerts_resolved
            ON model_health_alerts (resolved, id);

            CREATE TABLE IF NOT EXISTS digest_prediction_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts_utc TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                event_id INTEGER NOT NULL,
                digest_slug TEXT,
                local_date_iso TEXT NOT NULL,
                home_team TEXT NOT NULL,
                away_team TEXT NOT NULL,
                ph REAL NOT NULL,
                pd REAL NOT NULL,
                pa REAL NOT NULL,
                used_ml INTEGER NOT NULL DEFAULT 0,
                blend_ml_w REAL NOT NULL DEFAULT 0.0
            );

            CREATE INDEX IF NOT EXISTS idx_digest_pred_audit_date_teams
            ON digest_prediction_audit (local_date_iso, home_team, away_team);

            CREATE INDEX IF NOT EXISTS idx_digest_pred_audit_event
            ON digest_prediction_audit (event_id);

            CREATE TABLE IF NOT EXISTS digest_api_football_fixture_map (
                digest_event_id INTEGER NOT NULL,
                local_date_iso TEXT NOT NULL,
                home_team TEXT NOT NULL,
                away_team TEXT NOT NULL,
                api_football_fixture_id INTEGER NOT NULL,
                ts_utc TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (digest_event_id)
            );

            CREATE INDEX IF NOT EXISTS idx_digest_af_map_date
            ON digest_api_football_fixture_map (local_date_iso);

            CREATE INDEX IF NOT EXISTS idx_digest_af_map_af
            ON digest_api_football_fixture_map (api_football_fixture_id);
            """
        )

