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

    @property
    def db_path(self) -> str:
        return self._db_path

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
        """Persist odds values by market. Usá sufijos `:open` / `:closing` en las claves para línea inicial vs cierre (ver odds_resolution)."""
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

    def count_unresolved_health_alerts(self, day_utc: str, model_used: str) -> int:
        with managed_connection(self._db_path) as conn:
            row = conn.execute(
                """
                SELECT COUNT(*) AS c FROM model_health_alerts
                WHERE resolved = 0 AND day_utc = ? AND model_used = ?
                """,
                (day_utc, model_used),
            ).fetchone()
            return int(row["c"] or 0)

    def insert_model_health_alert(
        self,
        *,
        day_utc: str,
        model_used: str,
        severity: str,
        roi: float | None,
    ) -> None:
        with managed_connection(self._db_path) as conn:
            conn.execute(
                """
                INSERT INTO model_health_alerts (day_utc, model_used, severity, resolved, roi)
                VALUES (?, ?, ?, 0, ?)
                """,
                (day_utc, model_used, severity, roi),
            )

    def resolve_model_health_alert_by_id(self, alert_id: int) -> int:
        """Marca resolved=1. Devuelve filas afectadas (0 o 1)."""
        with managed_connection(self._db_path) as conn:
            cur = conn.execute(
                "UPDATE model_health_alerts SET resolved = 1 WHERE id = ? AND resolved = 0",
                (int(alert_id),),
            )
            return int(cur.rowcount or 0)

    def upsert_digest_api_football_map(
        self,
        digest_event_id: int,
        local_date_iso: str,
        home_team: str,
        away_team: str,
        api_football_fixture_id: int,
    ) -> None:
        """Mapea event_id del digest → fixture id API-Football (cuotas / fixtures v3)."""
        with managed_connection(self._db_path) as conn:
            conn.execute(
                """
                INSERT INTO digest_api_football_fixture_map (
                    digest_event_id, local_date_iso, home_team, away_team, api_football_fixture_id
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(digest_event_id) DO UPDATE SET
                    local_date_iso = excluded.local_date_iso,
                    home_team = excluded.home_team,
                    away_team = excluded.away_team,
                    api_football_fixture_id = excluded.api_football_fixture_id,
                    ts_utc = CURRENT_TIMESTAMP
                """,
                (
                    int(digest_event_id),
                    local_date_iso,
                    home_team,
                    away_team,
                    int(api_football_fixture_id),
                ),
            )

    def list_af_map_for_date(self, local_date_iso: str) -> list[tuple[int, int]]:
        """Devuelve (digest_event_id, api_football_fixture_id) para la fecha local."""
        with managed_connection(self._db_path) as conn:
            cur = conn.execute(
                """
                SELECT digest_event_id, api_football_fixture_id
                FROM digest_api_football_fixture_map
                WHERE local_date_iso = ?
                ORDER BY digest_event_id
                """,
                (local_date_iso,),
            )
            return [(int(r["digest_event_id"]), int(r["api_football_fixture_id"])) for r in cur.fetchall()]

    def resolve_model_health_alerts_open(self, *, model_used: str | None = None) -> int:
        """Marca todas las alertas abiertas; opcionalmente filtra por model_used."""
        with managed_connection(self._db_path) as conn:
            if model_used:
                cur = conn.execute(
                    "UPDATE model_health_alerts SET resolved = 1 WHERE resolved = 0 AND model_used = ?",
                    ((model_used or "").strip(),),
                )
            else:
                cur = conn.execute("UPDATE model_health_alerts SET resolved = 1 WHERE resolved = 0")
            return int(cur.rowcount or 0)

    def save_digest_prediction_audit(
        self,
        *,
        event_id: int,
        digest_slug: str,
        local_date_iso: str,
        home_team: str,
        away_team: str,
        ph: float,
        pd: float,
        pa: float,
        used_ml: bool,
        blend_ml_w: float,
    ) -> None:
        """Una fila por partido para evaluación post-digest (`digest_live_evaluation`)."""
        with managed_connection(self._db_path) as conn:
            conn.execute(
                """
                INSERT INTO digest_prediction_audit (
                    event_id, digest_slug, local_date_iso, home_team, away_team,
                    ph, pd, pa, used_ml, blend_ml_w
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(event_id),
                    (digest_slug or "").strip(),
                    local_date_iso,
                    home_team,
                    away_team,
                    float(ph),
                    float(pd),
                    float(pa),
                    1 if used_ml else 0,
                    float(blend_ml_w),
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

