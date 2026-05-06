import tempfile
from pathlib import Path

from src.storage.database import init_db, managed_connection


def test_init_db_creates_digest_audit_and_settlements() -> None:
    with tempfile.TemporaryDirectory() as td:
        db = str(Path(td) / "t.db")
        init_db(db)
        with managed_connection(db) as conn:
            conn.execute("INSERT INTO digest_prediction_audit (event_id, digest_slug, local_date_iso, home_team, away_team, ph, pd, pa, used_ml, blend_ml_w) VALUES (1,'eng.1','2024-08-10','A','B',0.5,0.25,0.25,0,0)")
            conn.execute(
                "INSERT INTO alert_settlements (fixture_id, market_key, stake, odds_value, won, pnl) VALUES (1,'1X2',1,2.0,1,1.0)"
            )
        with managed_connection(db) as conn:
            n = conn.execute("SELECT COUNT(*) AS c FROM digest_prediction_audit").fetchone()["c"]
            assert int(n) == 1
