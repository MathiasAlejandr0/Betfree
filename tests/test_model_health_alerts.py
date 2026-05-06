import tempfile
from pathlib import Path

from src.predictor.digest_live_evaluation import MODEL_USED_DIGEST_LIVE_EVAL, record_digest_eval_breach_alert
from src.storage.database import init_db, managed_connection
from src.storage.repository import TimeSeriesRepository


def test_insert_and_count_health_alert() -> None:
    with tempfile.TemporaryDirectory() as td:
        db = str(Path(td) / "x.db")
        init_db(db)
        repo = TimeSeriesRepository(db)
        assert repo.count_unresolved_health_alerts("2026-05-01", MODEL_USED_DIGEST_LIVE_EVAL) == 0
        repo.insert_model_health_alert(
            day_utc="2026-05-01",
            model_used=MODEL_USED_DIGEST_LIVE_EVAL,
            severity="warning",
            roi=1.09,
        )
        assert repo.count_unresolved_health_alerts("2026-05-01", MODEL_USED_DIGEST_LIVE_EVAL) == 1


def test_record_breach_dedupes_same_day() -> None:
    with tempfile.TemporaryDirectory() as td:
        db = str(Path(td) / "x.db")
        init_db(db)
        repo = TimeSeriesRepository(db)
        report = {"status": "breach", "metrics": {"multiclass_log_loss": 1.5}, "thresholds": {"warn_log_loss": 1.08}}
        record_digest_eval_breach_alert(repo, report, enabled=True)
        record_digest_eval_breach_alert(repo, report, enabled=True)
        with managed_connection(db) as conn:
            n = conn.execute(
                "SELECT COUNT(*) AS c FROM model_health_alerts WHERE model_used = ?",
                (MODEL_USED_DIGEST_LIVE_EVAL,),
            ).fetchone()["c"]
        assert int(n) == 1


def test_resolve_alert_by_id() -> None:
    with tempfile.TemporaryDirectory() as td:
        db = str(Path(td) / "x.db")
        init_db(db)
        repo = TimeSeriesRepository(db)
        repo.insert_model_health_alert(
            day_utc="2026-06-01",
            model_used=MODEL_USED_DIGEST_LIVE_EVAL,
            severity="warning",
            roi=1.1,
        )
        with managed_connection(db) as conn:
            aid = int(conn.execute("SELECT id FROM model_health_alerts LIMIT 1").fetchone()["id"])
        assert repo.resolve_model_health_alert_by_id(aid) == 1
        assert repo.resolve_model_health_alert_by_id(aid) == 0
        with managed_connection(db) as conn:
            r = int(conn.execute("SELECT resolved FROM model_health_alerts WHERE id=?", (aid,)).fetchone()["resolved"])
        assert r == 1
