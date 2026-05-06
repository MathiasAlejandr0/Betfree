import tempfile
from pathlib import Path

import pytest

from src.predictor.digest_live_evaluation import build_historical_lookup, evaluate_digest_vs_historical
from src.storage.database import init_db, managed_connection


@pytest.fixture()
def tiny_db_and_csv(tmp_path: Path) -> tuple[str, Path]:
    csv = tmp_path / "hist.csv"
    csv.write_text(
        "date,competition,home_team,away_team,home_goals,away_goals,result_1x2\n"
        "2024-08-10,E0,Arsenal,Brentford,2,0,H\n",
        encoding="utf-8",
    )
    db = str(tmp_path / "db.sqlite")
    init_db(db)
    with managed_connection(db) as conn:
        conn.execute(
            "INSERT INTO digest_prediction_audit (event_id, digest_slug, local_date_iso, home_team, away_team, ph, pd, pa, used_ml, blend_ml_w) "
            "VALUES (99, 'eng.1', '2024-08-10', 'Arsenal', 'Brentford', 0.7, 0.2, 0.1, 1, 0.72)"
        )
    return db, csv


def test_build_historical_lookup(tiny_db_and_csv: tuple[str, Path]) -> None:
    _, csv = tiny_db_and_csv
    lu = build_historical_lookup(csv)
    assert lu[("2024-08-10", "arsenal", "brentford")] == 0


def test_evaluate_digest_matched(tiny_db_and_csv: tuple[str, Path]) -> None:
    db, csv = tiny_db_and_csv
    rep = evaluate_digest_vs_historical(db_path=db, csv_path=csv, since_iso="2024-01-01", until_iso="2025-12-31")
    assert rep["matched"] == 1
    assert rep["metrics"]["n"] == 1
    assert rep["status"] == "ok"
