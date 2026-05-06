from datetime import date
from pathlib import Path

from src.predictor.pages_h2h_history import PagesH2HIndex


def test_h2h_arsenal_brentford():
    p = Path(__file__).resolve().parent / "fixtures" / "historical_minimal.csv"
    idx = PagesH2HIndex(str(p))
    out = idx.summarize(date(2024, 8, 20), "Arsenal", "Brentford", "E0", max_meetings=6)
    assert out["meetings"] == 2
    assert out["record_wdl_home"]["w"] >= 1
    assert len(out["recent"]) == 2
    assert out["recent"][0]["date_iso"] >= out["recent"][1]["date_iso"]
