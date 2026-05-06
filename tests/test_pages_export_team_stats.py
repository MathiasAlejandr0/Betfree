from pathlib import Path

from src.predictor.pages_export_team_stats import build_pages_team_stats, build_pages_team_stats_from_context
from src.predictor.digest_roll_context import DigestRollContext
from datetime import date


def test_build_pages_team_stats_from_csv(tmp_path):
    p = Path(__file__).resolve().parent / "fixtures" / "historical_minimal.csv"
    out = build_pages_team_stats(
        str(p),
        before_local_date_iso="2024-08-20",
        digest_slug="eng.1",
        home_team="Arsenal",
        away_team="Brentford",
        recent_matches=5,
    )
    assert out is not None
    assert out["home"]["name"] == "Arsenal"
    assert out["away"]["name"] == "Brentford"
    assert out["home"]["n_matches"] >= 1
    assert out["match_model_estimate"]["corners_total"] > 0


def test_avg_goals_cached_context():
    p = Path(__file__).resolve().parent / "fixtures" / "historical_minimal.csv"
    ctx = DigestRollContext.from_csv(str(p), before_day=date(2024, 8, 20))
    a = build_pages_team_stats_from_context(ctx, digest_slug="eng.1", home_team="Arsenal", away_team="Brentford")
    b = build_pages_team_stats_from_context(ctx, digest_slug="eng.1", home_team="Arsenal", away_team="Brentford")
    assert a["home"]["gf_avg"] == b["home"]["gf_avg"]
