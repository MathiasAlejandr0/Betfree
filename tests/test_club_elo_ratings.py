from datetime import date

from src.data_engine.club_elo_ratings import (
    ClubEloRanking,
    ClubEloRow,
    clubelo_country_hint_for_digest_slug,
    parse_clubelo_csv,
)


_SAMPLE = """Rank,Club,Country,Level,Elo,From,To
1,Liverpool,ENG,1,1900.0,2024-01-01,2024-06-01
2,Man United,ENG,1,1820.0,2024-01-01,2024-06-01
3,Sporting,POR,1,1720.0,2024-01-01,2024-06-01
4,Sporting,BRA,1,1580.0,2024-01-01,2024-06-01
"""


def test_parse_clubelo_csv():
    rows = parse_clubelo_csv(_SAMPLE)
    assert len(rows) >= 4
    assert rows[0].club == "Liverpool"


def test_resolve_exact_eng():
    rks = ClubEloRanking(parse_clubelo_csv(_SAMPLE), as_of=date(2024, 1, 15))
    out = rks.resolve("Liverpool FC", "ENG")
    assert out is not None
    row, kind = out
    assert row.club == "Liverpool"
    assert row.elo > 1890
    assert kind in ("exact", "exact_country", "fuzzy")


def test_country_disambiguates_sporting():
    rks = ClubEloRanking(parse_clubelo_csv(_SAMPLE), as_of=date(2024, 1, 15))
    out_pt = rks.resolve("Sporting CP", "POR")
    assert out_pt is not None
    assert out_pt[0].country == "POR"


def test_fuzzy_man_city_vs_generic():
    sample2 = (
        _SAMPLE
        + """5,Man City,ENG,1,1830.5,2024-01-01,2024-06-01
"""
    )
    rks = ClubEloRanking(parse_clubelo_csv(sample2), as_of=date(2024, 1, 15))
    out = rks.resolve("Manchester City", "ENG")
    assert out is not None
    assert abs(out[0].elo - 1830.5) < 0.02


def test_clubelo_hint_from_slug_eng():
    assert clubelo_country_hint_for_digest_slug("eng.1") == "ENG"
    assert clubelo_country_hint_for_digest_slug("conmebol.sudamericana") is None
