from src.data_engine.digest_af_fixture_bridge import DigestRow, match_digest_to_api_football


def test_match_single() -> None:
    body = {
        "response": [
            {
                "fixture": {"id": 999},
                "teams": {
                    "home": {"name": "Arsenal FC"},
                    "away": {"name": "Brentford"},
                },
            }
        ]
    }
    rows = [DigestRow(42, "Arsenal", "Brentford")]
    pairs, issues = match_digest_to_api_football(rows, body)
    assert pairs == [(42, 999)]
    assert not issues


def test_match_reversed_teams() -> None:
    body = {
        "response": [
            {
                "fixture": {"id": 1},
                "teams": {
                    "home": {"name": "Brentford"},
                    "away": {"name": "Arsenal"},
                },
            }
        ]
    }
    rows = [DigestRow(1, "Arsenal", "Brentford")]
    pairs, _ = match_digest_to_api_football(rows, body)
    assert pairs == [(1, 1)]


def test_ambiguous() -> None:
    dup = {
        "fixture": {"id": 10},
        "teams": {"home": {"name": "A"}, "away": {"name": "B"}},
    }
    body = {"response": [dup, {**dup, "fixture": {"id": 11}}]}
    rows = [DigestRow(5, "A", "B")]
    pairs, issues = match_digest_to_api_football(rows, body)
    assert pairs == []
    assert any(i.get("type") == "ambiguous" for i in issues)
