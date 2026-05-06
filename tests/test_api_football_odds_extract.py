from src.data_engine.api_football_odds_extract import extract_first_bookmaker_1x2_decimals


def test_extract_match_winner() -> None:
    body = {
        "response": [
            {
                "bookmakers": [
                    {
                        "bets": [
                            {
                                "name": "Match Winner",
                                "values": [
                                    {"value": "Home", "odd": "2.0"},
                                    {"value": "Draw", "odd": "3.5"},
                                    {"value": "Away", "odd": "4.2"},
                                ],
                            }
                        ]
                    }
                ]
            }
        ]
    }
    t = extract_first_bookmaker_1x2_decimals(body)
    assert t == (2.0, 3.5, 4.2)


def test_extract_1x2_labels() -> None:
    body = {
        "response": [
            {
                "bookmakers": [
                    {
                        "bets": [
                            {
                                "name": "1X2",
                                "values": [
                                    {"value": "1", "odd": "1.9"},
                                    {"value": "X", "odd": "3.4"},
                                    {"value": "2", "odd": "5.0"},
                                ],
                            }
                        ]
                    }
                ]
            }
        ]
    }
    t = extract_first_bookmaker_1x2_decimals(body)
    assert t is not None
    assert abs(t[0] - 1.9) < 1e-9


def test_extract_empty() -> None:
    assert extract_first_bookmaker_1x2_decimals({"response": []}) is None
