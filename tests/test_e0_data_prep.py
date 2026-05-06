from pathlib import Path

import pytest

from src.predictor.e0_data_prep import load_clean_e0

FIXTURE_CSV = Path(__file__).resolve().parent / "fixtures" / "historical_minimal.csv"


def test_load_clean_e0_minimal() -> None:
    df, audit = load_clean_e0(FIXTURE_CSV)
    assert len(df) == 2
    assert audit["rows_competition"] == 2


def test_load_clean_e0_missing_column() -> None:
    import pandas as pd
    import tempfile

    bad = pd.DataFrame({"date": ["2024-01-01"], "competition": ["E0"]})
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="w", encoding="utf-8") as f:
        bad.to_csv(f.name, index=False)
        path = f.name
    with pytest.raises(ValueError, match="Faltan columnas"):
        load_clean_e0(path)
    Path(path).unlink(missing_ok=True)
