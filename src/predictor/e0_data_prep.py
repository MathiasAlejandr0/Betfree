"""Limpieza y validacion de datos E0 antes del modelado."""

from __future__ import annotations

from pathlib import Path

import pandas as pd


def _result_from_goals(h: int, a: int) -> str:
    if h > a:
        return "H"
    if h < a:
        return "A"
    return "D"


def load_clean_e0(
    csv_path: str | Path,
    *,
    competition: str = "E0",
) -> tuple[pd.DataFrame, dict]:
    """
    Carga CSV robusto, filtra competicion, valida integridad.
    Devuelve (dataframe limpio, reporte de auditoria).
    """
    path = Path(csv_path)
    df = pd.read_csv(path)
    audit: dict = {"source": str(path.resolve()), "rows_raw": int(len(df))}

    required = {
        "date",
        "competition",
        "home_team",
        "away_team",
        "home_goals",
        "away_goals",
        "result_1x2",
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Faltan columnas: {missing}")

    d = df.loc[df["competition"].astype(str) == competition].copy()
    audit["rows_competition"] = int(len(d))

    d["home_team"] = d["home_team"].astype(str).str.strip()
    d["away_team"] = d["away_team"].astype(str).str.strip()
    d = d.loc[d["home_team"] != d["away_team"]].copy()
    audit["rows_after_drop_same_team"] = int(len(d))

    d["home_goals"] = pd.to_numeric(d["home_goals"], errors="coerce")
    d["away_goals"] = pd.to_numeric(d["away_goals"], errors="coerce")
    bad_goals = d["home_goals"].isna() | d["away_goals"].isna()
    d = d.loc[~bad_goals].copy()
    d["home_goals"] = d["home_goals"].astype(int)
    d["away_goals"] = d["away_goals"].astype(int)
    audit["rows_after_numeric_goals"] = int(len(d))

    d = d.loc[(d["home_goals"] >= 0) & (d["away_goals"] >= 0)].copy()
    d = d.loc[(d["home_goals"] <= 30) & (d["away_goals"] <= 30)].copy()
    audit["rows_after_goal_sanity"] = int(len(d))

    d["result_1x2"] = d["result_1x2"].astype(str).str.strip().str.upper()
    d = d.loc[d["result_1x2"].isin(["H", "D", "A"])].copy()
    exp = d.apply(lambda x: _result_from_goals(int(x.home_goals), int(x.away_goals)), axis=1)
    mismatch = d["result_1x2"].values != exp.values
    n_bad = int(mismatch.sum())
    audit["result_goal_mismatches_dropped"] = n_bad
    if n_bad:
        d = d.loc[~mismatch].copy()

    d["date"] = pd.to_datetime(d["date"], errors="coerce")
    d = d.loc[d["date"].notna()].copy()
    audit["rows_after_valid_dates"] = int(len(d))

    dup = d.duplicated(subset=["date", "home_team", "away_team"], keep="first")
    audit["duplicate_fixtures_dropped"] = int(dup.sum())
    d = d.loc[~dup].copy()

    d = d.sort_values(["date", "home_team", "away_team"]).reset_index(drop=True)
    audit["rows_final"] = int(len(d))
    audit["date_min"] = str(d["date"].min().date())
    audit["date_max"] = str(d["date"].max().date())

    counts = d["result_1x2"].value_counts()
    audit["class_distribution"] = {k: int(v) for k, v in counts.items()}

    return d, audit
