"""
Descarga CSV públicos Football-Data.co.uk (divisiones conocidas) y escribe un único CSV
tipo ``historical_robust`` para ejecutar digest/export en CI (sin commitear el histórico completo).

Uso:

  python scripts/ci_prepare_historical_merge.py
"""

from __future__ import annotations

import argparse
import sys
from io import BytesIO
from pathlib import Path

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.data_engine.fd_co_uk_urls import fd_co_uk_csv_url

UA = "Betfree-CI/1 (+https://github.com/MathiasAlejandr0/Betfree; educational)"

_DEFAULT_PAIRS = [("E0", "2425"), ("SP1", "2425")]


def fetch_div(div: str, season_folder: str) -> pd.DataFrame:
    url = fd_co_uk_csv_url(season_folder=season_folder, division=div)
    r = requests.get(url, headers={"User-Agent": UA}, timeout=75)
    r.raise_for_status()
    return pd.read_csv(BytesIO(r.content), encoding="latin-1", on_bad_lines="skip")


def normalize(df: pd.DataFrame, competition: str) -> pd.DataFrame:
    need_cols = {"Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG", "FTR"}
    miss = need_cols - set(df.columns)
    if miss:
        raise ValueError(f"{competition}: faltan columnas {sorted(miss)}")
    out = pd.DataFrame(
        {
            "date": pd.to_datetime(df["Date"], dayfirst=True, errors="coerce"),
            "competition": competition,
            "home_team": df["HomeTeam"].astype(str).str.strip(),
            "away_team": df["AwayTeam"].astype(str).str.strip(),
            "home_goals": pd.to_numeric(df["FTHG"], errors="coerce"),
            "away_goals": pd.to_numeric(df["FTAG"], errors="coerce"),
            "result_1x2": df["FTR"].astype(str).str.strip().str.upper(),
        }
    )
    out = out.loc[out["date"].notna() & out["home_goals"].notna() & out["away_goals"].notna()].copy()
    return out


def main() -> None:
    p = argparse.ArgumentParser(description="Merge FD Co UK CSVs for CI digest")
    p.add_argument(
        "--out",
        type=str,
        default=str(ROOT / "data/open_datasets/historical_robust_ci.csv"),
        help="Ruta CSV de salida",
    )
    p.add_argument(
        "--pairs",
        type=str,
        default="E0:2425,SP1:2425",
        help="Lista div:season_folder separada por comas",
    )
    args = p.parse_args()
    out_path = Path(args.out.strip())
    if not out_path.is_absolute():
        out_path = ROOT / out_path

    pairs = []
    for chunk in args.pairs.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        parts = chunk.split(":")
        if len(parts) != 2:
            raise SystemExit(f"Par inválido (usar DIV:season): {chunk!r}")
        pairs.append((parts[0].strip().upper(), parts[1].strip()))
    if not pairs:
        pairs = list(_DEFAULT_PAIRS)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    merged_chunks: list[pd.DataFrame] = []
    for div, season_folder in pairs:
        raw = fetch_div(div, season_folder)
        merged_chunks.append(normalize(raw, div))
    merged = pd.concat(merged_chunks, ignore_index=True)
    merged = merged.sort_values("date")
    merged["date"] = merged["date"].dt.strftime("%Y-%m-%d")
    merged.to_csv(out_path, index=False)
    print(f"Escrito {out_path} ({len(merged)} filas) desde {pairs}")


if __name__ == "__main__":
    main()
