"""URLs públicas CSV de Football-Data.co.uk (resultados + cuotas; sin token)."""

from __future__ import annotations

BASE = "https://www.football-data.co.uk/mmz4281"


def season_folder_from_start_year(start_year: int) -> str:
    """
    Carpeta en el servidor para la temporada que *empieza* en agosto de `start_year`.
    Caso especial 2020/21 → '2021' (convención del sitio).
    """
    y = int(start_year)
    if y == 2020:
        return "2021"
    return f"{y % 100:02d}{(y + 1) % 100:02d}"


def fd_co_uk_csv_url(*, season_folder: str, division: str) -> str:
    div = (division or "").strip().upper()
    sf = (season_folder or "").strip()
    return f"{BASE}/{sf}/{div}.csv"
