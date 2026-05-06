"""Enfrentamientos directos (H2H) desde CSV histórico para el export estático de Pages."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

import pandas as pd

from src.predictor.csv_roll_state import norm_team


@dataclass
class _H2HRowView:
    match_date: date
    goals_for_home_upcoming: int
    goals_for_away_upcoming: int

    def outcome_for_home_upcoming(self) -> str:
        a, b = self.goals_for_home_upcoming, self.goals_for_away_upcoming
        if a > b:
            return "W"
        if a < b:
            return "L"
        return "D"


class PagesH2HIndex:
    """Índice ligero del CSV; una instancia por ruta de archivo en el export."""

    __slots__ = ("_df", "_has_competition")

    def __init__(self, csv_path: str) -> None:
        df = pd.read_csv(csv_path)
        need = {"date", "home_team", "away_team", "home_goals", "away_goals"}
        if not need.issubset(df.columns):
            raise ValueError(f"CSV H2H incompleto: faltan {need - set(df.columns)}")
        d = df.copy()
        d["date"] = pd.to_datetime(d["date"], errors="coerce")
        d = d.loc[d["date"].notna()].copy()
        d["_d"] = d["date"].dt.date
        d["_nh"] = d["home_team"].astype(str).map(norm_team)
        d["_na"] = d["away_team"].astype(str).map(norm_team)
        d["hg"] = pd.to_numeric(d["home_goals"], errors="coerce")
        d["ag"] = pd.to_numeric(d["away_goals"], errors="coerce")
        d = d.loc[d["hg"].notna() & d["ag"].notna()].copy()
        self._has_competition = "competition" in d.columns
        if self._has_competition:
            d["_c"] = d["competition"].astype(str).str.strip()
        self._df = d

    def summarize(
        self,
        before_day: date,
        home_upcoming: str,
        away_upcoming: str,
        hist_competition: str | None,
        *,
        max_meetings: int = 6,
    ) -> dict[str, Any]:
        """Últimos N cruces entre los dos equipos antes de ``before_day`` (perspectiva del local del partido a venir)."""
        hu = norm_team(home_upcoming)
        au = norm_team(away_upcoming)
        if not hu or not au:
            return _empty_h2h("Equipos vacíos.")

        sub = self._df.loc[self._df["_d"] < before_day].copy()
        same = ((sub["_nh"] == hu) & (sub["_na"] == au)) | ((sub["_nh"] == au) & (sub["_na"] == hu))
        sub = sub.loc[same]
        if self._has_competition and hist_competition:
            hc = str(hist_competition).strip()
            sub = sub.loc[sub["_c"] == hc]
        if sub.empty:
            return _empty_h2h(
                "Sin enfrentamientos previos en este CSV"
                + (f" (competición {hist_competition})" if hist_competition else "")
                + "."
            )

        sub = sub.sort_values("date", ascending=False).head(max(int(max_meetings), 1))

        views: list[_H2HRowView] = []
        for _, r in sub.iterrows():
            row_h, row_a = str(r["_nh"]), str(r["_na"])
            gh_i, ga_i = int(r["hg"]), int(r["ag"])
            if row_h == hu:
                g_hu, g_au = gh_i, ga_i
            elif row_h == au:
                g_hu, g_au = ga_i, gh_i
            else:
                continue
            views.append(_H2HRowView(match_date=r["_d"], goals_for_home_upcoming=g_hu, goals_for_away_upcoming=g_au))

        if not views:
            return _empty_h2h("Sin filas válidas tras filtrar.")

        w = d = l_ = 0
        for v in views:
            o = v.outcome_for_home_upcoming()
            if o == "W":
                w += 1
            elif o == "D":
                d += 1
            else:
                l_ += 1

        total_g = sum(v.goals_for_home_upcoming + v.goals_for_away_upcoming for v in views)
        n = len(views)
        recent = [
            {
                "date_iso": v.match_date.isoformat(),
                "goals_home": v.goals_for_home_upcoming,
                "goals_away": v.goals_for_away_upcoming,
                "outcome_for_upcoming_home": v.outcome_for_home_upcoming(),
            }
            for v in views
        ]

        return {
            "schema": "betfree.pages_h2h.v1",
            "meetings": n,
            "competition_filter": hist_competition,
            "record_wdl_home": {"w": w, "d": d, "l": l_},
            "avg_total_goals": round(total_g / n, 2) if n else None,
            "recent": recent,
            "note_es": "Últimos cruces entre estos equipos en el histórico (local próximo partido en perspectiva W/D/L).",
        }


def _empty_h2h(note: str) -> dict[str, Any]:
    return {
        "schema": "betfree.pages_h2h.v1",
        "meetings": 0,
        "competition_filter": None,
        "record_wdl_home": {"w": 0, "d": 0, "l": 0},
        "avg_total_goals": None,
        "recent": [],
        "note_es": note,
    }
