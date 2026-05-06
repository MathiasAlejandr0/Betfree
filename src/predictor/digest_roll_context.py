"""Estado Poisson+Elo para el digest: global y por competición (códigos CSV Football-Data)."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from src.predictor.csv_roll_state import GlobalCsvRollState

DaySchedule = defaultdict[Any, list[Any]]


@dataclass(frozen=True)
class ScheduledE0Pred:
    ml_index: int
    home: str
    away: str
    y_true: int


@dataclass(frozen=True)
class ScheduledPoissonAudit:
    competition: str
    home: str
    away: str
    y_true: int


def merge_scheduled_by_day(*parts: dict[Any, list[Any]]) -> DaySchedule:
    m: DaySchedule = defaultdict(list)
    for p in parts:
        for day, lst in p.items():
            m[day].extend(lst)
    return m


def competition_eligible_codes(df: pd.DataFrame, *, min_rows: int) -> frozenset[str]:
    if "competition" not in df.columns or min_rows <= 0:
        return frozenset()
    vc = df["competition"].astype(str).str.strip().value_counts()
    return frozenset(str(c).strip() for c, n in vc.items() if int(n) >= int(min_rows))


def run_chronological_poisson_simulation(
    global_df: pd.DataFrame,
    scheduled_by_day: dict[Any, list[Any]],
    draw_factor: float,
    *,
    per_league: bool,
    min_rows_per_league: int,
) -> tuple[dict[int, tuple[float, float, float]], defaultdict[str, list[tuple[int, tuple[float, float, float]]]]]:
    """Recorre días: tareas programadas (Poisson) y luego observa todos los partidos del día."""
    poi_e0: dict[int, tuple[float, float, float]] = {}
    poi_audit: defaultdict[str, list[tuple[int, tuple[float, float, float]]]] = defaultdict(list)

    eligible = competition_eligible_codes(global_df, min_rows=min_rows_per_league) if per_league else frozenset()
    st_g = GlobalCsvRollState(draw_calibration_factor=float(draw_factor))
    st_l: dict[str, GlobalCsvRollState] = {c: GlobalCsvRollState(draw_calibration_factor=float(draw_factor)) for c in eligible}

    def _pick_state(comp_hist: str | None) -> GlobalCsvRollState:
        if comp_hist and comp_hist in st_l:
            return st_l[comp_hist]
        return st_g

    days_sorted = sorted(global_df["_d"].unique().tolist())
    for dday in days_sorted:
        day_tasks = scheduled_by_day.get(dday) or []
        for task in day_tasks:
            if isinstance(task, ScheduledE0Pred):
                st = _pick_state("E0")
                ph0, pd0, pa0, _, _ = st.poisson_probs(task.home, task.away)
                poi_e0[task.ml_index] = (ph0, pd0, pa0)
            elif isinstance(task, ScheduledPoissonAudit):
                st = _pick_state(str(task.competition).strip().upper())
                ph0, pd0, pa0, _, _ = st.poisson_probs(task.home, task.away)
                poi_audit[str(task.competition).strip().upper()].append((task.y_true, (ph0, pd0, pa0)))

        sort_cols = (
            ["date", "competition", "home_team", "away_team"]
            if "competition" in global_df.columns
            else ["date", "home_team", "away_team"]
        )
        grp = global_df.loc[global_df["_d"] == dday].sort_values(sort_cols)
        for _, r in grp.iterrows():
            try:
                h, a = str(r["home_team"]), str(r["away_team"])
                gh, ga = int(r["home_goals"]), int(r["away_goals"])
            except (TypeError, ValueError, KeyError):
                continue
            st_g.observe(h, a, gh, ga)
            if eligible and "competition" in global_df.columns:
                c = str(r["competition"]).strip()
                if c in st_l:
                    st_l[c].observe(h, a, gh, ga)

    return poi_e0, poi_audit


@dataclass
class DigestRollContext:
    """Contexto de pronósticos auxiliares (Poisson+Elo) para el digest."""

    global_state: GlobalCsvRollState
    per_league: dict[str, GlobalCsvRollState] = field(default_factory=dict)
    min_rows_threshold: int = 0
    per_league_enabled: bool = False

    def pick_state(self, hist_competition: str | None) -> GlobalCsvRollState:
        hc = (hist_competition or "").strip().upper()
        if self.per_league_enabled and hc and hc in self.per_league:
            return self.per_league[hc]
        return self.global_state

    def is_isolated_league(self, hist_competition: str | None) -> bool:
        hc = (hist_competition or "").strip().upper()
        return bool(self.per_league_enabled and hc and hc in self.per_league)

    @staticmethod
    def from_csv(
        path: str,
        *,
        before_day,
        primary_w: int = 15,
        secondary_w: int = 8,
        draw_calibration_factor: float = 1.0,
        per_league_enabled: bool = True,
        min_rows_per_league: int = 220,
    ) -> DigestRollContext:
        path = (path or "").strip()
        st_g = GlobalCsvRollState(
            primary_window=primary_w,
            secondary_window=secondary_w,
            draw_calibration_factor=float(draw_calibration_factor),
        )
        if not path:
            return DigestRollContext(st_g, {}, 0, False)
        try:
            df = pd.read_csv(path)
        except OSError:
            return DigestRollContext(st_g, {}, 0, False)
        need = {"date", "home_team", "away_team", "home_goals", "away_goals"}
        if not need.issubset(df.columns):
            return DigestRollContext(st_g, {}, 0, False)
        d = df.copy()
        d["date"] = pd.to_datetime(d["date"], errors="coerce")
        d = d.loc[d["date"].notna()].copy()
        d["_d"] = d["date"].dt.date
        d = d.loc[d["_d"] < before_day].sort_values("date")

        use_pl = bool(per_league_enabled and "competition" in d.columns)
        eligible = competition_eligible_codes(d, min_rows=min_rows_per_league) if use_pl else frozenset()
        st_l = {
            c: GlobalCsvRollState(
                primary_window=primary_w,
                secondary_window=secondary_w,
                draw_calibration_factor=float(draw_calibration_factor),
            )
            for c in eligible
        }

        for _, r in d.iterrows():
            try:
                h, a = str(r["home_team"]), str(r["away_team"])
                gh, ga = int(r["home_goals"]), int(r["away_goals"])
            except (TypeError, ValueError, KeyError):
                continue
            st_g.observe(h, a, gh, ga)
            if use_pl:
                c = str(r["competition"]).strip()
                if c in st_l:
                    st_l[c].observe(h, a, gh, ga)

        return DigestRollContext(
            st_g,
            st_l,
            int(min_rows_per_league),
            bool(use_pl and len(st_l) > 0),
        )
