"""Features cronologicos (sin fuga) para partidos por competicion."""

from __future__ import annotations

import hashlib
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta

import numpy as np
import pandas as pd

HOME_ELO_ADV = 60.0
K_ELO = 32.0
DEFAULT_ELO = 1500.0


def _elo_expected(r_a: float, r_b: float) -> float:
    return 1.0 / (1.0 + 10 ** ((r_b - r_a) / 400.0))


def _md5_unit(comp: str) -> float:
    h = hashlib.md5(comp.encode("utf-8")).hexdigest()
    return int(h[:12], 16) / float(16**12)


def _roll_stats(rows: list[tuple[int, int, int]], window: int) -> tuple[float, float, float]:
    if not rows:
        return 1.35, 1.35, 1.15
    tail = rows[-window:]
    n = len(tail)
    gf = sum(r[0] for r in tail) / n
    ga = sum(r[1] for r in tail) / n
    pts = sum(r[2] for r in tail) / n
    return gf, ga, pts


@dataclass
class TeamState:
    elo: float = DEFAULT_ELO
    history: list[tuple[int, int, int]] | None = None
    last_date: object | None = None
    past_dates: list | None = None

    def __post_init__(self) -> None:
        if self.history is None:
            self.history = []
        if self.past_dates is None:
            self.past_dates = []


def build_e0_chronological_frames(
    df: pd.DataFrame,
    *,
    competition: str = "E0",
    clip_roll: bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame, np.ndarray]:
    d = df.loc[df["competition"] == competition].copy()
    d["date"] = pd.to_datetime(d["date"], errors="coerce")
    bad_dates = d["date"].isna()
    if bad_dates.any():
        d = d.loc[~bad_dates].copy()
    d = d.sort_values(["date", "home_team", "away_team"]).reset_index(drop=True)

    elos: dict[tuple[str, str], TeamState] = defaultdict(lambda: TeamState())
    rows_legacy: list[dict] = []
    rows_tab: list[dict] = []
    y_list: list[int] = []
    res_map = {"H": 0, "D": 1, "A": 2}

    def clip_gf(v: float) -> float:
        if not clip_roll:
            return v
        return float(np.clip(v, 0.0, 5.0))

    def clip_ga(v: float) -> float:
        if not clip_roll:
            return v
        return float(np.clip(v, 0.0, 5.0))

    def clip_pts(v: float) -> float:
        if not clip_roll:
            return v
        return float(np.clip(v, 0.0, 3.0))

    for _, r in d.iterrows():
        comp = str(r["competition"])
        home = str(r["home_team"]).strip()
        away = str(r["away_team"]).strip()
        dt = r["date"]
        if hasattr(dt, "to_pydatetime"):
            dt = dt.to_pydatetime()
        gh, ga = int(r["home_goals"]), int(r["away_goals"])
        res = str(r["result_1x2"]).strip()
        y_list.append(res_map[res])

        kh, ka = (comp, home), (comp, away)
        sh, sa = elos[kh], elos[ka]

        elo_h, elo_a = sh.elo, sa.elo
        margin = elo_h - elo_a
        exp_h = _elo_expected(elo_h + HOME_ELO_ADV, elo_a)

        def feats(ts: TeamState, w: int) -> tuple[float, float, float]:
            gf, ga_, pts = _roll_stats(ts.history, w)
            return clip_gf(gf), clip_ga(ga_), clip_pts(pts)

        h_gf5, h_ga5, h_pts5 = feats(sh, 5)
        a_gf5, a_ga5, a_pts5 = feats(sa, 5)
        h_gf10, h_ga10, h_pts10 = feats(sh, 10)
        a_gf10, a_ga10, a_pts10 = feats(sa, 10)

        gf_diff = h_gf5 - a_gf5
        pts_diff = h_pts5 - a_pts5

        def rest_days(ts: TeamState) -> float:
            if ts.last_date is None:
                return 14.0
            rd = max(0.0, (dt - ts.last_date).days)
            return float(np.clip(rd, 0.0, 45.0)) if clip_roll else rd

        def matches_7d(ts: TeamState) -> float:
            lo = dt - timedelta(days=7)
            c = sum(1 for x in ts.past_dates if lo < x < dt)
            return float(np.clip(c, 0.0, 5.0)) if clip_roll else float(c)

        chash = _md5_unit(comp)

        rows_legacy.append(
            {
                "elo_home": elo_h,
                "elo_away": elo_a,
                "elo_margin": margin,
                "elo_expected_home": exp_h,
                "roll_gf_home": h_gf5,
                "roll_ga_home": h_ga5,
                "roll_pts_home": h_pts5,
                "roll_gf_away": a_gf5,
                "roll_ga_away": a_ga5,
                "roll_pts_away": a_pts5,
                "gf_diff_roll": gf_diff,
                "pts_diff_roll": pts_diff,
            }
        )
        rows_tab.append(
            {
                "elo_home": elo_h,
                "elo_away": elo_a,
                "elo_margin": margin,
                "elo_win_expect": exp_h,
                "gf_roll_5_home": h_gf5,
                "ga_roll_5_home": h_ga5,
                "pts_roll_5_home": h_pts5,
                "gf_roll_10_home": h_gf10,
                "ga_roll_10_home": h_ga10,
                "pts_roll_10_home": h_pts10,
                "gf_roll_5_away": a_gf5,
                "ga_roll_5_away": a_ga5,
                "pts_roll_5_away": a_pts5,
                "gf_roll_10_away": a_gf10,
                "ga_roll_10_away": a_ga10,
                "pts_roll_10_away": a_pts10,
                "rest_days_home": rest_days(sh),
                "rest_days_away": rest_days(sa),
                "matches_last_7d_home": matches_7d(sh),
                "matches_last_7d_away": matches_7d(sa),
                "competition_hash": chash,
                # Metadatos (no van en TABULAR_COLS del modelo salvo inclusión explícita)
                "season_year": int(pd.Timestamp(dt).year),
            }
        )

        _observe_result(elos, comp, home, away, dt, gh, ga)

    y = np.array(y_list, dtype=np.int64)
    return pd.DataFrame(rows_legacy), pd.DataFrame(rows_tab), y


def _observe_result(
    elos: dict[tuple[str, str], TeamState],
    comp: str,
    home: str,
    away: str,
    dt,
    gh: int,
    ga: int,
) -> None:
    kh, ka = (comp, home), (comp, away)
    sh, sa = elos[kh], elos[ka]
    elo_h, elo_a = sh.elo, sa.elo
    pts_h = 3 if gh > ga else 1 if gh == ga else 0
    pts_a = 3 if ga > gh else 1 if gh == ga else 0
    act_h = 1.0 if gh > ga else 0.5 if gh == ga else 0.0
    act_a = 1.0 - act_h
    sh.elo += K_ELO * (act_h - _elo_expected(elo_h + HOME_ELO_ADV, elo_a))
    sa.elo += K_ELO * (act_a - _elo_expected(elo_a, elo_h + HOME_ELO_ADV))
    sh.history.append((gh, ga, pts_h))
    sa.history.append((ga, gh, pts_a))
    sh.last_date = dt
    sa.last_date = dt
    sh.past_dates.append(dt)
    sa.past_dates.append(dt)


def tabular_row_pre_match(
    elos: dict[tuple[str, str], TeamState],
    comp: str,
    home: str,
    away: str,
    dt,
    *,
    clip_roll: bool = True,
) -> dict[str, float]:
    """Misma fila tabular que en entrenamiento, instante previo al partido (sin actualizar estado)."""

    def clip_gf(v: float) -> float:
        if not clip_roll:
            return v
        return float(np.clip(v, 0.0, 5.0))

    def clip_ga(v: float) -> float:
        if not clip_roll:
            return v
        return float(np.clip(v, 0.0, 5.0))

    def clip_pts(v: float) -> float:
        if not clip_roll:
            return v
        return float(np.clip(v, 0.0, 3.0))

    kh, ka = (comp, home), (comp, away)
    sh, sa = elos[kh], elos[ka]
    elo_h, elo_a = sh.elo, sa.elo
    margin = elo_h - elo_a
    exp_h = _elo_expected(elo_h + HOME_ELO_ADV, elo_a)

    def feats(ts: TeamState, w: int) -> tuple[float, float, float]:
        gf, ga_, pts = _roll_stats(ts.history, w)
        return clip_gf(gf), clip_ga(ga_), clip_pts(pts)

    h_gf5, h_ga5, h_pts5 = feats(sh, 5)
    a_gf5, a_ga5, a_pts5 = feats(sa, 5)
    h_gf10, h_ga10, h_pts10 = feats(sh, 10)
    a_gf10, a_ga10, a_pts10 = feats(sa, 10)

    def rest_days(ts: TeamState) -> float:
        if ts.last_date is None:
            return 14.0
        rd = max(0.0, (dt - ts.last_date).days)
        return float(np.clip(rd, 0.0, 45.0)) if clip_roll else rd

    def matches_7d(ts: TeamState) -> float:
        lo = dt - timedelta(days=7)
        c = sum(1 for x in ts.past_dates if lo < x < dt)
        return float(np.clip(c, 0.0, 5.0)) if clip_roll else float(c)

    chash = _md5_unit(comp)
    return {
        "elo_home": elo_h,
        "elo_away": elo_a,
        "elo_margin": margin,
        "elo_win_expect": exp_h,
        "gf_roll_5_home": h_gf5,
        "ga_roll_5_home": h_ga5,
        "pts_roll_5_home": h_pts5,
        "gf_roll_10_home": h_gf10,
        "ga_roll_10_home": h_ga10,
        "pts_roll_10_home": h_pts10,
        "gf_roll_5_away": a_gf5,
        "ga_roll_5_away": a_ga5,
        "pts_roll_5_away": a_pts5,
        "gf_roll_10_away": a_gf10,
        "ga_roll_10_away": a_ga10,
        "pts_roll_10_away": a_pts10,
        "rest_days_home": rest_days(sh),
        "rest_days_away": rest_days(sa),
        "matches_last_7d_home": matches_7d(sh),
        "matches_last_7d_away": matches_7d(sa),
        "competition_hash": chash,
    }


def replay_team_states_through_day(
    df: pd.DataFrame,
    *,
    competition: str,
    before_day: date,
) -> dict[tuple[str, str], TeamState]:
    """
    Reproduce Elo/forma solo con filas estrictamente anteriores a before_day (fecha calendario de la fila).
    """
    d = df.loc[df["competition"].astype(str) == competition].copy()
    d["date"] = pd.to_datetime(d["date"], errors="coerce")
    d = d.loc[d["date"].notna()].copy()
    d["_d"] = d["date"].dt.date
    d = d.loc[d["_d"] < before_day].sort_values(["date", "home_team", "away_team"]).reset_index(drop=True)
    elos: dict[tuple[str, str], TeamState] = defaultdict(lambda: TeamState())
    for _, r in d.iterrows():
        home = str(r["home_team"]).strip()
        away = str(r["away_team"]).strip()
        dt = r["date"]
        if hasattr(dt, "to_pydatetime"):
            dt = dt.to_pydatetime()
        gh, ga = int(r["home_goals"]), int(r["away_goals"])
        _observe_result(elos, competition, home, away, dt, gh, ga)
    return elos


def split_ml_e0(n_total: int) -> tuple[slice, slice, slice]:
    return slice(0, 2184), slice(2184, 3016), slice(3016, n_total)
