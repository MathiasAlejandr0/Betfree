"""Elo + forma desde CSV."""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, field
import unicodedata

import numpy as np
import pandas as pd

from src.predictor.model_trainer import TeamStrength, estimate_expected_goals, predict_1x2_probabilities

HOME_ADV = 60.0
K = 32.0
ELO0 = 1500.0


def norm_team(name: str) -> str:
    s = unicodedata.normalize("NFKD", str(name)).encode("ascii", "ignore").decode()
    s = "".join(c if c.isalnum() else " " for c in s.lower())
    return " ".join(s.split())


def _exp(a: float, b: float) -> float:
    return 1.0 / (1.0 + 10 ** ((b - a) / 400.0))


def _roll(rows: list[tuple[int, int, int]], w: int) -> tuple[float, float, float]:
    if not rows:
        return 1.35, 1.35, 1.12
    t = rows[-w:]
    n = len(t)
    return sum(x[0] for x in t) / n, sum(x[1] for x in t) / n, sum(x[2] for x in t) / n


@dataclass
class Snap:
    elo: float = ELO0
    hist: list[tuple[int, int, int]] = field(default_factory=list)


class GlobalCsvRollState:
    def __init__(
        self,
        *,
        primary_window: int = 15,
        secondary_window: int = 8,
        draw_calibration_factor: float = 1.0,
    ) -> None:
        self.primary_window = primary_window
        self.secondary_window = secondary_window
        self.draw_calibration_factor = float(draw_calibration_factor)
        self._t: dict[str, Snap] = defaultdict(Snap)
        self._tot: deque[int] = deque(maxlen=4000)

    def league_mu(self) -> float:
        return float(sum(self._tot)) / len(self._tot) if self._tot else 2.58

    def tail(self, name: str, w: int) -> tuple[float, float, float]:
        return _roll(self._t[norm_team(name)].hist, w)

    def elo(self, name: str) -> float:
        return float(self._t[norm_team(name)].elo)

    def poisson_probs(self, home: str, away: str) -> tuple[float, float, float, float, float]:
        mu = self.league_mu()
        h = max(mu / 2.0, 0.62)
        hgf, hga, _ = self.tail(home, self.primary_window)
        agf, aga, _ = self.tail(away, self.primary_window)
        atk_h = float(np.clip(hgf / h, 0.22, 2.9))
        def_h = float(np.clip(hga / h, 0.22, 2.9))
        atk_a = float(np.clip(agf / h, 0.22, 2.9))
        def_a = float(np.clip(aga / h, 0.22, 2.9))
        eh, ea = self.elo(home), self.elo(away)
        hs = TeamStrength(atk_h, def_h, eh)
        aws = TeamStrength(atk_a, def_a, ea)
        xh, xa = estimate_expected_goals(hs, aws, league_avg_goals=mu)
        mp = predict_1x2_probabilities(xh, xa)
        fh, fd, fa = float(mp.home_win), float(mp.draw), float(mp.away_win)
        dcf = getattr(self, "draw_calibration_factor", 1.0)
        fd = fd * float(dcf)
        s = fh + fd + fa
        if s > 0:
            inv = 1.0 / s
            fh, fd, fa = fh * inv, fd * inv, fa * inv
        return fh, fd, fa, xh, xa

    def observe(self, ho: str, aw: str, gh: int, ga: int) -> None:
        hk, ak = norm_team(ho), norm_team(aw)
        sh, sa = self._t[hk], self._t[ak]
        elo_h, elo_a = sh.elo, sa.elo
        ah = 1.0 if gh > ga else 0.5 if gh == ga else 0.0
        aa = 1.0 - ah
        sh.elo += K * (ah - _exp(elo_h + HOME_ADV, elo_a))
        sa.elo += K * (aa - _exp(elo_a, elo_h + HOME_ADV))
        ph = 3 if gh > ga else 1 if gh == ga else 0
        pa = 3 if ga > gh else 1 if gh == ga else 0
        sh.hist.append((gh, ga, ph))
        sa.hist.append((ga, gh, pa))
        self._tot.append(int(gh + ga))


def estimated_match_cards(state: GlobalCsvRollState, home: str, away: str, expected_total_goals: float) -> float:
    """Tarjetas totales (amarillas aprox.) esperadas por partido: heurística ligada a intensidad (goles esperados)."""
    mu = state.league_mu()
    delta = float(np.clip(expected_total_goals - mu, -1.35, 1.35))
    base = 4.05
    return float(np.clip(base + 0.52 * delta, 2.75, 6.85))


def estimated_match_cards_split(
    state: GlobalCsvRollState, home: str, away: str, expected_total_goals: float, xh: float, xa: float
) -> tuple[float, float, float]:
    """Amarillas aprox. esperadas: local, visitante, total (reparto por fuerza ofensiva relativa)."""
    tot = estimated_match_cards(state, home, away, expected_total_goals)
    g = xh + xa + 1e-9
    w = float(np.clip(xh / g, 0.36, 0.64))
    ch = tot * w
    return ch, tot - ch, tot


def estimated_match_corners_split(
    state: GlobalCsvRollState, home: str, away: str, expected_total_goals: float, xh: float, xa: float
) -> tuple[float, float, float]:
    """Corners esperados: local, visitante, total (más corners si se esperan más goles)."""
    mu = state.league_mu()
    delta = float(np.clip(expected_total_goals - mu, -1.6, 1.6))
    tot = float(np.clip(9.5 + 0.75 * delta, 7.0, 15.0))
    g = xh + xa + 1e-9
    w = float(np.clip(0.5 + 0.15 * (xh - xa) / g, 0.35, 0.65))
    ch = tot * w
    return ch, tot - ch, tot


def rolling_goal_summary(state: GlobalCsvRollState, team: str, window: int | None = None) -> tuple[float, float]:
    w = window or state.primary_window
    gf, ga, _ = state.tail(team, w)
    return gf, ga


def build_global_state_from_csv(
    path: str,
    *,
    before_day,
    primary_w: int = 15,
    secondary_w: int = 8,
    draw_calibration_factor: float = 1.0,
) -> GlobalCsvRollState:
    st = GlobalCsvRollState(
        primary_window=primary_w,
        secondary_window=secondary_w,
        draw_calibration_factor=draw_calibration_factor,
    )
    path = (path or "").strip()
    if not path:
        return st
    try:
        df = pd.read_csv(path)
    except OSError:
        return st
    need = {"date", "home_team", "away_team", "home_goals", "away_goals"}
    if not need.issubset(df.columns):
        return st
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df[df["date"].notna()]
    df["_d"] = df["date"].dt.date
    df = df[df["_d"] < before_day].sort_values("date")
    for _, r in df.iterrows():
        try:
            st.observe(str(r["home_team"]), str(r["away_team"]), int(r["home_goals"]), int(r["away_goals"]))
        except (TypeError, ValueError):
            continue
    return st
