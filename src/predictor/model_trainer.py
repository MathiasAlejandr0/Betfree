"""Initial quantitative predictor using Poisson + ELO."""

from __future__ import annotations

from dataclasses import dataclass
import math


@dataclass(frozen=True)
class TeamStrength:
    """Team profile for initial expected-goals estimation."""

    attack_rate: float
    defense_rate: float
    elo: float


@dataclass(frozen=True)
class MatchPrediction:
    """1X2 probabilities."""

    home_win: float
    draw: float
    away_win: float


def poisson_pmf(k: int, lambda_goals: float) -> float:
    """Poisson PMF."""
    if lambda_goals <= 0:
        return 0.0
    return math.exp(-lambda_goals) * (lambda_goals**k) / math.factorial(k)


def elo_expected_score(elo_a: float, elo_b: float) -> float:
    """Expected score from ELO."""
    return 1.0 / (1.0 + 10 ** ((elo_b - elo_a) / 400.0))


def estimate_expected_goals(
    home: TeamStrength,
    away: TeamStrength,
    league_avg_goals: float = 2.45,
    home_advantage_factor: float = 1.08,
) -> tuple[float, float]:
    """Estimate home/away expected goals combining rates and ELO."""
    home_xg = max(0.1, home.attack_rate * away.defense_rate * home_advantage_factor)
    away_xg = max(0.1, away.attack_rate * home.defense_rate)
    e_home = elo_expected_score(home.elo, away.elo)
    e_away = 1.0 - e_home
    home_xg *= 0.85 + 0.30 * e_home
    away_xg *= 0.85 + 0.30 * e_away
    total = home_xg + away_xg
    if total <= 0:
        return league_avg_goals * 0.55, league_avg_goals * 0.45
    scale = league_avg_goals / total
    return home_xg * scale, away_xg * scale


def predict_1x2_probabilities(
    expected_home_goals: float, expected_away_goals: float, max_goals: int = 8
) -> MatchPrediction:
    """Compute 1X2 probabilities with independent Poisson assumptions."""
    home_win = 0.0
    draw = 0.0
    away_win = 0.0
    for h in range(max_goals + 1):
        p_h = poisson_pmf(h, expected_home_goals)
        for a in range(max_goals + 1):
            p_a = poisson_pmf(a, expected_away_goals)
            p = p_h * p_a
            if h > a:
                home_win += p
            elif h == a:
                draw += p
            else:
                away_win += p
    total = home_win + draw + away_win
    if total <= 0:
        return MatchPrediction(0.0, 0.0, 0.0)
    return MatchPrediction(home_win / total, draw / total, away_win / total)


def implied_probability_from_odds(decimal_odds: float) -> float:
    """Convert decimal odds to implied probability."""
    if decimal_odds <= 1.0:
        raise ValueError("decimal_odds must be > 1.0")
    return 1.0 / decimal_odds


def calculate_value_edge(model_probability: float, bookmaker_odds: float) -> float:
    """Value edge = model probability - implied probability."""
    if not 0.0 <= model_probability <= 1.0:
        raise ValueError("model_probability must be in [0,1]")
    return model_probability - implied_probability_from_odds(bookmaker_odds)


def expected_value_per_unit(model_probability: float, bookmaker_odds: float) -> float:
    """Expected value for 1 unit stake."""
    if not 0.0 <= model_probability <= 1.0:
        raise ValueError("model_probability must be in [0,1]")
    return (model_probability * bookmaker_odds) - 1.0

