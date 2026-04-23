"""Bankroll management with fractional Kelly criterion."""

from __future__ import annotations


def kelly_fraction(
    win_probability: float,
    decimal_odds: float,
    kelly_multiplier: float = 0.25,
    max_fraction: float = 0.05,
) -> float:
    """Return bankroll fraction using fractional Kelly."""
    if not 0.0 <= win_probability <= 1.0:
        raise ValueError("win_probability must be in [0,1]")
    if decimal_odds <= 1.0:
        raise ValueError("decimal_odds must be > 1.0")
    b = decimal_odds - 1.0
    q = 1.0 - win_probability
    raw = ((b * win_probability) - q) / b
    safe = max(0.0, raw) * kelly_multiplier
    return min(safe, max_fraction)


def recommended_stake(
    bankroll: float,
    win_probability: float,
    decimal_odds: float,
    kelly_multiplier: float = 0.25,
    max_fraction: float = 0.05,
    min_stake: float = 0.0,
) -> float:
    """Return monetary stake recommendation."""
    if bankroll <= 0:
        raise ValueError("bankroll must be > 0")
    fraction = kelly_fraction(
        win_probability=win_probability,
        decimal_odds=decimal_odds,
        kelly_multiplier=kelly_multiplier,
        max_fraction=max_fraction,
    )
    stake = bankroll * fraction
    if stake < min_stake:
        return 0.0
    return round(stake, 2)

