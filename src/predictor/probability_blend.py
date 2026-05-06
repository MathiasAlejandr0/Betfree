"""Mezcla convexa de probabilidades 1X2 (multiclass orden H, D, A)."""

from __future__ import annotations


def convex_blend_1x2(
    p_ml: tuple[float, float, float],
    p_base: tuple[float, float, float],
    ml_weight: float,
) -> tuple[float, float, float]:
    """Interpola ML y modelo base y renormaliza a simplex."""
    w = max(0.0, min(1.0, float(ml_weight)))
    ph = w * float(p_ml[0]) + (1.0 - w) * float(p_base[0])
    pd_ = w * float(p_ml[1]) + (1.0 - w) * float(p_base[1])
    pa = w * float(p_ml[2]) + (1.0 - w) * float(p_base[2])
    s = ph + pd_ + pa
    if s <= 0:
        return (1.0 / 3.0,) * 3
    inv = 1.0 / s
    return ph * inv, pd_ * inv, pa * inv
