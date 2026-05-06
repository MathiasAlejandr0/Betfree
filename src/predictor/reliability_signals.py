"""Señales legibles de incertidumbre para el digest (no sustituyen métricas offline)."""

from __future__ import annotations


def max_confidence_1x2(ph: float, pd_: float, pa: float) -> float:
    return float(max(ph, pd_, pa))


def reliability_band_spanish(mx: float, *, hi: float = 0.55, mid: float = 0.42) -> str:
    if mx >= hi:
        return "alta"
    if mx >= mid:
        return "media"
    return "baja"


def source_label_spanish(used_ml: bool, blend_ml_w: float, *, poisson_basis: str) -> str:
    """poisson_basis: 'liga' = estado aislado por competición CSV; 'global' = mezcla todo el histórico."""
    pois = "Poisson+Elo por liga (hist. CSV)" if poisson_basis == "liga" else "Poisson+Elo global (hist. CSV)"
    if used_ml and blend_ml_w >= 0.995:
        return f"modelo tabular E0 (Premier) + {pois}"
    if used_ml:
        return f"mezcla ML E0+{pois} ({int(round(100 * blend_ml_w))}% ML)"
    return f"{pois} (sin ML tabular esta fila)"


def digest_reliability_html_line(
    ph: float,
    pd_: float,
    pa: float,
    *,
    used_ml: bool,
    blend_ml_w: float,
    pct_fmt: str,
    poisson_basis: str,
) -> str:
    mx = max_confidence_1x2(ph, pd_, pa)
    band = reliability_band_spanish(mx)
    src = source_label_spanish(used_ml, blend_ml_w, poisson_basis=poisson_basis)
    return (
        f"🔎 <i>Confianza 1X2: <b>{band}</b> (máx. {pct_fmt}) · fuente: {src}. "
        "No es garantía de resultado.</i>"
    )
