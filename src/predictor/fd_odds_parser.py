"""Cuotas 1X2 desde CSV estilo Football-Data.co.uk (B365H/D/A o AvgH/D/A)."""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

RES_MAP = {"H": 0, "D": 1, "A": 2}


def _safe_float(x: Any) -> float | None:
    try:
        v = float(x)
        if v <= 1.0 or np.isnan(v):
            return None
        return v
    except (TypeError, ValueError):
        return None


def closing_decimal_odds_row(row: pd.Series, *, prefer: str = "b365") -> tuple[float | None, float | None, float | None]:
    """Devuelve (home, draw, away) en decimal o None si faltan."""
    if prefer == "avg":
        groups = (("AvgH", "AvgD", "AvgA"), ("B365H", "B365D", "B365A"))
    else:
        groups = (("B365H", "B365D", "B365A"), ("AvgH", "AvgD", "AvgA"))
    for oh, od_, oa in groups:
        if oh not in row.index or od_ not in row.index or oa not in row.index:
            continue
        h = _safe_float(row.get(oh))
        d = _safe_float(row.get(od_))
        a = _safe_float(row.get(oa))
        if h and d and a:
            return h, d, a
    return None, None, None


def implied_probs_normalized(h: float, d: float, a: float) -> tuple[float, float, float]:
    """Invierte cuotas decimales y quita overround (normaliza a simplex)."""
    ih, id_, ia = 1.0 / h, 1.0 / d, 1.0 / a
    s = ih + id_ + ia
    if s <= 0:
        return 1 / 3, 1 / 3, 1 / 3
    return ih / s, id_ / s, ia / s


def load_fd_odds_frame(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, encoding="utf-8-sig", encoding_errors="replace")
    if "Date" not in df.columns:
        raise ValueError("CSV sin columna Date")
    df["Date"] = pd.to_datetime(df["Date"], dayfirst=True, errors="coerce")
    return df


def y_true_from_ftr(row: pd.Series) -> int | None:
    r = str(row.get("FTR", "")).strip().upper()
    if r not in RES_MAP:
        return None
    return RES_MAP[r]
