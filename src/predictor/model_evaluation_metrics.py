"""
Métricas de probabilidad y calibración para 1X2 (3 clases).

Uso típico: evaluar vectores `y_true` ∈ {0,1,2} y `probabilities` forma (n, 3).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence

import numpy as np
from sklearn.metrics import accuracy_score, log_loss
from sklearn.utils import assert_all_finite


@dataclass(frozen=True)
class BaselinePrediction:
    name: str
    probas: np.ndarray  # (n, 3)


def multiclass_brier(y_true: np.ndarray, probas: np.ndarray) -> float:
    assert_all_finite(probas)
    y_true = np.asarray(y_true).astype(np.int64).reshape(-1)
    p = np.asarray(probas, dtype=np.float64).reshape(len(y_true), -1)
    k = p.shape[1]
    oh = np.zeros_like(p)
    oh[np.arange(len(y_true)), y_true] = 1.0
    return float(np.mean(np.sum((p - oh) ** 2, axis=1)))


def multiclass_log_loss_stable(y_true: np.ndarray, probas: np.ndarray, *, eps: float = 1e-15) -> float:
    """Log-loss con clipping numérico; labels orden {0..K-1}."""
    yt = np.asarray(y_true).astype(np.int64).reshape(-1)
    p = np.clip(np.asarray(probas, dtype=np.float64), eps, 1.0 - eps)
    row_sums = p.sum(axis=1, keepdims=True)
    row_sums[row_sums <= 0] = 1.0
    p = p / row_sums
    return float(log_loss(yt, p, labels=list(range(p.shape[1]))))


def ece_multiclass_max_confidence(
    y_true: np.ndarray,
    probas: np.ndarray,
    *,
    n_bins: int = 12,
    min_samples_per_bin: int = 25,
) -> dict[str, Any]:
    """
    Expected Calibration Error vía bins por confianza = max probabilidad prevista.

    En cada cubeta: |accuracy - mean_confidence| ponderado por frecuencia.
    Si una cubeta tiene muy pocas muestras, se omite del ECE declarado pero se registra en `bins_detail`.
    """
    y = np.asarray(y_true).astype(np.int64).reshape(-1)
    p = np.asarray(probas, dtype=np.float64).reshape(len(y), -1)
    pred = np.argmax(p, axis=1)
    conf = np.max(p, axis=1)
    acc = (pred == y).astype(np.float64)

    order = np.argsort(conf)
    n = len(y)
    if n_bins < 2 or n < n_bins * 5:
        n_bins = max(2, min(10, max(4, n // 100)))

    idxs = np.array_split(order, n_bins)
    buckets: list[dict[str, float]] = []
    ece_accum = 0.0
    weight_used = 0.0

    for part in idxs:
        if part.size == 0:
            continue
        mw = np.mean(acc[part])
        mc = np.mean(conf[part])
        w = part.size / n
        rec = {"n": float(part.size), "mean_accuracy": mw, "mean_confidence": mc, "gap": abs(mw - mc)}
        buckets.append(rec)
        if part.size >= min_samples_per_bin:
            ece_accum += w * abs(mw - mc)
            weight_used += w

    return {
        "ece_max_confidence": float(ece_accum),
        "fraction_weight_bins_used": float(weight_used),
        "n_bins": int(len(idxs)),
        "bins_detail": buckets,
        "notes": (
            "ECE multinivel simple (max probabilidad como confianza). "
            "Bins por cuantiles de confianza (misma masa aprox)."
        ),
    }


def baseline_frequency_probas(y_train: np.ndarray) -> tuple[np.ndarray, BaselinePrediction]:
    """Distribución empírica del train (constante sobre el test)."""
    y_train = np.asarray(y_train).astype(np.int64).reshape(-1)
    counts = np.bincount(y_train, minlength=3).astype(np.float64)
    totals = counts.sum()
    freq = counts / totals if totals > 0 else np.ones(3) / 3.0
    return freq, BaselinePrediction("prior_train_frequency", freq.reshape(1, 3))


def baseline_argmax_prior(y_train: np.ndarray, n_te: int) -> BaselinePrediction:
    """Distribución degenerada sobre la clase más frecuente en train."""
    y_train = np.asarray(y_train).astype(np.int64).reshape(-1)
    k = int(np.bincount(y_train, minlength=3).argmax())
    p = np.zeros((1, 3), dtype=np.float64)
    p[0, k] = 1.0
    return BaselinePrediction("prior_train_argmax_repeat", np.repeat(p, n_te, axis=0))


def full_metric_bundle(
    y_te: np.ndarray,
    probas_te: np.ndarray,
    *,
    y_tr: Sequence[int] | np.ndarray | None = None,
) -> dict[str, Any]:
    """Paquete listo para JSON (test + opcional líneas baseline)."""
    y_te = np.asarray(y_te).astype(np.int64).reshape(-1)
    p_te = np.asarray(probas_te, dtype=np.float64)
    metrics: dict[str, Any] = {
        "test_rows": len(y_te),
        "accuracy": float(accuracy_score(y_te, np.argmax(p_te, axis=1))),
        "log_loss": multiclass_log_loss_stable(y_te, p_te),
        "brier_multiclass": multiclass_brier(y_te, p_te),
        "calibration_max_confidence": ece_multiclass_max_confidence(y_te, p_te),
    }
    if y_tr is None or len(y_tr) == 0:
        return metrics
    y_tr = np.asarray(y_tr).astype(np.int64).reshape(-1)
    freq_1row, fc = baseline_frequency_probas(y_tr)
    bm = baseline_argmax_prior(y_tr, len(y_te)).probas
    metrics["baselines_vs_test"] = {
        fc.name: {
            "log_loss": multiclass_log_loss_stable(y_te, np.broadcast_to(fc.probas, (len(y_te), 3))),
            "brier_multiclass": multiclass_brier(y_te, np.broadcast_to(fc.probas, (len(y_te), 3))),
            "accuracy": float(accuracy_score(y_te, np.argmax(np.broadcast_to(fc.probas, (len(y_te), 3)), axis=1))),
            "prior_vector_h_d_a": [float(freq_1row[0]), float(freq_1row[1]), float(freq_1row[2])],
        },
        "prior_train_argmax_repeat": {
            "log_loss": multiclass_log_loss_stable(y_te, bm),
            "brier_multiclass": multiclass_brier(y_te, bm),
            "accuracy": float(accuracy_score(y_te, np.argmax(bm, axis=1))),
        },
    }
    metrics["baseline_improvements"] = {
        "delta_log_loss_vs_frequency_prior": metrics["log_loss"]
        - metrics["baselines_vs_test"][fc.name]["log_loss"],
    }
    return metrics


def metrics_by_masks(
    y: np.ndarray,
    probas: np.ndarray,
    masks: dict[str, np.ndarray],
    *,
    min_support: int = 80,
) -> dict[str, Any]:
    """Métricas por subconjunto (ej.: máscara temporada). Omitir si n < min_support."""
    out: dict[str, Any] = {}
    for name, mask in masks.items():
        m = np.asarray(mask, dtype=bool).reshape(-1)
        ys, ps = np.asarray(y).reshape(-1)[m], np.asarray(probas)[m]
        if len(ys) < min_support:
            out[name] = {"skipped": True, "reason": "n_below_min_support", "n": int(len(ys))}
            continue
        out[name] = {
            "n": int(len(ys)),
            "accuracy": float(accuracy_score(ys, np.argmax(ps, axis=1))),
            "log_loss": multiclass_log_loss_stable(ys, ps),
            "brier_multiclass": multiclass_brier(ys, ps),
            "ece": ece_multiclass_max_confidence(
                ys,
                ps,
                min_samples_per_bin=max(5, min(len(ys) // 30, 20)),
            ),
        }
    return out
