"""Contrato mínimo del artefacto joblib E0 (1X2 tabular)."""

from __future__ import annotations

from typing import Any


def collect_e0_artifact_issues(artifact: Any, *, expected_features: list[str] | None = None) -> list[str]:
    """Devuelve lista de problemas detectados (vacía = usable con cautela)."""
    out: list[str] = []
    if not isinstance(artifact, dict):
        return ["artefacto no es un dict joblib"]
    model = artifact.get("model")
    fn = artifact.get("feature_names")
    if model is None:
        out.append("falta clave 'model'")
    if not fn:
        out.append("falta clave 'feature_names'")
        return out
    names = [str(x) for x in fn]
    if expected_features is not None:
        exp = list(expected_features)
        if names != exp:
            miss = [c for c in exp if c not in names]
            extra = [c for c in names if c not in exp]
            if miss or extra:
                out.append(
                    f"feature_names distinto al contrato: faltan={miss[:6]!r} extra={extra[:6]!r} "
                    f"(n={len(names)} vs esperado {len(exp)})"
                )
    if hasattr(model, "n_features_in_") and len(names) > 0:
        try:
            nfi = int(getattr(model, "n_features_in_"))
            if nfi != len(names):
                out.append(f"model.n_features_in_={nfi} != len(feature_names)={len(names)}")
        except (TypeError, ValueError):
            pass
    return out
