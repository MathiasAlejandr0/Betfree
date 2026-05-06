"""Diagnóstico offline: calibración, baselines y desglose por temporada (CSV E0 + artefacto)."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import joblib
import numpy as np
from sklearn.inspection import permutation_importance

from src.config import get_settings, repo_root
from src.predictor.chronological_features import build_e0_chronological_frames, split_ml_e0
from src.predictor.e0_data_prep import load_clean_e0
from src.predictor.e0_expert_train import TABULAR_COLS
from src.predictor.model_evaluation_metrics import full_metric_bundle, metrics_by_masks

ROOT = Path(__file__).resolve().parents[2]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Reporte métricas / calibración modelo E0")
    p.add_argument("--historical-csv", default="", help="Ruta CSV; por defecto .env")
    p.add_argument("--model-path", default="models/e0_expert_calibrated.pkl")
    p.add_argument("--burn-in", type=int, default=120)
    p.add_argument(
        "--permutation-importance",
        action="store_true",
        help="Permutation importance (lento): subsample de train.",
    )
    p.add_argument("--perm-samples", type=int, default=1400)
    p.add_argument("--out-json", default="data/model_diagnostic_e0.json")
    return p.parse_args()


def _resolve(p: Path) -> Path:
    return p if p.is_absolute() else (ROOT / p)


def _matrices(csv_path_resolved: Path, burn_arg: int) -> dict[str, Any]:
    clean, audit = load_clean_e0(csv_path_resolved)
    _, df_tab, y = build_e0_chronological_frames(clean, competition="E0", clip_roll=True)
    n = len(y)
    tr_sl, _ca, te_sl = split_ml_e0(n)
    burn = max(0, min(burn_arg, te_sl.start - 400))
    train_mask = np.zeros(n, dtype=bool)
    train_mask[burn : te_sl.start] = True
    test_mask = np.zeros(n, dtype=bool)
    test_mask[te_sl.start : te_sl.stop] = True
    X_tr = df_tab.loc[train_mask, TABULAR_COLS].to_numpy(dtype=np.float64, copy=False)
    X_te = df_tab.loc[test_mask, TABULAR_COLS].to_numpy(dtype=np.float64, copy=False)
    y_tr = y[train_mask]
    y_te = y[test_mask]
    seasons_te = df_tab.loc[test_mask, "season_year"].to_numpy()

    masks: dict[str, np.ndarray] = {}
    for yr in sorted(np.unique(seasons_te)):
        masks[f"test_season_{int(yr)}"] = seasons_te == yr

    return {
        "audit": audit,
        "y_tr": y_tr,
        "y_te": y_te,
        "X_tr": X_tr,
        "X_te": X_te,
        "masks_te": masks,
        "burn": burn,
        "te_sl_start": te_sl.start,
        "te_sl_stop": te_sl.stop,
        "train_rows": int(train_mask.sum()),
        "test_rows": int(len(y_te)),
        "n": n,
    }


def _permutation(model: Any, X_tr: np.ndarray, y_tr: np.ndarray, max_samples: int) -> dict[str, Any]:
    n = len(y_tr)
    if max_samples <= 0 or n < 600:
        return {"skipped": True, "reason": "insufficient_train_rows"}
    rng = np.random.default_rng(42)
    ix = rng.choice(np.arange(n), size=min(max_samples, n), replace=False)
    Xs = X_tr[ix]
    ys = y_tr[ix]
    r = permutation_importance(
        model,
        Xs,
        ys,
        n_repeats=6,
        random_state=0,
        n_jobs=-1,
        scoring="neg_log_loss",
    )
    order = np.argsort(r.importances_mean)
    names = list(TABULAR_COLS)
    top = [{"feature": names[i], "importance_mean": float(r.importances_mean[i]), "std": float(r.importances_std[i])} for i in order[-22:]]
    low = [{"feature": names[i], "importance_mean": float(r.importances_mean[i]), "std": float(r.importances_std[i])} for i in order[:8]]
    return {"top_positive_logloss_drop": top[::-1], "low_leverage": low}


def main() -> None:
    args = parse_args()
    settings = get_settings()
    csv = (args.historical_csv or "").strip() or settings.historical_csv_path
    csv_abs = _resolve(Path(csv))

    mtx = _matrices(csv_abs, args.burn_in)
    mp = _resolve(Path(args.model_path))

    report: dict[str, Any] = {
        "meta": {
            "n_total": mtx["n"],
            "train_rows": mtx["train_rows"],
            "test_rows": mtx["test_rows"],
            "historical_csv": str(csv_abs.resolve()),
        },
        "audit_stub": mtx["audit"],
        "splits": {
            "test_slice": [mtx["te_sl_start"], mtx["te_sl_stop"]],
            "burn_in_excluded_fit": mtx["burn"],
        },
    }

    if not mp.is_file():
        report["error"] = "model_missing"
        report["model_path"] = str(mp)
    else:
        art = joblib.load(mp)
        model = art.get("model")
        if model is None:
            report["error"] = "artifact_without_model"
        else:
            p_te = model.predict_proba(mtx["X_te"])
            report["model_path"] = str(mp.resolve())
            report["metrics_bundle"] = full_metric_bundle(mtx["y_te"], p_te, y_tr=mtx["y_tr"])
            report["metrics_by_segment"] = metrics_by_masks(mtx["y_te"], p_te, mtx["masks_te"])
            if args.permutation_importance:
                report["permutation_importance_log_loss"] = _permutation(
                    model, mtx["X_tr"], mtx["y_tr"], args.perm_samples
                )

    out_path = _resolve(Path(args.out_json))
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(report, indent=2, ensure_ascii=False))
    print(f"\n[{out_path}] escrito.", file=sys.stderr)


if __name__ == "__main__":
    main()
