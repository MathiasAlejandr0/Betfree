"""
Pipeline experto E0: limpieza, warm-up temporal, TSCV, anti-overfitting, calibracion, artefacto final.

  python -m src.predictor.e0_expert_train
  python -m src.predictor.e0_expert_train --full-search
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
from sklearn.base import clone
from sklearn.calibration import CalibratedClassifierCV
from sklearn.ensemble import HistGradientBoostingClassifier, RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, log_loss
from sklearn.model_selection import RandomizedSearchCV, TimeSeriesSplit, cross_val_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from src.predictor.chronological_features import build_e0_chronological_frames, split_ml_e0
from src.predictor.e0_data_prep import load_clean_e0
from src.predictor.model_evaluation_metrics import full_metric_bundle, metrics_by_masks

ROOT = Path(__file__).resolve().parents[2]

TABULAR_COLS = [
    "elo_home",
    "elo_away",
    "elo_margin",
    "elo_win_expect",
    "gf_roll_5_home",
    "ga_roll_5_home",
    "pts_roll_5_home",
    "gf_roll_10_home",
    "ga_roll_10_home",
    "pts_roll_10_home",
    "gf_roll_5_away",
    "ga_roll_5_away",
    "pts_roll_5_away",
    "gf_roll_10_away",
    "ga_roll_10_away",
    "pts_roll_10_away",
    "rest_days_home",
    "rest_days_away",
    "matches_last_7d_home",
    "matches_last_7d_away",
    "competition_hash",
]


def _log_loss(y: np.ndarray, p: np.ndarray) -> float:
    return float(log_loss(y, p, labels=[0, 1, 2]))


def _multiclass_brier(y: np.ndarray, p: np.ndarray) -> float:
    oh = np.zeros_like(p)
    oh[np.arange(len(y)), y] = 1.0
    return float(np.mean(np.sum((p - oh) ** 2, axis=1)))


def _recency_weights(n: int, tau: float) -> np.ndarray | None:
    """Exponente negativo en filas antiguas; la última fila del train pesa más."""
    tau = float(tau)
    if tau <= 0 or n <= 0:
        return None
    i = np.arange(n, dtype=np.float64)
    w = np.exp((i - (n - 1)) / tau)
    return (w / w.mean()).astype(np.float64, copy=False)


def _build_matrices(
    *,
    burn_in: int,
) -> tuple[pd.DataFrame, np.ndarray, np.ndarray, dict]:
    hist = ROOT / "data/open_datasets/historical_robust.csv"
    clean, audit = load_clean_e0(hist)
    _df_leg, df_tab, y = build_e0_chronological_frames(clean, competition="E0", clip_roll=True)
    meta = {"audit": audit, "burn_in": burn_in, "n_rows": len(y)}
    seasons = df_tab["season_year"].to_numpy(dtype=np.int64, copy=False)
    return df_tab[TABULAR_COLS], y, seasons, meta


def _pick_stable_model(
    scores: list[tuple[str, float, float]],
    *,
    epsilon: float = 0.006,
) -> tuple[str, Any]:
    """Elige entre candidatos con CV log-loss similar el de menor varianza (mas estable)."""
    scores = sorted(scores, key=lambda t: t[1])
    best_m = scores[0][1]
    close = [t for t in scores if t[1] <= best_m + epsilon]
    name, _, _ = min(close, key=lambda t: (t[2], t[1]))
    templates = dict(_candidate_models())
    return name, clone(templates[name])


def _cv_log_loss(estimator: Any, X: np.ndarray, y: np.ndarray, cv: int) -> tuple[float, float]:
    tscv = TimeSeriesSplit(n_splits=cv)
    scores = cross_val_score(
        estimator,
        X,
        y,
        cv=tscv,
        scoring="neg_log_loss",
        n_jobs=-1,
    )
    losses = -scores
    return float(np.mean(losses)), float(np.std(losses))


def _candidate_models(random_state: int = 42) -> list[tuple[str, Any]]:
    lr = Pipeline(
        [
            ("scaler", StandardScaler()),
            (
                "clf",
                LogisticRegression(
                    max_iter=800,
                    C=0.15,
                    solver="lbfgs",
                    class_weight="balanced",
                    random_state=random_state,
                ),
            ),
        ]
    )
    hgb = HistGradientBoostingClassifier(
        loss="log_loss",
        learning_rate=0.04,
        max_depth=5,
        max_iter=400,
        min_samples_leaf=60,
        l2_regularization=2.0,
        early_stopping=True,
        validation_fraction=0.12,
        n_iter_no_change=25,
        class_weight="balanced",
        random_state=random_state,
    )
    hgb_mid = HistGradientBoostingClassifier(
        loss="log_loss",
        learning_rate=0.03,
        max_depth=6,
        max_iter=500,
        min_samples_leaf=45,
        l2_regularization=1.0,
        early_stopping=True,
        validation_fraction=0.12,
        n_iter_no_change=20,
        class_weight="balanced",
        random_state=random_state + 1,
    )
    rf = RandomForestClassifier(
        n_estimators=180,
        max_depth=9,
        min_samples_leaf=35,
        max_features="sqrt",
        class_weight="balanced_subsample",
        random_state=random_state,
        n_jobs=-1,
    )
    return [
        ("logreg_strong", lr),
        ("hgb_conservative", hgb),
        ("hgb_mid", hgb_mid),
        ("rf_shallow", rf),
    ]


def _hgb_search_space(random_state: int) -> RandomizedSearchCV:
    base = HistGradientBoostingClassifier(
        loss="log_loss",
        early_stopping=True,
        validation_fraction=0.12,
        n_iter_no_change=22,
        class_weight="balanced",
        random_state=random_state,
    )
    param_dist = {
        "learning_rate": [0.02, 0.03, 0.045, 0.06],
        "max_depth": [4, 5, 6, 7],
        "max_iter": [350, 500, 650],
        "min_samples_leaf": [35, 50, 70, 100],
        "l2_regularization": [0.5, 1.0, 2.0, 5.0, 10.0],
    }
    return RandomizedSearchCV(
        base,
        param_distributions=param_dist,
        n_iter=28,
        cv=TimeSeriesSplit(n_splits=4),
        scoring="neg_log_loss",
        random_state=random_state,
        n_jobs=-1,
        verbose=0,
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--burn-in",
        type=int,
        default=120,
        help="Partidos iniciales solo para estado ELO/roll, no para entrenar.",
    )
    parser.add_argument(
        "--cv-splits",
        type=int,
        default=4,
        help="Pliegues temporales para seleccion de modelo.",
    )
    parser.add_argument(
        "--full-search",
        action="store_true",
        help="RandomizedSearchCV amplio sobre HistGradientBoosting.",
    )
    parser.add_argument(
        "--calib-cv",
        type=int,
        default=3,
        help="Pliegues temporales internos para CalibratedClassifierCV (sklearn>=1.8).",
    )
    parser.add_argument(
        "--recency-tau-matches",
        type=float,
        default=0.0,
        help=(
            "Si >0: sample_weight exponencial sobre el conjunto temporal de entrenamiento (mayor tau = menos contraste). "
            "Ej. 400-900 para Premier larga."
        ),
    )
    args = parser.parse_args()

    X_df, y, seasons, meta = _build_matrices(burn_in=args.burn_in)
    X = X_df.to_numpy(dtype=np.float64, copy=False)
    n = len(y)
    tr_sl, _ca_sl, te_sl = split_ml_e0(n)
    train_end = te_sl.start
    burn = max(0, min(args.burn_in, train_end - 400))

    train_mask = np.zeros(n, dtype=bool)
    train_mask[burn:train_end] = True
    test_mask = np.zeros(n, dtype=bool)
    test_mask[te_sl.start : te_sl.stop] = True

    X_tr = X[train_mask]
    y_tr = y[train_mask]
    X_te = X[test_mask]
    y_te = y[test_mask]

    sw_fit = _recency_weights(len(X_tr), args.recency_tau_matches)
    fit_kw: dict[str, Any] = {"sample_weight": sw_fit} if sw_fit is not None else {}

    report: dict[str, Any] = {
        "meta": meta,
        "split": {
            "burn_in_excluded_from_fit": burn,
            "train_rows": int(len(X_tr)),
            "test_rows": int(len(X_te)),
            "test_slice": [te_sl.start, te_sl.stop],
            "calibration": f"CalibratedClassifierCV(isotonic, cv=TimeSeriesSplit({args.calib_cv}))",
            "recency_tau_matches": float(args.recency_tau_matches),
            "sample_weight_recency_applied": sw_fit is not None,
        },
        "candidates_cv": {},
    }

    cv_splits = max(3, min(args.cv_splits, len(X_tr) // 400))

    if args.full_search:
        search = _hgb_search_space(42)
        search.fit(X_tr, y_tr, **fit_kw)
        report["random_search"] = {
            "best_params": search.best_params_,
            "best_cv_log_loss": float(-search.best_score_),
        }
        best_name = "hgb_random_search"
        template = HistGradientBoostingClassifier(
            loss="log_loss",
            early_stopping=True,
            validation_fraction=0.12,
            n_iter_no_change=22,
            class_weight="balanced",
            random_state=42,
            **search.best_params_,
        )
    else:
        scored: list[tuple[str, float, float]] = []
        for name, est in _candidate_models():
            mean_ll, std_ll = _cv_log_loss(est, X_tr, y_tr, cv=cv_splits)
            report["candidates_cv"][name] = {"mean_log_loss": mean_ll, "std_log_loss": std_ll}
            scored.append((name, mean_ll, std_ll))
        best_name, template = _pick_stable_model(scored)
        report["selection_rule"] = (
            "Entre modelos con mean log-loss dentro de 0.012 del minimo, se elige el de menor std (CV temporal)."
        )

    base_uncal = clone(template)
    base_uncal.fit(X_tr, y_tr, **fit_kw)
    pre_cal_train_tail = _log_loss(
        y_tr[-min(600, len(X_tr)) :],
        base_uncal.predict_proba(X_tr[-min(600, len(X_tr)) :]),
    )
    pre_cal_test = _log_loss(y_te, base_uncal.predict_proba(X_te))

    calib_cv = max(2, min(args.calib_cv, len(X_tr) // 500))
    tscv_cal = TimeSeriesSplit(n_splits=calib_cv)
    final = CalibratedClassifierCV(
        clone(template),
        method="isotonic",
        cv=tscv_cal,
    )
    final.fit(X_tr, y_tr, **fit_kw)
    p_test = final.predict_proba(X_te)
    p_tail = final.predict_proba(X_tr[-min(600, len(X_tr)) :])
    y_tail = y_tr[-min(600, len(X_tr)) :]

    post_cal_test_ll = _log_loss(y_te, p_test)
    post_cal_train_tail_ll = _log_loss(y_tail, p_tail)
    gap = post_cal_train_tail_ll - post_cal_test_ll

    report["selected_model"] = best_name if not args.full_search else "hgb_random_search"
    report["metrics"] = {
        "uncalibrated_test_log_loss": pre_cal_test,
        "uncalibrated_tail_train_log_loss": pre_cal_train_tail,
        "calibrated_test_log_loss": post_cal_test_ll,
        "calibrated_tail_train_log_loss": post_cal_train_tail_ll,
        "generalization_gap_tail_train_minus_test": gap,
        "test_accuracy": float(accuracy_score(y_te, np.argmax(p_test, axis=1))),
        "test_brier_multiclass": _multiclass_brier(y_te, p_test),
    }

    seasons_te = seasons[test_mask]
    masks_te: dict[str, np.ndarray] = {
        f"test_season_{int(yr)}": seasons_te == yr for yr in sorted(np.unique(seasons_te))
    }
    report["evaluation_suite"] = full_metric_bundle(y_te, p_test, y_tr=y_tr)
    report["evaluation_by_season_on_test"] = metrics_by_masks(y_te, p_test, masks_te)

    ml_ref = ROOT / "data/train_metrics_ml_e0.json"
    if ml_ref.is_file():
        j = json.loads(ml_ref.read_text(encoding="utf-8"))
        ne = j.get("new_pipeline") or j
        report["reference_ml_e0_json"] = {
            "log_loss_test": ne.get("log_loss_test"),
            "accuracy_test": ne.get("accuracy_test"),
        }
        if ne.get("log_loss_test"):
            report["delta_vs_reference_log_loss"] = round(
                post_cal_test_ll - float(ne["log_loss_test"]), 6
            )

    model_path = ROOT / "models/e0_expert_calibrated.pkl"
    artifact = {
        "model": final,
        "feature_names": TABULAR_COLS,
        "classes": [0, 1, 2],
        "labels": ["H", "D", "A"],
        "pipeline_version": "e0_expert_train_v2_calib_tscv",
        "burn_in": burn,
        "train_recency_tau_matches": float(args.recency_tau_matches),
        "recommended_digest_ml_blend_weight": 0.72,
    }
    model_path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(artifact, model_path)

    report_path = ROOT / "data/e0_expert_report.json"
    diag_path = ROOT / "data/model_diagnostic_e0.json"
    manifest_path = ROOT / "models/e0_training_manifest.json"

    from src.predictor.training_manifest import build_e0_training_manifest, write_training_manifest

    audit = meta.get("audit") or {}
    hist_csv = Path(str(audit.get("source", ROOT / "data/open_datasets/historical_robust.csv")))
    tll = report.get("metrics", {}).get("calibrated_test_log_loss")
    manifest = build_e0_training_manifest(
        model_path=model_path,
        historical_csv=hist_csv,
        report_meta={"audit": audit, "n_rows": int(meta.get("n_rows", 0)), "burn_in": int(meta.get("burn_in", 0))},
        test_log_loss=float(tll) if tll is not None else None,
    )
    write_training_manifest(manifest_path, manifest)

    report["artifacts"] = {
        "model_path": str(model_path.resolve()),
        "report_path": str(report_path.resolve()),
        "diagnostic_json": str(diag_path.resolve()),
        "training_manifest": str(manifest_path.resolve()),
    }
    diag_path.write_text(
        json.dumps(
            {
                "source": "e0_expert_train_run",
                "metrics_bundle": report["evaluation_suite"],
                "metrics_by_segment": report["evaluation_by_season_on_test"],
                "artifacts": report["artifacts"],
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")

    print(json.dumps(report, indent=2, ensure_ascii=False))
    if gap > 0.08:
        print(
            "\n[aviso] Brecha train tail vs test elevada; revisar regularizacion.",
            file=sys.stderr,
        )
    if post_cal_test_ll > pre_cal_test + 0.005:
        print(
            "\n[aviso] La calibracion empeoro ligeramente el log-loss en test (posible varianza de pocos pliegues).",
            file=sys.stderr,
        )


if __name__ == "__main__":
    main()
