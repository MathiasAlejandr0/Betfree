"""
Calibra mezcla ML E0 vs Poisson global (misma cronología que el digest).

  python -m src.predictor.e0_blend_calibration
  python -m src.predictor.e0_blend_calibration --multicomp SP1,G1,F1 --tail-frac-poisson 0.22

DIGEST_USE_BLEND_CALIBRATION_JSON=true aplica valores del JSON (si no pisás pesos explícitos en .env).

"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd

from src.predictor.chronological_features import build_e0_chronological_frames, split_ml_e0
from src.predictor.digest_roll_context import (
    ScheduledE0Pred,
    ScheduledPoissonAudit,
    merge_scheduled_by_day,
    run_chronological_poisson_simulation,
)
from src.predictor.e0_data_prep import load_clean_e0
from src.predictor.model_evaluation_metrics import full_metric_bundle, multiclass_log_loss_stable
from src.predictor.probability_blend import convex_blend_1x2

ROOT = Path(__file__).resolve().parents[2]

RES_ENCODING = {"H": 0, "D": 1, "A": 2}


def _prepare_global_sheet(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    need = {"date", "competition", "home_team", "away_team", "home_goals", "away_goals", "result_1x2"}
    miss = need - set(df.columns)
    if miss:
        raise ValueError(f"historical CSV faltan columnas {miss}")
    d = df.copy()
    d["home_team"] = d["home_team"].astype(str).str.strip()
    d["away_team"] = d["away_team"].astype(str).str.strip()
    d = d.loc[d["home_team"] != d["away_team"]].copy()
    d["home_goals"] = pd.to_numeric(d["home_goals"], errors="coerce")
    d["away_goals"] = pd.to_numeric(d["away_goals"], errors="coerce")
    d["result_1x2"] = d["result_1x2"].astype(str).str.strip().str.upper()
    bad = d["home_goals"].isna() | d["away_goals"].isna() | ~d["result_1x2"].isin(["H", "D", "A"])
    d = d.loc[~bad].copy()
    d["home_goals"] = d["home_goals"].astype(int)
    d["away_goals"] = d["away_goals"].astype(int)
    exp = np.where(
        d["home_goals"] > d["away_goals"],
        "H",
        np.where(d["home_goals"] < d["away_goals"], "A", "D"),
    )
    ok = exp == d["result_1x2"].astype(str).str.upper().values
    d = d.loc[ok].copy()
    d["date"] = pd.to_datetime(d["date"], errors="coerce")
    d = d.loc[d["date"].notna()].copy()
    d["_d"] = d["date"].dt.date
    return d.sort_values(["date", "competition", "home_team", "away_team"]).reset_index(drop=True)


def _ml_probas_matrix(estimator: Any, X: np.ndarray) -> np.ndarray:
    p = estimator.predict_proba(X)
    p = np.asarray(p, dtype=np.float64).reshape(len(X), -1)
    classes = np.asarray(getattr(estimator, "classes_", np.arange(p.shape[1]))).ravel()
    idx_map: dict[int, int] = {}
    for j, c in enumerate(classes):
        try:
            idx_map[int(c)] = j
        except (TypeError, ValueError):
            continue
    out = np.zeros((len(X), 3), dtype=np.float64)
    for k in (0, 1, 2):
        ij = idx_map.get(k)
        if ij is not None:
            out[:, k] = p[:, ij]
        elif p.shape[1] >= 3:
            out[:, k] = p[:, k]
    row_sums = out.sum(axis=1, keepdims=True)
    row_sums[row_sums <= 0] = 1.0
    return out / row_sums


def _multicomp_masks(
    global_df: pd.DataFrame,
    competitions: tuple[str, ...],
    *,
    frac: float,
    min_rows_holdout: int,
) -> dict[str, Any]:
    out: dict[str, Any] = {}
    frac = float(np.clip(frac, 0.05, 0.45))
    for comp_raw in competitions:
        comp_u = comp_raw.strip().upper()
        if not comp_u:
            continue
        idx = np.where(global_df["competition"].astype(str).str.upper() == comp_u)[0]
        if len(idx) < 400:
            out[comp_u] = {"skipped": True, "reason": "few_rows_global", "n": int(len(idx))}
            continue
        nh = max(min_rows_holdout, int(round(len(idx) * frac)))
        nh = min(int(nh), len(idx) - 50)
        hold_tail = idx[-nh:] if nh > 0 else idx[:0]
        if len(hold_tail) < min_rows_holdout:
            out[comp_u] = {"skipped": True, "reason": "holdout_small", "n_holdout": int(len(hold_tail))}
            continue
        mk = np.zeros(len(global_df), dtype=bool)
        mk[hold_tail] = True
        out[comp_u] = {"mask": mk, "n_holdout": int(len(hold_tail)), "n_total_comp": int(len(idx))}
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Calibrar mezcla ML E0 + Poisson cronológico")
    parser.add_argument("--historical", type=str, default="", help="CSV robusto (default repo data/open_datasets/historical_robust.csv)")
    parser.add_argument("--model", type=str, default="", help="joblib modelo E0 calibrado")
    parser.add_argument("--output", type=str, default="data/e0_blend_calibration.json")
    parser.add_argument("--blend-tune-rows", type=int, default=720)
    parser.add_argument("--blend-steps", type=int, default=15, help="n pesos ML en [0,1]")
    parser.add_argument(
        "--draw-grid",
        type=str,
        default="0.98,1.0,1.03,1.06,1.09,1.12",
        help="factor empate Poisson (coma)",
    )
    parser.add_argument("--multicomp", type=str, default="", help="ej. SP1,G1 auditoría Poisson cola cronológica")
    parser.add_argument("--tail-frac-poisson", type=float, default=0.2)
    parser.add_argument("--no-per-league", action="store_true", help="Poisson sólo estado global (como antes)")
    parser.add_argument("--min-rows-per-league", type=int, default=220, help="mín. partidos en CSV para estado aislado por liga")
    args = parser.parse_args()

    hist_raw = (
        ROOT / (args.historical.strip() or "data/open_datasets/historical_robust.csv")
    ).resolve()
    model_path = (ROOT / (args.model.strip() or "models/e0_expert_calibrated.pkl")).resolve()
    out_path = Path(args.output)
    if not out_path.is_absolute():
        out_path = (ROOT / out_path).resolve()

    clean, audit = load_clean_e0(hist_raw)
    _leg, df_tab, y_arr = build_e0_chronological_frames(clean, competition="E0", clip_roll=True)
    n_tot = len(y_arr)
    _, _, te_slice = split_ml_e0(n_tot)
    test_start = te_slice.start
    blend_tune_start = max(0, test_start - max(180, int(args.blend_tune_rows)))

    art = joblib.load(model_path)
    model = art.get("model")
    fnames = art.get("feature_names")
    burn_art = max(120, int(art.get("burn_in", 120)))
    burn = burn_art

    if model is None or not fnames:
        raise RuntimeError(f"Artefacto incompleto: {model_path}")
    cols = [str(x) for x in fnames]
    X = df_tab[cols].to_numpy(dtype=np.float64)
    ml_p_all = _ml_probas_matrix(model, X)

    global_df = _prepare_global_sheet(hist_raw)

    blend_tune_start = max(int(blend_tune_start), burn)

    e0_preds_by_day: defaultdict[Any, list[ScheduledE0Pred]] = defaultdict(list)
    for i_row in range(int(blend_tune_start), int(n_tot)):
        rr = clean.iloc[i_row]
        dd = pd.Timestamp(rr["date"]).date()
        e0_preds_by_day[dd].append(
            ScheduledE0Pred(
                ml_index=int(i_row),
                home=str(rr["home_team"]),
                away=str(rr["away_team"]),
                y_true=int(y_arr[i_row]),
            )
        )

    comps_report: tuple[str, ...] = tuple(c.strip().upper() for c in args.multicomp.split(",") if c.strip())
    comp_meta = _multicomp_masks(
        global_df,
        comps_report,
        frac=float(args.tail_frac_poisson),
        min_rows_holdout=80,
    )

    audit_preds_by_day: defaultdict[Any, list[ScheduledPoissonAudit]] = defaultdict(list)
    for comp_raw, blob in comp_meta.items():
        if blob.get("skipped"):
            continue
        mk = blob["mask"]
        for ix in np.flatnonzero(mk):
            row = global_df.iloc[int(ix)]
            dd = pd.Timestamp(row["date"]).date()
            res = RES_ENCODING[str(row["result_1x2"]).strip().upper()]
            audit_preds_by_day[dd].append(
                ScheduledPoissonAudit(
                    competition=str(comp_raw),
                    home=str(row["home_team"]).strip(),
                    away=str(row["away_team"]).strip(),
                    y_true=res,
                )
            )

    full_schedule = merge_scheduled_by_day(e0_preds_by_day, audit_preds_by_day)
    use_per_league = not bool(args.no_per_league)

    draw_grid = [float(x.strip().replace(",", ".")) for x in args.draw_grid.split(",") if x.strip()]
    if not draw_grid:
        draw_grid = [1.0]
    blend_ws = np.linspace(0.0, 1.0, max(7, int(args.blend_steps)))

    tune_idx = np.arange(blend_tune_start, test_start, dtype=np.int64)
    eval_idx_test = np.arange(test_start, n_tot, dtype=np.int64)

    best_ll_tune = np.inf
    best_w = 0.72
    best_df = 1.0
    tune_details: dict[str, Any] = {}

    for dcf in draw_grid:
        poi_e0_tune, _ = run_chronological_poisson_simulation(
            global_df,
            e0_preds_by_day,
            float(dcf),
            per_league=use_per_league,
            min_rows_per_league=int(args.min_rows_per_league),
        )
        inner_best = np.inf
        inner_w = 0.0
        for w in blend_ws:
            p_bl_list: list[tuple[float, float, float]] = []
            y_tun: list[int] = []
            for i in tune_idx:
                i = int(i)
                poi = poi_e0_tune[i]
                y_tun.append(int(y_arr[i]))
                p_bl_list.append(
                    convex_blend_1x2(
                        (float(ml_p_all[i, 0]), float(ml_p_all[i, 1]), float(ml_p_all[i, 2])),
                        poi,
                        float(w),
                    )
                )
            pbla = np.array(p_bl_list, dtype=np.float64)
            yta = np.array(y_tun, dtype=np.int64)
            ll = multiclass_log_loss_stable(yta, pbla)
            if ll < inner_best:
                inner_best = ll
                inner_w = float(w)

        tune_details[f"draw_{dcf}"] = {"best_ml_weight_inner": inner_w, "log_loss_tune": float(inner_best)}

        if inner_best < best_ll_tune:
            best_ll_tune = inner_best
            best_w = inner_w
            best_df = float(dcf)

    selection_note_blend = "min log-loss mezcla en ventana tune (pre-test)."

    if float(best_w) >= 0.995:
        poisson_tune_ll_map: dict[float, float] = {}
        stacked_y = np.array([int(y_arr[int(i)]) for i in tune_idx], dtype=np.int64)
        for dcf in draw_grid:
            poi_tmp, _ = run_chronological_poisson_simulation(
                global_df,
                e0_preds_by_day,
                float(dcf),
                per_league=use_per_league,
                min_rows_per_league=int(args.min_rows_per_league),
            )
            rows_po = []
            ok = True
            for ik in tune_idx:
                ik = int(ik)
                if ik not in poi_tmp:
                    ok = False
                    break
                pr = poi_tmp[ik]
                rows_po.append([float(pr[0]), float(pr[1]), float(pr[2])])
            if not ok:
                continue
            pmat = np.array(rows_po, dtype=np.float64)
            poisson_tune_ll_map[float(dcf)] = multiclass_log_loss_stable(stacked_y, pmat)
        if poisson_tune_ll_map:
            best_df = sorted(
                poisson_tune_ll_map,
                key=lambda d: (
                    poisson_tune_ll_map[d],
                    abs(float(d) - 1.0),
                ),
            )[0]
            selection_note_blend = (
                "ML ~100% tune: recomendamos draw-factor por Poisson puro sobre la misma ventana; blend elegido igual."
            )

    poi_e0_fin, poi_audit_fin = run_chronological_poisson_simulation(
        global_df,
        full_schedule,
        best_df,
        per_league=use_per_league,
        min_rows_per_league=int(args.min_rows_per_league),
    )

    def _gather(idx_list: np.ndarray, poi_map: dict[int, tuple[float, float, float]], w_ml: float) -> tuple[np.ndarray, dict[str, np.ndarray]]:
        yv = []
        pm = []
        pp = []
        for i_raw in idx_list:
            i = int(i_raw)
            yv.append(int(y_arr[i]))
            mr = ml_p_all[i]
            prow = poi_map[i]
            pm.append(
                convex_blend_1x2(
                    (float(mr[0]), float(mr[1]), float(mr[2])), prow, float(w_ml)
                )
            )
            pp.append(tuple(prow))
        return np.array(yv, dtype=np.int64), {
            "ml_blend": np.array(pm, dtype=np.float64),
            "poisson_global": np.array(pp, dtype=np.float64),
        }

    yt_test, pack_test = _gather(eval_idx_test, poi_e0_fin, best_w)
    p_blend = pack_test["ml_blend"]
    p_poisson_only = pack_test["poisson_global"]
    yt_tune, pack_tune = _gather(tune_idx, poi_e0_fin, best_w)
    p_tune_blend = pack_tune["ml_blend"]

    multicomp_out: dict[str, Any] = {}
    if comps_report:
        for comp in comps_report:
            cm = comp_meta.get(comp)
            if not cm or cm.get("skipped"):
                multicomp_out[comp] = cm if cm else {"skipped": True}
                continue
            acc = poi_audit_fin.get(comp, [])
            if not acc:
                multicomp_out[comp] = {"skipped": True, "reason": "no_audit_rows"}
                continue
            yv = np.array([a[0] for a in acc], dtype=np.int64)
            pv = np.array([list(a[1]) for a in acc], dtype=np.float64)
            multicomp_out[comp] = {
                "n": int(len(yv)),
                "log_loss_poisson_global": multiclass_log_loss_stable(yv, pv),
                "metrics_bundle_poisson_global": full_metric_bundle(yv, pv, y_tr=None),
                "audit_draw_factor_used": float(best_df),
                "fraction_tail": float(args.tail_frac_poisson),
            }

    tr_reference = np.asarray(y_arr[:test_start])

    report: dict[str, Any] = {
        "historical_csv": str(hist_raw),
        "model_path": str(model_path),
        "e0_audit_nested": audit,
        "method": (
            "dia-calendario: preds antes de observar ese dia; Poisson por liga si CSV y min_rows;"
            " tune ultimos ~N rows pre-test; multicomp opcional auditoria Poisson sobre cola."
        ),
        "per_league_poisson": bool(use_per_league),
        "min_rows_per_league": int(args.min_rows_per_league),
        "blend_draw_selection_notes": selection_note_blend,
        "split_notes": {"test_slice_python_exclusive_end": [int(test_start), int(n_tot)], "burn_in_exclude_scheduling": burn},
        "recommended_ml_blend_weight": float(best_w),
        "recommended_poisson_draw_factor": float(best_df),
        "tune_blend_window_indices_exclusive_end": [int(blend_tune_start), int(test_start)],
        "tune_log_loss": float(best_ll_tune),
        "tune_detail_by_draw": tune_details,
        "metrics_blended_test": full_metric_bundle(yt_test, p_blend, y_tr=tr_reference),
        "metrics_ml_only_test": full_metric_bundle(yt_test, ml_p_all[eval_idx_test], y_tr=tr_reference),
        "metrics_poisson_global_only_test": full_metric_bundle(yt_test, p_poisson_only, y_tr=tr_reference),
        "multicomp_poisson_tail_holdout": multicomp_out if comps_report else {},
        "verification_tune_log_loss_recomputed_best_params": float(
            multiclass_log_loss_stable(yt_tune, p_tune_blend)
        ),
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")

    print(json.dumps(report, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
