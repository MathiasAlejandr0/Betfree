"""
Benchmark de cuotas históricas (Football-Data.co.uk CSV) vs resultado y vs modelo opcional.

  python -m src.predictor.odds_benchmark_cli --fd-csv data/open_datasets/E0_2425.csv
  python -m src.predictor.odds_benchmark_cli --fd-glob "data/open_datasets/fd_co_uk/2425/*.csv"

Salida: data/odds_benchmark_report.json
"""

from __future__ import annotations

import argparse
import json
import sys
from glob import glob
from pathlib import Path

import numpy as np
import pandas as pd

from src.predictor.fd_odds_parser import (
    closing_decimal_odds_row,
    implied_probs_normalized,
    load_fd_odds_frame,
    y_true_from_ftr,
)
from src.predictor.model_evaluation_metrics import multiclass_log_loss_stable

ROOT = Path(__file__).resolve().parents[2]


def _norm_team(s: str) -> str:
    return " ".join(str(s).lower().split())


def _bench_one_csv(path: Path, *, odds_prefer: str) -> dict:
    df = load_fd_odds_frame(str(path))
    y_list: list[int] = []
    p_market: list[list[float]] = []
    skipped = 0
    for _, row in df.iterrows():
        yt = y_true_from_ftr(row)
        oh, od_, oa = closing_decimal_odds_row(row, prefer=odds_prefer)
        if yt is None or oh is None or od_ is None or oa is None:
            skipped += 1
            continue
        ph, pd_, pa = implied_probs_normalized(oh, od_, oa)
        y_list.append(yt)
        p_market.append([ph, pd_, pa])
    if not y_list:
        return {"path": str(path), "error": "sin filas válidas", "skipped_rows": skipped}
    y = np.array(y_list, dtype=np.int64)
    pm = np.array(p_market, dtype=np.float64)
    ll = multiclass_log_loss_stable(y, pm)
    return {
        "path": str(path.resolve()),
        "n": int(len(y)),
        "skipped_rows": int(skipped),
        "market_log_loss": float(ll),
        "odds_prefer": odds_prefer,
    }


def _merge_model_probs(fd: pd.DataFrame, model_csv: Path) -> dict | None:
    m = pd.read_csv(model_csv, encoding="utf-8-sig", encoding_errors="replace")
    need = {"date", "home_team", "away_team", "ph", "pd", "pa"}
    miss = need - set(str(c).strip().lower() for c in m.columns)
    if miss:
        return {"error": f"modelo CSV faltan columnas {miss} (esperadas minúsculas date,home_team,away_team,ph,pd,pa)"}
    m = m.rename(columns={c: c.strip().lower() for c in m.columns})
    m["date"] = pd.to_datetime(m["date"], errors="coerce")
    fd2 = fd.copy()
    fd2["_hk"] = fd2["Date"].dt.normalize().astype(str) + "|" + fd2["HomeTeam"].map(_norm_team) + "|" + fd2["AwayTeam"].map(_norm_team)
    m["_hk"] = m["date"].dt.normalize().astype(str) + "|" + m["home_team"].map(_norm_team) + "|" + m["away_team"].map(_norm_team)
    idx = m.set_index("_hk")
    y_list: list[int] = []
    p_mod: list[list[float]] = []
    for _, row in fd2.iterrows():
        yt = y_true_from_ftr(row)
        if yt is None or row["_hk"] not in idx.index:
            continue
        mr = idx.loc[row["_hk"]]
        if isinstance(mr, pd.DataFrame):
            mr = mr.iloc[0]
        try:
            vec = [float(mr["ph"]), float(mr["pd"]), float(mr["pa"])]
        except (TypeError, ValueError, KeyError):
            continue
        s = sum(vec)
        if s <= 0:
            continue
        vec = [v / s for v in vec]
        y_list.append(yt)
        p_mod.append(vec)
    if len(y_list) < 30:
        return {"error": "muy pocas coincidencias modelo↔FD", "n": len(y_list)}
    y = np.array(y_list, dtype=np.int64)
    p = np.array(p_mod, dtype=np.float64)
    return {
        "n_matched_model": int(len(y)),
        "model_log_loss": float(multiclass_log_loss_stable(y, p)),
    }


def main() -> None:
    p = argparse.ArgumentParser(description="Benchmark cuotas Football-Data CSV")
    p.add_argument("--fd-csv", type=str, default="", help="un CSV")
    p.add_argument("--fd-glob", type=str, default="", help="glob, ej. data/open_datasets/fd_co_uk/2425/*.csv")
    p.add_argument("--odds-prefer", choices=("b365", "avg"), default="b365")
    p.add_argument("--model-csv", type=str, default="", help="opcional: probs modelo para mismos partidos")
    p.add_argument("--out", type=str, default="data/odds_benchmark_report.json")
    args = p.parse_args()

    paths: list[Path] = []
    if args.fd_csv.strip():
        paths.append(Path(args.fd_csv.strip()))
    if args.fd_glob.strip():
        root = repo_root()
        for g in glob(str(root / args.fd_glob.strip().lstrip("/\\"))):
            paths.append(Path(g))
    paths = [pth if pth.is_absolute() else (ROOT / pth) for pth in paths]
    paths = [p for p in paths if p.is_file()]
    if not paths:
        print("Indica --fd-csv o --fd-glob con archivos existentes.", file=sys.stderr)
        sys.exit(2)

    report: dict = {"files": []}
    for pth in paths:
        one = _bench_one_csv(pth, odds_prefer=str(args.odds_prefer))
        if args.model_csv.strip() and "error" not in one:
            try:
                fd = load_fd_odds_frame(str(pth))
                mc = Path(args.model_csv.strip())
                if not mc.is_absolute():
                    mc = ROOT / mc
                merge = _merge_model_probs(fd, mc)
                if merge:
                    one["model_compare"] = merge
            except Exception as exc:
                one["model_compare"] = {"error": str(exc)}
        report["files"].append(one)

    out = Path(args.out)
    if not out.is_absolute():
        out = ROOT / out
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(report, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
