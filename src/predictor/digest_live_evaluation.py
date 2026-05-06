"""
Evaluación post-digest: cruza `digest_prediction_audit` con el CSV histórico por fecha + equipos.

No sustituye un backtest temporal del modelo E0; mide calidad operativa del digest (Poisson/ML mezclado)
cuando el partido ya está en el histórico con resultado.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from src.config import bootstrap_dotenv, get_settings, repo_root
from src.predictor.csv_roll_state import norm_team
from src.predictor.model_evaluation_metrics import multiclass_log_loss_stable
from src.storage.database import get_connection

LOG = logging.getLogger(__name__)

MODEL_USED_DIGEST_LIVE_EVAL = "digest_live_evaluation"


def _result_to_y1x2(h: int, a: int) -> int:
    if h > a:
        return 0
    if h < a:
        return 2
    return 1


def _norm_pair(h: str, a: str) -> tuple[str, str]:
    return norm_team(h), norm_team(a)


def build_historical_lookup(csv_path: Path) -> dict[tuple[str, str, str], int]:
    """Clave (date_iso, nhome, naway) -> clase 0/1/2 (H/D/A visita)."""
    df = pd.read_csv(csv_path)
    need = {"date", "home_team", "away_team", "home_goals", "away_goals"}
    if not need.issubset(df.columns):
        raise ValueError(f"CSV histórico incompleto: faltan {need - set(df.columns)}")
    d = df.copy()
    d["date"] = pd.to_datetime(d["date"], errors="coerce")
    d = d.loc[d["date"].notna()].copy()
    d["iso"] = d["date"].dt.strftime("%Y-%m-%d")
    d["hg"] = pd.to_numeric(d["home_goals"], errors="coerce")
    d["ag"] = pd.to_numeric(d["away_goals"], errors="coerce")
    d = d.loc[d["hg"].notna() & d["ag"].notna()].copy()
    out: dict[tuple[str, str, str], int] = {}
    for _, r in d.iterrows():
        key = (str(r["iso"]), *_norm_pair(str(r["home_team"]), str(r["away_team"])))
        out[key] = _result_to_y1x2(int(r["hg"]), int(r["ag"]))
    return out


@dataclass
class AuditRow:
    event_id: int
    local_date_iso: str
    home_team: str
    away_team: str
    ph: float
    pd: float
    pa: float
    used_ml: bool


def load_audit_rows(db_path: str, *, since_iso: str | None, until_iso: str | None) -> list[AuditRow]:
    conn = get_connection(db_path)
    try:
        q = (
            "SELECT event_id, local_date_iso, home_team, away_team, ph, pd, pa, used_ml "
            "FROM digest_prediction_audit WHERE 1=1"
        )
        params: list[Any] = []
        if since_iso:
            q += " AND local_date_iso >= ?"
            params.append(since_iso)
        if until_iso:
            q += " AND local_date_iso <= ?"
            params.append(until_iso)
        q += " ORDER BY id DESC"
        cur = conn.execute(q, params)
        rows: list[AuditRow] = []
        seen: set[int] = set()
        for r in cur.fetchall():
            eid = int(r["event_id"])
            if eid in seen:
                continue
            seen.add(eid)
            rows.append(
                AuditRow(
                    event_id=eid,
                    local_date_iso=str(r["local_date_iso"]),
                    home_team=str(r["home_team"]),
                    away_team=str(r["away_team"]),
                    ph=float(r["ph"]),
                    pd=float(r["pd"]),
                    pa=float(r["pa"]),
                    used_ml=bool(int(r["used_ml"])),
                )
            )
        rows.reverse()
        return rows
    finally:
        conn.close()


def evaluate_digest_vs_historical(
    *,
    db_path: str,
    csv_path: Path,
    since_iso: str | None = None,
    until_iso: str | None = None,
    min_matched: int = 15,
    warn_log_loss: float = 1.08,
) -> dict[str, Any]:
    lookup = build_historical_lookup(csv_path)
    audits = load_audit_rows(db_path, since_iso=since_iso, until_iso=until_iso)
    ys: list[int] = []
    ps: list[list[float]] = []
    matched_meta: list[dict[str, Any]] = []
    unmatched = 0
    for a in audits:
        key = (a.local_date_iso, *_norm_pair(a.home_team, a.away_team))
        y = lookup.get(key)
        if y is None:
            key_rev = (a.local_date_iso, *_norm_pair(a.away_team, a.home_team))
            y = lookup.get(key_rev)
        if y is None:
            unmatched += 1
            continue
        p = np.array([a.ph, a.pd, a.pa], dtype=np.float64)
        s = float(p.sum())
        if s <= 0:
            unmatched += 1
            continue
        p = p / s
        ys.append(y)
        ps.append([float(p[0]), float(p[1]), float(p[2])])
        matched_meta.append({"event_id": a.event_id, "date": a.local_date_iso, "used_ml": a.used_ml})

    n = len(ys)
    report: dict[str, Any] = {
        "schema": "betfree.digest_live_evaluation.v1",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "db_path": db_path,
        "historical_csv": str(csv_path.resolve()),
        "audit_rows": len(audits),
        "matched": n,
        "unmatched_or_invalid": unmatched,
        "since_iso": since_iso,
        "until_iso": until_iso,
    }
    if n == 0:
        report["status"] = "no_data"
        report["note"] = "Sin cruces audit↔CSV (¿digest nunca corrido o fechas/equipos distintos al CSV?)."
        return report

    y_arr = np.array(ys, dtype=np.int64)
    p_arr = np.array(ps, dtype=np.float64)
    ll = float(multiclass_log_loss_stable(y_arr, p_arr))
    acc = float(np.mean(np.argmax(p_arr, axis=1) == y_arr))
    report["metrics"] = {"n": n, "multiclass_log_loss": ll, "accuracy_argmax": acc}
    report["thresholds"] = {"min_matched": min_matched, "warn_log_loss": warn_log_loss}
    issues: list[str] = []
    if n >= min_matched and ll > warn_log_loss:
        issues.append(f"log_loss={ll:.4f} > umbral {warn_log_loss} (n={n})")
    report["issues"] = issues
    report["status"] = "breach" if issues else "ok"

    by_ml = {"true": {"idx": []}, "false": {"idx": []}}
    for i, m in enumerate(matched_meta):
        k = "true" if m["used_ml"] else "false"
        by_ml[k]["idx"].append(i)
    for k, blob in by_ml.items():
        idx = blob["idx"]
        if len(idx) >= max(8, min_matched // 3):
            yi, pi = y_arr[idx], p_arr[idx]
            report.setdefault("by_used_ml", {})[k] = {
                "n": len(idx),
                "log_loss": float(multiclass_log_loss_stable(yi, pi)),
                "accuracy": float(np.mean(np.argmax(pi, axis=1) == yi)),
            }
    return report


def run_and_write_digest_live_evaluation(
    *,
    root: Path,
    db_path: str,
    csv_path: Path,
    since_days: int,
    min_matched: int,
    warn_log_loss: float,
    out_relative: str = "data/digest_live_evaluation.json",
) -> dict[str, Any]:
    """Ejecuta evaluación, escribe JSON bajo `root` y devuelve el dict del reporte."""
    db = db_path.strip()
    if not Path(db).is_absolute():
        db = str((root / db).resolve())
    csv_p = csv_path if csv_path.is_absolute() else (root / csv_path).resolve()
    until_d = date.today()
    since_d = until_d - timedelta(days=max(1, int(since_days)))
    rep = evaluate_digest_vs_historical(
        db_path=db,
        csv_path=csv_p,
        since_iso=since_d.isoformat(),
        until_iso=until_d.isoformat(),
        min_matched=int(min_matched),
        warn_log_loss=float(warn_log_loss),
    )
    raw_out = (out_relative or "").strip() or "data/digest_live_evaluation.json"
    out = Path(raw_out) if Path(raw_out).is_absolute() else (root / raw_out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(rep, indent=2, ensure_ascii=False), encoding="utf-8")
    LOG.info("digest_live_evaluation escrito → %s (status=%s matched=%s)", out, rep.get("status"), rep.get("matched"))
    return rep


def record_digest_eval_breach_alert(
    repo: Any,
    report: dict[str, Any],
    *,
    enabled: bool,
) -> None:
    """Si `enabled` y el reporte es breach, inserta una fila en `model_health_alerts` (sin duplicar el mismo día UTC)."""
    if not enabled or report.get("status") != "breach":
        return
    metrics = report.get("metrics") or {}
    ll_raw = metrics.get("multiclass_log_loss")
    th_raw = (report.get("thresholds") or {}).get("warn_log_loss", 1.08)
    try:
        ll = float(ll_raw)
        th = float(th_raw)
    except (TypeError, ValueError):
        return
    severity = "critical" if ll > th * 1.12 else "warning"
    day_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if repo.count_unresolved_health_alerts(day_utc, MODEL_USED_DIGEST_LIVE_EVAL) > 0:
        LOG.debug("digest_live_evaluation: ya hay alerta abierta hoy para %s", MODEL_USED_DIGEST_LIVE_EVAL)
        return
    repo.insert_model_health_alert(
        day_utc=day_utc,
        model_used=MODEL_USED_DIGEST_LIVE_EVAL,
        severity=severity,
        roi=ll,
    )
    LOG.warning(
        "digest_live_evaluation breach registrado en model_health_alerts | day_utc=%s ll=%.4f severity=%s",
        day_utc,
        ll,
        severity,
    )


def main() -> None:
    bootstrap_dotenv()
    p = argparse.ArgumentParser(description="Evalúa predicciones del digest vs CSV histórico")
    p.add_argument("--db-path", default="")
    p.add_argument("--csv-path", default="")
    p.add_argument("--since-days", type=int, default=120)
    p.add_argument("--out-json", default="data/digest_live_evaluation.json")
    p.add_argument("--min-matched", type=int, default=15)
    p.add_argument("--warn-log-loss", type=float, default=1.08)
    args = p.parse_args()

    settings = get_settings()
    root = repo_root()
    db = (args.db_path or "").strip() or settings.sqlite_db_path
    if not Path(db).is_absolute():
        db = str((root / db).resolve())
    csv_p = Path((args.csv_path or "").strip() or settings.historical_csv_path)
    if not csv_p.is_absolute():
        csv_p = (root / csv_p).resolve()

    rep = run_and_write_digest_live_evaluation(
        root=root,
        db_path=db,
        csv_path=csv_p,
        since_days=int(args.since_days),
        min_matched=int(args.min_matched),
        warn_log_loss=float(args.warn_log_loss),
        out_relative=(args.out_json or "").strip() or "data/digest_live_evaluation.json",
    )
    raw_out = (args.out_json or "").strip() or "data/digest_live_evaluation.json"
    out = Path(raw_out) if Path(raw_out).is_absolute() else (root / raw_out)
    print(json.dumps(rep, indent=2, ensure_ascii=False))
    print(f"\nEscrito: {out}", file=sys.stderr)
    if rep.get("status") == "breach":
        sys.exit(3)


if __name__ == "__main__":
    main()
