"""Monitor basico de salud del modelo."""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timezone

from src.config import repo_root

LOGGER = logging.getLogger(__name__)


def run_monitor(
    db_path: str,
    lookback_days: int = 7,
    min_bets: int = 5,
    roi_threshold: float = -0.05,
    critical_roi_threshold: float = -0.12,
    notify_telegram: bool = False,
) -> dict:
    del lookback_days, notify_telegram
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            """
            SELECT COUNT(*) AS bets, COALESCE(SUM(won), 0) AS wins, COALESCE(SUM(pnl), 0.0) AS pnl
            FROM alert_settlements
            """
        ).fetchone()
    except sqlite3.OperationalError:
        row = None
    finally:
        conn.close()

    settlements_missing = row is None
    if row is None:
        bets, wins, roi = 0, 0, 0.0
    else:
        bets = int(row["bets"] or 0)
        wins = int(row["wins"] or 0)
        roi = (float(row["pnl"] or 0.0) / bets) if bets > 0 else 0.0
    severity = "none"
    status = "ok"
    if bets >= min_bets and roi <= critical_roi_threshold:
        severity = "critical"
        status = "breach"
    elif bets >= min_bets and roi <= roi_threshold:
        severity = "warning"
        status = "breach"
    LOGGER.info("Health monitor | status=%s | severity=%s | bets=%s | wins=%s | roi=%.4f", status, severity, bets, wins, roi)
    payload: dict[str, float | int | str | dict[str, float | None]] = {
        "day_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "status": status,
        "severity": severity,
        "bets": bets,
        "wins": wins,
        "roi": roi,
        "evaluation_snapshot": {},
    }

    snap_eval: dict[str, object] = {}

    diag_path = repo_root() / "data" / "model_diagnostic_e0.json"
    if diag_path.is_file():
        try:
            j = json.loads(diag_path.read_text(encoding="utf-8"))
            bundle = j.get("metrics_bundle") or {}
            ece_blob = bundle.get("calibration_max_confidence") or {}
            snap_eval.update(
                {
                    "log_loss_holdout": bundle.get("log_loss"),
                    "brier": bundle.get("brier_multiclass"),
                    "accuracy": bundle.get("accuracy"),
                    "ece_max_confidence": ece_blob.get("ece_max_confidence") if isinstance(ece_blob, dict) else None,
                }
            )
            LOGGER.info(
                "Diagnostico modelo E0 (si existe artefacto) | log_loss=%s | acc=%s | ece_mc=%s | fuente=%s",
                snap_eval.get("log_loss_holdout"),
                snap_eval.get("accuracy"),
                snap_eval.get("ece_max_confidence"),
                diag_path,
            )
        except (json.JSONDecodeError, OSError) as exc:
            LOGGER.debug("Sin lectura de diagnostico: %s %s", diag_path, exc)

    cal_path = repo_root() / "data" / "e0_blend_calibration.json"
    if cal_path.is_file():
        try:
            cj = json.loads(cal_path.read_text(encoding="utf-8"))
            mbt = cj.get("metrics_blended_test")
            blended = mbt if isinstance(mbt, dict) else {}
            snap_eval["blend_calibration"] = {
                "recommended_ml_blend_weight": cj.get("recommended_ml_blend_weight"),
                "recommended_poisson_draw_factor": cj.get("recommended_poisson_draw_factor"),
                "test_rows": blended.get("test_rows"),
                "log_loss_blend_test": blended.get("log_loss"),
            }
        except (json.JSONDecodeError, OSError) as exc:
            LOGGER.debug("Sin lectura de calibracion mezcla: %s %s", cal_path, exc)

    odds_rep = repo_root() / "data" / "odds_benchmark_report.json"
    if odds_rep.is_file():
        try:
            oj = json.loads(odds_rep.read_text(encoding="utf-8"))
            files = oj.get("files") if isinstance(oj.get("files"), list) else []
            mll = [f.get("market_log_loss") for f in files if isinstance(f, dict) and f.get("market_log_loss") is not None]
            snap_eval["odds_benchmark_fd"] = {
                "n_files": len(files),
                "market_log_loss_mean": float(sum(float(x) for x in mll) / len(mll)) if mll else None,
            }
        except (json.JSONDecodeError, OSError, TypeError, ZeroDivisionError) as exc:
            LOGGER.debug("Sin lectura odds_benchmark: %s %s", odds_rep, exc)

    if snap_eval:
        payload["evaluation_snapshot"] = snap_eval  # type: ignore[assignment]
    if settlements_missing:
        payload["note"] = "alert_settlements ausente; ejecutá init_db con la versión actual del código."

    return payload
