"""Manifiesto de reproducibilidad tras entrenar (versiones + huella de datos)."""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _file_sha256(path: Path, *, max_bytes: int = 80_000_000) -> str | None:
    if not path.is_file():
        return None
    h = hashlib.sha256()
    total = 0
    with path.open("rb") as f:
        while True:
            chunk = f.read(1 << 20)
            if not chunk:
                break
            total += len(chunk)
            if total > max_bytes:
                h.update(f"...truncado>{max_bytes}b".encode())
                break
            h.update(chunk)
    return h.hexdigest()


def _git_rev() -> str | None:
    try:
        p = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            timeout=6,
            cwd=Path(__file__).resolve().parents[2],
        )
        if p.returncode == 0 and p.stdout.strip():
            return p.stdout.strip()
    except (OSError, subprocess.SubprocessError):
        pass
    return None


def build_e0_training_manifest(
    *,
    model_path: Path,
    historical_csv: Path,
    report_meta: dict[str, Any],
    test_log_loss: float | None,
) -> dict[str, Any]:
    import numpy as np
    import sklearn

    csv_sha = _file_sha256(historical_csv)
    return {
        "schema": "betfree.training_manifest.v1",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "python": sys.version.split()[0],
        "numpy": getattr(np, "__version__", "?"),
        "sklearn": getattr(sklearn, "__version__", "?"),
        "git_rev": _git_rev(),
        "model_path": str(model_path.resolve()),
        "historical_csv": str(historical_csv.resolve()),
        "historical_csv_sha256": csv_sha,
        "train_report_meta": report_meta,
        "calibrated_test_log_loss": test_log_loss,
    }


def write_training_manifest(path: Path, manifest: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
