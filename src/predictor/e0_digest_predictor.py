"""Inferencia 1X2 con el artefacto e0_expert_calibrated para el digest (Premier = CSV E0 + ESPN eng.1)."""

from __future__ import annotations

import difflib
import logging
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import joblib
import numpy as np

from src.config import Settings, repo_root
from src.data_engine.digest_fixture import DigestFixtureRow
from src.predictor.artifact_contract import collect_e0_artifact_issues
from src.predictor.chronological_features import replay_team_states_through_day, tabular_row_pre_match
from src.predictor.csv_roll_state import norm_team
from src.predictor.e0_data_prep import load_clean_e0
from src.predictor.e0_expert_train import TABULAR_COLS

LOG = logging.getLogger(__name__)

# En historical_robust la competición "E0" es la máxima categoría inglesa; ESPN la etiqueta eng.1.
ESPN_SLUG_TO_HISTORICAL_COMP: dict[str, str] = {
    "eng.1": "E0",
}

_LONDON = ZoneInfo("Europe/London")


class E0DigestPredictor:
    """Carga modelo + estado cronológico E0; pronóstico 1X2 tabular alineado con e0_expert_train."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._model: Any = None
        self._feature_names: list[str] = []
        self._states: dict | None = None
        self._exact_cf: dict[str, str] = {}
        self._by_norm: dict[str, list[str]] = {}
        self._norm_flat: list[str] = []
        self.active = False

    def prepare(self, today: date) -> None:
        self.active = False
        self._model = None
        self._states = None
        self._feature_names = []
        if not self._settings.digest_use_e0_ml:
            return
        root = repo_root()
        raw_path = (self._settings.e0_expert_model_path or "").strip()
        path = Path(raw_path) if Path(raw_path).is_absolute() else (root / raw_path)
        path = path.resolve()
        if not path.is_file():
            LOG.info("E0 ML omitido: no existe el artefacto %s", path)
            return
        try:
            clean, _audit = load_clean_e0(self._settings.historical_csv_path)
        except (OSError, ValueError) as exc:
            LOG.warning("E0 ML: histórico o limpieza E0 falló: %s", exc)
            return
        try:
            art = joblib.load(path)
        except Exception as exc:
            LOG.warning("E0 ML: no se pudo leer %s: %s", path, exc)
            return
        model = art.get("model")
        fn = art.get("feature_names")
        if model is None or not fn:
            LOG.warning("E0 ML: artefacto incompleto (model / feature_names)")
            return
        issues = collect_e0_artifact_issues(art, expected_features=TABULAR_COLS)
        for msg in issues:
            LOG.warning("E0 ML contrato artefacto: %s", msg)
        if issues and any("feature_names distinto" in x or "falta clave" in x for x in issues):
            return
        self._model = model
        self._feature_names = [str(x) for x in fn]
        self._states = replay_team_states_through_day(clean, competition="E0", before_day=today)
        teams = sorted(
            set(clean["home_team"].astype(str).str.strip())
            | set(clean["away_team"].astype(str).str.strip())
        )
        self._build_name_index(teams)
        self.active = True
        LOG.info("E0 ML activo (artefacto %s, %s equipos en índice)", path.name, len(teams))

    def _build_name_index(self, teams: list[str]) -> None:
        self._exact_cf = {t.casefold(): t for t in teams}
        by_norm: dict[str, list[str]] = defaultdict(list)
        for t in teams:
            by_norm[norm_team(t)].append(t)
        self._by_norm = dict(by_norm)
        self._norm_flat = sorted(self._by_norm.keys())

    def _resolve_team(self, raw: str) -> str | None:
        s = (raw or "").strip()
        if not s:
            return None
        cf = s.casefold()
        if cf in self._exact_cf:
            return self._exact_cf[cf]
        nt = norm_team(s)
        group = self._by_norm.get(nt, [])
        if len(group) == 1:
            return group[0]
        if len(group) > 1:
            for t in group:
                if t.casefold() == cf:
                    return t
            return None
        matches = difflib.get_close_matches(nt, self._norm_flat, n=1, cutoff=0.82)
        if not matches:
            return None
        cand = self._by_norm.get(matches[0], [])
        if len(cand) == 1:
            return cand[0]
        return None

    def probs_1x2_for_row(self, row: DigestFixtureRow) -> tuple[float, float, float] | None:
        if not self.active or self._model is None or self._states is None:
            return None
        comp = ESPN_SLUG_TO_HISTORICAL_COMP.get((row.slug or "").strip())
        if comp is None:
            return None
        if row.kickoff_utc is None:
            return None
        home_c = self._resolve_team(row.home_name)
        away_c = self._resolve_team(row.away_name)
        if home_c is None or away_c is None:
            LOG.debug("E0 ML sin mapeo equipo→CSV: %r vs %r", row.home_name, row.away_name)
            return None
        dt = row.kickoff_utc.astimezone(_LONDON).replace(tzinfo=None)
        try:
            tab = tabular_row_pre_match(self._states, comp, home_c, away_c, dt)
        except Exception as exc:
            LOG.debug("E0 ML features: %s", exc)
            return None
        try:
            x = np.array([[float(tab[name]) for name in self._feature_names]], dtype=np.float64)
        except KeyError as exc:
            LOG.warning("E0 ML: falta columna en features %s", exc)
            return None
        proba = np.asarray(self._model.predict_proba(x)).ravel()
        classes = np.asarray(getattr(self._model, "classes_", np.arange(len(proba))))
        idx_map: dict[int, int] = {}
        for i, c in enumerate(np.ravel(classes)):
            try:
                idx_map[int(c)] = i
            except (TypeError, ValueError):
                continue
        try:
            ph, pd_, pa = float(proba[idx_map[0]]), float(proba[idx_map[1]]), float(proba[idx_map[2]])
        except KeyError:
            if len(proba) >= 3:
                ph, pd_, pa = float(proba[0]), float(proba[1]), float(proba[2])
            else:
                return None
        s = ph + pd_ + pa
        if s <= 0:
            return None
        return ph / s, pd_ / s, pa / s
