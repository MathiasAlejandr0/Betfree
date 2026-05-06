"""Config desde .env en la raíz del repo."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

_DONE = False


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def bootstrap_dotenv() -> None:
    global _DONE
    if _DONE:
        return
    r = repo_root()
    load_dotenv(r / ".env", override=True)
    load_dotenv(r / ".env.local", override=True)
    _DONE = True


# Copas domésticas ESPN (1ª suele participar) — Europa y Sudamérica; amplía con FREE_ESPN_EXTRA_CUP_SLUGS.
DEFAULT_FREE_ESPN_CUP_SLUGS: tuple[str, ...] = (
    "eng.fa",
    "eng.league_cup",
    "esp.copa_del_rey",
    "ita.coppa_italia",
    "fra.coupe_de_france",
    "ger.dfb_pokal",
    "ned.cup",
    "sco.tennents",
    "bra.copa_do_brazil",
    "arg.copa",
    "arg.supercopa",
    "col.copa",
)

DEFAULT_FREE_ESPN_EXTRA_SLUGS_FIRST_TIER_CUPS: frozenset[str] = frozenset(DEFAULT_FREE_ESPN_CUP_SLUGS)

# Primeras divisiones Europa + Sudamérica, copas país anteriores, UEFA/CONMEBOL/FIFA CWC (sin MLS/Liga MX/Asia/África).
# Slugs desconocidos en ESPN fallan en silencio vía log en jobs; amplía con FREE_ESPN_SOCCER_SLUGS si hace falta.
# Slugs verificados en site.api.espn.com; otros códigos *.1 suelen devolver 400 (p. ej. pol.1 → usar FREE_ESPN_SOCCER_SLUGS si encuentras el slug correcto).
_DEFAULT_EU_TIER1_SLUGS: tuple[str, ...] = (
    "eng.1",
    "esp.1",
    "ita.1",
    "ger.1",
    "fra.1",
    "por.1",
    "ned.1",
    "bel.1",
    "tur.1",
    "sco.1",
    "aut.1",
    "gre.1",
    "swe.1",
    "den.1",
    "nor.1",
    "irl.1",
    "cze.1",
    "rou.1",
    "fin.1",
    "sui.1",
    "cyp.1",
    "isr.1",
    "rus.1",
    "wal.1",
)

_DEFAULT_SA_TIER1_SLUGS: tuple[str, ...] = (
    "chi.1",
    "bra.1",
    "arg.1",
    "col.1",
    "ecu.1",
    "par.1",
    "uru.1",
    "per.1",
    "bol.1",
    "ven.1",
)

_DEFAULT_INTL_CLUB_SLUGS: tuple[str, ...] = (
    "uefa.champions",
    "uefa.europa",
    "uefa.europa.conf",
    "uefa.super_cup",
    "uefa.nations",
    "conmebol.libertadores",
    "conmebol.sudamericana",
    "conmebol.recopa",
    "fifa.cwc",
    "fifa.world",
)

DEFAULT_FREE_ESPN_SLUGS: tuple[str, ...] = (
    *_DEFAULT_INTL_CLUB_SLUGS,
    *_DEFAULT_EU_TIER1_SLUGS,
    *_DEFAULT_SA_TIER1_SLUGS,
    *DEFAULT_FREE_ESPN_CUP_SLUGS,
)

# Refuerzo “solo hoy” (misma región); vacío por defecto porque el listado principal ya cubre EU+SA 1ª.
DEFAULT_GLOBAL_ESPN_TIER1_SLUGS: tuple[str, ...] = ()


# Slugs ESPN de las ligas con pronóstico detallado (primera división; amplía en .env si quieres copas u otras divisiones).
DEFAULT_PREDICTION_DETAIL_SLUGS: tuple[str, ...] = (
    "chi.1",
    "arg.1",
    "bra.1",
    "eng.1",
    "esp.1",
    "por.1",
    "fra.1",
    "ecu.1",
    "col.1",
)

# Subcadenas (case-insensitive) para subir partidos en la agenda si no cupieron todos por el límite global.
DEFAULT_FIXTURE_TEAM_PRIORITY: tuple[str, ...] = (
    "colo colo",
    "boca juniors",
    "river plate",
    "flamengo",
    "palmeiras",
    "fc barcelona",
    "real madrid",
    "chelsea",
    "manchester united",
    "manchester city",
)

# ESPN 1ª que siempre van delante si hay que recortar agenda/pronósticos (tras incluir todo con prefijo conmebol./uefa./fifa.).
DEFAULT_DIGEST_FORCE_FIRST_ES_SLUGS: frozenset[str] = frozenset(
    {"chi.1", "arg.1", "bra.1", "esp.1", "eng.1", "fra.1"}
)


def _comma_tokens(raw: str) -> tuple[str, ...]:
    raw = (raw or "").strip()
    if not raw:
        return ()
    return tuple(x.strip() for x in raw.split(",") if x.strip())


def _prediction_detail_slugs(raw: str) -> tuple[str, ...]:
    raw = (raw or "").strip()
    if raw:
        return tuple(x.strip() for x in raw.split(",") if x.strip())
    return DEFAULT_PREDICTION_DETAIL_SLUGS


def _env_bool(key: str, default: bool) -> bool:
    v = os.getenv(key)
    if v is None or not str(v).strip():
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "on")


def _float_env(key: str, default: float) -> float:
    raw = os.getenv(key, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _fixture_team_priority(raw: str) -> tuple[str, ...]:
    t = _comma_tokens(raw)
    return t if t else DEFAULT_FIXTURE_TEAM_PRIORITY


def _digest_force_first_es_slugs(raw: str) -> frozenset[str]:
    t = _comma_tokens(raw)
    if not t:
        return DEFAULT_DIGEST_FORCE_FIRST_ES_SLUGS
    return frozenset(x.casefold() for x in t if x.strip())


def _merged_soccer_slugs(main_raw: str, extra_cup_raw: str) -> tuple[str, ...]:
    """Lista de slugs a consultar: por defecto DEFAULT_FREE_ESPN_SLUGS + copas extra en .env."""
    extra = _comma_tokens(extra_cup_raw)
    base_raw = (main_raw or "").strip()
    if base_raw:
        base = tuple(x.strip() for x in base_raw.split(",") if x.strip())
    else:
        base = DEFAULT_FREE_ESPN_SLUGS
    seen: set[str] = set()
    out: list[str] = []
    for s in base + extra:
        if s not in seen:
            seen.add(s)
            out.append(s)
    return tuple(out)


def _cup_slug_allowlist(extra_cup_raw: str) -> frozenset[str]:
    return frozenset(DEFAULT_FREE_ESPN_CUP_SLUGS) | frozenset(_comma_tokens(extra_cup_raw))


def _fraction_01(raw: str, *, default: float) -> float:
    raw = str(raw).strip().replace(",", ".")
    if not raw:
        return default
    try:
        return max(0.0, min(1.0, float(raw)))
    except ValueError:
        return default


def _positive_float_bounded(raw: str, *, default: float, lo: float, hi: float) -> float:
    raw = str(raw).strip().replace(",", ".")
    if not raw:
        return default
    try:
        return max(lo, min(hi, float(raw)))
    except ValueError:
        return default


def _explicit_env_nonempty(key: str) -> bool:
    v = os.getenv(key)
    return v is not None and str(v).strip() != ""


def _try_load_calibration_json(repo: Path, path_raw: str) -> dict[str, Any] | None:
    raw = str(path_raw or "").strip()
    if not raw:
        return None
    pth = Path(raw)
    resolved = pth if pth.is_absolute() else (repo / pth)
    try:
        return json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError):
        return None


def _resolve_ml_blend_weight(
    repo: Path,
    *,
    use_calib_json: bool,
    calib_path: str,
) -> float:
    if _explicit_env_nonempty("DIGEST_E0_ML_BLEND_WEIGHT"):
        return _fraction_01(os.getenv("DIGEST_E0_ML_BLEND_WEIGHT", ""), default=0.72)
    if use_calib_json:
        blob = _try_load_calibration_json(repo, calib_path or "data/e0_blend_calibration.json")
        if blob and "recommended_ml_blend_weight" in blob:
            try:
                return max(0.0, min(1.0, float(blob["recommended_ml_blend_weight"])))
            except (TypeError, ValueError):
                pass
    return _fraction_01(os.getenv("DIGEST_E0_ML_BLEND_WEIGHT", ""), default=0.72)


def _resolve_poisson_draw_factor(
    repo: Path,
    *,
    use_calib_json: bool,
    calib_path: str,
    lo: float,
    hi: float,
    default_plain: float,
) -> float:
    if _explicit_env_nonempty("DIGEST_POISSON_DRAW_FACTOR"):
        return _positive_float_bounded(os.getenv("DIGEST_POISSON_DRAW_FACTOR", ""), default=default_plain, lo=lo, hi=hi)
    if use_calib_json:
        blob = _try_load_calibration_json(repo, calib_path or "data/e0_blend_calibration.json")
        if blob and "recommended_poisson_draw_factor" in blob:
            try:
                v = float(blob["recommended_poisson_draw_factor"])
                return max(lo, min(hi, v))
            except (TypeError, ValueError):
                pass
    return _positive_float_bounded(os.getenv("DIGEST_POISSON_DRAW_FACTOR", ""), default=default_plain, lo=lo, hi=hi)


def _int_min_positive(key: str, *, default: int, floor: int) -> int:
    raw = os.getenv(key, "").strip()
    if not raw:
        return max(floor, int(default))
    try:
        return max(floor, int(raw))
    except ValueError:
        return max(floor, int(default))


def _fetch_day_radius(default: int = 2) -> int:
    v = os.getenv("FIXTURE_DIGEST_FETCH_DAY_RADIUS", "").strip()
    if not v:
        return max(1, default)
    try:
        return max(1, min(4, int(v)))
    except ValueError:
        return max(1, default)


@dataclass(frozen=True)
class Settings:
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""
    sqlite_db_path: str = "betfree.db"
    request_timeout_seconds: int = 20
    min_request_interval_seconds: float = 1.2
    historical_csv_path: str = "data/open_datasets/historical_robust.csv"
    free_espn_soccer_slugs: tuple[str, ...] = DEFAULT_FREE_ESPN_SLUGS
    daily_digest_max_fixtures: int = 900
    # Tope de partidos con pronóstico (mismo orden que la agenda); sube si Telegram trocea mucho.
    daily_digest_max_prediction_messages: int = 900
    prediction_detail_slugs: tuple[str, ...] = DEFAULT_PREDICTION_DETAIL_SLUGS
    free_espn_first_division_only: bool = True
    fixture_digest_team_priority: tuple[str, ...] = DEFAULT_FIXTURE_TEAM_PRIORITY
    fixture_digest_fetch_day_radius: int = 2
    free_espn_extra_first_tier_cup_slugs: frozenset[str] = DEFAULT_FREE_ESPN_EXTRA_SLUGS_FIRST_TIER_CUPS
    thesportsdb_api_key: str = ""
    digest_use_thesportsdb: bool = True
    free_espn_global_tier1_slugs: tuple[str, ...] = DEFAULT_GLOBAL_ESPN_TIER1_SLUGS
    digest_fetch_global_espn_today: bool = True
    # TheSportsDB: solo países Europa + Sudamérica y copas UEFA/CONMEBOL/FIFA CWC (no AFC/CAF/CONCACAF por defecto).
    digest_tsdb_eu_sa_only: bool = True
    # ESPN: descarta slugs fuera de Europa/Sudamérica si los añades manualmente en .env (MLS, Asia, etc.).
    digest_espn_eu_sa_only: bool = True
    api_football_key: str = ""
    # GET /fixtures por día (puente digest ↔ API-Football); cupo aparte de /odds.
    api_football_fixtures_daily_cap: int = 25
    football_data_token: str = ""
    # Digest: pronóstico 1X2 con models/e0_expert_calibrated.pkl cuando exista (Premier / CSV E0).
    digest_use_e0_ml: bool = True
    e0_expert_model_path: str = "models/e0_expert_calibrated.pkl"
    # Peso del ML E0 vs Poisson+Elo en 1X2 cuando el modelo está activo (0 = solo Poisson; 1 = solo ML).
    digest_e0_ml_blend_weight: float = 0.72
    # Multiplicador suave sobre P(empate) en Poisson+Elo antes de mezclar con ML (>1 corrige infra-empates típicos).
    digest_poisson_draw_factor: float = 1.0
    # Poisson+Elo por código `competition` del CSV (E0, SP1, …) cuando hay suficientes partidos; si no, estado global.
    digest_per_league_poisson: bool = True
    digest_per_league_min_rows: int = 220
    # Football-Data.org: tope de peticiones GET de red por día (UTC); la caché no consume cupo al repetir URL.
    football_data_daily_cap: int = 90
    # Agenda/pronósticos: primero copas intl (slug conmebol./uefa./fifa.) + slugs / TSDB / nombre de liga prioritarios.
    digest_force_first_priority: bool = True
    digest_force_first_es_slugs: frozenset[str] = DEFAULT_DIGEST_FORCE_FIRST_ES_SLUGS
    # Un mensaje con botones; cada botón envía agenda o pronósticos por competición (requiere --menu-bot en marcha).
    telegram_digest_menu: bool = False
    # Línea HTML con banda de confianza (según max prob 1X2) y fuente ML/mezcla/Poisson.
    digest_show_reliability_hint: bool = True
    # Tras el digest: evaluar audit vs CSV y escribir data/digest_live_evaluation.json (hilo sync, ~1 s con CSV grande).
    digest_run_live_eval_after_digest: bool = False
    digest_live_eval_since_days: int = 120
    digest_live_eval_min_matched: int = 15
    digest_live_eval_warn_log_loss: float = 1.08
    # Si true y la evaluación supera el umbral: INSERT en model_health_alerts (sin duplicar el mismo día UTC).
    digest_live_eval_alert_on_breach: bool = False


def get_settings() -> Settings:
    bootstrap_dotenv()
    global_tier1_env = _comma_tokens(os.getenv("FREE_ESPN_GLOBAL_TIER1_SLUGS"))
    r_repo = repo_root()
    blend_cal_json = _env_bool("DIGEST_USE_BLEND_CALIBRATION_JSON", False)
    blend_cal_path_raw = os.getenv("DIGEST_BLEND_CALIBRATION_PATH", "data/e0_blend_calibration.json").strip()
    blend_ml_w = _resolve_ml_blend_weight(
        r_repo,
        use_calib_json=blend_cal_json,
        calib_path=blend_cal_path_raw,
    )
    poisson_draw = _resolve_poisson_draw_factor(
        r_repo,
        use_calib_json=blend_cal_json,
        calib_path=blend_cal_path_raw,
        lo=0.85,
        hi=1.35,
        default_plain=1.0,
    )

    return Settings(
        telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", "").strip(),
        telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID", "").strip(),
        sqlite_db_path=os.getenv("SQLITE_DB_PATH", "betfree.db").strip() or "betfree.db",
        request_timeout_seconds=int(os.getenv("REQUEST_TIMEOUT_SECONDS", "20")),
        min_request_interval_seconds=float(os.getenv("MIN_REQUEST_INTERVAL_SECONDS", "1.2")),
        historical_csv_path=os.getenv(
            "HISTORICAL_CSV_PATH",
            "data/open_datasets/historical_robust.csv",
        ).strip(),
        free_espn_soccer_slugs=_merged_soccer_slugs(
            os.getenv("FREE_ESPN_SOCCER_SLUGS", ""),
            os.getenv("FREE_ESPN_EXTRA_CUP_SLUGS", ""),
        ),
        daily_digest_max_fixtures=(
            digest_max := max(1, int(os.getenv("DAILY_DIGEST_MAX_FIXTURES", "900")))
        ),
        daily_digest_max_prediction_messages=(
            max(1, int(pred_cap_raw))
            if (pred_cap_raw := os.getenv("DAILY_DIGEST_MAX_PREDICTION_MESSAGES", "").strip())
            else digest_max
        ),
        prediction_detail_slugs=_prediction_detail_slugs(os.getenv("PREDICTION_DETAIL_SLUGS", "")),
        free_espn_first_division_only=_env_bool("FREE_ESPN_FIRST_DIVISION_ONLY", True),
        fixture_digest_team_priority=_fixture_team_priority(os.getenv("FIXTURE_DIGEST_TEAM_PRIORITY", "")),
        fixture_digest_fetch_day_radius=_fetch_day_radius(2),
        free_espn_extra_first_tier_cup_slugs=_cup_slug_allowlist(os.getenv("FREE_ESPN_EXTRA_CUP_SLUGS", "")),
        thesportsdb_api_key=os.getenv("THESPORTSDB_API_KEY", "").strip(),
        digest_use_thesportsdb=_env_bool("DIGEST_USE_THESPORTSDB", True),
        free_espn_global_tier1_slugs=global_tier1_env if global_tier1_env else DEFAULT_GLOBAL_ESPN_TIER1_SLUGS,
        digest_fetch_global_espn_today=_env_bool("DIGEST_FETCH_GLOBAL_ESPN_TODAY", True),
        digest_tsdb_eu_sa_only=_env_bool("DIGEST_TSDB_EU_SA_ONLY", True),
        digest_espn_eu_sa_only=_env_bool("DIGEST_ESPN_EU_SA_ONLY", True),
        api_football_key=os.getenv("API_FOOTBALL_KEY", "").strip(),
        api_football_fixtures_daily_cap=max(0, int(os.getenv("API_FOOTBALL_FIXTURES_DAILY_CAP", "25") or 25)),
        football_data_token=os.getenv("FOOTBALL_DATA_TOKEN", "").strip(),
        digest_use_e0_ml=_env_bool("DIGEST_USE_E0_ML", True),
        e0_expert_model_path=os.getenv("E0_EXPERT_MODEL_PATH", "models/e0_expert_calibrated.pkl").strip()
        or "models/e0_expert_calibrated.pkl",
        digest_e0_ml_blend_weight=blend_ml_w,
        digest_poisson_draw_factor=poisson_draw,
        football_data_daily_cap=max(0, int(os.getenv("FOOTBALL_DATA_DAILY_CAP", "90"))),
        digest_force_first_priority=_env_bool("DIGEST_FORCE_FIRST_PRIORITY", True),
        digest_force_first_es_slugs=_digest_force_first_es_slugs(os.getenv("DIGEST_FORCE_FIRST_ES_SLUGS", "")),
        telegram_digest_menu=_env_bool("TELEGRAM_DIGEST_MENU", False),
        digest_show_reliability_hint=_env_bool("DIGEST_SHOW_RELIABILITY_HINT", True),
        digest_per_league_poisson=_env_bool("DIGEST_PER_LEAGUE_POISSON", True),
        digest_per_league_min_rows=_int_min_positive("DIGEST_PER_LEAGUE_MIN_ROWS", default=220, floor=50),
        digest_run_live_eval_after_digest=_env_bool("DIGEST_RUN_LIVE_EVAL_AFTER_DIGEST", False),
        digest_live_eval_since_days=_int_min_positive("DIGEST_LIVE_EVAL_SINCE_DAYS", default=120, floor=7),
        digest_live_eval_min_matched=_int_min_positive("DIGEST_LIVE_EVAL_MIN_MATCHED", default=15, floor=5),
        digest_live_eval_warn_log_loss=max(0.5, _float_env("DIGEST_LIVE_EVAL_WARN_LOG_LOSS", 1.08)),
        digest_live_eval_alert_on_breach=_env_bool("DIGEST_LIVE_EVAL_ALERT_ON_BREACH", False),
    )


def validate_provider_token(settings: Settings, provider: str) -> None:
    _ = settings
    if provider.strip().lower() == "free":
        return
