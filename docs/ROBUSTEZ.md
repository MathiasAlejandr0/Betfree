# Robustez, límites y operación profesional

Este documento resume **qué hace Betfree hoy**, **qué no garantiza** y **qué herramientas** hay para operarlo con criterio de ingeniería.

## Límites del modelo (honestidad de producto)

- **1X2 con ML tabular**: principalmente **Premier League** (`eng.1` ↔ competición `E0` en el CSV). Otras ligas del digest usan **Poisson + Elo** (o mezcla si aplica).
- **Goles, tarjetas y corners**: heurísticas a partir del histórico; **no** son mercados calibrados contra bookmakers por partido.
- **Cuotas**: API-Football y The Odds API son **proveedores distintos** (IDs no intercambiables). Ver `src/data_engine/odds_resolution.py` para convenciones (`:open` / `:closing` en claves de mercado al persistir en SQLite).
- **Sin datos de plantilla / lesiones / xG óptico**: las señales vienen del **histórico y calendario**, con techo frente a mercados eficientes.

## Checklist implementado en el repo

| Área | Qué hay |
|------|---------|
| **Tests** | `pytest` en `tests/` (`pytest.ini` + `requirements.txt`). |
| **Manifiesto de entrenamiento** | Tras `e0_expert_train`: `models/e0_training_manifest.json` (Python, numpy, sklearn, `git rev` si existe, SHA256 del CSV). |
| **Contrato del artefacto E0** | `artifact_contract.py` + validación al cargar en `E0DigestPredictor` (rechaza desalineación grave de `feature_names`). |
| **Auditoría de predicciones del digest** | Tabla `digest_prediction_audit` + inserción en cada corrida del digest (`jobs.py`). |
| **Evaluación post-digest vs CSV** | `python -m src.predictor.digest_live_evaluation` — cruza auditoría con resultados del histórico por fecha + equipos normalizados; escribe `data/digest_live_evaluation.json`; exit code 3 si supera umbral de log-loss. |
| **Tras el digest (opcional)** | Con `DIGEST_RUN_LIVE_EVAL_AFTER_DIGEST=true` se ejecuta la misma evaluación al final de `run_prediction_pipeline_free`. Con `DIGEST_LIVE_EVAL_ALERT_ON_BREACH=true` se inserta una fila en `model_health_alerts` (sin duplicar el mismo día UTC para `digest_live_evaluation`). |
| **CI** | `.github/workflows/tests.yml` ejecuta `pytest` en push/PR. |
| **Cuotas de cierre (API-Football)** | `python -m src.data_engine.closing_odds_cli --fixture <id_api_football> [--persist-db] [--digest-event-id]` — parsea 1X2 del JSON `/odds` y opcionalmente guarda en `odds_snapshot` claves `1X2_*:closing`. |
| **Puente digest ↔ API-Football** | Tabla `digest_api_football_fixture_map`. `python -m src.data_engine.fixture_bridge_cli --date YYYY-MM-DD` (GET `/fixtures?date=` + match por equipos). Luego lote: `python -m src.data_engine.closing_odds_batch_cli --date ...` guarda cuotas `:closing` bajo **digest_event_id**. |
| **Resolver alertas** | `python -m src.predictor.model_health_resolve_cli --id N` o `--all-open --yes` (opcional `--model-used digest_live_evaluation`). |
| **SQLite operativo** | `alert_settlements`, `model_health_alerts` creados en `init_db` (antes el monitor asumía tablas que no existían). |
| **Monitor** | Si falta `alert_settlements` en DBs viejos, no rompe; añade nota en el payload. |

## Comandos útiles

```bash
python -m pytest tests -q
python -m src.predictor.digest_live_evaluation --since-days 120 --warn-log-loss 1.08
python -m src.predictor.model_diagnostic_e0 --help
python -m src.predictor.e0_expert_train
```

## Qué sigue siendo mejora futura (no automatizado aquí)

- **Closing line** totalmente automática: programar **Task Scheduler** / cron: (1) `fixture_bridge_cli` tras el digest; (2) `closing_odds_batch_cli` 1–2 h antes del pitazo (consume cupo `/odds` por partido).
- **Alertas `model_health_alerts`**: la tabla existe; la lógica de inserción ante degradación puede ampliarse según tus reglas.
- **Features externas** (lesiones, xG proveedor): integración caso por caso.
