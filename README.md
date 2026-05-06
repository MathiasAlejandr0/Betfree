# Betfree

Ecosistema de predicción deportiva en Python: agenda **ESPN + TheSportsDB** centrada en **primera división de Europa y Sudamérica** más **copas internacionales de clubes** (UEFA, CONMEBOL, FIFA CWC). Pronósticos por Telegram con **1X2, goles esperados, tarjetas y corners** (heurísticas a partir del CSV histórico; **1X2** con modelo tabular **E0** en Premier / `eng.1` si existe `models/e0_expert_calibrated.pkl`).

## Requisitos

```bash
pip install -r requirements.txt
```

Copia `.env.example` a `.env` y rellena al menos Telegram si quieres envíos reales.

## Digest diario (entrada principal)

Ejecución única:

```bash
python -m src.main --run-once --bankroll 1000
```

Programación con cron (por defecto `0 8 * * *`):

```bash
python -m src.main --cron "0 8 * * *" --bankroll 1000
```

- **1X2**: para partidos `eng.1` se usa el modelo entrenado con `python -m src.predictor.e0_expert_train` si el artefacto existe; el resto sigue con Poisson+Elo sobre el CSV histórico.
- **Goles / tarjetas heurísticas**: siguen saliendo del bloque Poisson (misma línea que antes).

Variables relevantes: `DIGEST_USE_E0_ML`, `E0_EXPERT_MODEL_PATH`, `HISTORICAL_CSV_PATH` (ver `.env.example`).

## Football-Data.org (opcional, cupo + caché)

Cliente HTTP con tope diario (UTC) y caché en `data/cache/football_data/`:

```bash
python -m src.data_engine.football_data_client --areas
```

Ajusta `FOOTBALL_DATA_DAILY_CAP` al límite de tu plan; las respuestas repetidas dentro del TTL no vuelven a consumir cupo de red.

## Entrenar modelo E0

```bash
python -m src.predictor.e0_expert_train
```

Genera `models/e0_expert_calibrated.pkl`, `data/e0_expert_report.json` y `models/e0_training_manifest.json` (versiones + huella del CSV).

## Tests y evaluación en vivo del digest

```bash
python -m pytest tests -q
python -m src.predictor.digest_live_evaluation --since-days 120
```

Opcional: tras cada digest, evaluación automática y alertas SQLite — variables `DIGEST_RUN_LIVE_EVAL_*` en `.env.example`. Cuotas de cierre / puente ESPN↔API-Football: `fixture_bridge_cli`, `closing_odds_batch_cli`, `closing_odds_cli` (`--help` en cada uno). Cerrar alertas: `model_health_resolve_cli`.

Detalle de límites del producto y checklist de robustez: [docs/ROBUSTEZ.md](docs/ROBUSTEZ.md).

## Panel web en el móvil (GitHub Pages)

En `docs/` hay un panel estático (`index.html`) que lee `betfree_predictions.json`. Tras cada digest local:

```bash
python scripts/export_pages_predictions.py
git add docs/betfree_predictions.json docs/index.html
git commit -m "chore: actualizar predicciones para Pages" && git push
```

En GitHub: **Settings → Pages → Branch `main` / folder `/docs`**. La URL será `https://<usuario>.github.io/<repo>/` (respetá mayúsculas del nombre del repo si GitHub las muestra).

**Nota:** `appbetfree.html` en la raíz es documentación exportada del proyecto **ProBet** (Next.js), no el panel Betfree; el panel del modelo es `docs/index.html`. Instrucciones detalladas: [docs/instalacion.html](docs/instalacion.html).
