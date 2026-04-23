# Betfree

Ecosistema de prediccion deportiva en Python con ingesta de datos, modelado cuantitativo, gestion de riesgo y notificaciones por Telegram.

## Ejecutar

1. Instalar dependencias:

```bash
pip install -r requirements.txt
```

2. Configurar variables de entorno usando `.env.example`.

3. Ejecutar una sola vez:

```bash
python -m src.main --run-once --provider football-data --bankroll 1000
```

4. Ejecutar en modo scheduler:

```bash
python -m src.main --cron "*/20 * * * *" --provider football-data --bankroll 1000
```
