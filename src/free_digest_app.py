"""CLI digest gratuito — entrada estable si main.py legacy sobrescribe."""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
from pathlib import Path

from telegram import Update
from telegram.ext import Application, CallbackQueryHandler

from src.config import bootstrap_dotenv, get_settings, repo_root, validate_provider_token
from src.notifier.digest_callback_handlers import on_digest_menu_callback
from src.notifier.telegram_bot import build_default_notifier
from src.scheduler.jobs import PipelineConfig, build_scheduler, run_prediction_pipeline_free
from src.storage.database import init_db
from src.storage.repository import TimeSeriesRepository

LOG = logging.getLogger(__name__)


def _menu_bot_notify_startup() -> bool:
    """Por defecto desactivado: al autoarranque conviene no disparar Telegram."""
    return str(os.getenv("MENU_BOT_NOTIFY_ON_START", "")).strip().lower() in ("1", "true", "yes", "on")


def _configure_menu_bot_file_logging() -> Path:
    """Sin consola (`pythonw`) el archivo sirve para ver errores."""
    r = repo_root()
    raw = os.getenv("MENU_BOT_LOG_PATH", "").strip()
    path = Path(raw) if raw else (r / "data" / "logs" / "menu_bot.log")
    if not path.is_absolute():
        path = r / path
    path.parent.mkdir(parents=True, exist_ok=True)
    fh = logging.FileHandler(path, encoding="utf-8")
    fh.setLevel(logging.INFO)
    fh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s"))
    logging.getLogger().addHandler(fh)
    return path


async def _menu_bot_post_init(application: Application) -> None:
    """Avisa en Telegram solo si MENU_BOT_NOTIFY_ON_START=true."""
    if not _menu_bot_notify_startup():
        return
    settings = get_settings()
    cid = (settings.telegram_chat_id or "").strip()
    if not cid:
        LOG.warning("TELEGRAM_CHAT_ID vacío en .env: no puedo enviar el aviso de arranque.")
        return
    text = (
        "✅ <b>Betfree — listener del menú activo</b>\n\n"
        "Este proceso solo <b>atiende los botones</b> y reenvía el menú después de cada elección.\n"
        "Para generar el fixture + caché del día usá <code>--run-once</code> (con <code>TELEGRAM_DIGEST_MENU=true</code>).\n\n"
        "Si no te llega este aviso, abrí el chat del bot y mandá <code>/start</code>."
    )
    try:
        await application.bot.send_message(
            chat_id=cid,
            text=text,
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
    except Exception as exc:
        LOG.warning(
            "No se pudo enviar el aviso de arranque a Telegram (%s). "
            "¿Ya enviaste /start al bot desde tu cuenta?",
            exc,
        )


def run_menu_bot_polling() -> None:
    """Dejar corriendo para que respondan los botones del digest (TELEGRAM_DIGEST_MENU=true)."""
    settings = get_settings()
    tok = settings.telegram_bot_token.strip()
    if not tok:
        raise RuntimeError(f"TELEGRAM_BOT_TOKEN vacío → {repo_root() / '.env'}")
    if not (settings.telegram_chat_id or "").strip():
        LOG.warning("TELEGRAM_CHAT_ID vacío: los callbacks validan contra este chat; revisa .env.")

    application = (
        Application.builder()
        .token(tok)
        .post_init(_menu_bot_post_init)
        .build()
    )
    application.add_handler(CallbackQueryHandler(on_digest_menu_callback, pattern=r"^bf\|"))
    LOG.info(
        "Menú Telegram: polling (callback_query). No envía el digest; usa --run-once con TELEGRAM_DIGEST_MENU=true."
    )
    application.run_polling(allowed_updates=[Update.CALLBACK_QUERY])


def main() -> None:
    bootstrap_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")

    p = argparse.ArgumentParser(description="Betfree digest (ESPN + CSV + Telegram)")
    p.add_argument("--run-once", action="store_true")
    p.add_argument("--cron", default="0 8 * * *")
    p.add_argument("--bankroll", type=float, default=1000.0)
    p.add_argument(
        "--menu-bot",
        action="store_true",
        help="Solo Telegram: escucha pulsaciones del menú con botones (TELEGRAM_DIGEST_MENU).",
    )
    args = p.parse_args()

    if args.menu_bot:
        lp = _configure_menu_bot_file_logging()
        LOG.info("Log menú-bot → %s", lp)
        run_menu_bot_polling()
        return

    settings = get_settings()
    validate_provider_token(settings, "free")
    init_db(settings.sqlite_db_path)
    repo = TimeSeriesRepository(settings.sqlite_db_path)
    notifier = build_default_notifier()
    cfg = PipelineConfig(bankroll=args.bankroll)

    async def job() -> None:
        await run_prediction_pipeline_free(notifier, repo, cfg)

    if args.run_once:
        asyncio.run(job())
        return
    build_scheduler(async_job=lambda: asyncio.run(job()), cron_expression=args.cron).start()


if __name__ == "__main__":
    main()
